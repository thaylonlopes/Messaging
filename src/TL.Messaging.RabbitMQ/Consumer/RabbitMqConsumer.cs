using System;
using System.Collections.Generic;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using TL.Messaging.Abstractions;
using TL.Messaging.RabbitMQ.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Polly;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace TL.Messaging.RabbitMQ.Consumer
{
    /// <summary>
    /// Background Service consumidor resiliente com declaração automática de topologia, Dead-Letter Queue (DLQ) e retentativas.
    /// </summary>
    /// <typeparam name="TEvent">Tipo do payload de evento a ser consumido.</typeparam>
    /// <typeparam name="THandler">Tipo do manipulador de negócio que implementa <see cref="IEventHandler{TEvent}"/>.</typeparam>
    public class RabbitMqConsumer<TEvent, THandler> : BackgroundService
        where TEvent : class
        where THandler : IEventHandler<TEvent>
    {
        private readonly RabbitMqOptions _options;
        private readonly IServiceProvider _serviceProvider;
        private readonly IConnectionFactory _connectionFactory;
        private readonly ILogger<RabbitMqConsumer<TEvent, THandler>>? _logger;
        private readonly string _queueName;
        private readonly string _routingKey;

        private IConnection? _connection;
        private IModel? _channel;

        /// <summary>
        /// Inicializa uma nova instância do consumidor RabbitMQ.
        /// </summary>
        public RabbitMqConsumer(
            IOptions<RabbitMqOptions> options,
            IServiceProvider serviceProvider,
            IConnectionFactory? connectionFactory = null,
            ILogger<RabbitMqConsumer<TEvent, THandler>>? logger = null,
            string? queueName = null,
            string? routingKey = null)
        {
            _options = options?.Value ?? throw new ArgumentNullException(nameof(options));
            _serviceProvider = serviceProvider ?? throw new ArgumentNullException(nameof(serviceProvider));
            _logger = logger;

            string eventName = typeof(TEvent).Name.ToLowerInvariant();
            _queueName = string.IsNullOrWhiteSpace(queueName) ? $"app.{eventName}" : queueName;
            _routingKey = string.IsNullOrWhiteSpace(routingKey) ? eventName : routingKey;

            _connectionFactory = connectionFactory ?? new ConnectionFactory
            {
                HostName = _options.HostName,
                Port = _options.Port,
                UserName = _options.UserName,
                Password = _options.Password,
                VirtualHost = _options.VirtualHost,
                DispatchConsumersAsync = true
            };
        }

        /// <inheritdoc />
        protected override Task ExecuteAsync(CancellationToken stoppingToken)
        {
            try
            {
                _connection = _connectionFactory.CreateConnection();
                _channel = _connection.CreateModel();

                _channel.BasicQos(0, _options.PrefetchCount, false);

                string mainExchange = _options.ExchangeName;
                string dlxExchange = $"{mainExchange}{_options.DeadLetterExchangeSuffix}";
                string dlqQueue = $"{_queueName}{_options.DeadLetterQueueSuffix}";

                DeclareDeadLetterTopology(dlxExchange, dlqQueue);
                DeclareMainExchange(mainExchange);
                DeclareMainQueueWithDeadLetter(mainExchange, dlxExchange);
                StartBasicConsumer(stoppingToken);

                _logger?.LogInformation("Consumidor RabbitMQ iniciado na fila {QueueName} vinculada à chave {RoutingKey}",
                    _queueName, _routingKey);
            }
            catch (Exception ex)
            {
                _logger?.LogError(ex, "Falha crítica ao inicializar o consumidor RabbitMQ na fila {QueueName}", _queueName);
            }

            return Task.CompletedTask;
        }

        private void DeclareDeadLetterTopology(string dlxExchange, string dlqQueue)
        {
            _channel!.ExchangeDeclare(dlxExchange, _options.ExchangeType, durable: true, autoDelete: false);
            _channel.QueueDeclare(dlqQueue, durable: true, exclusive: false, autoDelete: false);
            _channel.QueueBind(dlqQueue, dlxExchange, routingKey: _routingKey);
        }

        private void DeclareMainExchange(string mainExchange)
        {
            _channel!.ExchangeDeclare(mainExchange, _options.ExchangeType, durable: true, autoDelete: false);
        }

        private void DeclareMainQueueWithDeadLetter(string mainExchange, string dlxExchange)
        {
            var queueArgs = new Dictionary<string, object>
            {
                { "x-dead-letter-exchange", dlxExchange },
                { "x-dead-letter-routing-key", _routingKey }
            };

            _channel!.QueueDeclare(_queueName, durable: true, exclusive: false, autoDelete: false, arguments: queueArgs);
            _channel.QueueBind(_queueName, mainExchange, routingKey: _routingKey);
        }

        private void StartBasicConsumer(CancellationToken stoppingToken)
        {
            var consumer = new AsyncEventingBasicConsumer(_channel!);
            consumer.Received += async (sender, ea) =>
            {
                await ProcessMessageAsync(ea, stoppingToken).ConfigureAwait(false);
            };

            _channel!.BasicConsume(queue: _queueName, autoAck: false, consumer: consumer);
        }

        private async Task ProcessMessageAsync(BasicDeliverEventArgs ea, CancellationToken stoppingToken)
        {
            byte[] body = ea.Body.ToArray();
            ulong deliveryTag = ea.DeliveryTag;

            try
            {
                var eventMessage = JsonSerializer.Deserialize<EventMessage<TEvent>>(body);
                if (eventMessage == null || eventMessage.Payload == null)
                {
                    _logger?.LogWarning("Mensagem inválida ou nula recebida na fila {Queue}. Encaminhando para DLQ.", _queueName);
                    _channel?.BasicNack(deliveryTag, multiple: false, requeue: false);
                    return;
                }

                var result = await ExecuteHandlerWithRetryAsync(eventMessage, stoppingToken).ConfigureAwait(false);

                if (result.IsSuccess)
                {
                    _channel?.BasicAck(deliveryTag, multiple: false);
                    _logger?.LogDebug("Mensagem {EventId} processada com sucesso via ACK.", eventMessage.EventId);
                }
                else
                {
                    _logger?.LogWarning("Handler retornou falha [{Code}]: {Message}. Encaminhando para DLQ.",
                        result.Error.Code, result.Error.Message);
                    _channel?.BasicNack(deliveryTag, multiple: false, requeue: false);
                }
            }
            catch (Exception ex)
            {
                _logger?.LogError(ex, "Exceção não tratada ao processar mensagem na fila {Queue}. Encaminhando para DLQ.", _queueName);
                _channel?.BasicNack(deliveryTag, multiple: false, requeue: false);
            }
        }

        private async Task<Result> ExecuteHandlerWithRetryAsync(EventMessage<TEvent> eventMessage, CancellationToken stoppingToken)
        {
            using var scope = _serviceProvider.CreateScope();
            var handler = scope.ServiceProvider.GetRequiredService<THandler>();

            var retryPolicy = Policy
                .Handle<Exception>()
                .WaitAndRetryAsync(_options.RetryCount, attempt => TimeSpan.FromMilliseconds(200 * Math.Pow(2, attempt - 1)));

            return await retryPolicy.ExecuteAsync(async () =>
                await handler.HandleAsync(eventMessage, stoppingToken).ConfigureAwait(false)
            );
        }

        /// <inheritdoc />
        public override void Dispose()
        {
            DisposeConnectionAndChannelSafely();
            base.Dispose();
        }

        private void DisposeConnectionAndChannelSafely()
        {
            try
            {
                _channel?.Close();
                _channel?.Dispose();
                _connection?.Close();
                _connection?.Dispose();
            }
            catch (Exception)
            {
                // Supressão defensiva no encerramento de recursos
            }
        }
    }
}

