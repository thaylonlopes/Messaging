using System;
using System.Collections.Generic;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using TL.BaseContracts;
using TL.BaseContracts.Messaging;
using TL.Messaging.RabbitMQ.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Polly;
using Polly.Retry;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System.Diagnostics;
using TL.Messaging.RabbitMQ.Dlq;

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
        private static readonly JsonSerializerOptions JsonOptions = new()
        {
            PropertyNameCaseInsensitive = true,
            PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
            Converters = { new TL.Messaging.RabbitMQ.Serialization.EventMessageJsonConverterFactory() }
        };

        private readonly RabbitMqOptions _options;
        private readonly IServiceProvider _serviceProvider;
        private readonly IConnectionFactory _connectionFactory;
        private readonly ILogger<RabbitMqConsumer<TEvent, THandler>>? _logger;
        private readonly ResiliencePipeline _retryPipeline;
        private readonly string _queueName;
        private readonly string _routingKey;

        private IConnection? _connection;
        private IModel? _channel;

        /// <summary>
        /// Inicializa uma nova instância do consumidor RabbitMQ com suporte a DLQ e retentativas Polly.
        /// </summary>
        /// <param name="options">Opções de configuração de conexão e topologia do RabbitMQ.</param>
        /// <param name="serviceProvider">Provedor de serviços para criação de escopo de DI e resolução de handlers de eventos.</param>
        /// <param name="connectionFactory">Fábrica customizada de conexões AMQP (opcional, para testes ou mocks).</param>
        /// <param name="logger">Instância de logger para diagnósticos operacionais de consumo.</param>
        /// <param name="queueName">Nome customizado da fila principal (opcional; padrão infere pelo nome do evento).</param>
        /// <param name="routingKey">Chave de roteamento customizada (opcional; padrão infere pelo nome do evento).</param>
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

            _retryPipeline = BuildRetryPipeline();
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

        internal void SetChannelForTesting(IModel channel)
        {
            _channel = channel;
        }

        internal async Task ProcessMessageAsync(BasicDeliverEventArgs ea, CancellationToken stoppingToken)
        {
            ulong deliveryTag = ea.DeliveryTag;

            try
            {
                EventMessage<TEvent>? eventMessage;
                try
                {
                    eventMessage = JsonSerializer.Deserialize<EventMessage<TEvent>>(ea.Body.Span, JsonOptions);
                }
                catch (JsonException jEx)
                {
                    _logger?.LogWarning(jEx, "Mensagem com formato inválido detectada na fila {Queue}. Encaminhando para DLQ com metadados de diagnóstico.", _queueName);
                    ForwardToDeadLetterQueue(
                        body: ea.Body,
                        originalProps: ea.BasicProperties,
                        exceptionMessage: jEx.Message,
                        exceptionType: jEx.GetType().Name,
                        retryCount: 0);
                    _channel?.BasicAck(deliveryTag, multiple: false);
                    return;
                }

                if (eventMessage == null || eventMessage.Payload == null)
                {
                    _logger?.LogWarning("Mensagem invalida ou nula recebida na fila {Queue}. Encaminhando para DLQ.", _queueName);
                    ForwardToDeadLetterQueue(
                        body: ea.Body,
                        originalProps: ea.BasicProperties,
                        exceptionMessage: "Payload invalido ou nulo",
                        exceptionType: "InvalidPayloadException",
                        retryCount: 0);
                    _channel?.BasicAck(deliveryTag, multiple: false);
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
                    ForwardToDeadLetterQueue(
                        body: ea.Body,
                        originalProps: ea.BasicProperties,
                        exceptionMessage: $"{result.Error.Code}: {result.Error.Message}",
                        exceptionType: "ResultFailure",
                        retryCount: _options.RetryCount);
                    _channel?.BasicAck(deliveryTag, multiple: false);
                }
            }
            catch (Exception ex)
            {
                _logger?.LogError(ex, "Excecao nao tratada ao processar mensagem na fila {Queue}. Encaminhando para DLQ.", _queueName);
                ForwardToDeadLetterQueue(
                    body: ea.Body,
                    originalProps: ea.BasicProperties,
                    exceptionMessage: ex.Message,
                    exceptionType: ex.GetType().Name,
                    retryCount: _options.RetryCount);
                _channel?.BasicAck(deliveryTag, multiple: false);
            }
        }

        private void ForwardToDeadLetterQueue(
            ReadOnlyMemory<byte> body,
            IBasicProperties? originalProps,
            string exceptionMessage,
            string exceptionType,
            int retryCount)
        {
            if (_channel == null || !_channel.IsOpen)
            {
                return;
            }

            string dlxExchange = $"{_options.ExchangeName}{_options.DeadLetterExchangeSuffix}";
            var dlqProps = _channel.CreateBasicProperties();

            dlqProps.Persistent = true;
            dlqProps.ContentType = originalProps?.ContentType ?? "application/json";
            dlqProps.CorrelationId = originalProps?.CorrelationId ?? Guid.NewGuid().ToString();

            var headers = CloneHeadersOrInitialize(originalProps?.Headers);
            headers[MessagingDiagnosticHeaders.ExceptionMessage] = exceptionMessage;
            headers[MessagingDiagnosticHeaders.ExceptionType] = exceptionType;
            headers[MessagingDiagnosticHeaders.RetryCount] = retryCount.ToString();
            headers[MessagingDiagnosticHeaders.FailedAtUtc] = DateTimeOffset.UtcNow.ToString("O");

            if (!headers.ContainsKey(MessagingDiagnosticHeaders.TraceParent))
            {
                headers[MessagingDiagnosticHeaders.TraceParent] = ResolveTraceParent(originalProps);
            }

            dlqProps.Headers = headers;

            _channel.BasicPublish(
                exchange: dlxExchange,
                routingKey: _routingKey,
                mandatory: false,
                basicProperties: dlqProps,
                body: body);
        }

        private static IDictionary<string, object> CloneHeadersOrInitialize(IDictionary<string, object>? sourceHeaders)
        {
            var headers = new Dictionary<string, object>();
            if (sourceHeaders != null)
            {
                foreach (var entry in sourceHeaders)
                {
                    headers[entry.Key] = entry.Value;
                }
            }
            return headers;
        }

        private static string ResolveTraceParent(IBasicProperties? originalProps)
        {
            if (Activity.Current?.Id != null)
            {
                return Activity.Current.Id;
            }

            if (!string.IsNullOrWhiteSpace(originalProps?.CorrelationId))
            {
                return originalProps.CorrelationId;
            }

            return Guid.NewGuid().ToString();
        }

        private async Task<Result> ExecuteHandlerWithRetryAsync(EventMessage<TEvent> eventMessage, CancellationToken stoppingToken)
        {
            using var scope = _serviceProvider.CreateScope();
            var handler = scope.ServiceProvider.GetRequiredService<THandler>();

            return await _retryPipeline.ExecuteAsync(
                async ct => await handler.HandleAsync(eventMessage, ct).ConfigureAwait(false),
                stoppingToken
            ).ConfigureAwait(false);
        }

        private ResiliencePipeline BuildRetryPipeline()
        {
            return new ResiliencePipelineBuilder()
                .AddRetry(new RetryStrategyOptions
                {
                    MaxRetryAttempts = _options.RetryCount,
                    Delay = TimeSpan.FromMilliseconds(200),
                    BackoffType = DelayBackoffType.Exponential,
                    UseJitter = true,
                    ShouldHandle = new PredicateBuilder()
                        .Handle<Exception>(ex => ex is not OperationCanceledException),
                    OnRetry = args =>
                    {
                        _logger?.LogWarning(
                            args.Outcome.Exception,
                            "Tentativa #{AttemptNumber} de processamento da mensagem na fila {QueueName} apos falha transitoria.",
                            args.AttemptNumber,
                            _queueName);
                        return ValueTask.CompletedTask;
                    }
                })
                .Build();
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
                if (_channel?.IsOpen == true)
                {
                    _channel.Close();
                }
                _channel?.Dispose();

                if (_connection?.IsOpen == true)
                {
                    _connection.Close();
                }
                _connection?.Dispose();
            }
            catch (Exception ex)
            {
                _logger?.LogWarning(ex, "Falha não-bloqueante ao encerrar recursos de conexão do consumidor RabbitMQ no descarte.");
            }
        }
    }
}

