using System;
using System.Collections.Generic;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using TL.BaseContracts;
using TL.BaseContracts.Messaging;
using TL.Messaging.RabbitMQ.Configuration;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using RabbitMQ.Client;

namespace TL.Messaging.RabbitMQ.Producer
{
    /// <summary>
    /// Implementação padrão do produtor de eventos com RabbitMQ, suporte a Publisher Confirms e serialização UTF-8 / JSON.
    /// </summary>
    public class RabbitMqProducer : IRabbitMqProducer, IDisposable
    {
        private static readonly JsonSerializerOptions JsonOptions = new()
        {
            PropertyNameCaseInsensitive = true,
            PropertyNamingPolicy = JsonNamingPolicy.CamelCase
        };

        private readonly RabbitMqOptions _options;
        private readonly IConnectionFactory _connectionFactory;
        private readonly ILogger<RabbitMqProducer>? _logger;
        private readonly object _lock = new();

        private IConnection? _connection;
        private IModel? _channel;
        private bool _disposed;

        /// <summary>
        /// Inicializa uma nova instância de <see cref="RabbitMqProducer"/>.
        /// </summary>
        /// <param name="options">Opções de configuração do RabbitMQ.</param>
        /// <param name="connectionFactory">Fábrica de conexões AMQP opcional (se nula, usa ConnectionFactory padrão).</param>
        /// <param name="logger">Logger opcional para diagnóstico.</param>
        public RabbitMqProducer(
            IOptions<RabbitMqOptions> options,
            IConnectionFactory? connectionFactory = null,
            ILogger<RabbitMqProducer>? logger = null)
        {
            _options = options?.Value ?? throw new ArgumentNullException(nameof(options));
            _logger = logger;

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
        public Task<Result> PublishAsync<T>(
            T message,
            EventMetadata? metadata = null,
            CancellationToken cancellationToken = default) where T : class
        {
            if (message == null) throw new ArgumentNullException(nameof(message));

            string exchange = metadata?.Exchange ?? _options.ExchangeName;
            string routingKey = metadata?.RoutingKey ?? typeof(T).Name.ToLowerInvariant();

            return PublishDirectAsync(exchange, routingKey, message, metadata, cancellationToken);
        }

        /// <inheritdoc />
        public async Task<Result> PublishBatchAsync<T>(
            IEnumerable<T> messages,
            EventMetadata? metadata = null,
            CancellationToken cancellationToken = default) where T : class
        {
            if (messages == null) throw new ArgumentNullException(nameof(messages));

            foreach (var message in messages)
            {
                if (cancellationToken.IsCancellationRequested)
                {
                    return Result.Failure(Error.Failure("RabbitMq.Cancellation", "Operação cancelada pelo usuário."));
                }

                var result = await PublishAsync(message, metadata, cancellationToken).ConfigureAwait(false);
                if (result.IsFailure)
                {
                    return result;
                }
            }

            return Result.Success();
        }

        /// <inheritdoc />
        public Task<Result> PublishDirectAsync<T>(
            string exchange,
            string routingKey,
            T message,
            EventMetadata? metadata = null,
            CancellationToken cancellationToken = default) where T : class
        {
            if (string.IsNullOrWhiteSpace(exchange)) exchange = _options.ExchangeName;
            if (string.IsNullOrWhiteSpace(routingKey)) routingKey = typeof(T).Name.ToLowerInvariant();
            if (message == null) throw new ArgumentNullException(nameof(message));

            try
            {
                EnsureChannel();

                var eventEnvelope = EventMessage<T>.Create(
                    payload: message,
                    correlationId: metadata?.CorrelationId,
                    eventType: typeof(T).Name,
                    headers: metadata?.Headers);

                byte[] body = JsonSerializer.SerializeToUtf8Bytes(eventEnvelope, JsonOptions);

                IBasicProperties properties = _channel!.CreateBasicProperties();
                properties.Persistent = true;
                properties.ContentType = "application/json";
                properties.CorrelationId = eventEnvelope.CorrelationId;
                properties.Type = eventEnvelope.EventType;
                properties.Timestamp = new AmqpTimestamp(eventEnvelope.Timestamp.ToUnixTimeSeconds());
                properties.Headers = new Dictionary<string, object>();

                if (metadata?.Headers != null)
                {
                    foreach (var (k, v) in metadata.Headers)
                    {
                        properties.Headers[k] = v;
                    }
                }

                lock (_lock)
                {
                    _channel.BasicPublish(
                        exchange: exchange,
                        routingKey: routingKey,
                        basicProperties: properties,
                        body: body);

                    bool confirmed = _channel.WaitForConfirms(TimeSpan.FromSeconds(5));
                    if (!confirmed)
                    {
                        _logger?.LogWarning("Mensagem {EventType} não confirmada pelo broker RabbitMQ (NACK recebido).", eventEnvelope.EventType);
                        return Task.FromResult(Result.Failure(Error.Failure("RabbitMq.PublishNack", "O broker RabbitMQ não confirmou a entrega da mensagem (NACK recebido).")));
                    }
                }

                _logger?.LogDebug("Mensagem {EventType} publicada com sucesso no RabbitMQ (Exchange: {Exchange}, RoutingKey: {RoutingKey})",
                    eventEnvelope.EventType, exchange, routingKey);

                return Task.FromResult(Result.Success());
            }
            catch (Exception ex)
            {
                _logger?.LogError(ex, "Erro ao publicar mensagem no RabbitMQ (Exchange: {Exchange}, RoutingKey: {RoutingKey})",
                    exchange, routingKey);
                return Task.FromResult(Result.Failure(Error.Failure("RabbitMq.PublishError", "Falha ao publicar mensagem no broker RabbitMQ.")));
            }
        }

        private void EnsureChannel()
        {
            if (_channel != null && _channel.IsOpen) return;

            lock (_lock)
            {
                if (_channel != null && _channel.IsOpen) return;

                if (_connection == null || !_connection.IsOpen)
                {
                    _connection = _connectionFactory.CreateConnection();
                }

                _channel = _connection.CreateModel();
                EnablePublisherConfirms(_channel);
            }
        }

        private static void EnablePublisherConfirms(IModel channel)
        {
            channel.ConfirmSelect();
        }

        /// <inheritdoc />
        public void Dispose()
        {
            if (_disposed) return;
            _disposed = true;

            DisposeConnectionAndChannelSafely();
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
                _logger?.LogWarning(ex, "Falha não-bloqueante ao encerrar recursos de conexão do RabbitMQ no descarte.");
            }
        }
    }
}

