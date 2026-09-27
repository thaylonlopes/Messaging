using System;
using System.Collections.Generic;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using TL.BaseContracts;
using TL.BaseContracts.Messaging;
using TL.BaseContracts.Messaging.Helpers;
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
        private IConnection? _connection;
        private IModel? _channel;
        private readonly object _lock = new();
        private bool _disposed;

        /// <summary>
        /// Inicializa uma nova instância de <see cref="RabbitMqProducer"/>.
        /// </summary>
        /// <param name="options">Opções de configuração de conexão AMQP.</param>
        /// <param name="connectionFactory">Instância customizada de IConnectionFactory (opcional, para testes unitários ou mock).</param>
        /// <param name="logger">Logger para diagnósticos de infraestrutura.</param>
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

        /// <summary>
        /// Publica um evento no RabbitMQ utilizando a exchange configurada ou inferida por convenção.
        /// </summary>
        /// <typeparam name="T">Tipo do evento de integração a ser publicado.</typeparam>
        /// <param name="message">Instância do evento.</param>
        /// <param name="cancellationToken">Token de cancelamento da operação.</param>
        /// <returns>Task representando a conclusão da operação de publicação.</returns>
        public Task PublishAsync<T>(T message, CancellationToken cancellationToken = default) where T : class
        {
            ArgumentNullException.ThrowIfNull(message);

            string exchange = !string.IsNullOrWhiteSpace(_options.ExchangeName)
                ? _options.ExchangeName
                : EventMetadataExtractor.GetTopicName<T>();

            return PublishAsync(exchange, message, cancellationToken);
        }

        /// <summary>
        /// Publica um evento em uma exchange/tópico específico do RabbitMQ.
        /// </summary>
        /// <typeparam name="T">Tipo do evento de integração a ser publicado.</typeparam>
        /// <param name="topicOrExchange">Nome da exchange de destino no RabbitMQ.</param>
        /// <param name="message">Instância do evento.</param>
        /// <param name="cancellationToken">Token de cancelamento da operação.</param>
        /// <returns>Task representando a conclusão da publicação com confirmação do broker.</returns>
        /// <exception cref="InvalidOperationException">Lançada caso o broker não confirme a entrega (NACK).</exception>
        public async Task PublishAsync<T>(string topicOrExchange, T message, CancellationToken cancellationToken = default) where T : class
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(topicOrExchange);
            ArgumentNullException.ThrowIfNull(message);

            string routingKey = EventMetadataExtractor.GetTopicName<T>();

            var result = await PublishDirectAsync(topicOrExchange, routingKey, message, metadata: null, cancellationToken).ConfigureAwait(false);
            if (result.IsFailure)
            {
                throw new InvalidOperationException(
                    $"Falha ao publicar mensagem no RabbitMQ (Exchange: '{topicOrExchange}', RoutingKey: '{routingKey}'): {result.Error.Message}");
            }
        }

        /// <summary>
        /// Publica um evento acompanhado de metadados contextuais (CorrelationId, Headers) retornando <see cref="Result"/>.
        /// </summary>
        /// <typeparam name="T">Tipo do evento a ser publicado.</typeparam>
        /// <param name="message">Instância do evento.</param>
        /// <param name="metadata">Metadados adicionais, incluindo CorrelationId e cabeçalhos customizados.</param>
        /// <param name="cancellationToken">Token de cancelamento.</param>
        /// <returns>Objeto <see cref="Result"/> indicando sucesso ou detalhe de falha.</returns>
        public Task<Result> PublishAsync<T>(
            T message,
            EventMetadata? metadata,
            CancellationToken cancellationToken = default) where T : class
        {
            ArgumentNullException.ThrowIfNull(message);

            string exchange = metadata?.Exchange ?? _options.ExchangeName;
            string routingKey = metadata?.RoutingKey ?? EventMetadataExtractor.GetTopicName<T>();

            return PublishDirectAsync(exchange, routingKey, message, metadata, cancellationToken);
        }

        /// <summary>
        /// Publica um lote de eventos sequencialmente no RabbitMQ com verificação de confirmações.
        /// </summary>
        /// <typeparam name="T">Tipo dos eventos contidos no lote.</typeparam>
        /// <param name="messages">Coleção de eventos a transmitir.</param>
        /// <param name="metadata">Metadados compartilhados a serem aplicados nas mensagens do lote.</param>
        /// <param name="cancellationToken">Token de cancelamento da operação.</param>
        /// <returns>Resultado da operação em lote indicando sucesso ou a primeira falha encontrada.</returns>
        public async Task<Result> PublishBatchAsync<T>(
            IEnumerable<T> messages,
            EventMetadata? metadata = null,
            CancellationToken cancellationToken = default) where T : class
        {
            ArgumentNullException.ThrowIfNull(messages);

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

        /// <summary>
        /// Publica uma mensagem diretamente em uma Exchange e RoutingKey específicas do RabbitMQ com Publisher Confirms ativado.
        /// </summary>
        /// <typeparam name="T">O tipo da mensagem de evento.</typeparam>
        /// <param name="exchange">Nome da Exchange de destino.</param>
        /// <param name="routingKey">Chave de roteamento AMQP.</param>
        /// <param name="message">Instância do evento.</param>
        /// <param name="metadata">Metadados contextuais opcionais.</param>
        /// <param name="cancellationToken">Token de cancelamento.</param>
        /// <returns>Resultado da operação indicando sucesso ou detalhe de falha de publicação/NACK.</returns>
        public Task<Result> PublishDirectAsync<T>(
            string exchange,
            string routingKey,
            T message,
            EventMetadata? metadata = null,
            CancellationToken cancellationToken = default) where T : class
        {
            if (string.IsNullOrWhiteSpace(exchange))
            {
                exchange = _options.ExchangeName;
            }

            if (string.IsNullOrWhiteSpace(routingKey))
            {
                routingKey = EventMetadataExtractor.GetTopicName<T>();
            }

            ArgumentNullException.ThrowIfNull(message);

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

        /// <summary>
        /// Libera os recursos não gerenciados de conexão e canais AMQP do RabbitMQ.
        /// </summary>
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

