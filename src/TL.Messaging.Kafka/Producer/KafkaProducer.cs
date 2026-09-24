using System;
using System.Collections.Generic;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using TL.BaseContracts;
using TL.BaseContracts.Messaging;
using TL.BaseContracts.Messaging.Attributes;
using TL.BaseContracts.Messaging.Helpers;
using TL.Messaging.Kafka.Configuration;
using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace TL.Messaging.Kafka.Producer
{
    /// <summary>
    /// Implementação resiliente do produtor de mensagens para Apache Kafka com injeção de headers de telemetria e idempotência.
    /// </summary>
    public class KafkaProducer : IKafkaProducer, IDisposable
    {
        private static readonly JsonSerializerOptions JsonOptions = new()
        {
            PropertyNameCaseInsensitive = true,
            PropertyNamingPolicy = JsonNamingPolicy.CamelCase
        };

        private readonly KafkaOptions _options;
        private readonly IProducer<string, string> _producer;
        private readonly ILogger<KafkaProducer>? _logger;
        private bool _disposed;

        /// <summary>
        /// Inicializa uma nova instância de <see cref="KafkaProducer"/>.
        /// </summary>
        /// <param name="options">Opções de configuração do Kafka.</param>
        /// <param name="producer">Instância customizada de IProducer (opcional, para testes unitários ou configurações avançadas).</param>
        /// <param name="logger">Logger para diagnósticos.</param>
        public KafkaProducer(
            IOptions<KafkaOptions> options,
            IProducer<string, string>? producer = null,
            ILogger<KafkaProducer>? logger = null)
        {
            _options = options?.Value ?? throw new ArgumentNullException(nameof(options));
            _logger = logger;

            if (producer != null)
            {
                _producer = producer;
            }
            else
            {
                var producerConfig = new ProducerConfig
                {
                    BootstrapServers = _options.BootstrapServers,
                    EnableIdempotence = _options.EnableIdempotence,
                    Acks = Acks.All
                };

                if (!string.IsNullOrWhiteSpace(_options.SaslUsername) && !string.IsNullOrWhiteSpace(_options.SaslPassword))
                {
                    producerConfig.SaslUsername = _options.SaslUsername;
                    producerConfig.SaslPassword = _options.SaslPassword;

                    if (Enum.TryParse<SecurityProtocol>(_options.SecurityProtocol, true, out var secProto))
                    {
                        producerConfig.SecurityProtocol = secProto;
                    }

                    if (!string.IsNullOrWhiteSpace(_options.SaslMechanism) &&
                        Enum.TryParse<SaslMechanism>(_options.SaslMechanism, true, out var saslMech))
                    {
                        producerConfig.SaslMechanism = saslMech;
                    }
                }

                _producer = new ProducerBuilder<string, string>(producerConfig).Build();
            }
        }

        /// <inheritdoc />
        public Task PublishAsync<T>(T message, CancellationToken cancellationToken = default) where T : class
        {
            ArgumentNullException.ThrowIfNull(message);

            string inferredTopic = EventMetadataExtractor.GetTopicName<T>();
            return PublishAsync(inferredTopic, message, cancellationToken);
        }

        /// <inheritdoc />
        public async Task PublishAsync<T>(string topicOrExchange, T message, CancellationToken cancellationToken = default) where T : class
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(topicOrExchange);
            ArgumentNullException.ThrowIfNull(message);

            string? partitionKey = EventMetadataExtractor.ExtractPartitionKey(message);

            var result = await ProduceToTopicAsync(topicOrExchange, partitionKey, message, metadata: null, cancellationToken).ConfigureAwait(false);
            if (result.IsFailure)
            {
                throw new InvalidOperationException(
                    $"Falha ao publicar mensagem no Kafka (Tópico: {topicOrExchange}): {result.Error.Message}");
            }
        }

        /// <inheritdoc />
        public Task<Result> PublishAsync<T>(
            T message,
            EventMetadata? metadata,
            CancellationToken cancellationToken = default) where T : class
        {
            ArgumentNullException.ThrowIfNull(message);

            string topic = metadata?.Topic ?? _options.DefaultTopic;
            if (string.IsNullOrWhiteSpace(topic))
            {
                topic = EventMetadataExtractor.GetTopicName<T>();
            }

            string? partitionKey = metadata?.PartitionKey ?? metadata?.RoutingKey ?? EventMetadataExtractor.ExtractPartitionKey(message);

            return ProduceToTopicAsync(topic, partitionKey, message, metadata, cancellationToken);
        }

        /// <inheritdoc />
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
                    return Result.Failure(TL.BaseContracts.Error.Failure("Kafka.Cancellation", "Operação de publicação em lote cancelada."));
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
        public async Task<Result> ProduceToTopicAsync<T>(
            string topic,
            string? partitionKey,
            T message,
            EventMetadata? metadata = null,
            CancellationToken cancellationToken = default) where T : class
        {
            if (string.IsNullOrWhiteSpace(topic))
            {
                topic = _options.DefaultTopic;
            }

            ArgumentNullException.ThrowIfNull(message);

            try
            {
                var eventEnvelope = EventMessage<T>.Create(
                    payload: message,
                    correlationId: metadata?.CorrelationId,
                    eventType: typeof(T).Name,
                    headers: metadata?.Headers);

                string jsonPayload = JsonSerializer.Serialize(eventEnvelope, JsonOptions);

                var kafkaMessage = new Message<string, string>
                {
                    Key = partitionKey!,
                    Value = jsonPayload,
                    Timestamp = new Timestamp(eventEnvelope.Timestamp.UtcDateTime),
                    Headers = new Headers()
                };

                PopulateTracingHeaders(kafkaMessage.Headers, eventEnvelope, metadata);

                var deliveryResult = await _producer.ProduceAsync(topic, kafkaMessage, cancellationToken).ConfigureAwait(false);

                _logger?.LogDebug("Evento {EventType} publicado no Kafka (Tópico: {Topic}, Partição: {Partition}, Offset: {Offset})",
                    eventEnvelope.EventType, topic, deliveryResult.Partition.Value, deliveryResult.Offset.Value);

                return Result.Success();
            }
            catch (ProduceException<string, string> pEx)
            {
                _logger?.LogError(pEx, "Erro de entrega Kafka no tópico {Topic}: {Reason}", topic, pEx.Error.Reason);
                return Result.Failure(TL.BaseContracts.Error.Failure("Kafka.DeliveryError", "Falha na entrega da mensagem ao tópico Kafka."));
            }
            catch (Exception ex)
            {
                _logger?.LogError(ex, "Erro inesperado ao produzir para o tópico Kafka {Topic}", topic);
                return Result.Failure(TL.BaseContracts.Error.Failure("Kafka.PublishError", "Erro inesperado na comunicação com o Apache Kafka."));
            }
        }

        /// <inheritdoc />
        public async Task<Result> ProduceRawAsync(
            string topic,
            string? partitionKey,
            string rawPayload,
            EventMetadata? metadata = null,
            CancellationToken cancellationToken = default)
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(topic);
            ArgumentNullException.ThrowIfNull(rawPayload);

            try
            {
                var kafkaMessage = new Message<string, string>
                {
                    Key = partitionKey ?? string.Empty,
                    Value = rawPayload,
                    Timestamp = Timestamp.Default,
                    Headers = new Headers()
                };

                PopulateRawHeaders(kafkaMessage.Headers, metadata);

                var deliveryResult = await _producer!.ProduceAsync(topic, kafkaMessage, cancellationToken).ConfigureAwait(false);

                _logger?.LogDebug("Payload bruto publicado no Kafka (Tópico: {Topic}, Partição: {Partition}, Offset: {Offset})",
                    topic, deliveryResult.Partition.Value, deliveryResult.Offset.Value);

                return Result.Success();
            }
            catch (ProduceException<string, string> pEx)
            {
                _logger?.LogError(pEx, "Erro de entrega Kafka de payload bruto no tópico {Topic}: {Reason}", topic, pEx.Error.Reason);
                return Result.Failure(TL.BaseContracts.Error.Failure("Kafka.DeliveryError", "Falha na entrega da mensagem ao tópico Kafka."));
            }
            catch (Exception ex)
            {
                _logger?.LogError(ex, "Erro inesperado ao produzir payload bruto para o tópico Kafka {Topic}", topic);
                return Result.Failure(TL.BaseContracts.Error.Failure("Kafka.PublishError", "Erro inesperado na comunicação com o Apache Kafka."));
            }
        }

        private static void PopulateRawHeaders(Headers headers, EventMetadata? metadata)
        {
            if (metadata?.Headers == null) return;

            foreach (var (k, v) in metadata.Headers)
            {
                headers.Add(k, Encoding.UTF8.GetBytes(v ?? string.Empty));
            }
        }

        private static void PopulateTracingHeaders<T>(Headers headers, EventMessage<T> eventEnvelope, EventMetadata? metadata) where T : class
        {
            headers.Add("correlation-id", Encoding.UTF8.GetBytes(eventEnvelope.CorrelationId));
            headers.Add("event-type", Encoding.UTF8.GetBytes(eventEnvelope.EventType));
            headers.Add("event-id", Encoding.UTF8.GetBytes(eventEnvelope.EventId.ToString()));

            if (metadata?.Headers == null) return;

            foreach (var (k, v) in metadata.Headers)
            {
                headers.Add(k, Encoding.UTF8.GetBytes(v ?? string.Empty));
            }
        }

        /// <inheritdoc />
        public void Dispose()
        {
            if (_disposed) return;
            _disposed = true;

            FlushAndDisposeProducerSafely();
        }

        private void FlushAndDisposeProducerSafely()
        {
            try
            {
                _producer.Flush(TimeSpan.FromSeconds(5));
                _producer.Dispose();
            }
            catch (Exception ex)
            {
                _logger?.LogWarning(ex, "Falha não-bloqueante ao descarregar e descartar produtor do Apache Kafka no descarte.");
            }
        }
    }
}

