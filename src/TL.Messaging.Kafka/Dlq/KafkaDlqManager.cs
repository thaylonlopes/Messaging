using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using TL.BaseContracts.Messaging;
using TL.Messaging.Kafka.Configuration;
using TL.Messaging.Kafka.Producer;

namespace TL.Messaging.Kafka.Dlq
{
    /// <summary>
    /// Implementação padrão do gestor de operações de Dead-Letter Topic (DLT) para Apache Kafka.
    /// </summary>
    public class KafkaDlqManager : IKafkaDlqManager
    {
        private readonly KafkaOptions _options;
        private readonly IKafkaProducer _producer;
        private readonly Func<ConsumerConfig, IConsumer<string, string>>? _consumerFactory;
        private readonly ILogger<KafkaDlqManager>? _logger;

        /// <summary>
        /// Inicializa uma nova instância de <see cref="KafkaDlqManager"/>.
        /// </summary>
        public KafkaDlqManager(
            IOptions<KafkaOptions> options,
            IKafkaProducer producer,
            ILogger<KafkaDlqManager>? logger = null,
            Func<ConsumerConfig, IConsumer<string, string>>? consumerFactory = null)
        {
            _options = options?.Value ?? throw new ArgumentNullException(nameof(options));
            _producer = producer ?? throw new ArgumentNullException(nameof(producer));
            _logger = logger;
            _consumerFactory = consumerFactory;
        }

        /// <inheritdoc />
        public async Task<int> ReplayAsync(
            string dltTopic,
            string? targetTopic = null,
            int maxMessages = 100,
            CancellationToken cancellationToken = default)
        {
            if (string.IsNullOrWhiteSpace(dltTopic))
            {
                throw new ArgumentException("Nome do tópico DLT não pode ser nulo ou vazio.", nameof(dltTopic));
            }

            if (maxMessages <= 0)
            {
                return 0;
            }

            string target = string.IsNullOrWhiteSpace(targetTopic)
                ? InferTargetTopic(dltTopic)
                : targetTopic;

            var consumerConfig = new ConsumerConfig
            {
                BootstrapServers = _options.BootstrapServers,
                GroupId = $"{_options.GroupId}-replay-{Guid.NewGuid():N}",
                AutoOffsetReset = Confluent.Kafka.AutoOffsetReset.Earliest,
                EnableAutoCommit = false,
                EnableAutoOffsetStore = false
            };

            using var consumer = _consumerFactory != null
                ? _consumerFactory(consumerConfig)
                : new ConsumerBuilder<string, string>(consumerConfig).Build();

            consumer.Subscribe(dltTopic);
            int replayedCount = 0;

            try
            {
                while (replayedCount < maxMessages && !cancellationToken.IsCancellationRequested)
                {
                    ConsumeResult<string, string>? consumeResult;
                    try
                    {
                        consumeResult = consumer.Consume(TimeSpan.FromSeconds(2));
                    }
                    catch (ConsumeException cEx)
                    {
                        _logger?.LogWarning(cEx, "Erro transitório ao consumir DLT no replay do tópico {Topic}", dltTopic);
                        break;
                    }

                    if (consumeResult?.Message == null)
                    {
                        break;
                    }

                    var cleanHeaders = ExtractCleanHeaders(consumeResult.Message.Headers);
                    var metadata = new EventMetadata
                    {
                        Headers = cleanHeaders
                    };

                    if (!string.IsNullOrWhiteSpace(consumeResult.Message.Key))
                    {
                        metadata = metadata.WithKafkaPartitionKey(consumeResult.Message.Key);
                    }

                    await _producer.ProduceToTopicAsync(
                        target,
                        consumeResult.Message.Key,
                        consumeResult.Message.Value,
                        metadata,
                        cancellationToken).ConfigureAwait(false);

                    consumer.Commit(consumeResult);
                    replayedCount++;
                }
            }
            finally
            {
                CloseConsumerSafely(consumer);
            }

            _logger?.LogInformation("Replay concluído: {Count} mensagem(ns) reprocessadas do tópico {DltTopic} para {TargetTopic}.",
                replayedCount, dltTopic, target);

            return replayedCount;
        }

        private string InferTargetTopic(string dltTopic)
        {
            string suffix = _options.DeadLetterTopicSuffix;
            if (!string.IsNullOrEmpty(suffix) && dltTopic.EndsWith(suffix, StringComparison.OrdinalIgnoreCase))
            {
                return dltTopic.Substring(0, dltTopic.Length - suffix.Length);
            }

            return dltTopic;
        }

        private static Dictionary<string, string> ExtractCleanHeaders(Headers? headers)
        {
            var clean = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
            if (headers == null)
            {
                return clean;
            }

            foreach (var header in headers)
            {
                if (!MessagingDiagnosticHeaders.AllDiagnosticHeaders.Contains(header.Key) && header.GetValueBytes() != null)
                {
                    clean[header.Key] = Encoding.UTF8.GetString(header.GetValueBytes());
                }
            }

            return clean;
        }

        private void CloseConsumerSafely(IConsumer<string, string> consumer)
        {
            try
            {
                consumer.Close();
            }
            catch (Exception ex)
            {
                _logger?.LogWarning(ex, "Falha não-bloqueante ao encerrar consumidor no replay do DLT.");
            }
        }
    }
}
