using System;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using TL.BaseContracts.Messaging;
using TL.Messaging.Kafka.Configuration;
using TL.Messaging.Kafka.Producer;
using Confluent.Kafka;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Polly;
using Polly.Retry;

namespace TL.Messaging.Kafka.Consumer
{
    /// <summary>
    /// Background Service consumidor resiliente de Apache Kafka com commit manual de offsets e suporte a Dead Letter Topic (DLT).
    /// </summary>
    /// <typeparam name="TEvent">Tipo do payload de evento.</typeparam>
    /// <typeparam name="THandler">Tipo do manipulador de negócio <see cref="IEventHandler{TEvent}"/>.</typeparam>
    public class KafkaConsumer<TEvent, THandler> : BackgroundService
        where TEvent : class
        where THandler : IEventHandler<TEvent>
    {
        private static readonly JsonSerializerOptions JsonOptions = new()
        {
            PropertyNameCaseInsensitive = true,
            PropertyNamingPolicy = JsonNamingPolicy.CamelCase
        };

        private readonly KafkaOptions _options;
        private readonly IServiceProvider _serviceProvider;
        private readonly IKafkaProducer? _dltProducer;
        private readonly ILogger<KafkaConsumer<TEvent, THandler>>? _logger;
        private readonly ResiliencePipeline _retryPipeline;
        private readonly string _topic;
        private readonly string _dltTopic;

        /// <summary>
        /// Inicializa uma nova instância do consumidor Kafka.
        /// </summary>
        public KafkaConsumer(
            IOptions<KafkaOptions> options,
            IServiceProvider serviceProvider,
            IKafkaProducer? dltProducer = null,
            ILogger<KafkaConsumer<TEvent, THandler>>? logger = null,
            string? topic = null)
        {
            _options = options?.Value ?? throw new ArgumentNullException(nameof(options));
            _serviceProvider = serviceProvider ?? throw new ArgumentNullException(nameof(serviceProvider));
            _dltProducer = dltProducer;
            _logger = logger;

            string eventName = typeof(TEvent).Name.ToLowerInvariant();
            _topic = string.IsNullOrWhiteSpace(topic) ? $"events.{eventName}" : topic;
            _dltTopic = $"{_topic}{_options.DeadLetterTopicSuffix}";

            _retryPipeline = BuildRetryPipeline();
        }

        /// <inheritdoc />
        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            await Task.Yield(); // Libera a inicialização do host

            var consumerConfig = new ConsumerConfig
            {
                BootstrapServers = _options.BootstrapServers,
                GroupId = _options.GroupId,
                AutoOffsetReset = Enum.TryParse<AutoOffsetReset>(_options.AutoOffsetReset, true, out var aor)
                    ? aor
                    : Confluent.Kafka.AutoOffsetReset.Earliest,
                EnableAutoCommit = false, // Commit manual para confiabilidade
                EnableAutoOffsetStore = false
            };

            using var consumer = new ConsumerBuilder<string, string>(consumerConfig).Build();

            try
            {
                consumer.Subscribe(_topic);
                _logger?.LogInformation("Consumidor Kafka inscrito no tópico {Topic} (GroupId: {GroupId})", _topic, _options.GroupId);

                while (!stoppingToken.IsCancellationRequested)
                {
                    try
                    {
                        var consumeResult = consumer.Consume(stoppingToken);
                        if (consumeResult?.Message == null) continue;

                        await ProcessMessageAsync(consumeResult, consumer, stoppingToken).ConfigureAwait(false);
                    }
                    catch (ConsumeException cEx)
                    {
                        _logger?.LogError(cEx, "Erro ao consumir mensagem no tópico {Topic}: {Reason}", _topic, cEx.Error.Reason);
                    }
                    catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
                    {
                        break;
                    }
                    catch (Exception ex)
                    {
                        _logger?.LogError(ex, "Erro inesperado no loop do consumidor Kafka no tópico {Topic}", _topic);
                    }
                }
            }
            catch (Exception ex)
            {
                _logger?.LogError(ex, "Falha crítica ao executar o consumidor Kafka no tópico {Topic}", _topic);
            }
            finally
            {
                CloseConsumerSafely(consumer);
            }
        }

        private void CloseConsumerSafely(IConsumer<string, string> consumer)
        {
            try
            {
                consumer.Close();
            }
            catch (Exception ex)
            {
                _logger?.LogWarning(ex, "Falha não-bloqueante ao encerrar consumidor do Apache Kafka no descarte.");
            }
        }

        private async Task ProcessMessageAsync(
            ConsumeResult<string, string> consumeResult,
            IConsumer<string, string> consumer,
            CancellationToken stoppingToken)
        {
            string rawJson = consumeResult.Message.Value;

            try
            {
                var eventMessage = JsonSerializer.Deserialize<EventMessage<TEvent>>(rawJson, JsonOptions);
                if (eventMessage == null || eventMessage.Payload == null)
                {
                    _logger?.LogWarning("Mensagem nula ou inválida recebida no tópico {Topic}. Enviando para DLT.", _topic);
                    await ForwardToDltAsync(consumeResult.Message.Key, rawJson, "Payload inválido ou nulo", stoppingToken).ConfigureAwait(false);
                    consumer.Commit(consumeResult);
                    return;
                }

                using var scope = _serviceProvider.CreateScope();
                var handler = scope.ServiceProvider.GetRequiredService<THandler>();

                var result = await _retryPipeline.ExecuteAsync(
                    async ct => await handler.HandleAsync(eventMessage, ct).ConfigureAwait(false),
                    stoppingToken
                ).ConfigureAwait(false);

                if (result.IsSuccess)
                {
                    consumer.Commit(consumeResult);
                    _logger?.LogDebug("Mensagem {EventId} commitada com sucesso no Kafka.", eventMessage.EventId);
                }
                else
                {
                    _logger?.LogWarning("Handler falhou [{Code}]: {Message}. Encaminhando mensagem para DLT {DltTopic}.",
                        result.Error.Code, result.Error.Message, _dltTopic);
                    await ForwardToDltAsync(consumeResult.Message.Key, rawJson, $"{result.Error.Code}: {result.Error.Message}", stoppingToken).ConfigureAwait(false);
                    consumer.Commit(consumeResult); // Commit no tópico principal após encaminhar para DLT
                }
            }
            catch (Exception ex)
            {
                _logger?.LogError(ex, "Exceção não tratada ao processar mensagem no tópico {Topic}. Encaminhando para DLT.", _topic);
                await ForwardToDltAsync(consumeResult.Message.Key, rawJson, ex.Message, stoppingToken).ConfigureAwait(false);
                consumer.Commit(consumeResult);
            }
        }

        private async Task ForwardToDltAsync(string key, string rawJson, string reason, CancellationToken stoppingToken)
        {
            if (_dltProducer == null) return;

            try
            {
                var headers = new Dictionary<string, string>
                {
                    ["x-dlt-original-topic"] = _topic,
                    ["x-dlt-reason"] = reason,
                    ["x-dlt-timestamp-utc"] = DateTimeOffset.UtcNow.ToString("O")
                };

                var metadata = new EventMetadata
                {
                    Headers = headers
                }.WithKafkaPartitionKey(key);

                await _dltProducer.ProduceToTopicAsync(_dltTopic, key, rawJson, metadata, stoppingToken).ConfigureAwait(false);
                _logger?.LogInformation("Mensagem encaminhada com sucesso para o Dead Letter Topic {DltTopic}", _dltTopic);
            }
            catch (Exception ex)
            {
                _logger?.LogError(ex, "Falha ao encaminhar mensagem para o Dead Letter Topic {DltTopic}", _dltTopic);
            }
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
                            "Tentativa #{AttemptNumber} de processamento da mensagem no topico {Topic} apos falha transitoria.",
                            args.AttemptNumber,
                            _topic);
                        return ValueTask.CompletedTask;
                    }
                })
                .Build();
        }
    }
}

