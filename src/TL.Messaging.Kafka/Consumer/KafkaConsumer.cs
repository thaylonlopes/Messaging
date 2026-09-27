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
using System.Diagnostics;
using TL.Messaging.Kafka.Dlq;

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
            PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
            Converters = { new TL.Messaging.Kafka.Serialization.EventMessageJsonConverterFactory() }
        };

        private readonly KafkaOptions _options;
        private readonly IServiceProvider _serviceProvider;
        private readonly IKafkaProducer? _dltProducer;
        private readonly ILogger<KafkaConsumer<TEvent, THandler>>? _logger;
        private readonly ResiliencePipeline _retryPipeline;
        private readonly string _topic;
        private readonly string _dltTopic;

        /// <summary>
        /// Inicializa uma nova instância do consumidor Kafka com commit manual de offsets e suporte a Dead Letter Topic (DLT).
        /// </summary>
        /// <param name="options">Opções de configuração de conexão do cluster Apache Kafka.</param>
        /// <param name="serviceProvider">Provedor de serviços para resolução de escopos de injeção de dependência e handlers.</param>
        /// <param name="dltProducer">Produtor Kafka opcional utilizado para encaminhar mensagens não recuperáveis ao DLT.</param>
        /// <param name="logger">Logger opcional para diagnósticos de consumo e telemetria.</param>
        /// <param name="topic">Nome customizado do tópico de consumo (opcional; se nulo, infere convenção pelo nome do evento).</param>
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

        internal async Task ProcessMessageAsync(
            ConsumeResult<string, string> consumeResult,
            IConsumer<string, string> consumer,
            CancellationToken stoppingToken)
        {
            string rawJson = consumeResult.Message.Value;

            try
            {
                EventMessage<TEvent>? eventMessage;
                try
                {
                    eventMessage = JsonSerializer.Deserialize<EventMessage<TEvent>>(rawJson, JsonOptions);
                }
                catch (JsonException jEx)
                {
                    _logger?.LogWarning(jEx, "Mensagem com formato inválido detectada no tópico {Topic}. Encaminhando para DLT com metadados de diagnóstico.", _topic);
                    await ForwardToDltAsync(
                        consumeResult.Message.Key,
                        rawJson,
                        consumeResult.Message.Headers,
                        exceptionMessage: jEx.Message,
                        exceptionType: jEx.GetType().Name,
                        retryCount: 0,
                        stoppingToken).ConfigureAwait(false);
                    consumer.Commit(consumeResult);
                    return;
                }

                if (eventMessage == null || eventMessage.Payload == null)
                {
                    _logger?.LogWarning("Mensagem nula ou invalida recebida no topico {Topic}. Enviando para DLT.", _topic);
                    await ForwardToDltAsync(
                        consumeResult.Message.Key,
                        rawJson,
                        consumeResult.Message.Headers,
                        exceptionMessage: "Payload invalido ou nulo",
                        exceptionType: "InvalidPayloadException",
                        retryCount: 0,
                        stoppingToken).ConfigureAwait(false);
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
                    await ForwardToDltAsync(
                        consumeResult.Message.Key,
                        rawJson,
                        consumeResult.Message.Headers,
                        exceptionMessage: $"{result.Error.Code}: {result.Error.Message}",
                        exceptionType: "ResultFailure",
                        retryCount: _options.RetryCount,
                        stoppingToken).ConfigureAwait(false);
                    consumer.Commit(consumeResult);
                }
            }
            catch (Exception ex)
            {
                _logger?.LogError(ex, "Excecao nao tratada ao processar mensagem no topico {Topic}. Encaminhando para DLT.", _topic);
                await ForwardToDltAsync(
                    consumeResult.Message.Key,
                    rawJson,
                    consumeResult.Message.Headers,
                    exceptionMessage: ex.Message,
                    exceptionType: ex.GetType().Name,
                    retryCount: _options.RetryCount,
                    stoppingToken).ConfigureAwait(false);
                consumer.Commit(consumeResult);
            }
        }

        private async Task ForwardToDltAsync(
            string key,
            string rawJson,
            Headers? incomingHeaders,
            string exceptionMessage,
            string exceptionType,
            int retryCount,
            CancellationToken stoppingToken)
        {
            if (_dltProducer == null)
            {
                return;
            }

            try
            {
                var headers = CloneHeadersOrInitialize(incomingHeaders);
                headers[MessagingDiagnosticHeaders.ExceptionMessage] = exceptionMessage;
                headers[MessagingDiagnosticHeaders.ExceptionType] = exceptionType;
                headers[MessagingDiagnosticHeaders.RetryCount] = retryCount.ToString();
                headers[MessagingDiagnosticHeaders.FailedAtUtc] = DateTimeOffset.UtcNow.ToString("O");
                headers[MessagingDiagnosticHeaders.OriginalTopic] = _topic;
                headers[MessagingDiagnosticHeaders.Reason] = exceptionMessage;

                if (!headers.ContainsKey(MessagingDiagnosticHeaders.TraceParent))
                {
                    headers[MessagingDiagnosticHeaders.TraceParent] = ResolveTraceParent(headers, key);
                }

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

        private static Dictionary<string, string> CloneHeadersOrInitialize(Headers? incomingHeaders)
        {
            var headers = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
            if (incomingHeaders == null)
            {
                return headers;
            }

            foreach (var header in incomingHeaders)
            {
                if (header.GetValueBytes() != null)
                {
                    headers[header.Key] = Encoding.UTF8.GetString(header.GetValueBytes());
                }
            }

            return headers;
        }

        private static string ResolveTraceParent(Dictionary<string, string> headers, string? fallbackKey)
        {
            if (Activity.Current?.Id != null)
            {
                return Activity.Current.Id;
            }

            if (headers.TryGetValue(MessagingDiagnosticHeaders.TraceParent, out var existingTrace) && !string.IsNullOrWhiteSpace(existingTrace))
            {
                return existingTrace;
            }

            if (!string.IsNullOrWhiteSpace(fallbackKey))
            {
                return fallbackKey;
            }

            return Guid.NewGuid().ToString();
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

