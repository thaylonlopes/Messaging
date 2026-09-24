using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using RabbitMQ.Client;
using TL.Messaging.RabbitMQ.Configuration;

namespace TL.Messaging.RabbitMQ.Dlq
{
    /// <summary>
    /// Implementação padrão do gestor de operações de Dead-Letter Queue (DLQ) para RabbitMQ.
    /// </summary>
    public class RabbitMqDlqManager : IRabbitMqDlqManager, IDisposable
    {
        private readonly RabbitMqOptions _options;
        private readonly IConnectionFactory _connectionFactory;
        private readonly ILogger<RabbitMqDlqManager>? _logger;
        private IConnection? _connection;
        private IModel? _channel;

        /// <summary>
        /// Inicializa uma nova instância de <see cref="RabbitMqDlqManager"/>.
        /// </summary>
        public RabbitMqDlqManager(
            IOptions<RabbitMqOptions> options,
            IConnectionFactory? connectionFactory = null,
            ILogger<RabbitMqDlqManager>? logger = null)
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
        public Task<int> ReplayAsync(
            string dlqQueueName,
            string? targetQueue = null,
            int maxMessages = 100,
            CancellationToken cancellationToken = default)
        {
            if (string.IsNullOrWhiteSpace(dlqQueueName))
            {
                throw new ArgumentException("Nome da fila DLQ não pode ser nulo ou vazio.", nameof(dlqQueueName));
            }

            if (maxMessages <= 0)
            {
                return Task.FromResult(0);
            }

            EnsureChannelIsOpen();

            string target = string.IsNullOrWhiteSpace(targetQueue)
                ? InferTargetQueue(dlqQueueName)
                : targetQueue;

            int replayedCount = 0;

            while (replayedCount < maxMessages && !cancellationToken.IsCancellationRequested)
            {
                var result = _channel!.BasicGet(dlqQueueName, autoAck: false);
                if (result == null)
                {
                    break;
                }

                var cleanProperties = CreateCleanProperties(_channel, result.BasicProperties);

                _channel.BasicPublish(
                    exchange: string.Empty,
                    routingKey: target,
                    mandatory: false,
                    basicProperties: cleanProperties,
                    body: result.Body);

                _channel.BasicAck(result.DeliveryTag, multiple: false);
                replayedCount++;
            }

            _logger?.LogInformation("Replay concluído: {Count} mensagem(ns) reprocessadas da fila {DlqQueue} para {TargetQueue}.",
                replayedCount, dlqQueueName, target);

            return Task.FromResult(replayedCount);
        }

        private void EnsureChannelIsOpen()
        {
            if (_connection == null || !_connection.IsOpen)
            {
                _connection = _connectionFactory.CreateConnection();
            }

            if (_channel == null || !_channel.IsOpen)
            {
                _channel = _connection.CreateModel();
            }
        }

        private string InferTargetQueue(string dlqQueueName)
        {
            string suffix = _options.DeadLetterQueueSuffix;
            if (!string.IsNullOrEmpty(suffix) && dlqQueueName.EndsWith(suffix, StringComparison.OrdinalIgnoreCase))
            {
                return dlqQueueName.Substring(0, dlqQueueName.Length - suffix.Length);
            }

            return dlqQueueName;
        }

        private static IBasicProperties CreateCleanProperties(IModel channel, IBasicProperties sourceProps)
        {
            var targetProps = channel.CreateBasicProperties();
            targetProps.Persistent = sourceProps.Persistent;
            targetProps.ContentType = sourceProps.ContentType ?? "application/json";
            targetProps.CorrelationId = sourceProps.CorrelationId;
            targetProps.MessageId = sourceProps.MessageId;
            targetProps.Type = sourceProps.Type;

            if (sourceProps.Headers != null)
            {
                var cleanHeaders = new Dictionary<string, object>();
                foreach (var header in sourceProps.Headers)
                {
                    if (!MessagingDiagnosticHeaders.AllDiagnosticHeaders.Contains(header.Key))
                    {
                        cleanHeaders[header.Key] = header.Value;
                    }
                }
                targetProps.Headers = cleanHeaders;
            }

            return targetProps;
        }

        /// <inheritdoc />
        public void Dispose()
        {
            try
            {
                _channel?.Dispose();
                _connection?.Dispose();
            }
            catch (Exception ex)
            {
                _logger?.LogWarning(ex, "Falha não-bloqueante ao descartar RabbitMqDlqManager.");
            }
        }
    }
}
