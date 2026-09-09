using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;


namespace TL.Messaging.Abstractions
{
    /// <summary>
    /// Contrato agnóstico (Port) para publicação de eventos assíncronos no ecossistema de mensageria.
    /// </summary>
    /// <remarks>
    /// Permite que a camada de aplicação e domínio envie mensagens sem nenhum acoplamento com
    /// bibliotecas específicas (RabbitMQ, Kafka, Azure Service Bus, AWS SQS).
    /// </remarks>
    /// <example>
    /// <code>
    /// await _eventProducer.PublishAsync(new OrderCreatedEvent(orderId, total), cancellationToken);
    /// </code>
    /// </example>
    public interface IEventProducer
    {
        /// <summary>
        /// Publica um evento assíncrono para o broker de mensagens configurado.
        /// </summary>
        /// <typeparam name="T">O tipo da mensagem de evento.</typeparam>
        /// <param name="message">A instância da mensagem a ser publicada.</param>
        /// <param name="metadata">Metadados opcionais para roteamento, partição ou cabeçalhos.</param>
        /// <param name="cancellationToken">Token de cancelamento da operação.</param>
        /// <returns>Resultado da operação encapsulado em um <see cref="Result"/>.</returns>
        Task<Result> PublishAsync<T>(
            T message,
            EventMetadata? metadata = null,
            CancellationToken cancellationToken = default) where T : class;

        /// <summary>
        /// Publica um lote de eventos de forma atômica ou otimizada para o broker.
        /// </summary>
        /// <typeparam name="T">O tipo das mensagens do lote.</typeparam>
        /// <param name="messages">A coleção de mensagens a serem publicadas.</param>
        /// <param name="metadata">Metadados opcionais aplicados ao lote.</param>
        /// <param name="cancellationToken">Token de cancelamento da operação.</param>
        /// <returns>Resultado da operação encapsulado em um <see cref="Result"/>.</returns>
        Task<Result> PublishBatchAsync<T>(
            IEnumerable<T> messages,
            EventMetadata? metadata = null,
            CancellationToken cancellationToken = default) where T : class;
    }
}

