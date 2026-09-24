using System.Threading;
using System.Threading.Tasks;

namespace TL.Messaging.RabbitMQ.Dlq
{
    /// <summary>
    /// Gerenciador de operações sobre a Dead-Letter Queue (DLQ) no RabbitMQ.
    /// </summary>
    public interface IRabbitMqDlqManager
    {
        /// <summary>
        /// Reprocessa mensagens acumuladas na Dead-Letter Queue, expurgando cabeçalhos de diagnóstico e republicando na fila de destino.
        /// </summary>
        /// <param name="dlqQueueName">Nome da fila DLQ de origem (ex: "app.orders.dlq").</param>
        /// <param name="targetQueue">Nome da fila ou routing key de destino (opcional; se nulo, infere removendo o sufixo .dlq).</param>
        /// <param name="maxMessages">Quantidade máxima de mensagens a reprocessar nesta execução (padrão: 100).</param>
        /// <param name="cancellationToken">Token de cancelamento da operação.</param>
        /// <returns>Quantidade de mensagens recuperadas e reenviadas com sucesso.</returns>
        Task<int> ReplayAsync(
            string dlqQueueName,
            string? targetQueue = null,
            int maxMessages = 100,
            CancellationToken cancellationToken = default);
    }
}
