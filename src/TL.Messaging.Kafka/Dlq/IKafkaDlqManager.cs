using System.Threading;
using System.Threading.Tasks;

namespace TL.Messaging.Kafka.Dlq
{
    /// <summary>
    /// Gerenciador de operações sobre o Dead-Letter Topic (DLT) no Apache Kafka.
    /// </summary>
    public interface IKafkaDlqManager
    {
        /// <summary>
        /// Reprocessa mensagens acumuladas no Dead-Letter Topic, expurgando cabeçalhos de diagnóstico e republicando no tópico de destino.
        /// </summary>
        /// <param name="dltTopic">Nome do tópico DLT de origem (ex: "events.orders.dlt").</param>
        /// <param name="targetTopic">Nome do tópico de destino (opcional; se nulo, infere removendo o sufixo .dlt).</param>
        /// <param name="maxMessages">Quantidade máxima de mensagens a reprocessar nesta execução (padrão: 100).</param>
        /// <param name="cancellationToken">Token de cancelamento da operação.</param>
        /// <returns>Quantidade de mensagens recuperadas e reenviadas com sucesso.</returns>
        Task<int> ReplayAsync(
            string dltTopic,
            string? targetTopic = null,
            int maxMessages = 100,
            CancellationToken cancellationToken = default);
    }
}
