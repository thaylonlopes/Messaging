using System.Threading;
using System.Threading.Tasks;
using TL.BaseContracts;
using TL.BaseContracts.Messaging;

namespace TL.Messaging.Kafka.Producer
{
    /// <summary>
    /// Especialização da interface de produtor para Apache Kafka, expondo recursos nativos como chaves de partição e tópicos específicos.
    /// </summary>
    public interface IKafkaProducer : IEventProducer
    {
        /// <summary>
        /// Publica uma mensagem em um tópico específico do Kafka com chave de partição informada.
        /// </summary>
        /// <typeparam name="T">O tipo da mensagem de evento.</typeparam>
        /// <param name="topic">Nome do tópico de destino.</param>
        /// <param name="partitionKey">Chave para determinação da partição de destino.</param>
        /// <param name="message">Instância do evento.</param>
        /// <param name="metadata">Metadados contextuais adicionais.</param>
        /// <param name="cancellationToken">Token de cancelamento.</param>
        /// <returns>Resultado da operação com confirmação do broker.</returns>
        Task<Result> ProduceToTopicAsync<T>(
            string topic,
            string partitionKey,
            T message,
            EventMetadata? metadata = null,
            CancellationToken cancellationToken = default) where T : class;
    }
}

