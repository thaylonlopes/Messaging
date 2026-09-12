using System.Threading;
using System.Threading.Tasks;
using TL.BaseContracts;
using TL.BaseContracts.Messaging;

namespace TL.Messaging.RabbitMQ.Producer
{
    /// <summary>
    /// Especialização da interface de produtor para RabbitMQ, expondo capacidades específicas do protocolo AMQP.
    /// </summary>
    public interface IRabbitMqProducer : IEventProducer
    {
        /// <summary>
        /// Publica uma mensagem diretamente em uma Exchange e RoutingKey específicas do RabbitMQ.
        /// </summary>
        /// <typeparam name="T">O tipo da mensagem de evento.</typeparam>
        /// <param name="exchange">Nome da Exchange de destino.</param>
        /// <param name="routingKey">Chave de roteamento AMQP.</param>
        /// <param name="message">Instância do evento.</param>
        /// <param name="metadata">Metadados opcionais.</param>
        /// <param name="cancellationToken">Token de cancelamento.</param>
        /// <returns>Resultado da operação.</returns>
        Task<Result> PublishDirectAsync<T>(
            string exchange,
            string routingKey,
            T message,
            EventMetadata? metadata = null,
            CancellationToken cancellationToken = default) where T : class;
    }
}

