using System;
using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using TL.Messaging.Kafka.Configuration;
using TL.Messaging.Kafka.Producer;

namespace TL.Messaging.Kafka
{
    /// <summary>
    /// Produtor de eventos de alta resiliência para Apache Kafka implementando <see cref="IKafkaProducer"/>.
    /// Especializa <see cref="KafkaProducer"/> para publicação simplificada e controle estrito de partições.
    /// </summary>
    public class KafkaEventProducer : KafkaProducer
    {
        /// <summary>
        /// Inicializa uma nova instância de <see cref="KafkaEventProducer"/>.
        /// </summary>
        /// <param name="options">Opções de configuração do Kafka.</param>
        /// <param name="producer">Instância customizada de IProducer para testes ou cenários avançados.</param>
        /// <param name="logger">Logger para diagnósticos operacionais.</param>
        public KafkaEventProducer(
            IOptions<KafkaOptions> options,
            IProducer<string, string>? producer = null,
            ILogger<KafkaProducer>? logger = null)
            : base(options, producer, logger)
        {
        }
    }
}

