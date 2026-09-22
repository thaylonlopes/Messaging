using System;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using RabbitMQ.Client;
using TL.Messaging.RabbitMQ.Configuration;
using TL.Messaging.RabbitMQ.Producer;

namespace TL.Messaging.RabbitMQ
{
    /// <summary>
    /// Produtor de eventos de alta resiliência para RabbitMQ implementando <see cref="IRabbitMqProducer"/>.
    /// Especializa <see cref="RabbitMqProducer"/> para publicação simplificada com inferência de tópicos e exchanges.
    /// </summary>
    public class RabbitMqEventProducer : RabbitMqProducer
    {
        /// <summary>
        /// Inicializa uma nova instância de <see cref="RabbitMqEventProducer"/>.
        /// </summary>
        /// <param name="options">Opções de configuração do RabbitMQ.</param>
        /// <param name="connectionFactory">Fábrica de conexões AMQP opcional (para testes unitários).</param>
        /// <param name="logger">Logger opcional para diagnóstico operacional.</param>
        public RabbitMqEventProducer(
            IOptions<RabbitMqOptions> options,
            IConnectionFactory? connectionFactory = null,
            ILogger<RabbitMqProducer>? logger = null)
            : base(options, connectionFactory, logger)
        {
        }
    }
}

