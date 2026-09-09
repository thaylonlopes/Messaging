using System;

namespace TL.Messaging.RabbitMQ.Configuration
{
    /// <summary>
    /// Opções de configuração para o cluster ou broker do RabbitMQ.
    /// </summary>
    public class RabbitMqOptions
    {
        /// <summary>
        /// Nome da seção padrão no arquivo appsettings.json.
        /// </summary>
        public const string SectionName = "RabbitMq";

        /// <summary>
        /// Hostname ou IP do servidor RabbitMQ (padrão: "localhost").
        /// </summary>
        public string HostName { get; set; } = "localhost";

        /// <summary>
        /// Porta TCP do broker AMQP (padrão: 5672).
        /// </summary>
        public int Port { get; set; } = 5672;

        /// <summary>
        /// Nome de usuário para autenticação (padrão: "guest").
        /// </summary>
        public string UserName { get; set; } = "guest";

        /// <summary>
        /// Senha do usuário para autenticação (padrão: "guest").
        /// </summary>
        public string Password { get; set; } = "guest";

        /// <summary>
        /// Virtual host do RabbitMQ (padrão: "/").
        /// </summary>
        public string VirtualHost { get; set; } = "/";

        /// <summary>
        /// Nome da Exchange principal de publicação/consumo (padrão: "amq.topic").
        /// </summary>
        public string ExchangeName { get; set; } = "amq.topic";

        /// <summary>
        /// Tipo de Exchange do RabbitMQ (direct, topic, fanout, headers - padrão: "topic").
        /// </summary>
        public string ExchangeType { get; set; } = "topic";

        /// <summary>
        /// Sufixo para a Exchange de Dead Letter (padrão: ".dlx").
        /// </summary>
        public string DeadLetterExchangeSuffix { get; set; } = ".dlx";

        /// <summary>
        /// Sufixo para a Fila de Dead Letter (padrão: ".dlq").
        /// </summary>
        public string DeadLetterQueueSuffix { get; set; } = ".dlq";

        /// <summary>
        /// Quantidade de retentativas antes de encaminhar para a DLQ (padrão: 3).
        /// </summary>
        public int RetryCount { get; set; } = 3;

        /// <summary>
        /// Prefetch count (QoS) para controle de vazão no consumidor (padrão: 10).
        /// </summary>
        public ushort PrefetchCount { get; set; } = 10;
    }
}

