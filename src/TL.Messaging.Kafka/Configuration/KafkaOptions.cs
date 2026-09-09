using System;

namespace TL.Messaging.Kafka.Configuration
{
    /// <summary>
    /// Opções de configuração para o cluster do Apache Kafka.
    /// </summary>
    public class KafkaOptions
    {
        /// <summary>
        /// Nome da seção padrão no arquivo appsettings.json.
        /// </summary>
        public const string SectionName = "Kafka";

        /// <summary>
        /// Lista de servidores bootstrap separados por vírgula (ex: "localhost:9092").
        /// </summary>
        public string BootstrapServers { get; set; } = "localhost:9092";

        /// <summary>
        /// Identificador padrão do grupo de consumidores (Consumer Group ID).
        /// </summary>
        public string GroupId { get; set; } = "app-consumer-group";

        /// <summary>
        /// Nome do tópico padrão para publicação/consumo.
        /// </summary>
        public string DefaultTopic { get; set; } = "events.default";

        /// <summary>
        /// Sufixo para tópicos de Dead Letter (padrão: ".dlt").
        /// </summary>
        public string DeadLetterTopicSuffix { get; set; } = ".dlt";

        /// <summary>
        /// Política de reset de offset quando não houver offset inicial ("Earliest", "Latest", "Error").
        /// </summary>
        public string AutoOffsetReset { get; set; } = "Earliest";

        /// <summary>
        /// Ativa a idempotência no produtor para prevenir mensagens duplicadas (padrão: true).
        /// </summary>
        public bool EnableIdempotence { get; set; } = true;

        /// <summary>
        /// Usuário para autenticação SASL (opcional).
        /// </summary>
        public string? SaslUsername { get; set; }

        /// <summary>
        /// Senha para autenticação SASL (opcional).
        /// </summary>
        public string? SaslPassword { get; set; }

        /// <summary>
        /// Mecanismo SASL ("Plain", "ScramSha256", "ScramSha512" - opcional).
        /// </summary>
        public string? SaslMechanism { get; set; }

        /// <summary>
        /// Protocolo de segurança ("Plaintext", "Ssl", "SaslPlaintext", "SaslSsl").
        /// </summary>
        public string SecurityProtocol { get; set; } = "Plaintext";

        /// <summary>
        /// Quantidade de retentativas antes de direcionar para o DLT (padrão: 3).
        /// </summary>
        public int RetryCount { get; set; } = 3;
    }
}

