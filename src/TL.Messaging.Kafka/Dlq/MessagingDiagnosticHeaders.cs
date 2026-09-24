using System;
using System.Collections.Generic;

namespace TL.Messaging.Kafka.Dlq
{
    /// <summary>
    /// Constantes padronizadas para cabeçalhos de diagnóstico do Dead-Letter Topic (DLT).
    /// </summary>
    public static class MessagingDiagnosticHeaders
    {
        /// <summary>
        /// Mensagem de erro descritiva da exceção capturada.
        /// </summary>
        public const string ExceptionMessage = "x-exception-message";

        /// <summary>
        /// Nome do tipo da exceção ocorrida (ex: JsonException, InvalidOperationException).
        /// </summary>
        public const string ExceptionType = "x-exception-type";

        /// <summary>
        /// Quantidade de tentativas de reprocessamento realizadas antes do encaminhamento para o DLT.
        /// </summary>
        public const string RetryCount = "x-retry-count";

        /// <summary>
        /// Carimbo de data/hora UTC em formato ISO 8601 do momento da falha.
        /// </summary>
        public const string FailedAtUtc = "x-failed-at-utc";

        /// <summary>
        /// Contexto de rastreabilidade distribuída padrão W3C traceparent.
        /// </summary>
        public const string TraceParent = "traceparent";

        /// <summary>
        /// Nome do tópico de origem onde ocorreu a falha.
        /// </summary>
        public const string OriginalTopic = "x-dlt-original-topic";

        /// <summary>
        /// Motivo sumário do direcionamento para o DLT.
        /// </summary>
        public const string Reason = "x-dlt-reason";

        /// <summary>
        /// Coleção de chaves de cabeçalhos de diagnóstico a serem expurgadas no momento do replay.
        /// </summary>
        public static readonly HashSet<string> AllDiagnosticHeaders = new(StringComparer.OrdinalIgnoreCase)
        {
            ExceptionMessage,
            ExceptionType,
            RetryCount,
            FailedAtUtc,
            OriginalTopic,
            Reason
        };
    }
}
