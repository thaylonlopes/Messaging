using System;
using System.Collections.Generic;

namespace TL.Messaging.RabbitMQ.Dlq
{
    /// <summary>
    /// Constantes padronizadas para cabeçalhos de diagnóstico da Dead-Letter Queue (DLQ).
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
        /// Quantidade de tentativas de reprocessamento realizadas antes do encaminhamento para a DLQ.
        /// </summary>
        public const string RetryCount = "x-retry-count";

        /// <summary>
        /// Carimbo de data/hora UTC em formato ISO 8601 do momento da falha.
        /// </summary>
        public const string FailedAtUtc = "x-failed-at-utc";

        /// <summary>
        /// Contexto de rastreabilidade distribuída OpenTelemetry (traceparent).
        /// </summary>
        public const string TraceParent = "traceparent";

        /// <summary>
        /// Coleção de chaves de cabeçalhos de diagnóstico a serem expurgadas no momento do replay.
        /// </summary>
        public static readonly HashSet<string> AllDiagnosticHeaders = new(StringComparer.OrdinalIgnoreCase)
        {
            ExceptionMessage,
            ExceptionType,
            RetryCount,
            FailedAtUtc,
            "x-death",
            "x-first-death-exchange",
            "x-first-death-queue",
            "x-first-death-reason"
        };
    }
}
