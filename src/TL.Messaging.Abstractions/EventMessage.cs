using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;

namespace TL.Messaging.Abstractions
{
    /// <summary>
    /// Envelope padronizado e imutável para transporte de eventos e mensagens assíncronas no ecossistema de microsserviços.
    /// </summary>
    /// <typeparam name="T">O tipo do payload de dados transportado na mensagem.</typeparam>
    /// <remarks>
    /// Garante a propagação uniforme de metadados fundamentais (ID único, CorrelationId, Timestamp, Tipo de Evento e Headers)
    /// compatíveis com especificações como CloudEvents v1.0.
    /// </remarks>
    /// <example>
    /// <code>
    /// var eventMessage = EventMessage&lt;OrderCreatedEvent&gt;.Create(orderEvent, correlationId: "req-123");
    /// </code>
    /// </example>
    public record EventMessage<T>
    {
        private static readonly IReadOnlyDictionary<string, string> EmptyHeaders =
            new ReadOnlyDictionary<string, string>(new Dictionary<string, string>());

        /// <summary>
        /// Identificador exclusivo global deste evento (UUID/Guid).
        /// </summary>
        public Guid EventId { get; init; }

        /// <summary>
        /// Identificador de correlação para rastreamento de fluxos distribuídos entre múltiplos serviços.
        /// </summary>
        public string CorrelationId { get; init; }

        /// <summary>
        /// Data e hora exatas em que o evento foi gerado em UTC.
        /// </summary>
        public DateTimeOffset Timestamp { get; init; }

        /// <summary>
        /// Nome qualificado ou identificador padronizado do tipo de evento (ex: "orders.v1.created").
        /// </summary>
        public string EventType { get; init; }

        /// <summary>
        /// O conteúdo de dados (payload) do evento de negócio.
        /// </summary>
        public T Payload { get; init; }

        /// <summary>
        /// Dicionário imutável de cabeçalhos de metadados contextuais anexados à mensagem.
        /// </summary>
        public IReadOnlyDictionary<string, string> Headers { get; init; }

        /// <summary>
        /// Construtor parametrizado para inicialização do envelope de evento.
        /// </summary>
        /// <param name="eventId">ID exclusivo do evento.</param>
        /// <param name="correlationId">Identificador de correlação.</param>
        /// <param name="timestamp">Data/hora UTC do evento.</param>
        /// <param name="eventType">Tipo semântico do evento.</param>
        /// <param name="payload">Objeto de dados transportado.</param>
        /// <param name="headers">Cabeçalhos adicionais opcionais.</param>
        public EventMessage(
            Guid eventId,
            string correlationId,
            DateTimeOffset timestamp,
            string eventType,
            T payload,
            IDictionary<string, string>? headers = null)
        {
            EventId = eventId == Guid.Empty ? Guid.NewGuid() : eventId;
            CorrelationId = string.IsNullOrWhiteSpace(correlationId) ? Guid.NewGuid().ToString() : correlationId;
            Timestamp = timestamp == default ? DateTimeOffset.UtcNow : timestamp;
            EventType = string.IsNullOrWhiteSpace(eventType) ? typeof(T).Name : eventType;
            Payload = payload ?? throw new ArgumentNullException(nameof(payload));
            Headers = headers != null
                ? new ReadOnlyDictionary<string, string>(headers.ToDictionary(k => k.Key, v => v.Value))
                : EmptyHeaders;
        }

        /// <summary>
        /// Cria uma nova instância de envelope para o payload informado com valores padrão automáticos.
        /// </summary>
        /// <param name="payload">Objeto de dados do evento.</param>
        /// <param name="correlationId">Identificador de correlação opcional.</param>
        /// <param name="eventType">Nome do evento opcional (se nulo, usa o nome do tipo <typeparamref name="T"/>).</param>
        /// <param name="headers">Cabeçalhos contextuais adicionais opcionais.</param>
        /// <returns>Uma nova instância imutável de <see cref="EventMessage{T}"/>.</returns>
        public static EventMessage<T> Create(
            T payload,
            string? correlationId = null,
            string? eventType = null,
            IDictionary<string, string>? headers = null)
        {
            return new EventMessage<T>(
                eventId: Guid.NewGuid(),
                correlationId: correlationId ?? Guid.NewGuid().ToString(),
                timestamp: DateTimeOffset.UtcNow,
                eventType: eventType ?? typeof(T).Name,
                payload: payload,
                headers: headers);
        }
    }
}

