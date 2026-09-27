using System;
using System.Collections.Generic;
using System.Text.Json;
using System.Text.Json.Serialization;
using TL.BaseContracts.Messaging;

namespace TL.Messaging.RabbitMQ.Serialization
{
    /// <summary>
    /// Factory de conversores JSON para suporte à desserialização do envelope imutável <see cref="EventMessage{T}"/>.
    /// </summary>
    public class EventMessageJsonConverterFactory : JsonConverterFactory
    {
        /// <summary>
        /// Determina se o tipo especificado pode ser convertido por esta factory (tipos genéricos fechados de <see cref="EventMessage{T}"/>).
        /// </summary>
        /// <param name="typeToConvert">Tipo a ser verificado para conversão.</param>
        /// <returns>True se o tipo for compatível com <see cref="EventMessage{T}"/>, caso contrário false.</returns>
        public override bool CanConvert(Type typeToConvert)
        {
            if (!typeToConvert.IsGenericType)
            {
                return false;
            }

            return typeToConvert.GetGenericTypeDefinition() == typeof(EventMessage<>);
        }

        /// <summary>
        /// Cria uma instância especializada de <see cref="JsonConverter"/> para o tipo de envelope solicitado.
        /// </summary>
        /// <param name="typeToConvert">Tipo do envelope de evento a ser convertido.</param>
        /// <param name="options">Opções de serialização JSON configuradas.</param>
        /// <returns>Instância do conversor JSON especializado.</returns>
        public override JsonConverter CreateConverter(Type typeToConvert, JsonSerializerOptions options)
        {
            Type payloadType = typeToConvert.GetGenericArguments()[0];
            Type converterType = typeof(EventMessageJsonConverter<>).MakeGenericType(payloadType);
            return (JsonConverter)Activator.CreateInstance(converterType)!;
        }

        private class EventMessageJsonConverter<T> : JsonConverter<EventMessage<T>>
        {
            private class EventMessageDto
            {
                public Guid EventId { get; set; }
                public string? CorrelationId { get; set; }
                public DateTimeOffset Timestamp { get; set; }
                public string? EventType { get; set; }
                public T? Payload { get; set; }
                public Dictionary<string, string>? Headers { get; set; }
            }

            public override EventMessage<T>? Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
            {
                var dto = JsonSerializer.Deserialize<EventMessageDto>(ref reader, options);
                if (dto == null || dto.Payload == null)
                {
                    return null;
                }

                return new EventMessage<T>(
                    dto.EventId,
                    dto.CorrelationId ?? Guid.NewGuid().ToString(),
                    dto.Timestamp,
                    dto.EventType ?? typeof(T).Name,
                    dto.Payload,
                    dto.Headers);
            }

            public override void Write(Utf8JsonWriter writer, EventMessage<T> value, JsonSerializerOptions options)
            {
                writer.WriteStartObject();
                writer.WriteString("eventId", value.EventId);
                writer.WriteString("correlationId", value.CorrelationId);
                writer.WriteString("timestamp", value.Timestamp);
                writer.WriteString("eventType", value.EventType);

                writer.WritePropertyName("payload");
                JsonSerializer.Serialize(writer, value.Payload, options);

                if (value.Headers != null)
                {
                    writer.WritePropertyName("headers");
                    JsonSerializer.Serialize(writer, value.Headers, options);
                }

                writer.WriteEndObject();
            }
        }
    }
}
