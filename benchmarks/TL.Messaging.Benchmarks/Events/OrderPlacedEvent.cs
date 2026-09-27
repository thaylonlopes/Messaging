using TL.BaseContracts.Messaging.Attributes;

namespace TL.Messaging.Benchmarks.Events;

/// <summary>
/// Evento de domínio representativo de um pedido com anotação declarativa de chave de partição para Apache Kafka.
/// </summary>
public record OrderPlacedEvent(
    string OrderId,
    [property: PartitionKey] string CustomerId,
    decimal TotalAmount,
    string Currency,
    int ItemCount);
