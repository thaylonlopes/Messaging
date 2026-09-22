using TL.BaseContracts.Messaging.Attributes;

namespace TL.Messaging.Showcase.Api.Events;

/// <summary>
/// Evento de domínio que representa a criação de um pedido com chave de partição declarativa por cliente.
/// </summary>
public record OrderCreatedEvent(
    string OrderId,
    [property: PartitionKey] string CustomerId,
    decimal TotalAmount);
