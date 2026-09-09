namespace TL.Messaging.Showcase.Api.Events;

/// <summary>
/// Evento de domínio que representa a criação de um pedido.
/// </summary>
public record OrderCreatedEvent(string OrderId, string CustomerId, decimal TotalAmount);
