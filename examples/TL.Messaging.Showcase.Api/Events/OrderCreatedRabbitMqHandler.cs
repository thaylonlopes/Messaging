using Microsoft.Extensions.Logging;
using TL.Messaging.Abstractions;

namespace TL.Messaging.Showcase.Api.Events;

/// <summary>
/// Manipulador de evento para mensagens recebidas via RabbitMQ.
/// </summary>
public class OrderCreatedRabbitMqHandler : IEventHandler<OrderCreatedEvent>
{
    private readonly ILogger<OrderCreatedRabbitMqHandler> _logger;

    public OrderCreatedRabbitMqHandler(ILogger<OrderCreatedRabbitMqHandler> logger)
    {
        _logger = logger;
    }

    public Task<Result> HandleAsync(EventMessage<OrderCreatedEvent> eventMessage, CancellationToken cancellationToken)
    {
        _logger.LogInformation(
            "[RabbitMQ] Processando pedido: {OrderId} | Cliente: {CustomerId} | Valor: {TotalAmount:C} | CorrelationId: {CorrelationId}",
            eventMessage.Payload.OrderId,
            eventMessage.Payload.CustomerId,
            eventMessage.Payload.TotalAmount,
            eventMessage.CorrelationId);

        return Task.FromResult(Result.Success());
    }
}
