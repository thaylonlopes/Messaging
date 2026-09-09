using Microsoft.Extensions.Logging;
using TL.Messaging.Abstractions;

namespace TL.Messaging.Showcase.Api.Events;

/// <summary>
/// Manipulador de evento para mensagens recebidas via Apache Kafka.
/// </summary>
public class OrderCreatedKafkaHandler : IEventHandler<OrderCreatedEvent>
{
    private readonly ILogger<OrderCreatedKafkaHandler> _logger;

    public OrderCreatedKafkaHandler(ILogger<OrderCreatedKafkaHandler> logger)
    {
        _logger = logger;
    }

    public Task<Result> HandleAsync(EventMessage<OrderCreatedEvent> eventMessage, CancellationToken cancellationToken)
    {
        _logger.LogInformation(
            "[Kafka] Processando pedido: {OrderId} | Cliente: {CustomerId} | Valor: {TotalAmount:C} | CorrelationId: {CorrelationId}",
            eventMessage.Payload.OrderId,
            eventMessage.Payload.CustomerId,
            eventMessage.Payload.TotalAmount,
            eventMessage.CorrelationId);

        return Task.FromResult(Result.Success());
    }
}
