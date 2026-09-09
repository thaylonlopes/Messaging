using Microsoft.AspNetCore.Mvc;
using TL.Messaging.Abstractions;
using TL.Messaging.Kafka.Extensions;
using TL.Messaging.Kafka.Producer;
using TL.Messaging.RabbitMQ.Extensions;
using TL.Messaging.RabbitMQ.Producer;
using TL.Messaging.Showcase.Api.Events;

var builder = WebApplication.CreateBuilder(args);

ConfigureServices(builder.Services, builder.Configuration);

var app = builder.Build();

ConfigurePipeline(app);
MapEndpoints(app);

app.Run();

static void ConfigureServices(IServiceCollection services, IConfiguration configuration)
{
    services.AddEndpointsApiExplorer();
    services.AddSwaggerGen(options =>
    {
        options.SwaggerDoc("v1", new()
        {
            Title = "TL.Messaging Showcase API",
            Version = "v1",
            Description = "Vitrine técnica demonstrando a integração com RabbitMQ e Apache Kafka via TL.Messaging."
        });
    });

    services.AddRabbitMqMessaging(configuration);
    services.AddKafkaMessaging(configuration);

    services.AddRabbitMqConsumer<OrderCreatedEvent, OrderCreatedRabbitMqHandler>(
        queueName: "showcase.orders.created",
        routingKey: "orders.created");

    services.AddKafkaConsumer<OrderCreatedEvent, OrderCreatedKafkaHandler>(
        topic: "events.orders.v1");
}

static void ConfigurePipeline(WebApplication app)
{
    app.UseSwagger();
    app.UseSwaggerUI(options =>
    {
        options.SwaggerEndpoint("/swagger/v1/swagger.json", "TL.Messaging Showcase v1");
        options.RoutePrefix = string.Empty;
    });
}

static void MapEndpoints(WebApplication app)
{
    var group = app.MapGroup("/api/messaging")
        .WithTags("Messaging");

    group.MapPost("/rabbitmq/orders", async (
        [FromBody] OrderCreatedEvent order,
        [FromServices] IRabbitMqProducer producer,
        CancellationToken cancellationToken) =>
    {
        var metadata = new EventMetadata()
            .WithHeader("origin", "Showcase.Api.RabbitMQ")
            .WithHeader("environment", "Development");

        var result = await producer.PublishDirectAsync(
            exchange: "amq.topic",
            routingKey: "orders.created",
            message: order,
            metadata: metadata,
            cancellationToken: cancellationToken);

        return result.IsSuccess
            ? Results.Ok(new { Message = "Evento publicado no RabbitMQ com sucesso", Order = order })
            : Results.BadRequest(new { Error = result.Error });
    })
    .WithName("PublishRabbitMqOrder")
    .WithSummary("Publica evento de pedido no RabbitMQ com Publisher Confirms.");

    group.MapPost("/kafka/orders", async (
        [FromBody] OrderCreatedEvent order,
        [FromServices] IKafkaProducer producer,
        CancellationToken cancellationToken) =>
    {
        var metadata = new EventMetadata()
            .WithHeader("origin", "Showcase.Api.Kafka")
            .WithHeader("environment", "Development");

        var result = await producer.ProduceToTopicAsync(
            topic: "events.orders.v1",
            partitionKey: order.CustomerId,
            message: order,
            metadata: metadata,
            cancellationToken: cancellationToken);

        return result.IsSuccess
            ? Results.Ok(new { Message = "Evento publicado no Kafka com sucesso", Order = order, PartitionKey = order.CustomerId })
            : Results.BadRequest(new { Error = result.Error });
    })
    .WithName("PublishKafkaOrder")
    .WithSummary("Publica evento de pedido no Apache Kafka com chave de partição.");

    group.MapGet("/diagnostics", () =>
    {
        return Results.Ok(new
        {
            Suite = "TL.Messaging",
            Status = "Healthy",
            SupportedBrokers = new[] { "RabbitMQ (AMQP 0-9-1)", "Apache Kafka (TCP)" },
            Runtimes = new[] { ".NET 8.0", ".NET 9.0" },
            Architecture = "Ports & Adapters (Hexagonal)"
        });
    })
    .WithName("GetDiagnostics")
    .WithSummary("Retorna o diagnóstico dos adaptadores de mensageria.");
}
