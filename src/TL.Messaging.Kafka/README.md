# 🦅 TL.Messaging.Kafka

> Adaptador resiliente de mensageria para **Apache Kafka** implementando a abstração unificada **`IEventProducer`** e **`IEventHandler<T>`**, com suporte nativo a **Partition Keys**, **Dead Letter Topic (`.dlt`)**, **Idempotência**, injeção de headers de telemetria e injeção de dependência simplificada para .NET 8 e .NET 9.

[![.NET 8.0](https://img.shields.io/badge/.NET-8.0%20(LTS)-blue.svg)](https://dotnet.microsoft.com/)
[![.NET 9.0](https://img.shields.io/badge/.NET-9.0-blue.svg)](https://dotnet.microsoft.com/)
[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)

---

## 📦 Instalação

Adicione o pacote ao seu projeto via .NET CLI:

```bash
dotnet add package TL.Messaging.Kafka --version 0.1.0
```

---

## 🚀 Como Usar

### 1. Configuração no `Program.cs`

```csharp
using TL.Messaging.Kafka.Extensions;

var builder = WebApplication.CreateBuilder(args);

// Registra a infraestrutura do Kafka e o IEventProducer agnóstico
builder.Services.AddKafkaMessaging(builder.Configuration);

// Registra o consumidor de eventos para um handler específico
builder.Services.AddKafkaConsumer<OrderCreatedEvent, OrderCreatedHandler>(topic: "events.orders.v1");

var app = builder.Build();
app.Run();
```

---

### 2. Publicando Eventos de Forma Agnóstica (`IEventProducer`)

Sua camada de aplicação ou domínio depende apenas da interface agnóstica `IEventProducer`:

```csharp
using TL.BaseContracts.Messaging;

public class OrderService
{
    private readonly IEventProducer _producer;

    public OrderService(IEventProducer producer)
    {
        _producer = producer;
    }

    public async Task CreateOrderAsync(OrderDto order, CancellationToken ct)
    {
        var orderEvent = new OrderCreatedEvent(order.Id, order.Total);

        // Opcional: Garante partição estrita no Kafka usando PartitionKey
        var metadata = new EventMetadata().WithKafkaPartitionKey(order.CustomerId);

        // Publica de forma transparente!
        await _producer.PublishAsync(orderEvent, metadata, ct);
    }
}
```

---

### 3. Consumindo Eventos (`IEventHandler<T>`)

```csharp
using TL.BaseContracts;
using TL.BaseContracts.Messaging;

public class OrderCreatedHandler : IEventHandler<OrderCreatedEvent>
{
    public async Task<Result> HandleAsync(EventMessage<OrderCreatedEvent> message, CancellationToken ct)
    {
        Console.WriteLine($"Processando pedido {message.Payload.OrderId}, CorrelationId: {message.CorrelationId}");

        // Retornar Result.Success() executa o commit manual de offset no Kafka
        // Retornar Result.Failure(...) encaminha a mensagem para o Dead Letter Topic (DLT)
        return Result.Success();
    }
}
```

---

### 4. Configuração no `appsettings.json`

```json
{
  "Kafka": {
    "BootstrapServers": "localhost:9092",
    "GroupId": "orders-consumer-group",
    "DefaultTopic": "events.orders.v1",
    "AutoOffsetReset": "Earliest",
    "EnableIdempotence": true,
    "RetryCount": 3
  }
}
```

---

## 🏛️ Recursos de Resiliência
- **Idempotência Nativa**: Evita duplicação de mensagens no cluster Kafka.
- **Headers de Tracing**: Propaga `correlation-id`, `event-type` e `event-id` automaticamente.
- **Dead Letter Topic (DLT)**: Mensagens que falham após as retentativas configuradas são publicadas automaticamente no tópico `.dlt` antes de efetuar o commit do offset, evitando travamento de partição.
