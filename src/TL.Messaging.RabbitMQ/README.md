# 🐰 TL.Messaging.RabbitMQ

> Adaptador resiliente de mensageria para **RabbitMQ** implementando a abstração unificada **`IEventProducer`** e **`IEventHandler<T>`**, com suporte automático a **Publisher Confirms**, **Dead-Letter Queue (`.dlq`)**, retentativas com **Polly** e injeção de dependência simplificada para .NET 8 e .NET 9.

[![.NET 8.0](https://img.shields.io/badge/.NET-8.0%20(LTS)-blue.svg)](https://dotnet.microsoft.com/)
[![.NET 9.0](https://img.shields.io/badge/.NET-9.0-blue.svg)](https://dotnet.microsoft.com/)
[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)

---

## 📦 Instalação

Adicione o pacote ao seu projeto via .NET CLI:

```bash
dotnet add package TL.Messaging.RabbitMQ --version 0.1.0
```

---

## 🚀 Como Usar

### 1. Configuração no `Program.cs`

```csharp
using TL.Messaging.RabbitMQ.Extensions;

var builder = WebApplication.CreateBuilder(args);

// Registra a infraestrutura do RabbitMQ e o IEventProducer agnóstico
builder.Services.AddRabbitMqMessaging(builder.Configuration);

// Registra o consumidor de eventos para um handler específico
builder.Services.AddRabbitMqConsumer<OrderCreatedEvent, OrderCreatedHandler>(
    queueName: "app.orders.created", 
    routingKey: "orders.created");

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

        // Publica sem nenhum acoplamento com o RabbitMQ!
        await _producer.PublishAsync(orderEvent, cancellationToken: ct);
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

        // Retornar Result.Success() envia ACK para o RabbitMQ
        // Retornar Result.Failure(...) direciona para a Dead-Letter Queue (.dlq)
        return Result.Success();
    }
}
```

---

### 4. Configuração no `appsettings.json`

```json
{
  "RabbitMq": {
    "HostName": "localhost",
    "Port": 5672,
    "UserName": "guest",
    "Password": "guest",
    "VirtualHost": "/",
    "ExchangeName": "amq.topic",
    "ExchangeType": "topic",
    "RetryCount": 3,
    "PrefetchCount": 10
  }
}
```

---

## 🏛️ Topologia Automática de Filas e DLQ

Ao registrar um consumidor com `AddRabbitMqConsumer`:
1. A exchange principal (`ExchangeName`) é declarada automaticamente.
2. A exchange de Dead-Letter (`ExchangeName.dlx`) e a fila `.dlq` são declaradas e vinculadas.
3. A fila principal é criada com argumentos `x-dead-letter-exchange` apontando para a `.dlx`. Em caso de erro fatal ou retentativas esgotadas, a mensagem vai automaticamente para a `.dlq`.
