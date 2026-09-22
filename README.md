# TL.Messaging

[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)
[![.NET](https://img.shields.io/badge/.NET-8.0%20%7C%209.0-purple.svg)](https://dotnet.microsoft.com/)

Biblioteca corporativa de mensageria assíncrona resiliente para ecossistemas de microsserviços orientados a eventos (Event-Driven Architecture), com suporte unificado a **RabbitMQ** e **Apache Kafka**.

---

## 🌟 Visão Geral

`TL.Messaging` implementa o padrão **Ports & Adapters (Hexagonal Architecture)** para abstrair completamente os provedores de mensageria da camada de aplicação e domínio. Ela fornece:

- **Contratos Agnósticos:** Interfaces `IEventProducer` e `IEventHandler<T>` desacopladas de qualquer broker.
- **Envelope Padronizado:** `EventMessage<T>` compatível com a especificação CloudEvents (ID único, CorrelationId, Timestamp UTC, EventType e cabeçalhos contextuais).
- **Resiliência Integrada:** Políticas de retentativa com Polly (exponential backoff), Dead-Letter Queue / Topic (.dlq/.dlt) automáticos e confirmações de entrega (Publisher Confirms no RabbitMQ e Acks.All/Idempotence no Kafka).
- **Multi-Target:** Suporte nativo e otimizado para `.NET 8.0` (LTS) e `.NET 9.0` (STS).
- **Governança de Pacotes:** Central Package Management (CPM) via `Directory.Packages.props`.

---

## 📦 Estrutura de Projetos

| Projeto / Pacote | Descrição |
| :--- | :--- |
| **`TL.BaseContracts`** | Fundação corporativa de contratos em BCL pura (`IEventProducer`, `IEventHandler<T>`, `EventMessage<T>`, `EventMetadata`, anotações `[PartitionKey]`, `[Topic]`, `[MessageId]`, `EventMetadataExtractor` $O(1)$ e `Result`). |
| **`TL.RabbitMQ`** (`TL.Messaging.RabbitMQ`) | Adaptador AMQP 0-9-1 com topologia automática de Exchange/Queue/DLQ, Publisher Confirms e publicação em 1 linha. |
| **`TL.Kafka`** (`TL.Messaging.Kafka`) | Adaptador Apache Kafka com controle de partição por chave declarativa, headers de telemetria, commit manual, DLT e publicação em 1 linha. |
| **`TL.Messaging.Showcase.Api`** | Vitrine técnica executável (Minimal API com Swagger) para publicação e consumo nos brokers. |
| **`TL.Messaging.RabbitMQ.Tests`** | Suíte de testes unitários do adaptador RabbitMQ (20 testes em .NET 8 e .NET 9). |
| **`TL.Messaging.Kafka.Tests`** | Suíte de testes unitários do adaptador Apache Kafka (24 testes em .NET 8 e .NET 9). |

---

## 🚀 Como Usar

### 1. Definindo um Evento e Manipulador

```csharp
using TL.BaseContracts;
using TL.BaseContracts.Messaging;

public record OrderCreatedEvent(string OrderId, decimal TotalAmount);

public class OrderCreatedHandler : IEventHandler<OrderCreatedEvent>
{
    public Task<Result> HandleAsync(EventMessage<OrderCreatedEvent> eventMessage, CancellationToken cancellationToken)
    {
        return Task.FromResult(Result.Success());
    }
}
```

---

### 2. Configurando RabbitMQ

```csharp
using TL.Messaging.RabbitMQ.Extensions;

builder.Services.AddRabbitMqMessaging(builder.Configuration);
builder.Services.AddRabbitMqConsumer<OrderCreatedEvent, OrderCreatedHandler>(
    queueName: "orders.created.queue",
    routingKey: "orders.created");
```

---

### 3. Configurando Apache Kafka

```csharp
using TL.Messaging.Kafka.Extensions;

builder.Services.AddKafkaMessaging(builder.Configuration);
builder.Services.AddKafkaConsumer<OrderCreatedEvent, OrderCreatedHandler>(
    topic: "orders.created");
```

---

## 📄 Licença

Este projeto está sob a licença [MIT](LICENSE).

