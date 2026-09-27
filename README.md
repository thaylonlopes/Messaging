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
| **`TL.RabbitMQ`** (`TL.Messaging.RabbitMQ`) | Adaptador RabbitMQ com topologia automática de Exchange/Queue/DLQ, Publisher Confirms, proteção contra poison messages, gestor de replay e publicação simplificada. |
| **`TL.Kafka`** (`TL.Messaging.Kafka`) | Adaptador Apache Kafka com controle de partição por chave declarativa, headers OpenTelemetry, commit manual, DLT, gestor de replay e publicação simplificada. |
| **`TL.Messaging.Benchmarks`** | Suíte de performance com BenchmarkDotNet cobrindo Nível 1 (Envelope/Ping), Nível 2 (OrderPlaced/[PartitionKey]/Tracing) e Nível 3 (Despacho em lote com Polly). |
| **`TL.Messaging.Showcase.Api`** | Vitrine técnica executável (Minimal API com Swagger) para publicação e consumo nos brokers. |
| **`TL.Messaging.RabbitMQ.Tests`** | Suíte de testes unitários do adaptador RabbitMQ (32 execuções em .NET 8 e .NET 9 — 100% aprovado). |
| **`TL.Messaging.Kafka.Tests`** | Suíte de testes unitários do adaptador Apache Kafka (38 execuções em .NET 8 e .NET 9 — 100% aprovado). |

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

## ⚖️ Matriz Técnica de Escolha: Quando usar RabbitMQ vs Apache Kafka

| Critério Arquitetural | RabbitMQ (`TL.RabbitMQ`) | Apache Kafka (`TL.Kafka`) | Recomendação TL |
| :--- | :--- | :--- | :--- |
| **Paradigma Principal** | **Message Broker Tradicional** (Smart Broker, Dumb Consumer). As mensagens são descartadas após confirmação (`ACK`). | **Distributed Streaming Commit Log** (Dumb Broker, Smart Consumer). As mensagens persistem no log indexado por offset. | Use RabbitMQ para comandos e tarefas; use Kafka para histórico e fluxos contínuos. |
| **Roteamento de Mensagens** | **Roteamento Dinâmico e Complexo** via Direct, Topic, Fanout e Headers Exchanges. | **Roteamento Estático** por Tópico e Particionamento por chave (`[PartitionKey]`). | Se a topologia exigir roteamento fino multicamadas ou fanout flexível, prefira RabbitMQ. |
| **Throughput & Vazão** | Moderado a Alto (dezenas de milhares de msgs/segundo com Publisher Confirms). | **Massivo / Ultra-Alto** (centenas de milhares a milhões de msgs/segundo via I/O sequencial em disco). | Para ingestão massiva de telemetria, clickstream ou métricas, escolha Kafka. |
| **Garantia de Ordenação** | Ordenação garantida por canal/fila única; concorrência com múltiplos consumidores quebra ordenação estrita. | **Ordenação Estrita por Chave** dentro de cada Partição, preservando sequência temporal do agregado. | Use Kafka com `[PartitionKey]` para fluxos que exigem preservação rigorosa da ordem (ex.: ledger contábil). |
| **Retenção e Replay** | As mensagens saem da fila no consumo regular. Replay operacional exige requeue a partir da DLQ. | **Retenção Configurável** por tempo/tamanho. Permite reprocessamento arbitrário voltando os offsets do consumer group. | Use Kafka se múltiplos sistemas independentes precisarem reler o histórico em momentos distintos. |
| **Padrão de Consumo** | Filas de trabalho (*Competing Consumers*), RPC, eventos transacionais de microsserviços. | Event Streaming, Event Sourcing, pipelines de dados e auditoria imutável. | Ambos são suportados de forma transparente via `TL.BaseContracts.Messaging`. |
| **Resiliência e DLQ/DLT** | Topologia automática com Exchange `.dlx` e fila `.dlq` dedicadas; replay nativo via `IRabbitMqDlqManager`. | Redirecionamento seguro para `.dlt` com headers de diagnóstico; replay nativo via `IKafkaDlqManager`. | Tratamento unificado de *poison messages* e proteção anti-loop em ambos os brokers. |

---

## 📊 Benchmarks de Performance (Release v0.6.0)

A suíte executável `TL.Messaging.Benchmarks` valida a latência em nanossegundos e a alocação de memória no heap (`GC Heap`) para todos os componentes do pipeline sob `.NET 8.0` e `.NET 9.0`:

- **Nível 1 (Envelope & PingEvent):** Criação de envelope imutável em **121 ns** (80 B/op) com serialização UTF-8 sem geração de strings intermediárias.
- **Nível 2 (OrderPlacedEvent & Tracing):** Extração de chave de partição declarativa `[PartitionKey]` em **56 ns** com **0 B de alocação** (cache estático $O(1)$) e injeção de cabeçalhos de rastreabilidade OpenTelemetry (`traceparent`).
- **Nível 3 (Despacho em Lote com Polly):** Comparativo empírico de processamento em lote (10 e 50 mensagens) com overhead inferior a **800 ns/item** para resiliência de retentativas.

Consulte o relatório completo de evidências em [docs/benchmarks/results.md](docs/benchmarks/results.md) e o racional arquitetural em [docs/ADR-001-arquitetura-tl-messaging-casos-de-borda-e-benchmarks.md](docs/ADR-001-arquitetura-tl-messaging-casos-de-borda-e-benchmarks.md).

---

## 📄 Licença

Este projeto está sob a licença [MIT](LICENSE).

