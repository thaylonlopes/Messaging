# 🚀 TL.Messaging.Showcase.Api

> Aplicação executável e vitrine técnica demonstrando a utilização prática da suíte **TL.Messaging** (`TL.Messaging.Abstractions`, `TL.Messaging.RabbitMQ`, `TL.Messaging.Kafka`) em ASP.NET Core Minimal APIs (.NET 8 e .NET 9).

---

## 📋 Recursos Demonstrados

| Broker | Endpoint | Descrição |
| :--- | :--- | :--- |
| **RabbitMQ** | `POST /api/messaging/rabbitmq/orders` | Publicação de pedido com Publisher Confirms, cabeçalhos de tracing e consumo via `OrderCreatedRabbitMqHandler`. |
| **Apache Kafka** | `POST /api/messaging/kafka/orders` | Publicação com chave de partição (`CustomerId`), idempotência e consumo via `OrderCreatedKafkaHandler`. |
| **Diagnóstico** | `GET /api/messaging/diagnostics` | Metadados dos adaptadores ativos e status da suíte. |

---

## 🏃 Como Executar

### 1. Iniciar os Brokers (Docker)

```bash
# RabbitMQ com Management UI (porta 5672 e 15672)
docker run -d --name rabbitmq -p 5672:5672 -p 15672:15672 rabbitmq:3-management

# Kafka com KRaft (porta 9092)
docker run -d --name kafka -p 9092:9092 apache/kafka:latest
```

### 2. Rodar a API

```bash
dotnet run --project examples/TL.Messaging.Showcase.Api
```

Acesse o Swagger UI na raiz da aplicação: `http://localhost:5000/` (ou a porta informada no console).
