# ADR-000: Arquitetura de Mensageria Resiliente e Governança

## 📋 1. Contexto e Motivação

O ecossistema corporativo de microsserviços orientados a eventos demanda comunicação assíncrona de alta disponibilidade, rastreabilidade ponta a ponta e tolerância a falhas transitórias de infraestrutura.

Historicamente, as implementações de mensageria assíncrona residiam dispersas no projeto utilitário `CommonHelpers` (`CommonHelpers.RabbitMQ` e `CommonHelpers.Kafka`), gerando:
1. **Acoplamento Indevido:** Serviços que necessitavam apenas de utilitários gerais ficavam expostos a dependências pesadas de mensageria (AMQP, Confluent.Kafka, Polly).
2. **Duplicação e Inconsistência de Contratos:** Interfaces divergiam entre serviços, dificultando o intercâmbio de brokers em ambientes corporativos heterogêneos.
3. **Débito Técnico de Runtime:** Presença de runtimes descontinuados (`.NET 6.0` EOL), violando diretrizes de modernização corporativa.
4. **Falta de Padronização no Envelope de Eventos:** Mensagens trafegavam sem correlação estruturada (`CorrelationId`), timestamps UTC ou compatibilidade com especificações modernas de mercado como CloudEvents.

A biblioteca dedicada **`TL.Messaging`** foi criada para isolar e governar a mensageria corporativa como um produto de engenharia independente de primeira classe.

---

## 🎯 2. Decisões Arquiteturais e Invariantes

### 2.1. Arquitetura Ports & Adapters (Hexagonal Architecture)
A solução adota estritamente a segregação de responsabilidades entre contratos de negócio e implementações físicas:
- **Porta Base Fundacional ([`TL.BaseContracts.Messaging`](https://www.nuget.org/packages/TL.BaseContracts/0.3.1)):** Contratos agnósticos universais de produção e consumo (`IEventProducer`, `IEventHandler<T>`), interfaces de eventos (`IEvent`, `IIntegrationEvent`), envelope padronizado `EventMessage<T>` compatível com CloudEvents v1.0, anotações declarativas (`[PartitionKey]`, `[Topic]`, `[MessageId]`), extrator de metadados $O(1)$ (`EventMetadataExtractor`), metadados contextuais (`EventMetadata`) e envelope funcional (`Result`, `Error`). **100% BCL pura, sem dependências externas**, provido diretamente pelo pacote corporativo base do ecossistema.
- **Adapters de Infraestrutura (`TL.Messaging.RabbitMQ`, `TL.Messaging.Kafka`):** Especializam a comunicação de rede com os respectivos brokers, gerenciando conexões, serialização, resiliência, confirmações de entrega e criação automática de topologias contingenciais, implementando diretamente as interfaces de `TL.BaseContracts.Messaging`.

### 2.2. Multi-Targeting Moderno (.NET 8 e .NET 9)
Todos os projetos da solução suportam compilação multi-target para as versões ativas do ecossistema .NET:
- `<TargetFrameworks>net8.0;net9.0</TargetFrameworks>`
- Eliminação definitiva de runtimes descontinuados (`.NET 6.0`).

### 2.3. Governança via Central Package Management (CPM)
- Centralização de todas as versões de pacotes corporativos no arquivo raiz `Directory.Packages.props` (`TL.BaseContracts 0.3.1`, `RabbitMQ.Client 6.8.1`, `Confluent.Kafka 2.5.3`, `Polly 8.4.1`, `Swashbuckle.AspNetCore 6.5.0`, etc.).
- Compilação com qualidade estrita habilitada em `Directory.Build.props`:
  - `<Nullable>enable</Nullable>`
  - `<TreatWarningsAsErrors>true</TreatWarningsAsErrors>`
  - `<GenerateDocumentationFile>true</GenerateDocumentationFile>` (com exceção dos projetos de teste e showcase).

### 2.4. Resiliência por Padrão (Resilience by Default)
- **RabbitMQ:** Publisher Confirms ativados por padrão (`ConfirmSelect()`), declaração idempotente de topologia (Exchange principal, Fila principal, Dead-Letter Exchange `.dlx` e Dead-Letter Queue `.dlq`), retentativas inteligentes com Polly (backoff exponencial).
- **Apache Kafka:** Publicação com garantia `Acks.All` e `EnableIdempotence = true`, suporte a partição estrita por chave (`EventMetadata.WithKafkaPartitionKey` ou anotação declarativa `[PartitionKey]`), commit manual de offsets e redirecionamento seguro para Dead-Letter Topic (`.dlt`).

### 2.5. Vitrine Técnica Executável (Showcase API)
- Projeto executável Minimal API com documentação OpenAPI/Swagger em `examples/TL.Messaging.Showcase.Api`, demonstrando a publicação e consumo integrados para ambos os brokers.

---

## ⚖️ 3. Consequências e Trade-offs

### ✅ Vantagens:
- **Desacoplamento Absoluto:** Domínio e aplicação dependem apenas de `TL.BaseContracts`. Trocar de RabbitMQ para Kafka torna-se uma mera alteração de registro de injeção de dependência (`AddRabbitMqMessaging` vs `AddKafkaMessaging`).
- **Simplicidade & Zero Duplicação:** Inexistência de camadas intermediárias redundantes. O ecossistema compartilha uma única assinatura para `Result`, `Error`, `EventMessage` e `IEventProducer`.
- **Previsibilidade Operacional:** Zero filas travadas por *poison messages* graças ao tratamento nativo de DLQ e DLT.
- **Rastreabilidade Distribuída:** Propagação de `CorrelationId` e headers garante observabilidade ponta a ponta em ferramentas de APM e OpenTelemetry.
- **Qualidade Rigorosa:** Compilação com zero warnings e 100% dos testes unitários verdes em .NET 8 e .NET 9.

### ⚠️ Desvantagens / Trade-offs:
- Características ultraespecíficas dos brokers precisam ser informadas via `EventMetadata` ou acessadas através das interfaces especializadas (`IRabbitMqProducer`, `IKafkaProducer`).

---

## 🗺️ 4. Mapa de ADRs do Repositório

| ADR | Projeto | Foco Arquitetural |
| :--- | :--- | :--- |
| **`ADR-000`** | `TL.Messaging` (Geral) | Visão Geral, Ports & Adapters com `TL.BaseContracts`, Governança CPM e Resiliência. |
| [**`ADR-001`**](ADR-001-tl-rabbitmq.md) | `TL.Messaging.RabbitMQ` | Adaptador AMQP, Publisher Confirms, Topologia Automática, Publicação em 1 Linha e DLQ. |
| [**`ADR-002`**](ADR-002-tl-kafka.md) | `TL.Messaging.Kafka` | Adaptador Kafka, `[PartitionKey]`, Idempotência, Publicação em 1 Linha e Dead-Letter Topic. |

