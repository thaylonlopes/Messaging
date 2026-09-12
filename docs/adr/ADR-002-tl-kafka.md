# ADR-002: Decisões Arquiteturais do Pacote TL.Messaging.Kafka

## 📋 1. Contexto e Motivação

O Apache Kafka é a plataforma padrão da indústria para streaming de eventos de altíssimo volume e ordenação estrita garantida por partição. No entanto, o SDK oficial `Confluent.Kafka` exige tratamento minucioso de configurações (idempotência, ACKs, serialização manual, injeção de headers de telemetria, gerenciamento de offsets e fallback para Dead Letter Topics - DLT).

Sem abstração, microsserviços acabavam implementando consumidores com comportamentos divergentes (como auto-commit prematuro que causa perda de mensagens em caso de crash do pod ou reinicialização de contêiner).

No repositório `TL.Messaging`, o pacote **`TL.Messaging.Kafka`** implementa o adaptador de infraestrutura Kafka conectado diretamente aos contratos fundamentais de [`TL.BaseContracts.Messaging`](https://www.nuget.org/packages/TL.BaseContracts/0.2.0).

---

## 🎯 2. Decisões Arquiteturais

### 2.1. Implementação da Porta Agnóstica `IEventProducer`
- O `KafkaProducer` implementa `IEventProducer` e a interface especializada `IKafkaProducer`.
- **Idempotência Nativa**: Configura `EnableIdempotence = true` e `Acks = Acks.All` por padrão, garantindo que retentativas de rede no broker não dupliquem eventos na partição.
- **Particionamento por Chave**: Suporta `EventMetadata.WithKafkaPartitionKey(key)`, garantindo que mensagens relacionadas (ex: do mesmo cliente ou pedido) sejam roteadas para a mesma partição com ordenação estrita.
- **Injeção Automática de Headers de Tracing**: Propaga `correlation-id`, `event-type` e `event-id` nos headers do Kafka para rastreamento no OpenTelemetry.

### 2.2. Consumidor Resiliente (`KafkaConsumer<TEvent, THandler>`)
- Implementado como `BackgroundService` gerenciado pelo host .NET.
- **Commit Manual de Offsets (`EnableAutoCommit = false`)**: O offset só é commitado no Kafka após a execução bem-sucedida do `IEventHandler<T>` ou após o desvio seguro para o Dead Letter Topic.
- **Dead Letter Topic (DLT)**: Quando um evento falha após as retentativas do Polly, ele é publicado no tópico `.dlt` antes de efetuar o commit do offset no tópico principal, impedindo o travamento da partição sem perder dados.

### 2.3. Configuração Fluente no DI
- `services.AddKafkaMessaging(config)`
- `services.AddKafkaConsumer<OrderCreatedEvent, OrderCreatedHandler>("events.orders.v1")`

---

## ⚖️ 3. Consequências e Trade-offs

### ✅ Vantagens:
- **Alta Confiabilidade e Vazão:** Sem risco de perda de mensagens ou duplicidade acidental no broker.
- **Transparência de Troca:** Facilidade de migração entre Kafka e RabbitMQ apenas alterando o registro no `Program.cs`.
- **Multi-Target Moderno:** Compilado e validado em `.NET 8.0` e `.NET 9.0`.

### ⚠️ Desvantagens / Trade-offs:
- A dependência de drivers nativos compilados em C/C++ (`librdkafka` empacotado no NuGet `Confluent.Kafka`) exige mais recursos de compilação e teste do que drivers AMQP puros.

---

## 🧪 4. Status de Verificação
- Coberto por **10 testes unitários automatizados** no `TL.Messaging.Kafka.Tests` (5 em .NET 8 e 5 em .NET 9 - 100% passing).
