# ADR-002: Decisões Arquiteturais do Pacote TL.Kafka (TL.Messaging.Kafka)

## 📋 1. Contexto e Motivação

O Apache Kafka é a plataforma padrão da indústria para streaming de eventos de altíssimo volume e ordenação estrita garantida por partição. No entanto, o SDK oficial `Confluent.Kafka` exige tratamento minucioso de configurações (idempotência, ACKs, serialização manual, injeção de headers de telemetria, gerenciamento de offsets e fallback para Dead Letter Topics - DLT).

Sem abstração, microsserviços acabavam implementando consumidores com comportamentos divergentes (como auto-commit prematuro que causa perda de mensagens em caso de crash do pod ou reinicialização de contêiner).

No repositório `TL.Messaging`, o pacote publicado como **`TL.Kafka`** (projeto `TL.Messaging.Kafka`) implementa o adaptador de infraestrutura Kafka conectado diretamente aos contratos fundamentais de [`TL.BaseContracts.Messaging`](https://www.nuget.org/packages/TL.BaseContracts/0.3.1).

---

## 🎯 2. Decisões Arquiteturais

### 2.1. Implementação da Porta Agnóstica `IEventProducer`
- O `KafkaProducer` implementa `IEventProducer` e a interface especializada `IKafkaProducer`.
- **Idempotência Nativa**: Configura `EnableIdempotence = true` e `Acks = Acks.All` por padrão, garantindo que retentativas de rede no broker não dupliquem eventos na partição.
- **Particionamento por Chave**: Suporta `EventMetadata.WithKafkaPartitionKey(key)` ou anotação declarativa `[PartitionKey]`, garantindo que mensagens relacionadas (ex: do mesmo cliente ou pedido) sejam roteadas para a mesma partição com ordenação estrita.
- **Injeção Automática de Headers de Tracing**: Propaga `correlation-id`, `event-type` e `event-id` nos headers do Kafka para rastreamento no OpenTelemetry.

### 2.2. Consumidor Resiliente (`KafkaConsumer<TEvent, THandler>`)
- Implementado como `BackgroundService` gerenciado pelo host .NET.
- **Commit Manual de Offsets (`EnableAutoCommit = false`)**: O offset só é commitado no Kafka após a execução bem-sucedida do `IEventHandler<T>` ou após o desvio seguro para o Dead Letter Topic.
- **Dead Letter Topic (DLT)**: Quando um evento falha após as retentativas do pipeline Polly v8 com jitter decorrelacionado, ele é publicado no tópico `.dlt` antes de efetuar o commit do offset no tópico principal, impedindo o travamento da partição sem perder dados.

### 2.3. Configuração Fluente no DI
- `services.AddKafkaMessaging(config)`
- `services.AddKafkaConsumer<OrderCreatedEvent, OrderCreatedHandler>("events.orders.v1")`

### 2.4. Publicação Simplificada e Atributo `[PartitionKey]`
- Implementação das sobrecargas `PublishAsync(T message)` e `PublishAsync(string topic, T message)`.
- Extração de chave de partição declarativa via `[PartitionKey]` e `EventMetadataExtractor` de `TL.BaseContracts` com cache $O(1)$.
- Fallback seguro para chave nula (`Message.Key = null`), delegando ao particionador padrão (round-robin / sticky) do Apache Kafka quando o evento não é decorado com `[PartitionKey]`.
- Inferência automática do tópico via `EventMetadataExtractor.GetTopicName<T>()`.
- Disponibilização da classe derivada especializada `KafkaEventProducer`.

### 2.5. Release v0.4.0 — DLT com Metadados de Diagnóstico e Replay Operacional
- **Metadados de Diagnóstico em Headers Kafka:** Mensagens com esgotamento de retentativas ou payload corrompido (poison messages) recebem cabeçalhos de diagnóstico padronizados (`x-exception-message`, `x-exception-type`, `x-retry-count`, `x-failed-at-utc`, `x-dlt-original-topic`, `x-dlt-reason`, `traceparent`).
- **Proteção Anti-Poison Loop:** Interceptação imediata de falhas de desserialização no nível do envelope, publicando o conteúdo bruto no tópico DLT (`.dlt`) e efetuando commit imediato do offset no tópico principal para prevenir loops infinitos e travamento de partições.
- **Replay Operacional (`IKafkaDlqManager`):** Utilitário de infraestrutura nativo `dlqManager.ReplayAsync("topico.dlt", maxMessages: 50)` que consome o DLT com isolamento, expurga os cabeçalhos de diagnóstico e republica no tópico principal com ordenação de partição preservada.
- **Deserialização Desacoplada com `EventMessageJsonConverterFactory`:** Suporte transparente do System.Text.Json para o envelope imutável `EventMessage<T>`.

---

## ⚖️ 3. Consequências e Trade-offs

### ✅ Vantagens:
- **Alta Confiabilidade e Vazão:** Sem risco de perda de mensagens ou duplicidade acidental no broker.
- **Transparência de Troca:** Facilidade de migração entre Kafka e RabbitMQ apenas alterando o registro no `Program.cs`.
- **Multi-Target Moderno:** Compilado e validado em `.NET 8.0` e `.NET 9.0`.
- **Ergonomia e Garantia de FIFO:** Elimina erros humanos na passagem de chaves de partição através do atributo declarativo `[PartitionKey]`.
- **Replay Operacional:** Capacidade operacional nativa de reprocessar DLTs sem scripts manuais.

### ⚠️ Desvantagens / Trade-offs:
- A dependência de drivers nativos compilados em C/C++ (`librdkafka` empacotado no NuGet `Confluent.Kafka`) exige mais recursos de compilação e teste do que drivers AMQP puros.

---

## 🧪 4. Status de Verificação
- Coberto por **32 execuções de testes unitários automatizados** no `TL.Messaging.Kafka.Tests` (16 em .NET 8 e 16 em .NET 9 - 100% passing).
