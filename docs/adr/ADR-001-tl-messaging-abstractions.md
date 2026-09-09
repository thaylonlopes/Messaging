# ADR-001: Decisões Arquiteturais do Pacote TL.Messaging.Abstractions

## 📋 1. Contexto e Motivação

Historicamente, em ecossistemas de microsserviços orientados a eventos, implementações de mensageria sofrem com forte acoplamento direto com SDKs de brokers físicos (`RabbitMQ.Client`, `Confluent.Kafka`). Esse cenário traz diversos problemas:
1. **Acoplamento Indevido de Infraestrutura:** Camadas de domínio e aplicação são forçadas a referenciar bibliotecas pesadas de terceiros para publicar ou consumir mensagens.
2. **Inconsistência de Contratos:** Interfaces de publicação e consumo divergem entre times e serviços, impedindo a alternância simples de brokers conforme a necessidade do negócio (ex: mudar de RabbitMQ para Kafka).
3. **Ausência de Padronização de Envelopes:** Eventos trafegavam sem correlação estruturada (`CorrelationId`), identificadores universais de rastreamento (`EventId`), carimbos de data/hora UTC e compatibilidade com especificações modernas de mercado como CloudEvents v1.0.

O pacote **`TL.Messaging.Abstractions`** foi concebido para atuar como a **Porta Base** agnóstica da suíte, isolando 100% o domínio da infraestrutura com pureza BCL.

---

## 🎯 2. Decisões Arquiteturais

### 2.1. Arquitetura Ports & Adapters (Hexagonal)
- O pacote contém estritamente **interfaces, records e estruturas de dados de domínio** (`IEventProducer`, `IEventHandler<T>`, `EventMessage<T>`, `EventMetadata`, `Result`).
- Zero referências a pacotes de terceiros (pureza BCL), permitindo instalação em qualquer camada da aplicação (Domain, Application, Shared Kernel).

### 2.2. Envelope Imutável Padronizado (`EventMessage<T>`)
- Formato padronizado inspirado na especificação CloudEvents v1.0:
  - `Id`: Identificador único global do evento (`Guid` ou `string`).
  - `CorrelationId`: Identificador de rastreamento para observabilidade e distributed tracing (OpenTelemetry).
  - `Timestamp`: Momento exato da emissão em UTC (`DateTimeOffset`).
  - `EventType`: Nome semântico do evento para roteamento e serialização polimórfica.
  - `Payload`: O corpo tipado do evento de negócio imutável (`T where T : class`).
  - `Headers`: Dicionário somente leitura de cabeçalhos contextuais adicionais.

### 2.3. Contrato Agnóstico de Publicação (`IEventProducer`)
- `PublishAsync<T>(T message, EventMetadata? metadata, CancellationToken ct)`: Método desacoplado para publicação unitária.
- `PublishBatchAsync<T>(IEnumerable<T> messages, EventMetadata? metadata, CancellationToken ct)`: Método otimizado para publicação em lote com suporte a transações ou buffers do broker.

### 2.4. Contrato Agnóstico de Consumo (`IEventHandler<T>`)
- `Task<Result> HandleAsync(EventMessage<T> eventMessage, CancellationToken ct)`: Contrato para execução de lógica de negócio pelo consumidor.
- **Semântica Funcional de Retorno**:
  - `Result.Success()`: O consumidor confirma o processamento (ACK / Commit de Offset).
  - `Result.Failure(...)`: O consumidor sinaliza falha irrecuperável, direcionando o evento para retentativa exponencial ou Dead-Letter (DLQ / DLT).

### 2.5. Governança Multi-Target (.NET 8 e .NET 9) e CPM
- Suporte nativo aos runtimes suportados da Microsoft: `.NET 8.0` (LTS) e `.NET 9.0` (STS).
- Gestão centralizada de dependências no `Directory.Packages.props` da solução.

---

## ⚖️ 3. Consequências e Trade-offs

### ✅ Vantagens:
- **Desacoplamento Total:** Domínio e aplicação nunca referenciam RabbitMQ ou Kafka diretamente.
- **Intercambialidade de Brokers:** A troca ou convivência de múltiplos brokers resume-se a alterar o registro de injeção de dependência no `Program.cs`.
- **Rastreabilidade Distribuída:** Propagação de `CorrelationId` e headers garante observabilidade ponta a ponta.
- **Zero Bloqueios de Compilação:** Compilação ultra-rápida sem dependências transitivas.

### ⚠️ Desvantagens / Trade-offs:
- Recursos altamente proprietários de um broker específico (ex: headers exóticos AMQP ou partições manuais de baixo nível) precisam ser configurados via `EventMetadata` ou acessados através de interfaces especializadas (`IRabbitMqProducer`, `IKafkaProducer`).

---

## 🧪 4. Status de Verificação
- Integrado e verificado por **20 testes unitários automatizados** nos adaptadores RabbitMQ e Kafka (100% passing em net8.0 e net9.0).
