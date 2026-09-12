# ADR-001: Decisões Arquiteturais do Pacote TL.Messaging.RabbitMQ

## 📋 1. Contexto e Motivação

O RabbitMQ é amplamente utilizado em arquiteturas orientadas a eventos para mensageria assíncrona baseada no protocolo AMQP 0-9-1. No entanto, sua utilização direta via SDK oficial `RabbitMQ.Client` exige código complexo e repetitivo de gerenciamento de canais (`IModel`), declaração de topologia de exchanges e filas, Publisher Confirms e tratamento de contingência com Dead-Letter Queue (DLQ).

Sem uma camada padronizada, cada microsserviço implementava sua própria lógica de retentativas, gerando perda silenciosa de mensagens ou filas de trabalho bloqueadas por *poison messages*.

No repositório `TL.Messaging`, o pacote **`TL.Messaging.RabbitMQ`** implementa o adaptador de infraestrutura AMQP conectado diretamente aos contratos fundamentais de [`TL.BaseContracts.Messaging`](https://www.nuget.org/packages/TL.BaseContracts/0.2.0).

---

## 🎯 2. Decisões Arquiteturais

### 2.1. Implementação da Porta Agnóstica `IEventProducer`
- O `RabbitMqProducer` implementa `IEventProducer` (permitindo injeção desacoplada na aplicação) e a interface especializada `IRabbitMqProducer` (para publicações diretas com exchange e routing key customizadas).
- **Publisher Confirms**: Ativado por padrão (`ConfirmSelect()`), garantindo confirmação síncrona/canalizada de entrega do broker antes do retorno do método.
- **Serialização UTF-8 / JSON**: Serializa automaticamente no envelope `EventMessage<T>` e preenche os metadados AMQP (`BasicProperties.ContentType = "application/json"`, `CorrelationId`, `Type`, `Timestamp`).

### 2.2. Consumidor Resiliente em Background (`RabbitMqConsumer<TEvent, THandler>`)
- Implementado como `BackgroundService` gerenciado pelo runtime do .NET.
- **Declaração Automática de Topologia**:
  1. Cria a Exchange principal (`amq.topic` ou customizada).
  2. Cria a Dead-Letter Exchange (`.dlx`) e a fila Dead-Letter (`.dlq`).
  3. Cria a fila principal com argumentos `x-dead-letter-exchange` apontando para a `.dlx`.
- **Retentativas Inteligentes com Polly**: Aplica retentativas com backoff exponencial antes de rejeitar com NACK (`requeue: false`), enviando a mensagem automaticamente para a `.dlq`.
- **Delegação Limpa para `IEventHandler<T>`**: Executa o manipulador de negócio dentro de um escopo de injeção de dependência (`IServiceScope`).

### 2.3. Configuração Fluente no DI
- `services.AddRabbitMqMessaging(config)`
- `services.AddRabbitMqConsumer<OrderCreatedEvent, OrderCreatedHandler>("app.orders.created", "orders.created")`

---

## ⚖️ 3. Consequências e Trade-offs

### ✅ Vantagens:
- **Resiliência Pronta para Produção:** Zero esforço para configurar topologias robustas com proteção nativa contra *poison messages*.
- **Desacoplamento Completo:** Regra de negócio nunca referencia o driver de AMQP nem a biblioteca de terceiros.
- **Multi-Target Moderno:** Totalmente compilado e testado para `.NET 8.0` e `.NET 9.0`.

### ⚠️ Desvantagens / Trade-offs:
- Exige criação de uma conexão persistente e canais multiplexados por thread no host consumidor.

---

## 🧪 4. Status de Verificação
- Coberto por **10 testes unitários automatizados** no `TL.Messaging.RabbitMQ.Tests` (5 em .NET 8 e 5 em .NET 9 - 100% passing).
