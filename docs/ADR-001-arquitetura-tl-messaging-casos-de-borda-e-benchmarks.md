# ADR-001: Arquitetura TL.Messaging — Casos de Borda, Resiliência Operacional e Benchmarks

**Status:** Aprovado  
**Data:** 2026-09-25  
**Autores:** Squad Messaging (@arquiteto:perf, @dev:core, @techlead)  
**Versão:** v0.6.0  

---

## 📋 1. Contexto e Motivação

O ecossistema corporativo de microsserviços baseados em Arquitetura Orientada a Eventos (EDA) exige garantias rigorosas de entrega, isolamento contra falhas catastróficas e previsibilidade de latência e consumo de recursos de memória.

Durante o ciclo evolutivo até a release **v0.6.0**, a biblioteca `TL.Messaging` consolidou a abstração unificada Ports & Adapters sobre **RabbitMQ** e **Apache Kafka**, introduzindo salvaguardas avançadas para casos de borda operacionais e uma suíte abrangente de validação empírica de performance com BenchmarkDotNet.

---

## 🎯 2. Decisões Arquiteturais e Casos de Borda (Release v0.6.0)

### 2.1. Mitigação de *Poison Messages* e Desserialização Defensiva
- **Problema:** Mensagens com formato corrompido ou incompatíveis com o schema JSON causavam loops infinitos de NACK e retentativas, saturando 100% da CPU dos workers e travando as partições de consumo.
- **Decisão:** O pipeline de consumo intercepta exceções de desserialização (`JsonException`) antes da invocação do handler de negócio. Os bytes brutos do payload são preservados e encaminhados imediatamente para a Dead-Letter Queue (DLQ) ou Dead-Letter Topic (DLT), acompanhados de metadados padronizados de diagnóstico (`MessagingDiagnosticHeaders`), emitindo ACK na fila de entrada para liberar o tráfego regular.

### 2.2. Guardrails Anti-Loop no Replay de DLQ e DLT
- **Problema:** Em operações manuais ou automatizadas de reprocessamento, um erro na configuração da fila de destino poderia reinjetar mensagens na própria DLQ/DLT de origem, gerando tempestades de eventos e duplicações descontroladas.
- **Decisão:** Implementação de validação de inferência obrigatória em `RabbitMqDlqManager` e `KafkaDlqManager`. Caso o destino inferido ou informado coincida com o nome da fila/tópico de origem, a operação é rejeitada imediatamente com `InvalidOperationException`.

### 2.3. Isolamento Estrito de Canais AMQP por Operação
- **Problema:** O cliente AMQP oficial (`RabbitMQ.Client.IModel`) não é thread-safe. Compartilhar canais para publicação e consumo simultâneo ou em requisições concorrentes de replay causava fechamento abrupto de canal (`ChannelClosedException`).
- **Decisão:** Toda operação de replay em `RabbitMqDlqManager` instancia um `IModel` dedicado e descartável (`using var channel = ...`), garantindo thread-safety total e isolamento de falha enquanto o gerenciador permanece registrado como Singleton no DI.

### 2.4. Resolução de Metadados e Chave de Partição em $O(1)$
- **Problema:** A reflexão contínua em runtime para ler anotações declarativas (`[PartitionKey]`, `[Topic]`, `[MessageId]`) gerava alocações no heap e overhead de CPU em cenários de alta vazão.
- **Decisão:** Utilização do `EventMetadataExtractor` de `TL.BaseContracts`, que amortiza a inspeção de tipos por meio de um cache concorrente estático. Conforme comprovado nos benchmarks, a extração de chave atinge $56{,}2\text{ ns}$ com zero alocação de heap ($0\text{ B/op}$).

### 2.5. Rastreabilidade Distribuída Nativa com OpenTelemetry
- **Problema:** Perda de contexto de correlação distribuída em arquiteturas heterogêneas ao transitar por diferentes brokers.
- **Decisão:** Propagação padronizada dos cabeçalhos de contexto distribuído (`traceparent`, `tracestate`, `baggage`) nos cabeçalhos de mensagem AMQP (`IBasicProperties.Headers`) e Kafka (`Headers`), viabilizando observabilidade unificada ponta a ponta sem atrito.

---

## 📊 3. Resultados Empíricos dos Benchmarks de Performance

A validação foi conduzida na suíte `TL.Messaging.Benchmarks` sob .NET 8 e .NET 9 Release com `TreatWarningsAsErrors=true`. Os dados consolidados constam em [`results.md`](benchmarks/results.md):

### 3.1. Nível 1: Envelope & PingEvent (Micromedição em Nanossegundos)
- **Criação do Envelope (`EventMessage<T>.Create`):** $121{,}7\text{ ns}$ | $80\text{ B/op}$ | $0{,}0000\text{ Gen 0}$.
- **Serialização Direta de Payload JSON Utf8:** $527{,}7\text{ ns}$ | $112\text{ B/op}$.
- **Serialização Completa do Envelope CloudEvents:** $1409{,}0\text{ ns}$ | $304\text{ B/op}$.
- **Desserialização com Factory Especializada:** $6969{,}7\text{ ns}$ | $1104\text{ B/op}$.
- **Veredito:** O overhead do envelope padronizado é desprezível face ao ganho de tipagem forte, imutabilidade e interoperabilidade CloudEvents.

### 3.2. Nível 2: OrderPlacedEvent, [PartitionKey] e OpenTelemetry
- **Extração de `[PartitionKey]`:** $56{,}2\text{ ns}$ | $0\text{ B/op}$ (Zero Alocação via cache estático).
- **Inferência de Tópico Kebab-Case:** $29{,}6\text{ ns}$ | $0\text{ B/op}$ (Zero Alocação).
- **Composição de Headers de Tracing (OpenTelemetry):** $135{,}7\text{ ns}$ | $392\text{ B/op}$.
- **Ciclo Completo (Envelope + Chave + Headers + Serialização):** $2855{,}7\text{ ns}$ ($\approx 2{,}85\text{ µs}$) | $952\text{ B/op}$.

### 3.3. Nível 3: Despacho em Lote com Resiliência Polly v8
- **Lote de 10 mensagens:**
  - Baseline direto: $0{,}56\text{ µs/lote}$ ($55{,}6\text{ ns/item}$) | $392\text{ B}$.
  - Interceptador Polly (Happy Path): $7{,}98\text{ µs/lote}$ ($797{,}8\text{ ns/item}$) | $1272\text{ B}$.
  - Interceptador Polly (10% Retentativas Transitórias): $18{,}05\text{ µs/lote}$ ($1804{,}7\text{ ns/item}$) | $3240\text{ B}$.
- **Lote de 50 mensagens:**
  - Baseline direto: $1{,}88\text{ µs/lote}$ ($37{,}5\text{ ns/item}$) | $1672\text{ B}$.
  - Interceptador Polly (Happy Path): $39{,}96\text{ µs/lote}$ ($799{,}1\text{ ns/item}$) | $6072\text{ B}$.
  - Interceptador Polly (10% Retentativas Transitórias): $89{,}62\text{ µs/lote}$ ($1792{,}4\text{ ns/item}$) | $15912\text{ B}$.
- **Veredito:** O pipeline compilado `ResiliencePipeline` adiciona menos de $800\text{ ns}$ por item no caminho feliz, comprovando viabilidade para sistemas de alta taxa de transferência (dezenas de milhares de mensagens/segundo).

---

## ⚖️ 4. Consequências e Trade-offs

### ✅ Vantagens:
- **Resiliência Industrial:** Tratamento transparente de falhas transitórias e persistentes com zero bloqueio operacional.
- **Desempenho Validado:** Evidências empíricas atestam alocações mínimas e baixíssima latência.
- **Rastreabilidade Pronta para Produção:** Contexto OpenTelemetry propagado automaticamente nos dois brokers.
- **Zero Warnings:** Compilação 100% limpa com documentação XML técnica em todos os membros públicos.

### ⚠️ Trade-offs:
- Operações de replay em lote devem ser monitoradas para não gerar picos transitórios de consumo de canais e conexões AMQP.

---

## 🧪 5. Evidências de Testes e Homologação
- **70 testes unitários automatizados** cobrindo todos os cenários nos runtimes `.NET 8.0` e `.NET 9.0` (100% de sucesso).
- Suíte de benchmarks com três níveis executada e documentada em `docs/benchmarks/results.md`.
