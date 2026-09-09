# 🧩 TL.Messaging.Abstractions

> Contratos fundamentais e agnósticos (**Ports**) para arquiteturas orientadas a eventos em .NET com pureza BCL (zero dependências de terceiros), envelopes compatíveis com CloudEvents v1.0 e Result Pattern integrado para .NET 8 e .NET 9.

[![.NET 8.0](https://img.shields.io/badge/.NET-8.0%20(LTS)-blue.svg)](https://dotnet.microsoft.com/)
[![.NET 9.0](https://img.shields.io/badge/.NET-9.0-blue.svg)](https://dotnet.microsoft.com/)
[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)

---

## 📦 Instalação

Adicione o pacote ao seu projeto via .NET CLI:

```bash
dotnet add package TL.Messaging.Abstractions --version 0.1.0
```

---

## 🏛️ Contratos Principais

### 1. `IEventProducer` (Porta de Publicação)
Permite que classes de serviço e handlers de aplicação publiquem eventos sem nenhum acoplamento com RabbitMQ ou Kafka:

```csharp
public interface IEventProducer
{
    Task<Result> PublishAsync<T>(T message, EventMetadata? metadata = null, CancellationToken cancellationToken = default) where T : class;
    Task<Result> PublishBatchAsync<T>(IEnumerable<T> messages, EventMetadata? metadata = null, CancellationToken cancellationToken = default) where T : class;
}
```

### 2. `IEventHandler<T>` (Porta de Consumo)
Contrato implementado pelos consumidores de negócio para processar eventos:

```csharp
public interface IEventHandler<T> where T : class
{
    Task<Result> HandleAsync(EventMessage<T> eventMessage, CancellationToken cancellationToken);
}
```

### 3. `EventMessage<T>` (Envelope CloudEvents)
Envelope imutável para transporte de eventos com rastreabilidade distribuída:
- `Id`: Identificador único global do evento.
- `CorrelationId`: Identificador para distributed tracing e logs correlacionados.
- `Timestamp`: Data e hora da emissão em UTC.
- `EventType`: Nome qualificado do tipo de evento.
- `Payload`: O objeto de dados do evento de negócio.
- `Headers`: Dicionário somente leitura de cabeçalhos contextuais.

### 4. `Result` e `Error` (Semântica Funcional de Retorno)
Retorno explícito para sinalizar confirmação (ACK) ou contingência (NACK / Dead-Letter):
- `Result.Success()`: Mensagem processada com sucesso.
- `Result.Failure(Error.Validation(...))`: Falha que desencadeia retentativa exponencial ou desvio seguro para DLQ/DLT.
