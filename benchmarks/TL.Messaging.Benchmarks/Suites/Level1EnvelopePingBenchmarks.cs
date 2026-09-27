using System;
using System.Text.Json;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Order;
using TL.BaseContracts.Messaging;
using TL.Messaging.Benchmarks.Events;
using TL.Messaging.RabbitMQ.Serialization;

namespace TL.Messaging.Benchmarks.Suites;

/// <summary>
/// Benchmark de Nível 1: Medição em nanossegundos e alocação de memória no heap
/// para ciclo de vida do envelope imutável EventMessage e evento PingEvent.
/// </summary>
[MemoryDiagnoser]
[Orderer(SummaryOrderPolicy.FastestToSlowest)]
[RankColumn]
public class Level1EnvelopePingBenchmarks
{
    private PingEvent _pingEvent = null!;
    private EventMessage<PingEvent> _envelope = null!;
    private byte[] _serializedEnvelopeBytes = null!;
    private byte[] _serializedRawBytes = null!;

    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        PropertyNameCaseInsensitive = true,
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        Converters = { new EventMessageJsonConverterFactory() }
    };

    [GlobalSetup]
    public void Setup()
    {
        _pingEvent = new PingEvent(Guid.NewGuid(), DateTime.UtcNow.Ticks);
        _envelope = EventMessage<PingEvent>.Create(
            payload: _pingEvent,
            correlationId: "corr-benchmark-level1-001",
            eventType: nameof(PingEvent));

        _serializedEnvelopeBytes = JsonSerializer.SerializeToUtf8Bytes(_envelope, JsonOptions);
        _serializedRawBytes = JsonSerializer.SerializeToUtf8Bytes(_pingEvent, JsonOptions);
    }

    [Benchmark(Baseline = true, Description = "1. Criar EventMessage (Envelope Wrapping)")]
    public EventMessage<PingEvent> CreateEnvelope()
    {
        return EventMessage<PingEvent>.Create(
            payload: _pingEvent,
            correlationId: "corr-benchmark-level1-001",
            eventType: nameof(PingEvent));
    }

    [Benchmark(Description = "2. Serializar PingEvent Direto (JSON Utf8)")]
    public byte[] SerializeRawPingEvent()
    {
        return JsonSerializer.SerializeToUtf8Bytes(_pingEvent, JsonOptions);
    }

    [Benchmark(Description = "3. Serializar EventMessage com Envelope (JSON Utf8)")]
    public byte[] SerializeEnvelopePingEvent()
    {
        return JsonSerializer.SerializeToUtf8Bytes(_envelope, JsonOptions);
    }

    [Benchmark(Description = "4. Desserializar EventMessage com Envelope (JSON Utf8)")]
    public EventMessage<PingEvent>? DeserializeEnvelopePingEvent()
    {
        return JsonSerializer.Deserialize<EventMessage<PingEvent>>(_serializedEnvelopeBytes, JsonOptions);
    }
}
