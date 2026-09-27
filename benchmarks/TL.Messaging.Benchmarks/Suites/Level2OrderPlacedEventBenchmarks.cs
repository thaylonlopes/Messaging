using System.Collections.Generic;
using System.Text.Json;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Order;
using TL.BaseContracts.Messaging;
using TL.BaseContracts.Messaging.Helpers;
using TL.Messaging.Benchmarks.Events;
using TL.Messaging.Kafka.Serialization;

namespace TL.Messaging.Benchmarks.Suites;

/// <summary>
/// Benchmark de Nível 2: Medição de throughput e alocação para extração de [PartitionKey],
/// resolução de tópicos e propagação de cabeçalhos de Tracing Distribuído (OpenTelemetry).
/// </summary>
[MemoryDiagnoser]
[Orderer(SummaryOrderPolicy.FastestToSlowest)]
[RankColumn]
public class Level2OrderPlacedEventBenchmarks
{
    private OrderPlacedEvent _orderEvent = null!;
    private EventMetadata _metadataWithTracing = null!;
    private EventMessage<OrderPlacedEvent> _envelopeWithTracing = null!;

    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        PropertyNameCaseInsensitive = true,
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        Converters = { new EventMessageJsonConverterFactory() }
    };

    [GlobalSetup]
    public void Setup()
    {
        _orderEvent = new OrderPlacedEvent(
            OrderId: "ORD-BR-2026-998811",
            CustomerId: "CUST-SA-554422",
            TotalAmount: 2490.50m,
            Currency: "BRL",
            ItemCount: 5);

        var tracingHeaders = new Dictionary<string, string>
        {
            ["traceparent"] = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
            ["tracestate"] = "rojo=1,congo=2",
            ["baggage"] = "tenantId=acme-corp,environment=production,userId=usr-8812"
        };

        _metadataWithTracing = new EventMetadata
        {
            CorrelationId = "corr-otel-tracing-bench",
            Headers = tracingHeaders
        };

        _envelopeWithTracing = EventMessage<OrderPlacedEvent>.Create(
            payload: _orderEvent,
            correlationId: _metadataWithTracing.CorrelationId,
            eventType: nameof(OrderPlacedEvent),
            headers: _metadataWithTracing.Headers);
    }

    [Benchmark(Baseline = true, Description = "1. Extração de [PartitionKey] via MetadataExtractor")]
    public string? ExtractPartitionKey()
    {
        return EventMetadataExtractor.ExtractPartitionKey(_orderEvent);
    }

    [Benchmark(Description = "2. Inferência de Tópico via MetadataExtractor")]
    public string InferTopicName()
    {
        return EventMetadataExtractor.GetTopicName<OrderPlacedEvent>();
    }

    [Benchmark(Description = "3. Composição de Metadados com Headers OpenTelemetry")]
    public EventMetadata ComposeOpenTelemetryHeaders()
    {
        return new EventMetadata
        {
            CorrelationId = "corr-otel-tracing-bench",
            Headers = new Dictionary<string, string>
            {
                ["traceparent"] = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
                ["tracestate"] = "rojo=1,congo=2",
                ["baggage"] = "tenantId=acme-corp,environment=production,userId=usr-8812"
            }
        };
    }

    [Benchmark(Description = "4. Ciclo Completo: Envelope + PartitionKey + Headers Tracing + Serialize")]
    public (string? PartitionKey, byte[] Body) FullLifecycleWithTracing()
    {
        string? partitionKey = EventMetadataExtractor.ExtractPartitionKey(_orderEvent);
        var envelope = EventMessage<OrderPlacedEvent>.Create(
            payload: _orderEvent,
            correlationId: _metadataWithTracing.CorrelationId,
            eventType: nameof(OrderPlacedEvent),
            headers: _metadataWithTracing.Headers);

        byte[] body = JsonSerializer.SerializeToUtf8Bytes(envelope, JsonOptions);
        return (partitionKey, body);
    }
}
