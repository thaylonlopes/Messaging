using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Order;
using Polly;
using Polly.Retry;
using TL.BaseContracts;
using TL.BaseContracts.Messaging;
using TL.Messaging.Benchmarks.Events;

namespace TL.Messaging.Benchmarks.Suites;

/// <summary>
/// Benchmark de Nível 3: Despacho em lote com retries e interceptadores Polly v8,
/// comparando o overhead de tempo e alocação de memória (GC Heap) contra baseline sem resiliência.
/// </summary>
[MemoryDiagnoser]
[Orderer(SummaryOrderPolicy.FastestToSlowest)]
[RankColumn]
public class Level3BatchDispatchWithResilienceBenchmarks
{
    private List<EventMessage<OrderPlacedEvent>> _batchMessages = null!;
    private ResiliencePipeline _retryPipeline = null!;

    [Params(10, 50)]
    public int BatchSize { get; set; }

    [GlobalSetup]
    public void Setup()
    {
        _batchMessages = new List<EventMessage<OrderPlacedEvent>>(BatchSize);
        for (int i = 0; i < BatchSize; i++)
        {
            var order = new OrderPlacedEvent(
                OrderId: $"ORD-BATCH-{i:D4}",
                CustomerId: $"CUST-{i:D4}",
                TotalAmount: 100m + i,
                Currency: "BRL",
                ItemCount: 1);

            var envelope = EventMessage<OrderPlacedEvent>.Create(
                payload: order,
                correlationId: $"corr-batch-{i}",
                eventType: nameof(OrderPlacedEvent));

            _batchMessages.Add(envelope);
        }

        _retryPipeline = new ResiliencePipelineBuilder()
            .AddRetry(new RetryStrategyOptions
            {
                MaxRetryAttempts = 3,
                BackoffType = DelayBackoffType.Constant,
                Delay = TimeSpan.Zero, // Zero delay no benchmark para isolar overhead computacional e de alocação
                UseJitter = false
            })
            .Build();
    }

    [Benchmark(Baseline = true, Description = "1. Baseline: Despacho em Lote Direto (Sem Polly)")]
    public async Task<int> BaselineDirectBatchDispatch()
    {
        int processedCount = 0;
        for (int i = 0; i < BatchSize; i++)
        {
            var msg = _batchMessages[i];
            var result = await SimulateDirectDispatchAsync(msg).ConfigureAwait(false);
            if (result.IsSuccess)
            {
                processedCount++;
            }
        }
        return processedCount;
    }

    [Benchmark(Description = "2. Despacho com Interceptador Polly (Happy Path - 0 Falhas)")]
    public async Task<int> PollyInterceptedHappyPath()
    {
        int processedCount = 0;
        for (int i = 0; i < BatchSize; i++)
        {
            var msg = _batchMessages[i];
            var result = await _retryPipeline.ExecuteAsync(
                async token => await SimulateDirectDispatchAsync(msg).ConfigureAwait(false),
                CancellationToken.None).ConfigureAwait(false);

            if (result.IsSuccess)
            {
                processedCount++;
            }
        }
        return processedCount;
    }

    [Benchmark(Description = "3. Despacho com Interceptador Polly (10% Retentativas Transitórias)")]
    public async Task<int> PollyInterceptedWithTransientRetry()
    {
        int processedCount = 0;
        for (int i = 0; i < BatchSize; i++)
        {
            var msg = _batchMessages[i];
            bool shouldFailFirstAttempt = (i % 10 == 0);
            int attempt = 0;

            var result = await _retryPipeline.ExecuteAsync(
                async token =>
                {
                    attempt++;
                    if (shouldFailFirstAttempt && attempt == 1)
                    {
                        throw new InvalidOperationException("Falha transitória simulada de rede");
                    }
                    return await SimulateDirectDispatchAsync(msg).ConfigureAwait(false);
                },
                CancellationToken.None).ConfigureAwait(false);

            if (result.IsSuccess)
            {
                processedCount++;
            }
        }
        return processedCount;
    }

    private static ValueTask<Result> SimulateDirectDispatchAsync(EventMessage<OrderPlacedEvent> message)
    {
        // Simulação in-memory pura de publicação/ack para isolar o overhead do framework e interceptadores
        if (message == null)
        {
            return ValueTask.FromResult(Result.Failure(Error.Failure("NullMessage", "Mensagem nula.")));
        }

        return ValueTask.FromResult(Result.Success());
    }
}
