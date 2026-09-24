using System;
using System.Collections.Generic;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Moq;
using TL.BaseContracts;
using TL.BaseContracts.Messaging;
using TL.Messaging.Kafka.Configuration;
using TL.Messaging.Kafka.Consumer;
using TL.Messaging.Kafka.Dlq;
using TL.Messaging.Kafka.Producer;
using Xunit;

namespace TL.Messaging.Kafka.Tests
{
    public class FailingPaymentHandler : IEventHandler<SamplePaymentEvent>
    {
        public Task<Result> HandleAsync(EventMessage<SamplePaymentEvent> eventMessage, CancellationToken cancellationToken)
        {
            throw new InvalidOperationException("Falha no gateway bancario");
        }
    }

    public class KafkaDlqTests
    {
        [Fact]
        public async Task Given_PoisonMessage_With_MalformedJson_Should_Produce_To_Dlt_With_Diagnostic_Headers_And_Commit()
        {
            var services = new ServiceCollection();
            services.AddScoped<SamplePaymentHandler>();
            var sp = services.BuildServiceProvider();

            var options = Options.Create(new KafkaOptions
            {
                DefaultTopic = "events.samplepaymentevent",
                DeadLetterTopicSuffix = ".dlt"
            });

            var mockDltProducer = new Mock<IKafkaProducer>();
            var mockConsumer = new Mock<IConsumer<string, string>>();

            EventMetadata? capturedMetadata = null;
            mockDltProducer.Setup(p => p.ProduceToTopicAsync(
                "events.samplepaymentevent.dlt",
                "pay-key-1",
                "{ json invalido !",
                It.IsAny<EventMetadata>(),
                It.IsAny<CancellationToken>()))
                .Callback<string, string?, string, EventMetadata?, CancellationToken>((t, k, payload, meta, ct) => capturedMetadata = meta)
                .ReturnsAsync(Result.Success());

            using var consumer = new KafkaConsumer<SamplePaymentEvent, SamplePaymentHandler>(
                options,
                sp,
                dltProducer: mockDltProducer.Object,
                topic: "events.samplepaymentevent");

            var headers = new Headers
            {
                new Header("traceparent", Encoding.UTF8.GetBytes("trace-parent-999"))
            };

            var consumeResult = new ConsumeResult<string, string>
            {
                Topic = "events.samplepaymentevent",
                Partition = new Partition(0),
                Offset = new Offset(10),
                Message = new Message<string, string>
                {
                    Key = "pay-key-1",
                    Value = "{ json invalido !",
                    Headers = headers
                }
            };

            await consumer.ProcessMessageAsync(consumeResult, mockConsumer.Object, CancellationToken.None);

            mockDltProducer.Verify(p => p.ProduceToTopicAsync(
                "events.samplepaymentevent.dlt",
                "pay-key-1",
                "{ json invalido !",
                It.IsAny<EventMetadata>(),
                It.IsAny<CancellationToken>()), Times.Once);

            mockConsumer.Verify(c => c.Commit(consumeResult), Times.Once);

            capturedMetadata.Should().NotBeNull();
            capturedMetadata!.Headers.Should().NotBeNull();
            capturedMetadata.Headers![MessagingDiagnosticHeaders.ExceptionType].Should().Contain("JsonException");
            capturedMetadata.Headers[MessagingDiagnosticHeaders.RetryCount].Should().Be("0");
            capturedMetadata.Headers[MessagingDiagnosticHeaders.FailedAtUtc].Should().NotBeNullOrWhiteSpace();
            capturedMetadata.Headers[MessagingDiagnosticHeaders.OriginalTopic].Should().Be("events.samplepaymentevent");
            capturedMetadata.Headers[MessagingDiagnosticHeaders.TraceParent].Should().Be("trace-parent-999");
        }

        [Fact]
        public async Task Given_Handler_Exception_Exhausting_Retries_Should_Produce_To_Dlt_With_Diagnostic_Headers_And_Commit()
        {
            var services = new ServiceCollection();
            services.AddScoped<FailingPaymentHandler>();
            var sp = services.BuildServiceProvider();

            var options = Options.Create(new KafkaOptions
            {
                DefaultTopic = "events.samplepaymentevent",
                RetryCount = 3,
                DeadLetterTopicSuffix = ".dlt"
            });

            var mockDltProducer = new Mock<IKafkaProducer>();
            var mockConsumer = new Mock<IConsumer<string, string>>();

            EventMetadata? capturedMetadata = null;
            mockDltProducer.Setup(p => p.ProduceToTopicAsync(
                "events.samplepaymentevent.dlt",
                "pay-key-2",
                It.IsAny<string>(),
                It.IsAny<EventMetadata>(),
                It.IsAny<CancellationToken>()))
                .Callback<string, string?, string, EventMetadata?, CancellationToken>((t, k, payload, meta, ct) => capturedMetadata = meta)
                .ReturnsAsync(Result.Success());

            using var consumer = new KafkaConsumer<SamplePaymentEvent, FailingPaymentHandler>(
                options,
                sp,
                dltProducer: mockDltProducer.Object,
                topic: "events.samplepaymentevent");

            var eventMessage = EventMessage<SamplePaymentEvent>.Create(new SamplePaymentEvent("PAY-100", 250m));
            string validJson = JsonSerializer.Serialize(eventMessage);

            var headers = new Headers
            {
                new Header("traceparent", Encoding.UTF8.GetBytes("trace-parent-abc"))
            };

            var consumeResult = new ConsumeResult<string, string>
            {
                Topic = "events.samplepaymentevent",
                Partition = new Partition(0),
                Offset = new Offset(11),
                Message = new Message<string, string>
                {
                    Key = "pay-key-2",
                    Value = validJson,
                    Headers = headers
                }
            };

            await consumer.ProcessMessageAsync(consumeResult, mockConsumer.Object, CancellationToken.None);

            mockDltProducer.Verify(p => p.ProduceToTopicAsync(
                "events.samplepaymentevent.dlt",
                "pay-key-2",
                validJson,
                It.IsAny<EventMetadata>(),
                It.IsAny<CancellationToken>()), Times.Once);

            mockConsumer.Verify(c => c.Commit(consumeResult), Times.Once);

            capturedMetadata.Should().NotBeNull();
            capturedMetadata!.Headers.Should().NotBeNull();
            capturedMetadata.Headers![MessagingDiagnosticHeaders.ExceptionType].Should().Be("InvalidOperationException");
            capturedMetadata.Headers[MessagingDiagnosticHeaders.RetryCount].Should().Be("3");
            capturedMetadata.Headers[MessagingDiagnosticHeaders.ExceptionMessage].Should().Be("Falha no gateway bancario");
            capturedMetadata.Headers[MessagingDiagnosticHeaders.FailedAtUtc].Should().NotBeNullOrWhiteSpace();
            capturedMetadata.Headers[MessagingDiagnosticHeaders.TraceParent].Should().Be("trace-parent-abc");
        }

        [Fact]
        public async Task Given_KafkaDlqManager_ReplayAsync_Should_Consume_From_Dlt_Expunge_Diagnostic_Headers_And_Produce()
        {
            var mockProducer = new Mock<IKafkaProducer>();
            var mockConsumer = new Mock<IConsumer<string, string>>();

            var dltHeaders = new Headers
            {
                new Header(MessagingDiagnosticHeaders.ExceptionMessage, Encoding.UTF8.GetBytes("Erro anterior")),
                new Header(MessagingDiagnosticHeaders.ExceptionType, Encoding.UTF8.GetBytes("InvalidOperationException")),
                new Header(MessagingDiagnosticHeaders.RetryCount, Encoding.UTF8.GetBytes("3")),
                new Header(MessagingDiagnosticHeaders.FailedAtUtc, Encoding.UTF8.GetBytes(DateTimeOffset.UtcNow.ToString("O"))),
                new Header(MessagingDiagnosticHeaders.OriginalTopic, Encoding.UTF8.GetBytes("events.samplepaymentevent")),
                new Header(MessagingDiagnosticHeaders.Reason, Encoding.UTF8.GetBytes("Erro")),
                new Header("x-correlation-id", Encoding.UTF8.GetBytes("corr-alpha-99"))
            };

            var consumeResult = new ConsumeResult<string, string>
            {
                Topic = "events.samplepaymentevent.dlt",
                Partition = new Partition(0),
                Offset = new Offset(1),
                Message = new Message<string, string>
                {
                    Key = "key-1",
                    Value = "{\"PaymentId\":\"PAY-1\"}",
                    Headers = dltHeaders
                }
            };

            mockConsumer.SetupSequence(c => c.Consume(It.IsAny<TimeSpan>()))
                .Returns(consumeResult)
                .Returns((ConsumeResult<string, string>)null!);

            EventMetadata? capturedMetadata = null;
            mockProducer.Setup(p => p.ProduceRawAsync(
                "events.samplepaymentevent",
                "key-1",
                "{\"PaymentId\":\"PAY-1\"}",
                It.IsAny<EventMetadata>(),
                It.IsAny<CancellationToken>()))
                .Callback<string, string?, string, EventMetadata?, CancellationToken>((t, k, payload, meta, ct) => capturedMetadata = meta)
                .ReturnsAsync(Result.Success());

            var options = Options.Create(new KafkaOptions { DeadLetterTopicSuffix = ".dlt" });
            var dlqManager = new KafkaDlqManager(
                options,
                mockProducer.Object,
                consumerFactory: _ => mockConsumer.Object);

            int replayed = await dlqManager.ReplayAsync("events.samplepaymentevent.dlt", maxMessages: 10);

            replayed.Should().Be(1);
            mockProducer.Verify(p => p.ProduceRawAsync(
                "events.samplepaymentevent",
                "key-1",
                "{\"PaymentId\":\"PAY-1\"}",
                It.IsAny<EventMetadata>(),
                It.IsAny<CancellationToken>()), Times.Once);

            mockConsumer.Verify(c => c.Commit(consumeResult), Times.Once);

            capturedMetadata.Should().NotBeNull();
            capturedMetadata!.Headers.Should().NotBeNull();
            capturedMetadata.Headers!.ContainsKey(MessagingDiagnosticHeaders.ExceptionMessage).Should().BeFalse();
            capturedMetadata.Headers.ContainsKey(MessagingDiagnosticHeaders.ExceptionType).Should().BeFalse();
            capturedMetadata.Headers.ContainsKey(MessagingDiagnosticHeaders.RetryCount).Should().BeFalse();
            capturedMetadata.Headers.ContainsKey(MessagingDiagnosticHeaders.FailedAtUtc).Should().BeFalse();
            capturedMetadata.Headers.ContainsKey("x-correlation-id").Should().BeTrue();
            capturedMetadata.Headers["x-correlation-id"].Should().Be("corr-alpha-99");
        }

        [Fact]
        public async Task Given_KafkaDlqManager_With_Empty_Dlt_Should_Return_Zero()
        {
            var mockProducer = new Mock<IKafkaProducer>();
            var mockConsumer = new Mock<IConsumer<string, string>>();

            mockConsumer.Setup(c => c.Consume(It.IsAny<TimeSpan>()))
                .Returns((ConsumeResult<string, string>)null!);

            var options = Options.Create(new KafkaOptions());
            var dlqManager = new KafkaDlqManager(
                options,
                mockProducer.Object,
                consumerFactory: _ => mockConsumer.Object);

            int count = await dlqManager.ReplayAsync("events.samplepaymentevent.dlt", maxMessages: 5);

            count.Should().Be(0);
            mockProducer.Verify(p => p.ProduceToTopicAsync(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<string>(), It.IsAny<EventMetadata>(), It.IsAny<CancellationToken>()), Times.Never);
        }
    }
}
