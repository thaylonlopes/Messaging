using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using FluentAssertions;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;
using Moq;
using TL.BaseContracts;
using TL.BaseContracts.Messaging;
using TL.Messaging.Kafka.Configuration;
using TL.Messaging.Kafka.Extensions;
using TL.Messaging.Kafka.Producer;
using Xunit;

namespace TL.Messaging.Kafka.Tests
{
    public record SamplePaymentEvent(string PaymentId, decimal Amount);

    public class SamplePaymentHandler : IEventHandler<SamplePaymentEvent>
    {
        public Task<Result> HandleAsync(EventMessage<SamplePaymentEvent> eventMessage, CancellationToken cancellationToken)
        {
            return Task.FromResult(Result.Success());
        }
    }

    public class KafkaTests
    {
        [Fact]
        public void Given_KafkaOptions_Default_Values_Should_Be_Configured()
        {
            var options = new KafkaOptions();

            options.BootstrapServers.Should().Be("localhost:9092");
            options.GroupId.Should().Be("app-consumer-group");
            options.DefaultTopic.Should().Be("events.default");
            options.DeadLetterTopicSuffix.Should().Be(".dlt");
            options.AutoOffsetReset.Should().Be("Earliest");
            options.EnableIdempotence.Should().BeTrue();
            options.SecurityProtocol.Should().Be("Plaintext");
            options.RetryCount.Should().Be(3);
        }

        [Fact]
        public void Given_AddKafkaMessaging_Should_Register_Producer_In_DI()
        {
            var services = new ServiceCollection();
            var configData = new Dictionary<string, string?>
            {
                { "Kafka:BootstrapServers", "kafka-cluster:9092" },
                { "Kafka:GroupId", "orders-group" }
            };
            var configuration = new ConfigurationBuilder().AddInMemoryCollection(configData).Build();

            services.AddKafkaMessaging(configuration);
            var provider = services.BuildServiceProvider();

            var agnosticProducer = provider.GetService<IEventProducer>();
            var specificProducer = provider.GetService<IKafkaProducer>();

            agnosticProducer.Should().NotBeNull();
            specificProducer.Should().NotBeNull();
            agnosticProducer.Should().BeSameAs(specificProducer);
        }

        [Fact]
        public void Given_AddKafkaConsumer_Should_Register_HostedService_And_Handler()
        {
            var services = new ServiceCollection();
            services.AddKafkaMessaging(opts => opts.BootstrapServers = "localhost:9092");

            services.AddKafkaConsumer<SamplePaymentEvent, SamplePaymentHandler>("payments.topic");
            var provider = services.BuildServiceProvider();

            var handler = provider.GetService<SamplePaymentHandler>();
            var hostedServices = provider.GetServices<IHostedService>();

            handler.Should().NotBeNull();
            hostedServices.Should().NotBeEmpty();
        }

        [Fact]
        public async Task Given_KafkaProducer_PublishAsync_With_Mock_Producer_Should_Produce()
        {
            var mockConfluentProducer = new Mock<IProducer<string, string>>();
            var deliveryResult = new DeliveryResult<string, string>
            {
                Partition = new Partition(1),
                Offset = new Offset(42),
                Status = PersistenceStatus.Persisted
            };

            mockConfluentProducer.Setup(p => p.ProduceAsync(
                It.IsAny<string>(),
                It.IsAny<Message<string, string>>(),
                It.IsAny<CancellationToken>()))
                .ReturnsAsync(deliveryResult);

            var options = Options.Create(new KafkaOptions { DefaultTopic = "orders.topic" });
            using var producer = new KafkaProducer(options, mockConfluentProducer.Object);

            var payload = new SamplePaymentEvent("PAY-100", 50.00m);
            var metadata = new EventMetadata()
                .WithKafkaPartitionKey("customer-abc")
                .WithHeader("tenant", "corporate");

            var result = await producer.PublishAsync(payload, metadata);

            result.IsSuccess.Should().BeTrue();
            mockConfluentProducer.Verify(p => p.ProduceAsync(
                "orders.topic",
                It.Is<Message<string, string>>(m => m.Key == "customer-abc" && m.Headers.Count >= 3),
                It.IsAny<CancellationToken>()), Times.Once);
        }

        [Fact]
        public async Task Given_KafkaProducer_PublishBatchAsync_Should_Produce_All_Messages()
        {
            var mockConfluentProducer = new Mock<IProducer<string, string>>();
            mockConfluentProducer.Setup(p => p.ProduceAsync(
                It.IsAny<string>(),
                It.IsAny<Message<string, string>>(),
                It.IsAny<CancellationToken>()))
                .ReturnsAsync(new DeliveryResult<string, string>());

            var options = Options.Create(new KafkaOptions());
            using var producer = new KafkaProducer(options, mockConfluentProducer.Object);

            var batch = new[]
            {
                new SamplePaymentEvent("PAY-1", 10m),
                new SamplePaymentEvent("PAY-2", 20m)
            };

            var result = await producer.PublishBatchAsync(batch);

            result.IsSuccess.Should().BeTrue();
            mockConfluentProducer.Verify(p => p.ProduceAsync(
                It.IsAny<string>(),
                It.IsAny<Message<string, string>>(),
                It.IsAny<CancellationToken>()), Times.Exactly(2));
        }

        [Fact]
        public void Given_KafkaConsumer_With_Options_Should_Initialize_Successfully()
        {
            var services = new ServiceCollection();
            services.AddScoped<SamplePaymentHandler>();
            var sp = services.BuildServiceProvider();

            var options = Options.Create(new KafkaOptions { RetryCount = 4 });
            using var consumer = new TL.Messaging.Kafka.Consumer.KafkaConsumer<SamplePaymentEvent, SamplePaymentHandler>(
                options,
                sp,
                dltProducer: null,
                topic: "payments.topic");

            consumer.Should().NotBeNull();
        }
    }
}
