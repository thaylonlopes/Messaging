using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using FluentAssertions;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;
using Moq;
using RabbitMQ.Client;
using TL.BaseContracts;
using TL.BaseContracts.Messaging;
using TL.Messaging.RabbitMQ.Configuration;
using TL.Messaging.RabbitMQ.Extensions;
using TL.Messaging.RabbitMQ.Producer;
using Xunit;

namespace TL.Messaging.RabbitMQ.Tests
{
    public record SampleOrderEvent(string OrderId, decimal Total);

    public class SampleOrderHandler : IEventHandler<SampleOrderEvent>
    {
        public Task<Result> HandleAsync(EventMessage<SampleOrderEvent> eventMessage, CancellationToken cancellationToken)
        {
            return Task.FromResult(Result.Success());
        }
    }

    public class RabbitMqTests
    {
        [Fact]
        public void Given_RabbitMqOptions_Default_Values_Should_Be_Configured()
        {
            var options = new RabbitMqOptions();

            options.HostName.Should().Be("localhost");
            options.Port.Should().Be(5672);
            options.UserName.Should().Be("guest");
            options.Password.Should().Be("guest");
            options.VirtualHost.Should().Be("/");
            options.ExchangeName.Should().Be("amq.topic");
            options.ExchangeType.Should().Be("topic");
            options.RetryCount.Should().Be(3);
            options.PrefetchCount.Should().Be(10);
            options.DeadLetterExchangeSuffix.Should().Be(".dlx");
            options.DeadLetterQueueSuffix.Should().Be(".dlq");
        }

        [Fact]
        public void Given_AddRabbitMqMessaging_Should_Register_Producer_In_DI()
        {
            var services = new ServiceCollection();
            var configData = new Dictionary<string, string?>
            {
                { "RabbitMq:HostName", "rabbitmq.internal" },
                { "RabbitMq:Port", "5672" }
            };
            var configuration = new ConfigurationBuilder().AddInMemoryCollection(configData).Build();

            services.AddRabbitMqMessaging(configuration);
            var provider = services.BuildServiceProvider();

            var agnosticProducer = provider.GetService<IEventProducer>();
            var specificProducer = provider.GetService<IRabbitMqProducer>();

            agnosticProducer.Should().NotBeNull();
            specificProducer.Should().NotBeNull();
            agnosticProducer.Should().BeSameAs(specificProducer);
        }

        [Fact]
        public void Given_AddRabbitMqConsumer_Should_Register_HostedService_And_Handler()
        {
            var services = new ServiceCollection();
            services.AddRabbitMqMessaging(opts => opts.HostName = "localhost");

            services.AddRabbitMqConsumer<SampleOrderEvent, SampleOrderHandler>("custom.queue", "custom.key");
            var provider = services.BuildServiceProvider();

            var handler = provider.GetService<SampleOrderHandler>();
            var hostedServices = provider.GetServices<IHostedService>();

            handler.Should().NotBeNull();
            hostedServices.Should().NotBeEmpty();
        }

        [Fact]
        public async Task Given_RabbitMqProducer_PublishAsync_With_Mock_Connection_Should_Publish()
        {
            var mockFactory = new Mock<IConnectionFactory>();
            var mockConnection = new Mock<IConnection>();
            var mockChannel = new Mock<IModel>();
            var mockProperties = new Mock<IBasicProperties>();

            mockChannel.Setup(c => c.IsOpen).Returns(true);
            mockChannel.Setup(c => c.CreateBasicProperties()).Returns(mockProperties.Object);
            mockChannel.Setup(c => c.WaitForConfirms(It.IsAny<TimeSpan>())).Returns(true);
            mockConnection.Setup(c => c.IsOpen).Returns(true);
            mockConnection.Setup(c => c.CreateModel()).Returns(mockChannel.Object);
            mockFactory.Setup(f => f.CreateConnection()).Returns(mockConnection.Object);

            var options = Options.Create(new RabbitMqOptions { ExchangeName = "events.exchange" });
            using var producer = new RabbitMqProducer(options, mockFactory.Object);

            var payload = new SampleOrderEvent("ORD-1", 100m);
            var metadata = new EventMetadata().WithRabbitMqRoutingKey("orders.created");

            var result = await producer.PublishAsync(payload, metadata);

            result.IsSuccess.Should().BeTrue();
            mockChannel.Verify(c => c.BasicPublish(
                "events.exchange",
                "orders.created",
                false,
                mockProperties.Object,
                It.IsAny<ReadOnlyMemory<byte>>()), Times.Once);
        }

        [Fact]
        public async Task Given_RabbitMqProducer_PublishBatchAsync_Should_Publish_All_Messages()
        {
            var mockFactory = new Mock<IConnectionFactory>();
            var mockConnection = new Mock<IConnection>();
            var mockChannel = new Mock<IModel>();
            var mockProperties = new Mock<IBasicProperties>();

            mockChannel.Setup(c => c.IsOpen).Returns(true);
            mockChannel.Setup(c => c.CreateBasicProperties()).Returns(mockProperties.Object);
            mockChannel.Setup(c => c.WaitForConfirms(It.IsAny<TimeSpan>())).Returns(true);
            mockConnection.Setup(c => c.IsOpen).Returns(true);
            mockConnection.Setup(c => c.CreateModel()).Returns(mockChannel.Object);
            mockFactory.Setup(f => f.CreateConnection()).Returns(mockConnection.Object);

            var options = Options.Create(new RabbitMqOptions());
            using var producer = new RabbitMqProducer(options, mockFactory.Object);

            var list = new[]
            {
                new SampleOrderEvent("ORD-1", 10m),
                new SampleOrderEvent("ORD-2", 20m)
            };

            var result = await producer.PublishBatchAsync(list);

            result.IsSuccess.Should().BeTrue();
            mockChannel.Verify(c => c.BasicPublish(
                It.IsAny<string>(),
                It.IsAny<string>(),
                false,
                mockProperties.Object,
                It.IsAny<ReadOnlyMemory<byte>>()), Times.Exactly(2));
        }

        [Fact]
        public void Given_RabbitMqConsumer_With_Options_Should_Initialize_Successfully()
        {
            var services = new ServiceCollection();
            services.AddScoped<SampleOrderHandler>();
            var sp = services.BuildServiceProvider();

            var options = Options.Create(new RabbitMqOptions { RetryCount = 4 });
            using var consumer = new TL.Messaging.RabbitMQ.Consumer.RabbitMqConsumer<SampleOrderEvent, SampleOrderHandler>(
                options,
                sp,
                Mock.Of<IConnectionFactory>(),
                queueName: "orders.queue",
                routingKey: "orders.key");

            consumer.Should().NotBeNull();
        }
    }
}
