using System;
using System.Collections.Generic;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Moq;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using TL.BaseContracts;
using TL.BaseContracts.Messaging;
using TL.Messaging.RabbitMQ.Configuration;
using TL.Messaging.RabbitMQ.Consumer;
using TL.Messaging.RabbitMQ.Dlq;
using Xunit;

namespace TL.Messaging.RabbitMQ.Tests
{
    public class FailingOrderHandler : IEventHandler<SampleOrderEvent>
    {
        public Task<Result> HandleAsync(EventMessage<SampleOrderEvent> eventMessage, CancellationToken cancellationToken)
        {
            throw new InvalidOperationException("Falha persistente no processamento do pedido");
        }
    }

    public class RabbitMqDlqTests
    {
        [Fact]
        public async Task Given_PoisonMessage_With_MalformedJson_Should_Send_Directly_To_Dlq_With_Diagnostic_Headers_And_Ack()
        {
            var services = new ServiceCollection();
            services.AddScoped<SampleOrderHandler>();
            var sp = services.BuildServiceProvider();

            var options = Options.Create(new RabbitMqOptions
            {
                ExchangeName = "amq.topic",
                DeadLetterExchangeSuffix = ".dlx",
                DeadLetterQueueSuffix = ".dlq"
            });

            var mockChannel = new Mock<IModel>();
            var mockProps = new Mock<IBasicProperties>();
            IDictionary<string, object>? capturedHeaders = null;

            mockProps.SetupSet(p => p.Headers = It.IsAny<IDictionary<string, object>>())
                .Callback<IDictionary<string, object>>(h => capturedHeaders = h);
            mockChannel.Setup(c => c.IsOpen).Returns(true);
            mockChannel.Setup(c => c.CreateBasicProperties()).Returns(mockProps.Object);

            using var consumer = new RabbitMqConsumer<SampleOrderEvent, SampleOrderHandler>(
                options,
                sp,
                connectionFactory: null,
                routingKey: "app.sampleorderevent");

            consumer.SetChannelForTesting(mockChannel.Object);

            var rawMalformedBytes = Encoding.UTF8.GetBytes("{ este payload nao e um json valido!");
            var originalProps = new Mock<IBasicProperties>();
            originalProps.Setup(p => p.CorrelationId).Returns("corr-12345");

            var eventArgs = new BasicDeliverEventArgs(
                consumerTag: "test-consumer",
                deliveryTag: 999,
                redelivered: false,
                exchange: "amq.topic",
                routingKey: "app.sampleorderevent",
                properties: originalProps.Object,
                body: rawMalformedBytes);

            await consumer.ProcessMessageAsync(eventArgs, CancellationToken.None);

            mockChannel.Verify(c => c.BasicPublish(
                "amq.topic.dlx",
                "app.sampleorderevent",
                false,
                mockProps.Object,
                It.IsAny<ReadOnlyMemory<byte>>()), Times.Once);

            mockChannel.Verify(c => c.BasicAck(999, false), Times.Once);
            mockChannel.Verify(c => c.BasicNack(It.IsAny<ulong>(), It.IsAny<bool>(), It.IsAny<bool>()), Times.Never);

            capturedHeaders.Should().NotBeNull();
            capturedHeaders![MessagingDiagnosticHeaders.ExceptionType].ToString().Should().Contain("JsonException");
            capturedHeaders[MessagingDiagnosticHeaders.RetryCount].ToString().Should().Be("0");
            capturedHeaders[MessagingDiagnosticHeaders.ExceptionMessage].ToString().Should().NotBeNullOrWhiteSpace();
            capturedHeaders[MessagingDiagnosticHeaders.FailedAtUtc].ToString().Should().NotBeNullOrWhiteSpace();
            capturedHeaders[MessagingDiagnosticHeaders.TraceParent].ToString().Should().Be("corr-12345");
        }

        [Fact]
        public async Task Given_Handler_Exception_Exhausting_Retries_Should_Send_To_Dlq_With_Diagnostic_Headers_And_Ack()
        {
            var services = new ServiceCollection();
            services.AddScoped<FailingOrderHandler>();
            var sp = services.BuildServiceProvider();

            var options = Options.Create(new RabbitMqOptions
            {
                ExchangeName = "amq.topic",
                RetryCount = 3,
                DeadLetterExchangeSuffix = ".dlx",
                DeadLetterQueueSuffix = ".dlq"
            });

            var mockChannel = new Mock<IModel>();
            var mockProps = new Mock<IBasicProperties>();
            IDictionary<string, object>? capturedHeaders = null;

            mockProps.SetupSet(p => p.Headers = It.IsAny<IDictionary<string, object>>())
                .Callback<IDictionary<string, object>>(h => capturedHeaders = h);
            mockChannel.Setup(c => c.IsOpen).Returns(true);
            mockChannel.Setup(c => c.CreateBasicProperties()).Returns(mockProps.Object);

            using var consumer = new RabbitMqConsumer<SampleOrderEvent, FailingOrderHandler>(
                options,
                sp,
                connectionFactory: null,
                routingKey: "app.sampleorderevent");

            consumer.SetChannelForTesting(mockChannel.Object);

            var eventMessage = EventMessage<SampleOrderEvent>.Create(new SampleOrderEvent("ORD-999", 500m));
            var validJsonBytes = JsonSerializer.SerializeToUtf8Bytes(eventMessage);

            var originalProps = new Mock<IBasicProperties>();
            originalProps.Setup(p => p.CorrelationId).Returns("trace-parent-abc");

            var eventArgs = new BasicDeliverEventArgs(
                consumerTag: "test-consumer",
                deliveryTag: 1001,
                redelivered: false,
                exchange: "amq.topic",
                routingKey: "app.sampleorderevent",
                properties: originalProps.Object,
                body: validJsonBytes);

            await consumer.ProcessMessageAsync(eventArgs, CancellationToken.None);

            mockChannel.Verify(c => c.BasicPublish(
                "amq.topic.dlx",
                "app.sampleorderevent",
                false,
                mockProps.Object,
                It.IsAny<ReadOnlyMemory<byte>>()), Times.Once);

            mockChannel.Verify(c => c.BasicAck(1001, false), Times.Once);
            mockChannel.Verify(c => c.BasicNack(It.IsAny<ulong>(), It.IsAny<bool>(), It.IsAny<bool>()), Times.Never);

            capturedHeaders.Should().NotBeNull();
            capturedHeaders![MessagingDiagnosticHeaders.ExceptionType].ToString().Should().Be("InvalidOperationException");
            capturedHeaders[MessagingDiagnosticHeaders.RetryCount].ToString().Should().Be("3");
            capturedHeaders[MessagingDiagnosticHeaders.ExceptionMessage].ToString().Should().Be("Falha persistente no processamento do pedido");
            capturedHeaders[MessagingDiagnosticHeaders.FailedAtUtc].ToString().Should().NotBeNullOrWhiteSpace();
            capturedHeaders[MessagingDiagnosticHeaders.TraceParent].ToString().Should().Be("trace-parent-abc");
        }

        [Fact]
        public async Task Given_RabbitMqDlqManager_ReplayAsync_Should_Read_From_Dlq_Expunge_Diagnostic_Headers_And_Republish()
        {
            var mockFactory = new Mock<IConnectionFactory>();
            var mockConnection = new Mock<IConnection>();
            var mockChannel = new Mock<IModel>();

            mockConnection.Setup(c => c.IsOpen).Returns(true);
            mockConnection.Setup(c => c.CreateModel()).Returns(mockChannel.Object);
            mockFactory.Setup(f => f.CreateConnection()).Returns(mockConnection.Object);

            var initialHeaders = new Dictionary<string, object>
            {
                { MessagingDiagnosticHeaders.ExceptionMessage, "Erro fatal anterior" },
                { MessagingDiagnosticHeaders.ExceptionType, "InvalidOperationException" },
                { MessagingDiagnosticHeaders.RetryCount, "3" },
                { MessagingDiagnosticHeaders.FailedAtUtc, DateTimeOffset.UtcNow.ToString("O") },
                { "x-custom-tenant", "tenant-alpha" }
            };

            var deadProps = new Mock<IBasicProperties>();
            deadProps.Setup(p => p.Headers).Returns(initialHeaders);
            deadProps.Setup(p => p.CorrelationId).Returns("corr-xyz");

            var messageBytes = Encoding.UTF8.GetBytes("{\"OrderId\":\"ORD-1\"}");
            var basicGetResult = new BasicGetResult(
                deliveryTag: 55,
                redelivered: false,
                exchange: "amq.topic.dlx",
                routingKey: "app.orders",
                messageCount: 0,
                basicProperties: deadProps.Object,
                body: messageBytes);

            mockChannel.Setup(c => c.IsOpen).Returns(true);
            mockChannel.SetupSequence(c => c.BasicGet("app.orders.dlq", false))
                .Returns(basicGetResult)
                .Returns((BasicGetResult)null!);

            var cleanPropsMock = new Mock<IBasicProperties>();
            IDictionary<string, object>? replayedHeaders = null;
            cleanPropsMock.SetupSet(p => p.Headers = It.IsAny<IDictionary<string, object>>())
                .Callback<IDictionary<string, object>>(h => replayedHeaders = h);
            mockChannel.Setup(c => c.CreateBasicProperties()).Returns(cleanPropsMock.Object);

            var options = Options.Create(new RabbitMqOptions { DeadLetterQueueSuffix = ".dlq" });
            using var dlqManager = new RabbitMqDlqManager(options, mockFactory.Object);

            int replayedCount = await dlqManager.ReplayAsync("app.orders.dlq", maxMessages: 50);

            replayedCount.Should().Be(1);
            mockChannel.Verify(c => c.BasicPublish(
                string.Empty,
                "app.orders",
                false,
                cleanPropsMock.Object,
                It.IsAny<ReadOnlyMemory<byte>>()), Times.Once);

            mockChannel.Verify(c => c.BasicAck(55, false), Times.Once);

            replayedHeaders.Should().NotBeNull();
            replayedHeaders!.ContainsKey(MessagingDiagnosticHeaders.ExceptionMessage).Should().BeFalse();
            replayedHeaders.ContainsKey(MessagingDiagnosticHeaders.ExceptionType).Should().BeFalse();
            replayedHeaders.ContainsKey(MessagingDiagnosticHeaders.RetryCount).Should().BeFalse();
            replayedHeaders.ContainsKey(MessagingDiagnosticHeaders.FailedAtUtc).Should().BeFalse();
            replayedHeaders.ContainsKey("x-custom-tenant").Should().BeTrue();
            replayedHeaders["x-custom-tenant"].Should().Be("tenant-alpha");
        }

        [Fact]
        public async Task Given_RabbitMqDlqManager_With_Empty_Dlq_Should_Return_Zero()
        {
            var mockFactory = new Mock<IConnectionFactory>();
            var mockConnection = new Mock<IConnection>();
            var mockChannel = new Mock<IModel>();

            mockConnection.Setup(c => c.IsOpen).Returns(true);
            mockConnection.Setup(c => c.CreateModel()).Returns(mockChannel.Object);
            mockFactory.Setup(f => f.CreateConnection()).Returns(mockConnection.Object);

            mockChannel.Setup(c => c.IsOpen).Returns(true);
            mockChannel.Setup(c => c.BasicGet("app.orders.dlq", false)).Returns((BasicGetResult)null!);

            var options = Options.Create(new RabbitMqOptions());
            using var dlqManager = new RabbitMqDlqManager(options, mockFactory.Object);

            int count = await dlqManager.ReplayAsync("app.orders.dlq", maxMessages: 10);

            count.Should().Be(0);
            mockChannel.Verify(c => c.BasicPublish(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<bool>(), It.IsAny<IBasicProperties>(), It.IsAny<ReadOnlyMemory<byte>>()), Times.Never);
        }
    }
}
