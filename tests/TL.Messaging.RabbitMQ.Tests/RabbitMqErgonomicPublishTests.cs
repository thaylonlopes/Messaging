using System;
using System.Threading;
using System.Threading.Tasks;
using FluentAssertions;
using Microsoft.Extensions.Options;
using Moq;
using RabbitMQ.Client;
using TL.Messaging.RabbitMQ.Configuration;
using TL.Messaging.RabbitMQ.Producer;
using Xunit;

namespace TL.Messaging.RabbitMQ.Tests
{
    public class PedidoRabbitMqEvent
    {
        public Guid PedidoId { get; set; } = Guid.NewGuid();
        public decimal ValorTotal { get; set; } = 450.00m;
    }

    public class RabbitMqErgonomicPublishTests
    {
        [Fact]
        public async Task Given_Event_PublishAsync_One_Line_Should_Infer_Exchange_And_RoutingKey()
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

            var options = Options.Create(new RabbitMqOptions { ExchangeName = "pedidos.topic" });
            using var producer = new RabbitMqProducer(options, mockFactory.Object);

            var pedido = new PedidoRabbitMqEvent();

            await producer.PublishAsync(pedido);

            mockChannel.Verify(c => c.BasicPublish(
                "pedidos.topic",
                "pedido-rabbit-mq",
                false,
                mockProperties.Object,
                It.IsAny<ReadOnlyMemory<byte>>()), Times.Once);
        }

        [Fact]
        public async Task Given_Custom_Exchange_PublishAsync_Should_Publish_To_Specified_Exchange()
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

            var pedido = new PedidoRabbitMqEvent();

            await producer.PublishAsync("custom.direct", pedido);

            mockChannel.Verify(c => c.BasicPublish(
                "custom.direct",
                "pedido-rabbit-mq",
                false,
                mockProperties.Object,
                It.IsAny<ReadOnlyMemory<byte>>()), Times.Once);
        }

        [Fact]
        public async Task Given_Broker_Nack_PublishAsync_Should_Throw_InvalidOperationException()
        {
            var mockFactory = new Mock<IConnectionFactory>();
            var mockConnection = new Mock<IConnection>();
            var mockChannel = new Mock<IModel>();
            var mockProperties = new Mock<IBasicProperties>();

            mockChannel.Setup(c => c.IsOpen).Returns(true);
            mockChannel.Setup(c => c.CreateBasicProperties()).Returns(mockProperties.Object);
            mockChannel.Setup(c => c.WaitForConfirms(It.IsAny<TimeSpan>())).Returns(false);
            mockConnection.Setup(c => c.IsOpen).Returns(true);
            mockConnection.Setup(c => c.CreateModel()).Returns(mockChannel.Object);
            mockFactory.Setup(f => f.CreateConnection()).Returns(mockConnection.Object);

            var options = Options.Create(new RabbitMqOptions());
            using var producer = new RabbitMqProducer(options, mockFactory.Object);

            var pedido = new PedidoRabbitMqEvent();

            Func<Task> act = () => producer.PublishAsync(pedido);

            await act.Should().ThrowAsync<InvalidOperationException>()
                .WithMessage("*Falha ao publicar mensagem no RabbitMQ*");
        }

        [Fact]
        public async Task Given_RabbitMqEventProducer_Derived_Class_Should_Function_Identically()
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
            using var producer = new RabbitMqEventProducer(options, mockFactory.Object);

            var pedido = new PedidoRabbitMqEvent();

            await producer.PublishAsync(pedido);

            mockChannel.Verify(c => c.BasicPublish(
                "events.exchange",
                "pedido-rabbit-mq",
                false,
                mockProperties.Object,
                It.IsAny<ReadOnlyMemory<byte>>()), Times.Once);
        }
    }
}
