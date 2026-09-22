using System;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using FluentAssertions;
using Microsoft.Extensions.Options;
using Moq;
using TL.BaseContracts.Messaging.Attributes;
using TL.Messaging.Kafka.Configuration;
using TL.Messaging.Kafka.Producer;
using Xunit;

namespace TL.Messaging.Kafka.Tests
{
    public class PedidoComChaveEvent
    {
        [PartitionKey]
        public Guid PedidoId { get; set; } = Guid.Parse("11111111-2222-3333-4444-555555555555");

        public decimal Valor { get; set; } = 250.75m;
    }

    public class PedidoSemChaveEvent
    {
        public string Descricao { get; set; } = "Sem Chave de Particao";
    }

    public class KafkaErgonomicPublishTests
    {
        [Fact]
        public async Task Given_Event_With_PartitionKey_PublishAsync_One_Line_Should_Infer_Topic_And_Route_Key()
        {
            var mockProducer = new Mock<IProducer<string, string>>();
            mockProducer.Setup(p => p.ProduceAsync(
                It.IsAny<string>(),
                It.IsAny<Message<string, string>>(),
                It.IsAny<CancellationToken>()))
                .ReturnsAsync(new DeliveryResult<string, string> { Partition = new Partition(0), Offset = new Offset(1) });

            var options = Options.Create(new KafkaOptions());
            using var producer = new KafkaProducer(options, mockProducer.Object);

            var pedido = new PedidoComChaveEvent();

            await producer.PublishAsync(pedido);

            mockProducer.Verify(p => p.ProduceAsync(
                "pedido-com-chave",
                It.Is<Message<string, string>>(m => m.Key == "11111111-2222-3333-4444-555555555555"),
                It.IsAny<CancellationToken>()), Times.Once);
        }

        [Fact]
        public async Task Given_Event_Without_PartitionKey_PublishAsync_Should_Use_Null_Key_For_Safe_RoundRobin()
        {
            var mockProducer = new Mock<IProducer<string, string>>();
            mockProducer.Setup(p => p.ProduceAsync(
                It.IsAny<string>(),
                It.IsAny<Message<string, string>>(),
                It.IsAny<CancellationToken>()))
                .ReturnsAsync(new DeliveryResult<string, string>());

            var options = Options.Create(new KafkaOptions());
            using var producer = new KafkaProducer(options, mockProducer.Object);

            var pedido = new PedidoSemChaveEvent();

            await producer.PublishAsync(pedido);

            mockProducer.Verify(p => p.ProduceAsync(
                "pedido-sem-chave",
                It.Is<Message<string, string>>(m => m.Key == null),
                It.IsAny<CancellationToken>()), Times.Once);
        }

        [Fact]
        public async Task Given_Custom_Topic_PublishAsync_Should_Send_To_Specified_Topic()
        {
            var mockProducer = new Mock<IProducer<string, string>>();
            mockProducer.Setup(p => p.ProduceAsync(
                It.IsAny<string>(),
                It.IsAny<Message<string, string>>(),
                It.IsAny<CancellationToken>()))
                .ReturnsAsync(new DeliveryResult<string, string>());

            var options = Options.Create(new KafkaOptions());
            using var producer = new KafkaProducer(options, mockProducer.Object);

            var pedido = new PedidoComChaveEvent();

            await producer.PublishAsync("pedidos.criados.v1", pedido);

            mockProducer.Verify(p => p.ProduceAsync(
                "pedidos.criados.v1",
                It.Is<Message<string, string>>(m => m.Key == "11111111-2222-3333-4444-555555555555"),
                It.IsAny<CancellationToken>()), Times.Once);
        }

        [Fact]
        public async Task Given_Broker_Delivery_Error_PublishAsync_Should_Throw_InvalidOperationException()
        {
            var mockProducer = new Mock<IProducer<string, string>>();
            mockProducer.Setup(p => p.ProduceAsync(
                It.IsAny<string>(),
                It.IsAny<Message<string, string>>(),
                It.IsAny<CancellationToken>()))
                .ThrowsAsync(new ProduceException<string, string>(new Confluent.Kafka.Error(ErrorCode.BrokerNotAvailable, "Broker offline"), new DeliveryResult<string, string>()));

            var options = Options.Create(new KafkaOptions());
            using var producer = new KafkaProducer(options, mockProducer.Object);

            var pedido = new PedidoComChaveEvent();

            Func<Task> act = () => producer.PublishAsync(pedido);

            await act.Should().ThrowAsync<InvalidOperationException>()
                .WithMessage("*Falha ao publicar mensagem no Kafka*");
        }

        [Fact]
        public async Task Given_Null_Message_PublishAsync_Should_Throw_ArgumentNullException()
        {
            var options = Options.Create(new KafkaOptions());
            using var producer = new KafkaProducer(options, Mock.Of<IProducer<string, string>>());

            PedidoComChaveEvent? pedido = null;

            Func<Task> act = () => producer.PublishAsync(pedido!);

            await act.Should().ThrowAsync<ArgumentNullException>();
        }

        [Fact]
        public async Task Given_KafkaEventProducer_Derived_Class_Should_Function_Identically()
        {
            var mockProducer = new Mock<IProducer<string, string>>();
            mockProducer.Setup(p => p.ProduceAsync(
                It.IsAny<string>(),
                It.IsAny<Message<string, string>>(),
                It.IsAny<CancellationToken>()))
                .ReturnsAsync(new DeliveryResult<string, string>());

            var options = Options.Create(new KafkaOptions());
            using var eventProducer = new KafkaEventProducer(options, mockProducer.Object);

            var pedido = new PedidoComChaveEvent();

            await eventProducer.PublishAsync(pedido);

            mockProducer.Verify(p => p.ProduceAsync(
                "pedido-com-chave",
                It.Is<Message<string, string>>(m => m.Key == "11111111-2222-3333-4444-555555555555"),
                It.IsAny<CancellationToken>()), Times.Once);
        }
    }
}
