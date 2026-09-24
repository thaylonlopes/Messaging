using System;
using TL.BaseContracts.Messaging;
using TL.Messaging.RabbitMQ.Configuration;
using TL.Messaging.RabbitMQ.Consumer;
using TL.Messaging.RabbitMQ.Dlq;
using TL.Messaging.RabbitMQ.Producer;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace TL.Messaging.RabbitMQ.Extensions
{
    /// <summary>
    /// Métodos de extensão para registro de serviços de mensageria com RabbitMQ no container de injeção de dependência.
    /// </summary>
    public static class RabbitMqServiceExtensions
    {
        /// <summary>
        /// Registra a infraestrutura de mensageria com RabbitMQ e o produtor agnóstico <see cref="IEventProducer"/>.
        /// </summary>
        /// <param name="services">Coleção de serviços da aplicação.</param>
        /// <param name="configuration">Configuração da aplicação para leitura da seção "RabbitMq".</param>
        /// <returns>A própria coleção de serviços para encadeamento fluente.</returns>
        public static IServiceCollection AddRabbitMqMessaging(
            this IServiceCollection services,
            IConfiguration configuration)
        {
            if (services == null) throw new ArgumentNullException(nameof(services));
            if (configuration == null) throw new ArgumentNullException(nameof(configuration));

            services.Configure<RabbitMqOptions>(configuration.GetSection(RabbitMqOptions.SectionName));

            services.TryAddSingleton<IRabbitMqProducer, RabbitMqProducer>();
            services.TryAddSingleton<IEventProducer>(sp => sp.GetRequiredService<IRabbitMqProducer>());
            services.TryAddSingleton<IRabbitMqDlqManager, RabbitMqDlqManager>();

            return services;
        }

        /// <summary>
        /// Registra a infraestrutura de mensageria com RabbitMQ utilizando uma action de configuração manual.
        /// </summary>
        /// <param name="services">Coleção de serviços.</param>
        /// <param name="configure">Ação de configuração das opções do RabbitMQ.</param>
        /// <returns>A própria coleção de serviços.</returns>
        public static IServiceCollection AddRabbitMqMessaging(
            this IServiceCollection services,
            Action<RabbitMqOptions> configure)
        {
            if (services == null) throw new ArgumentNullException(nameof(services));
            if (configure == null) throw new ArgumentNullException(nameof(configure));

            services.Configure(configure);

            services.TryAddSingleton<IRabbitMqProducer, RabbitMqProducer>();
            services.TryAddSingleton<IEventProducer>(sp => sp.GetRequiredService<IRabbitMqProducer>());
            services.TryAddSingleton<IRabbitMqDlqManager, RabbitMqDlqManager>();

            return services;
        }

        /// <summary>
        /// Registra um manipulador de eventos e seu respectivo consumidor em background no RabbitMQ.
        /// </summary>
        /// <typeparam name="TEvent">Tipo do evento de negócio.</typeparam>
        /// <typeparam name="THandler">Tipo do manipulador que implementa <see cref="IEventHandler{TEvent}"/>.</typeparam>
        /// <param name="services">Coleção de serviços.</param>
        /// <param name="queueName">Nome customizado da fila (opcional).</param>
        /// <param name="routingKey">Chave de roteamento customizada (opcional).</param>
        /// <returns>A própria coleção de serviços.</returns>
        public static IServiceCollection AddRabbitMqConsumer<TEvent, THandler>(
            this IServiceCollection services,
            string? queueName = null,
            string? routingKey = null)
            where TEvent : class
            where THandler : class, IEventHandler<TEvent>
        {
            if (services == null) throw new ArgumentNullException(nameof(services));

            services.AddScoped<THandler>();
            services.AddHostedService(sp => new RabbitMqConsumer<TEvent, THandler>(
                options: sp.GetRequiredService<Microsoft.Extensions.Options.IOptions<RabbitMqOptions>>(),
                serviceProvider: sp,
                connectionFactory: null,
                logger: sp.GetService<Microsoft.Extensions.Logging.ILogger<RabbitMqConsumer<TEvent, THandler>>>(),
                queueName: queueName,
                routingKey: routingKey));

            return services;
        }
    }
}

