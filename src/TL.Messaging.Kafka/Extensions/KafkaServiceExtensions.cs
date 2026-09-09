using System;
using TL.Messaging.Kafka.Configuration;
using TL.Messaging.Kafka.Consumer;
using TL.Messaging.Kafka.Producer;
using TL.Messaging.Abstractions;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace TL.Messaging.Kafka.Extensions
{
    /// <summary>
    /// Métodos de extensão para registro de mensageria com Apache Kafka no container de DI.
    /// </summary>
    public static class KafkaServiceExtensions
    {
        /// <summary>
        /// Registra a infraestrutura do Kafka e o produtor agnóstico <see cref="IEventProducer"/>.
        /// </summary>
        /// <param name="services">Coleção de serviços da aplicação.</param>
        /// <param name="configuration">Configuração da aplicação para leitura da seção "Kafka".</param>
        /// <returns>A própria coleção de serviços para encadeamento fluente.</returns>
        public static IServiceCollection AddKafkaMessaging(
            this IServiceCollection services,
            IConfiguration configuration)
        {
            if (services == null) throw new ArgumentNullException(nameof(services));
            if (configuration == null) throw new ArgumentNullException(nameof(configuration));

            services.Configure<KafkaOptions>(configuration.GetSection(KafkaOptions.SectionName));

            services.TryAddSingleton<IKafkaProducer, KafkaProducer>();
            services.TryAddSingleton<IEventProducer>(sp => sp.GetRequiredService<IKafkaProducer>());

            return services;
        }

        /// <summary>
        /// Registra a infraestrutura do Kafka utilizando uma action de configuração manual.
        /// </summary>
        /// <param name="services">Coleção de serviços.</param>
        /// <param name="configure">Ação de configuração das opções do Kafka.</param>
        /// <returns>A própria coleção de serviços.</returns>
        public static IServiceCollection AddKafkaMessaging(
            this IServiceCollection services,
            Action<KafkaOptions> configure)
        {
            if (services == null) throw new ArgumentNullException(nameof(services));
            if (configure == null) throw new ArgumentNullException(nameof(configure));

            services.Configure(configure);

            services.TryAddSingleton<IKafkaProducer, KafkaProducer>();
            services.TryAddSingleton<IEventProducer>(sp => sp.GetRequiredService<IKafkaProducer>());

            return services;
        }

        /// <summary>
        /// Registra um manipulador de eventos e seu respectivo consumidor em background no Apache Kafka.
        /// </summary>
        /// <typeparam name="TEvent">Tipo do payload de evento.</typeparam>
        /// <typeparam name="THandler">Tipo do manipulador de negócio que implementa <see cref="IEventHandler{TEvent}"/>.</typeparam>
        /// <param name="services">Coleção de serviços.</param>
        /// <param name="topic">Nome customizado do tópico (opcional).</param>
        /// <returns>A própria coleção de serviços.</returns>
        public static IServiceCollection AddKafkaConsumer<TEvent, THandler>(
            this IServiceCollection services,
            string? topic = null)
            where TEvent : class
            where THandler : class, IEventHandler<TEvent>
        {
            if (services == null) throw new ArgumentNullException(nameof(services));

            services.AddScoped<THandler>();
            services.AddHostedService(sp => new KafkaConsumer<TEvent, THandler>(
                options: sp.GetRequiredService<Microsoft.Extensions.Options.IOptions<KafkaOptions>>(),
                serviceProvider: sp,
                dltProducer: sp.GetService<IKafkaProducer>(),
                logger: sp.GetService<Microsoft.Extensions.Logging.ILogger<KafkaConsumer<TEvent, THandler>>>(),
                topic: topic));

            return services;
        }
    }
}

