using System.Threading;
using System.Threading.Tasks;


namespace TL.Messaging.Abstractions
{
    /// <summary>
    /// Contrato agnóstico (Port) para consumidores de eventos assíncronos.
    /// </summary>
    /// <typeparam name="T">O tipo do payload de dados contido no evento.</typeparam>
    /// <remarks>
    /// Os manipuladores implementam esta interface para processar eventos recebidos de qualquer broker.
    /// Retornar <c>Result.Success()</c> sinaliza confirmação (ACK),
    /// enquanto <c>Result.Failure(...)</c> direciona o evento para retentativa ou Dead-Letter Queue (DLQ).
    /// </remarks>
    /// <example>
    /// <code>
    /// public class OrderCreatedHandler : IEventHandler&lt;OrderCreatedEvent&gt;
    /// {
    ///     public async Task&lt;Result&gt; HandleAsync(EventMessage&lt;OrderCreatedEvent&gt; message, CancellationToken ct)
    ///     {
    ///         // Lógica de negócio
    ///         return Result.Success();
    ///     }
    /// }
    /// </code>
    /// </example>
    public interface IEventHandler<T> where T : class
    {
        /// <summary>
        /// Processa o evento recebido.
        /// </summary>
        /// <param name="eventMessage">Envelope completo contendo metadados, identificadores e o payload de negócio.</param>
        /// <param name="cancellationToken">Token de cancelamento da operação.</param>
        /// <returns>Resultado da execução indicando sucesso ou falha no processamento.</returns>
        Task<Result> HandleAsync(EventMessage<T> eventMessage, CancellationToken cancellationToken);
    }
}

