using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace TL.Messaging.Abstractions
{


    /// <summary>
    /// Categorização semântica de erros para simplificar o mapeamento em status codes HTTP e logs.
    /// </summary>
    public enum ErrorType
    {
        /// <summary>
        /// Erro genérico ou inesperado no processamento (mapeado tipicamente para 500 Internal Server Error).
        /// </summary>
        Failure = 0,

        /// <summary>
        /// Erro de validação de dados de entrada ou parâmetros (mapeado tipicamente para 400 Bad Request).
        /// </summary>
        Validation = 1,

        /// <summary>
        /// Recurso solicitado não foi encontrado (mapeado tipicamente para 404 Not Found).
        /// </summary>
        NotFound = 2,

        /// <summary>
        /// Conflito de estado ou duplicidade de recurso (mapeado tipicamente para 409 Conflict).
        /// </summary>
        Conflict = 3,

        /// <summary>
        /// Falha de autenticação ou credenciais inválidas (mapeado tipicamente para 401 Unauthorized).
        /// </summary>
        Unauthorized = 4,

        /// <summary>
        /// Acesso negado por falta de permissão ou privilégios (mapeado tipicamente para 403 Forbidden).
        /// </summary>
        Forbidden = 5
    }

    /// <summary>
    /// Representa um erro tipado de negócio ou infraestrutura de forma imutável e expressiva.
    /// </summary>
    /// <remarks>
    /// Substitui o uso de exceções para fluxo de controle, fornecendo código de erro estável, mensagem clara e categoria semântica.
    /// </remarks>
    public record Error
    {
        /// <summary>
        /// Representa a ausência de erro (operação com sucesso).
        /// </summary>
        public static readonly Error None = new Error(string.Empty, string.Empty, ErrorType.Failure);

        /// <summary>
        /// Código textual padronizado do erro (ex: "User.NotFound", "Order.InvalidAmount").
        /// </summary>
        public string Code { get; }

        /// <summary>
        /// Descrição legível e humanizada do motivo da falha.
        /// </summary>
        public string Message { get; }

        /// <summary>
        /// Categoria semântica do erro para roteamento HTTP e observabilidade.
        /// </summary>
        public ErrorType Type { get; }

        /// <summary>
        /// Inicializa uma nova instância de <see cref="Error"/>.
        /// </summary>
        /// <param name="code">Código único do erro.</param>
        /// <param name="message">Mensagem humanizada explicando o erro.</param>
        /// <param name="type">Categoria semântica do erro.</param>
        public Error(string code, string message, ErrorType type = ErrorType.Failure)
        {
            Code = code ?? string.Empty;
            Message = message ?? string.Empty;
            Type = type;
        }

        /// <summary>
        /// Cria um erro de falha genérica.
        /// </summary>
        public static Error Failure(string code, string message) => new Error(code, message, ErrorType.Failure);

        /// <summary>
        /// Cria um erro de validação de entrada (400).
        /// </summary>
        public static Error Validation(string code, string message) => new Error(code, message, ErrorType.Validation);

        /// <summary>
        /// Cria um erro de recurso não encontrado (404).
        /// </summary>
        public static Error NotFound(string code, string message) => new Error(code, message, ErrorType.NotFound);

        /// <summary>
        /// Cria um erro de conflito ou duplicidade (409).
        /// </summary>
        public static Error Conflict(string code, string message) => new Error(code, message, ErrorType.Conflict);

        /// <summary>
        /// Cria um erro de não autenticado (401).
        /// </summary>
        public static Error Unauthorized(string code, string message) => new Error(code, message, ErrorType.Unauthorized);

        /// <summary>
        /// Cria um erro de acesso proibido (403).
        /// </summary>
        public static Error Forbidden(string code, string message) => new Error(code, message, ErrorType.Forbidden);

        /// <summary>
        /// Conversão implícita de string para um <see cref="Error"/> de validação.
        /// </summary>
        public static implicit operator Error(string message) => Validation("General.Validation", message);
    }

    /// <summary>
    /// Representa o resultado de uma operação sem retorno de valor, indicando sucesso ou falha com erro tipado.
    /// </summary>
    public class Result
    {
        /// <summary>
        /// Indica se a operação foi executada com sucesso.
        /// </summary>
        public bool IsSuccess { get; }

        /// <summary>
        /// Indica se a operação resultou em falha.
        /// </summary>
        public bool IsFailure => !IsSuccess;

        /// <summary>
        /// Objeto de erro tipado associado à falha (retorna <see cref="Error.None"/> se a operação foi bem-sucedida).
        /// </summary>
        public Error Error { get; }

        /// <summary>
        /// Inicializa o resultado com status de sucesso e sem erro.
        /// </summary>
        protected Result()
        {
            IsSuccess = true;
            Error = Error.None;
        }

        /// <summary>
        /// Inicializa o resultado indicando falha e associando o erro tipado correspondente.
        /// </summary>
        /// <param name="error">O erro ocorrido.</param>
        protected Result(Error error)
        {
            if (error == null || error == Error.None)
            {
                throw new ArgumentException("Um resultado com falha deve conter um erro válido e não nulo.", nameof(error));
            }
            IsSuccess = false;
            Error = error;
        }

        /// <summary>
        /// Cria um resultado bem-sucedido.
        /// </summary>
        public static Result Success() => new Result();

        /// <summary>
        /// Cria um resultado bem-sucedido com valor tipado de retorno.
        /// </summary>
        /// <typeparam name="TValue">Tipo do valor retornado.</typeparam>
        /// <param name="value">Valor da operação.</param>
        public static Result<TValue> Success<TValue>(TValue value) => new Result<TValue>(value);

        /// <summary>
        /// Cria um resultado com falha e erro tipado.
        /// </summary>
        /// <param name="error">Detalhes do erro.</param>
        public static Result Failure(Error error) => new Result(error);

        /// <summary>
        /// Cria um resultado com falha para um retorno tipado.
        /// </summary>
        /// <typeparam name="TValue">Tipo esperado em caso de sucesso.</typeparam>
        /// <param name="error">Detalhes do erro.</param>
        public static Result<TValue> Failure<TValue>(Error error) => new Result<TValue>(error);

        /// <summary>
        /// Executa uma das funções com base no sucesso ou na falha do resultado (Pattern Matching funcional).
        /// </summary>
        public TResult Match<TResult>(Func<TResult> onSuccess, Func<Error, TResult> onFailure)
        {
            if (onSuccess == null) throw new ArgumentNullException(nameof(onSuccess));
            if (onFailure == null) throw new ArgumentNullException(nameof(onFailure));

            return IsSuccess ? onSuccess() : onFailure(Error);
        }
    }

    /// <summary>
    /// Representa o resultado de uma operação que retorna um valor tipado <typeparamref name="TValue"/> em caso de sucesso.
    /// </summary>
    /// <typeparam name="TValue">Tipo do valor de sucesso.</typeparam>
    public class Result<TValue> : Result
    {
        private readonly TValue? _value;

        /// <summary>
        /// Obtém o valor retornado pela operação bem-sucedida.
        /// </summary>
        /// <exception cref="InvalidOperationException">Lançada caso tente acessar o valor em um resultado com falha.</exception>
        public TValue Value
        {
            get
            {
                if (IsFailure)
                {
                    throw new InvalidOperationException($"Não é possível acessar o valor de um resultado com falha: '{Error.Message}'.");
                }
                return _value!;
            }
        }

        internal Result(TValue value) : base()
        {
            if (value is null)
            {
                throw new ArgumentNullException(nameof(value), "O valor de um resultado bem-sucedido não pode ser nulo.");
            }
            _value = value;
        }

        internal Result(Error error) : base(error)
        {
            _value = default;
        }

        /// <summary>
        /// Executa uma das funções com base no sucesso ou na falha do resultado (Pattern Matching funcional).
        /// </summary>
        public TResult Match<TResult>(Func<TValue, TResult> onSuccess, Func<Error, TResult> onFailure)
        {
            if (onSuccess == null) throw new ArgumentNullException(nameof(onSuccess));
            if (onFailure == null) throw new ArgumentNullException(nameof(onFailure));

            return IsSuccess ? onSuccess(Value) : onFailure(Error);
        }

        /// <summary>
        /// Transforma o valor de sucesso aplicando uma função mapeadora.
        /// </summary>
        public Result<TResult> Map<TResult>(Func<TValue, TResult> mapper)
        {
            if (mapper == null) throw new ArgumentNullException(nameof(mapper));

            if (IsFailure)
            {
                return Result.Failure<TResult>(Error);
            }

            return Result.Success(mapper(Value));
        }

        /// <summary>
        /// Conversão implícita de um valor <typeparamref name="TValue"/> para <see cref="Result{TValue}"/> com sucesso.
        /// </summary>
        public static implicit operator Result<TValue>(TValue value) => Result.Success(value);

        /// <summary>
        /// Conversão implícita de um <see cref="Error"/> para <see cref="Result{TValue}"/> com falha.
        /// </summary>
        public static implicit operator Result<TValue>(Error error) => Result.Failure<TValue>(error);
    }
}

