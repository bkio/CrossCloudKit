// Copyright (c) 2022- Burak Kara, MIT License
// See LICENSE file in the project root for full license information.

using System.Net;

namespace CrossCloudKit.Interfaces.Classes;

/// <summary>
/// Represents the result of a cloud service operation.
/// </summary>
/// <typeparam name="T">The type of data returned by the operation</typeparam>
/// <remarks>
/// <para>
/// Every CrossCloudKit service method returns <c>OperationResult&lt;T&gt;</c>. Always check
/// <see cref="IsSuccessful"/> before accessing <see cref="Data"/> — accessing <c>Data</c> on a
/// failed result throws <see cref="InvalidOperationException"/>.
/// </para>
/// <para>
/// Instances are created exclusively via the <see cref="Success"/> and <see cref="Failure"/> static
/// factory methods. The constructor is private.
/// </para>
/// </remarks>
/// <example>
/// <code>
/// var result = await dbService.GetItemAsync("Users", key);
///
/// if (result.IsSuccessful)
/// {
///     var item = result.Data; // safe to access
///     Console.WriteLine(item?["Name"]);
/// }
/// else
/// {
///     Console.WriteLine($"Error ({result.StatusCode}): {result.ErrorMessage}");
/// }
/// </code>
/// </example>
public class OperationResult<T>
{
    /// <summary>
    /// Gets whether the operation was successful.
    /// </summary>
    /// <remarks>Always check this before accessing <see cref="Data"/>.</remarks>
    public bool IsSuccessful { get; }

    /// <summary>
    /// Gets the HTTP status code associated with the operation result.
    /// </summary>
    /// <remarks>
    /// Common codes: <c>200 OK</c> = success, <c>404 NotFound</c> = item/resource not found,
    /// <c>409 Conflict</c> = item already exists (when <c>overwriteIfExists</c> is false),
    /// <c>412 PreconditionFailed</c> = condition check failed,
    /// <c>500 InternalServerError</c> = unexpected error.
    /// </remarks>
    public HttpStatusCode StatusCode { get; }

    /// <summary>
    /// Gets the data returned by the operation, if successful.
    /// </summary>
    /// <remarks>Throws <see cref="InvalidOperationException"/> if <see cref="IsSuccessful"/> is <c>false</c>. Always check <see cref="IsSuccessful"/> first.</remarks>
    public T Data =>
        !IsSuccessful
            ? throw new InvalidOperationException("Operation failed.")
            // ReSharper disable once NullableWarningSuppressionIsUsed
            : _data!;

    /// <summary>
    /// Gets the error message if the operation failed.
    /// </summary>
    /// <remarks>Returns an empty string when <see cref="IsSuccessful"/> is <c>true</c>.</remarks>
    public string ErrorMessage
    {
        get
        {
            if (!IsSuccessful)
            {
                return _errorMessage ?? "";
            }
            return "";
        }
    }

    private OperationResult(bool isSuccessful, HttpStatusCode statusCode, T? data, string? errorMessage)
    {
        IsSuccessful = isSuccessful;
        StatusCode = statusCode;
        _data = data;
        _errorMessage = errorMessage;
    }

    private readonly T? _data;
    private readonly string? _errorMessage;

    /// <summary>
    /// Creates a successful result with data.
    /// </summary>
    /// <param name="data">The operation result data</param>
    /// <param name="statusCode">Success http status code</param>
    /// <returns>A successful OperationResult</returns>
    /// <example>
    /// <code>
    /// return OperationResult&lt;string&gt;.Success("done");
    /// return OperationResult&lt;int&gt;.Success(42, HttpStatusCode.Created);
    /// </code>
    /// </example>
    public static OperationResult<T> Success(T data, HttpStatusCode statusCode = HttpStatusCode.OK) => new(true, statusCode, data, "");

    /// <summary>
    /// Creates a failed result with an error message.
    /// </summary>
    /// <param name="errorMessage">The error message</param>
    /// <param name="statusCode">Failure http status code</param>
    /// <returns>A failed OperationResult</returns>
    /// <example>
    /// <code>
    /// return OperationResult&lt;bool&gt;.Failure("Item not found", HttpStatusCode.NotFound);
    /// </code>
    /// </example>
    public static OperationResult<T> Failure(string errorMessage, HttpStatusCode statusCode) => new(false, statusCode, default, errorMessage);
}
