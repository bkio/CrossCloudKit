// Copyright (c) 2022- Burak Kara, MIT License
// See LICENSE file in the project root for full license information.

using System.Net;

namespace CrossCloudKit.Interfaces.Classes;

/// <summary>
/// Represents the result of a cloud service operation.
/// </summary>
/// <typeparam name="T">The type of data returned by the operation</typeparam>
public class OperationResult<T>
{
    /// <summary>
    /// Gets whether the operation was successful.
    /// </summary>
    public bool IsSuccessful { get; }

    /// <summary>
    /// Gets the HTTP status code associated with the operation result.
    /// </summary>
    public HttpStatusCode StatusCode { get; }

    /// <summary>
    /// Gets the data returned by the operation, if successful.
    /// </summary>
    public T Data =>
        !IsSuccessful
            ? throw new InvalidOperationException("Operation failed.")
            // ReSharper disable once NullableWarningSuppressionIsUsed
            : _data!;

    /// <summary>
    /// Gets the error message if the operation failed.
    /// </summary>
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
    public static OperationResult<T> Success(T data, HttpStatusCode statusCode = HttpStatusCode.OK) => new(true, statusCode, data, "");

    /// <summary>
    /// Creates a failed result with an error message.
    /// </summary>
    /// <param name="errorMessage">The error message</param>
    /// <param name="statusCode">Failure http status code</param>
    /// <returns>A failed OperationResult</returns>
    public static OperationResult<T> Failure(string errorMessage, HttpStatusCode statusCode) => new(false, statusCode, default, errorMessage);
}
