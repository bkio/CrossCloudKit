// Copyright (c) 2022- Burak Kara, MIT License
// See LICENSE file in the project root for full license information.

namespace Cloud.Interfaces;

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
    /// Gets the data returned by the operation, if successful.
    /// </summary>
    public T? Data { get; }

    /// <summary>
    /// Gets the error message if the operation failed.
    /// </summary>
    public string? ErrorMessage { get; }

    private OperationResult(bool isSuccessful, T? data, string? errorMessage)
    {
        IsSuccessful = isSuccessful;
        Data = data;
        ErrorMessage = errorMessage;
    }

    /// <summary>
    /// Creates a successful result with data.
    /// </summary>
    /// <param name="data">The operation result data</param>
    /// <returns>A successful OperationResult</returns>
    public static OperationResult<T> Success(T data) => new(true, data, "");

    /// <summary>
    /// Creates a failed result with an error message.
    /// </summary>
    /// <param name="errorMessage">The error message</param>
    /// <returns>A failed OperationResult</returns>
    public static OperationResult<T> Failure(string errorMessage) => new(false, default, errorMessage);
}
