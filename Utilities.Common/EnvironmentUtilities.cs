// Copyright (c) 2022- Burak Kara, MIT License
// See LICENSE file in the project root for full license information.

// ReSharper disable MemberCanBePrivate.Global
namespace Utilities.Common;


/// <summary>
/// Provides utilities for environment variable operations.
/// </summary>
public static class EnvironmentUtilities
{
    /// <summary>
    /// Gets required environment variables with fallback options.
    /// </summary>
    /// <param name="variableKeyOptions">Collection of variable key options, where each inner collection represents fallback keys</param>
    /// <returns>Dictionary of successfully resolved environment variables</returns>
    /// <exception cref="InvalidOperationException">Thrown when required variables are not found</exception>
    public static Dictionary<string, string> GetRequiredEnvironmentVariables(IEnumerable<IEnumerable<string>> variableKeyOptions)
    {
        var keyOptionsList = variableKeyOptions.ToList();
        if (keyOptionsList.Count == 0)
        {
            throw new ArgumentException("No environment variable options provided", nameof(variableKeyOptions));
        }

        var result = new Dictionary<string, string>(keyOptionsList.Count);

        foreach (var keyOptionsList2 in keyOptionsList.Select(keyOptions => keyOptions.ToList()))
        {
            if (keyOptionsList2.Count == 0)
            {
                throw new ArgumentException("Empty environment variable key options provided", nameof(variableKeyOptions));
            }

            string? foundValue = null;
            string? foundKey = null;

            foreach (var key in keyOptionsList2)
            {
                var value = Environment.GetEnvironmentVariable(key);
                if (!string.IsNullOrEmpty(value))
                {
                    foundValue = value;
                    foundKey = key;
                    break;
                }
            }

            if (foundValue is null || foundKey is null)
            {
                var keyOptionsString = string.Join(", ", keyOptionsList2);
                throw new InvalidOperationException($"Required environment variable not found. Missing one of: {keyOptionsString}");
            }

            result[foundKey] = foundValue;
        }

        return result;
    }

    /// <summary>
    /// Tries to get required environment variables with fallback options.
    /// </summary>
    /// <param name="variableKeyOptions">Collection of variable key options</param>
    /// <param name="result">The resolved environment variables if successful</param>
    /// <returns>True if all required variables were found</returns>
    public static bool TryGetRequiredEnvironmentVariables(IEnumerable<IEnumerable<string>> variableKeyOptions, out Dictionary<string, string>? result)
    {
        try
        {
            result = GetRequiredEnvironmentVariables(variableKeyOptions);
            return true;
        }
        catch
        {
            result = null;
            return false;
        }
    }
}
