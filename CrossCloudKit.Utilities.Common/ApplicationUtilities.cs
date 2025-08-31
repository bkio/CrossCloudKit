// Copyright (c) 2022- Burak Kara, MIT License
// See LICENSE file in the project root for full license information.

using System.Reflection;

namespace CrossCloudKit.Utilities.Common;

/// <summary>
/// Provides utilities for application-specific operations.
/// </summary>
public static class ApplicationUtilities
{
    /// <summary>
    /// Gets the directory path where the executing assembly is located.
    /// </summary>
    /// <param name="endWith">Character to end the path with (default: directory separator)</param>
    /// <returns>The application directory path</returns>
    public static string GetApplicationDirectory(char? endWith = null)
    {
        var location = Assembly.GetExecutingAssembly().Location;
        var directory = Path.GetDirectoryName(location) ?? throw new InvalidOperationException("Could not determine application directory");

        endWith ??= Path.DirectorySeparatorChar;
        return directory.EndsWith(endWith.Value) ? directory : directory + endWith.Value;
    }

    /// <summary>
    /// Gets the drive letter where the executing assembly is located.
    /// </summary>
    /// <returns>The drive letter (without colon)</returns>
    public static string GetApplicationDriveLetter()
    {
        var location = Assembly.GetExecutingAssembly().Location;
        var colonIndex = location.IndexOf(':');
        return colonIndex == -1 ? string.Empty : location[..colonIndex];
    }
}
