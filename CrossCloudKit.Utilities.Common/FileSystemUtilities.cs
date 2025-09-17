// Copyright (c) 2022- Burak Kara, MIT License
// See LICENSE file in the project root for full license information.

// ReSharper disable UnusedAutoPropertyAccessor.Global

using System.Text;
using System.Text.RegularExpressions;

namespace CrossCloudKit.Utilities.Common;

/// <summary>
/// Represents the type of a directory tree node.
/// </summary>
public enum DirectoryTreeNodeType
{
    /// <summary>
    /// The node represents a file.
    /// </summary>
    File,

    /// <summary>
    /// The node represents a directory.
    /// </summary>
    Directory
}

/// <summary>
/// Represents a node in a directory tree structure.
/// </summary>
public sealed record DirectoryTreeNode
{
    /// <summary>
    /// Gets the type of this node (file or directory).
    /// </summary>
    public DirectoryTreeNodeType NodeType { get; init; }

    /// <summary>
    /// Gets the name of this node.
    /// </summary>
    public string Name { get; init; } = string.Empty;

    /// <summary>
    /// Gets the parent node, or null if this is the root.
    /// </summary>
    public DirectoryTreeNode? Parent { get; init; }

    /// <summary>
    /// Gets the children of this node (empty for files).
    /// </summary>
    public IReadOnlyList<DirectoryTreeNode> Children { get; init; } = [];

    /// <summary>
    /// Gets the full path of this node from the root.
    /// </summary>
    public string FullPath
    {
        get
        {
            if (Parent is null)
                return Name;

            var pathParts = new List<string>();
            var current = this;

            while (current is not null)
            {
                pathParts.Add(current.Name);
                current = current.Parent;
            }

            pathParts.Reverse();
            return string.Join(Path.DirectorySeparatorChar, pathParts);
        }
    }
}

/// <summary>
/// Provides utilities for file system operations.
/// </summary>
public static class FileSystemUtilities
{
    /// <summary>
    /// Gets the directory tree structure for the specified path.
    /// </summary>
    /// <param name="directoryPath">The directory path to analyze</param>
    /// <param name="cancellationToken">Cancellation token to observe</param>
    /// <returns>The root directory tree node</returns>
    public static async Task<DirectoryTreeNode> GetDirectoryTreeAsync(string directoryPath, CancellationToken cancellationToken = default)
    {
        return await Task.Run(() =>
        {
            var directoryInfo = new DirectoryInfo(directoryPath);
            return BuildDirectoryTree(null, directoryInfo);
        }, cancellationToken);
    }

    private static DirectoryTreeNode BuildDirectoryTree(DirectoryTreeNode? parent, DirectoryInfo directoryInfo)
    {
        // Add files
        var children = directoryInfo.GetFiles().Select(file => new DirectoryTreeNode { NodeType = DirectoryTreeNodeType.File, Name = file.Name, Parent = parent, Children = [] }).ToList();

        // Add subdirectories
        var directoryNode = new DirectoryTreeNode
        {
            NodeType = DirectoryTreeNodeType.Directory,
            Name = directoryInfo.Name,
            Parent = parent,
            Children = children
        };

        children.AddRange(directoryInfo.GetDirectories().Select(subDirectory => BuildDirectoryTree(directoryNode, subDirectory)));

        return directoryNode with { Children = children.AsReadOnly() };
    }

    /// <summary>
    /// Gets the size of a file in bytes.
    /// </summary>
    /// <param name="filePath">The path to the file</param>
    /// <returns>The file size in bytes</returns>
    public static long GetFileSize(string filePath)
    {
        return new FileInfo(filePath).Length;
    }

    /// <summary>
    /// Safely deletes a file if it exists.
    /// </summary>
    /// <param name="filePath">The path to the file to delete</param>
    /// <returns>True if the file was deleted or didn't exist</returns>
    public static bool TryDeleteFile(string filePath)
    {
        try
        {
            // If it's a directory, return false as we can't delete it as a file
            if (Directory.Exists(filePath))
            {
                return false;
            }

            if (File.Exists(filePath))
            {
                File.Delete(filePath);
            }
            return true;
        }
        catch
        {
            return false;
        }
    }

    /// <summary>
    /// Recursively deletes all contents of a directory.
    /// </summary>
    /// <param name="directoryPath">The directory path to clean</param>
    /// <param name="deleteDirectory">Whether to delete the directory itself after cleaning</param>
    public static void DeleteDirectoryContents(string directoryPath, bool deleteDirectory = false)
    {
        if (!Directory.Exists(directoryPath))
            return;

        try
        {
            var directoryInfo = new DirectoryInfo(directoryPath);

            // Delete files
            foreach (var file in directoryInfo.GetFiles())
            {
                try
                {
                    file.Delete();
                }
                catch
                {
                    // Continue with other files
                }
            }

            // Delete subdirectories recursively
            foreach (var subDirectory in directoryInfo.GetDirectories())
            {
                DeleteDirectoryContents(subDirectory.FullName, deleteDirectory: true);
            }

            // Delete the directory itself if requested
            if (!deleteDirectory) return;
            try
            {
                directoryInfo.Delete();
            }
            catch
            {
                // Directory might not be empty or in use
            }
        }
        catch
        {
            // Handle any other exceptions gracefully
        }
    }

    // Cross-platform invalid filename characters
    // Combines invalid characters from Windows, Linux, and macOS
    private static readonly char[] CrossPlatformInvalidFileNameChars =
    [
        // Windows invalid characters
        '<', '>', ':', '"', '/', '\\', '|', '?', '*',
        // Control characters (0-31)
        '\x00', '\x01', '\x02', '\x03', '\x04', '\x05', '\x06', '\x07',
        '\x08', '\x09', '\x0A', '\x0B', '\x0C', '\x0D', '\x0E', '\x0F',
        '\x10', '\x11', '\x12', '\x13', '\x14', '\x15', '\x16', '\x17',
        '\x18', '\x19', '\x1A', '\x1B', '\x1C', '\x1D', '\x1E', '\x1F'
    ];

    private static readonly string CrossPlatformInvalidRegStr =
        $@"([{Regex.Escape(new string(CrossPlatformInvalidFileNameChars))}]*\.+$)|([{Regex.Escape(new string(CrossPlatformInvalidFileNameChars))}]+)";

    /// <summary>
    /// Makes a string safe to use as a filename across all platforms by replacing invalid characters with underscores.
    /// This method uses a cross-platform set of invalid characters that includes restrictions from Windows, Linux, and macOS.
    /// </summary>
    /// <param name="input">The input string to make valid</param>
    /// <returns>A valid filename string with invalid characters replaced by underscores</returns>
    public static string MakeValidFileName(this string input)
    {
        if (string.IsNullOrEmpty(input))
        {
            return string.Empty;
        }

        // Replace invalid characters with underscores
        var result = Regex.Replace(input, CrossPlatformInvalidRegStr, "_");

        // Handle special cases for reserved names on Windows (CON, PRN, AUX, etc.)
        if (IsReservedFileName(result))
        {
            result = "_" + result;
        }

        // Trim trailing periods and spaces (Windows restriction)
        result = result.TrimEnd('.', ' ');

        // Ensure the result is not empty
        if (string.IsNullOrEmpty(result))
        {
            result = "_";
        }

        return result;
    }

    /// <summary>
    /// Writes the specified content to a file and ensures it is fully flushed to disk.
    /// </summary>
    /// <param name="content">The string content to write to the file.</param>
    /// <param name="filePath">The full path of the file to write.</param>
    /// <param name="cancellationToken">A <see cref="CancellationToken"/> to cancel the operation.</param>
    /// <remarks>
    /// This method writes the content using UTF-8 encoding, flushes both the .NET stream buffers
    /// and the OS buffers to ensure the data is persisted to disk. It uses <see cref="FileOptions.WriteThrough"/>
    /// to bypass OS-level caching where possible.
    /// </remarks>
    /// <exception cref="IOException">Thrown if an I/O error occurs.</exception>
    /// <exception cref="OperationCanceledException">Thrown if the operation is canceled via <paramref name="cancellationToken"/>.</exception>
    public static async Task WriteToFileEnsureWrittenToDiskAsync(string content, string filePath, CancellationToken cancellationToken = default)
    {
        var bytes = Encoding.UTF8.GetBytes(content);
        await using var fs = new FileStream(
            filePath,
            FileMode.Create,
            FileAccess.Write,
            FileShare.None,
            bufferSize: 4096,
            FileOptions.WriteThrough);
        await fs.WriteAsync(bytes, cancellationToken);
        await fs.FlushAsync(cancellationToken);       // flush .NET buffers
        fs.Flush(true);   // flush OS buffers to disk
    }

    /// <summary>
    /// Writes the specified content to a file and ensures it is fully flushed to disk.
    /// This is a synchronous wrapper around <see cref="WriteToFileEnsureWrittenToDisk"/>.
    /// </summary>
    /// <param name="content">The string content to write to the file.</param>
    /// <param name="filePath">The full path of the file to write.</param>
    /// <remarks>
    /// This method blocks the calling thread until the write operation is complete.
    /// It ensures that both .NET buffers and OS buffers are flushed to disk for durability.
    /// </remarks>
    /// <exception cref="IOException">Thrown if an I/O error occurs.</exception>
    /// <exception cref="AggregateException">
    /// Thrown if the underlying asynchronous operation throws an exception. The original exception
    /// can be accessed via <see cref="AggregateException.InnerException"/>.
    /// </exception>
    public static void WriteToFileEnsureWrittenToDisk(string content, string filePath)
    {
        WriteToFileEnsureWrittenToDiskAsync(content, filePath, CancellationToken.None).Wait();
    }

    /// <summary>
    /// Checks if a filename is reserved on Windows
    /// </summary>
    private static bool IsReservedFileName(string fileName)
    {
        if (string.IsNullOrEmpty(fileName))
            return false;

        var reservedNames = new[]
        {
            "CON", "PRN", "AUX", "NUL",
            "COM1", "COM2", "COM3", "COM4", "COM5", "COM6", "COM7", "COM8", "COM9",
            "LPT1", "LPT2", "LPT3", "LPT4", "LPT5", "LPT6", "LPT7", "LPT8", "LPT9"
        };

        var nameWithoutExtension = Path.GetFileNameWithoutExtension(fileName);
        return reservedNames.Contains(nameWithoutExtension.ToUpperInvariant());
    }

    /// <summary>
    /// Deletes a file if it exists and recursively deletes its parent directories
    /// if they become empty, stopping when the specified parent folder is reached.
    /// </summary>
    /// <param name="filePath">The full path of the file to delete.</param>
    /// <param name="untilParentFolderName">
    /// The name of the parent folder at which to stop cleanup.
    /// This folder will not be deleted, even if empty.
    /// </param>
    /// <returns>
    /// True if the file was deleted (or did not exist but cleanup ran successfully),
    /// false if an error occurred.
    /// </returns>
    public static bool DeleteFileAndCleanupParentFolders(string filePath, string untilParentFolderName)
    {
        try
        {
            if (File.Exists(filePath))
                File.Delete(filePath);

            return CleanupEmptyParentDirectories(Path.GetDirectoryName(filePath).NotNull(), untilParentFolderName);
        }
        catch
        {
            return false;
        }
    }

    /// <summary>
    /// Recursively deletes empty parent directories until reaching a specified parent folder.
    /// </summary>
    /// <param name="directory">The directory to check for emptiness.</param>
    /// <param name="untilParentFolderName">
    /// The name of the parent folder at which to stop cleanup.
    /// This folder will not be deleted.
    /// </param>
    /// <returns>
    /// True if cleanup completed without errors, false otherwise.
    /// </returns>
    private static bool CleanupEmptyParentDirectories(string? directory, string untilParentFolderName)
    {
        try
        {
            if (string.IsNullOrEmpty(directory))
                return true;

            var dirInfo = new DirectoryInfo(directory);

            // Stop if we reached the designated parent folder
            if (dirInfo.Name.Equals(untilParentFolderName, StringComparison.OrdinalIgnoreCase))
                return true;

            if (dirInfo.Exists && dirInfo.GetFileSystemInfos().Length == 0)
            {
                dirInfo.Delete();

                // Recurse upwards
                return CleanupEmptyParentDirectories(dirInfo.Parent?.FullName, untilParentFolderName);
            }

            return true;
        }
        catch
        {
            return false;
        }
    }
}
