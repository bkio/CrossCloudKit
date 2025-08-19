// Copyright (c) 2022- Burak Kara, MIT License
// See LICENSE file in the project root for full license information.

namespace Utilities.Common;

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
        ArgumentException.ThrowIfNullOrWhiteSpace(directoryPath);

        return await Task.Run(() =>
        {
            var directoryInfo = new DirectoryInfo(directoryPath);
            return BuildDirectoryTree(null, directoryInfo);
        }, cancellationToken).ConfigureAwait(false);
    }

    private static DirectoryTreeNode BuildDirectoryTree(DirectoryTreeNode? parent, DirectoryInfo directoryInfo)
    {
        var children = new List<DirectoryTreeNode>();

        // Add files
        foreach (var file in directoryInfo.GetFiles())
        {
            var fileNode = new DirectoryTreeNode
            {
                NodeType = DirectoryTreeNodeType.File,
                Name = file.Name,
                Parent = parent,
                Children = []
            };
            children.Add(fileNode);
        }

        // Add subdirectories
        var directoryNode = new DirectoryTreeNode
        {
            NodeType = DirectoryTreeNodeType.Directory,
            Name = directoryInfo.Name,
            Parent = parent,
            Children = children
        };

        foreach (var subDirectory in directoryInfo.GetDirectories())
        {
            var subDirectoryNode = BuildDirectoryTree(directoryNode, subDirectory);
            children.Add(subDirectoryNode);
        }

        return directoryNode with { Children = children.AsReadOnly() };
    }

    /// <summary>
    /// Gets the size of a file in bytes.
    /// </summary>
    /// <param name="filePath">The path to the file</param>
    /// <returns>The file size in bytes</returns>
    public static long GetFileSize(string filePath)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(filePath);
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
        ArgumentException.ThrowIfNullOrWhiteSpace(directoryPath);

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
            if (deleteDirectory)
            {
                try
                {
                    directoryInfo.Delete();
                }
                catch
                {
                    // Directory might not be empty or in use
                }
            }
        }
        catch
        {
            // Handle any other exceptions gracefully
        }
    }
}
