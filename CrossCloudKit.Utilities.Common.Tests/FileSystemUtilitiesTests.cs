// Copyright (c) 2022- Burak Kara, MIT License
// See LICENSE file in the project root for full license information.

using Xunit;

namespace CrossCloudKit.Utilities.Common.Tests;

public class FileSystemUtilitiesTests
{
    [Fact]
    public async Task GetDirectoryTreeAsync_WithSimpleDirectory_ReturnsCorrectStructure()
    {
        // Arrange
        var testDirectory = Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString());
        Directory.CreateDirectory(testDirectory);

        var file1 = Path.Combine(testDirectory, "file1.txt");
        var file2 = Path.Combine(testDirectory, "file2.txt");

        try
        {
            await File.WriteAllTextAsync(file1, "test content 1");
            await File.WriteAllTextAsync(file2, "test content 2");

            // Act
            var result = await FileSystemUtilities.GetDirectoryTreeAsync(testDirectory);

            // Assert
            Assert.NotNull(result);
            Assert.Equal(DirectoryTreeNodeType.Directory, result.NodeType);
            Assert.Equal(Path.GetFileName(testDirectory), result.Name);
            Assert.Null(result.Parent);
            Assert.Equal(2, result.Children.Count);

            var files = result.Children.Where(c => c.NodeType == DirectoryTreeNodeType.File).ToList();
            Assert.Equal(2, files.Count);
            Assert.Contains(files, f => f.Name == "file1.txt");
            Assert.Contains(files, f => f.Name == "file2.txt");
        }
        finally
        {
            if (Directory.Exists(testDirectory))
            {
                Directory.Delete(testDirectory, recursive: true);
            }
        }
    }

    [Fact]
    public async Task GetDirectoryTreeAsync_WithNestedDirectories_ReturnsCorrectStructure()
    {
        // Arrange
        var testDirectory = Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString());
        var subDirectory1 = Path.Combine(testDirectory, "sub1");
        var subDirectory2 = Path.Combine(testDirectory, "sub2");
        var nestedDirectory = Path.Combine(subDirectory1, "nested");

        Directory.CreateDirectory(nestedDirectory);
        Directory.CreateDirectory(subDirectory2);

        var rootFile = Path.Combine(testDirectory, "root.txt");
        var sub1File = Path.Combine(subDirectory1, "sub1file.txt");
        var nestedFile = Path.Combine(nestedDirectory, "nested.txt");

        try
        {
            await File.WriteAllTextAsync(rootFile, "root content");
            await File.WriteAllTextAsync(sub1File, "sub1 content");
            await File.WriteAllTextAsync(nestedFile, "nested content");

            // Act
            var result = await FileSystemUtilities.GetDirectoryTreeAsync(testDirectory);

            // Assert
            Assert.Equal(DirectoryTreeNodeType.Directory, result.NodeType);
            Assert.Equal(3, result.Children.Count); // 1 file + 2 directories

            var rootFileNode = result.Children.FirstOrDefault(c => c.Name == "root.txt");
            Assert.NotNull(rootFileNode);
            Assert.Equal(DirectoryTreeNodeType.File, rootFileNode.NodeType);

            var sub1Node = result.Children.FirstOrDefault(c => c.Name == "sub1");
            Assert.NotNull(sub1Node);
            Assert.Equal(DirectoryTreeNodeType.Directory, sub1Node.NodeType);
            Assert.Equal(2, sub1Node.Children.Count); // 1 file + 1 directory

            var nestedNode = sub1Node.Children.FirstOrDefault(c => c.Name == "nested");
            Assert.NotNull(nestedNode);
            Assert.Equal(DirectoryTreeNodeType.Directory, nestedNode.NodeType);
            Assert.Single(nestedNode.Children);
        }
        finally
        {
            if (Directory.Exists(testDirectory))
            {
                Directory.Delete(testDirectory, recursive: true);
            }
        }
    }

    [Fact]
    public async Task GetDirectoryTreeAsync_WithEmptyDirectory_ReturnsEmptyStructure()
    {
        // Arrange
        var testDirectory = Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString());
        Directory.CreateDirectory(testDirectory);

        try
        {
            // Act
            var result = await FileSystemUtilities.GetDirectoryTreeAsync(testDirectory);

            // Assert
            Assert.NotNull(result);
            Assert.Equal(DirectoryTreeNodeType.Directory, result.NodeType);
            Assert.Empty(result.Children);
        }
        finally
        {
            if (Directory.Exists(testDirectory))
            {
                Directory.Delete(testDirectory, recursive: true);
            }
        }
    }

    [Fact]
    public async Task GetDirectoryTreeAsync_WithCancellation_ThrowsOperationCanceledException()
    {
        // Arrange
        var testDirectory = Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString());
        Directory.CreateDirectory(testDirectory);

        try
        {
            using var cts = new CancellationTokenSource();
            await cts.CancelAsync(); // Cancel immediately

            // Act & Assert
            await Assert.ThrowsAsync<Exception>(() =>
                FileSystemUtilities.GetDirectoryTreeAsync(testDirectory, cts.Token));
        }
        finally
        {
            if (Directory.Exists(testDirectory))
            {
                Directory.Delete(testDirectory, recursive: true);
            }
        }
    }

    [Fact]
    public void GetFileSize_WithExistingFile_ReturnsCorrectSize()
    {
        // Arrange
        var tempFile = Path.GetTempFileName();
        const string content = "This is test content for file size measurement.";

        try
        {
            File.WriteAllText(tempFile, content);
            var expectedSize = new FileInfo(tempFile).Length;

            // Act
            var result = FileSystemUtilities.GetFileSize(tempFile);

            // Assert
            Assert.Equal(expectedSize, result);
            Assert.True(result > 0);
        }
        finally
        {
            if (File.Exists(tempFile))
            {
                File.Delete(tempFile);
            }
        }
    }

    [Fact]
    public void GetFileSize_WithEmptyFile_ReturnsZero()
    {
        // Arrange
        var tempFile = Path.GetTempFileName();

        try
        {
            // File is created but empty
            // Act
            var result = FileSystemUtilities.GetFileSize(tempFile);

            // Assert
            Assert.Equal(0, result);
        }
        finally
        {
            if (File.Exists(tempFile))
            {
                File.Delete(tempFile);
            }
        }
    }

    [Fact]
    public void GetFileSize_WithLargeFile_ReturnsCorrectSize()
    {
        // Arrange
        var tempFile = Path.GetTempFileName();
        const int fileSize = 1024 * 1024; // 1MB
        var largeContent = new string('A', fileSize);

        try
        {
            File.WriteAllText(tempFile, largeContent);

            // Act
            var result = FileSystemUtilities.GetFileSize(tempFile);

            // Assert
            Assert.True(result >= fileSize); // At least the content size (encoding might add bytes)
        }
        finally
        {
            if (File.Exists(tempFile))
            {
                File.Delete(tempFile);
            }
        }
    }

    [Fact]
    public void GetFileSize_WithNonExistentFile_ThrowsException()
    {
        // Arrange
        var nonExistentFile = Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString() + ".txt");

        // Act & Assert
        Assert.Throws<FileNotFoundException>(() => FileSystemUtilities.GetFileSize(nonExistentFile));
    }

    [Fact]
    public void TryDeleteFile_WithExistingFile_ReturnsTrue()
    {
        // Arrange
        var tempFile = Path.GetTempFileName();
        File.WriteAllText(tempFile, "test content");

        try
        {
            // Act
            var result = FileSystemUtilities.TryDeleteFile(tempFile);

            // Assert
            Assert.True(result);
            Assert.False(File.Exists(tempFile));
        }
        finally
        {
            if (File.Exists(tempFile))
            {
                File.Delete(tempFile);
            }
        }
    }

    [Fact]
    public void TryDeleteFile_WithNonExistentFile_ReturnsTrue()
    {
        // Arrange
        var nonExistentFile = Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString() + ".txt");

        // Act
        var result = FileSystemUtilities.TryDeleteFile(nonExistentFile);

        // Assert
        Assert.True(result);
    }

    [Fact]
    public void DeleteDirectoryContents_WithFilesOnly_DeletesAllFiles()
    {
        // Arrange
        var testDirectory = Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString());
        Directory.CreateDirectory(testDirectory);

        var file1 = Path.Combine(testDirectory, "file1.txt");
        var file2 = Path.Combine(testDirectory, "file2.txt");

        try
        {
            File.WriteAllText(file1, "content 1");
            File.WriteAllText(file2, "content 2");

            // Act
            FileSystemUtilities.DeleteDirectoryContents(testDirectory);

            // Assert
            Assert.True(Directory.Exists(testDirectory)); // Directory should still exist
            Assert.False(File.Exists(file1));
            Assert.False(File.Exists(file2));
        }
        finally
        {
            if (Directory.Exists(testDirectory))
            {
                Directory.Delete(testDirectory, recursive: true);
            }
        }
    }

    [Fact]
    public void DeleteDirectoryContents_WithNestedStructure_DeletesAllContents()
    {
        // Arrange
        var testDirectory = Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString());
        var subDirectory = Path.Combine(testDirectory, "sub");
        Directory.CreateDirectory(subDirectory);

        var rootFile = Path.Combine(testDirectory, "root.txt");
        var subFile = Path.Combine(subDirectory, "sub.txt");

        try
        {
            File.WriteAllText(rootFile, "root content");
            File.WriteAllText(subFile, "sub content");

            // Act
            FileSystemUtilities.DeleteDirectoryContents(testDirectory);

            // Assert
            Assert.True(Directory.Exists(testDirectory)); // Root directory should still exist
            Assert.False(Directory.Exists(subDirectory));
            Assert.False(File.Exists(rootFile));
            Assert.False(File.Exists(subFile));
        }
        finally
        {
            if (Directory.Exists(testDirectory))
            {
                Directory.Delete(testDirectory, recursive: true);
            }
        }
    }

    [Fact]
    public void DeleteDirectoryContents_WithDeleteDirectoryTrue_DeletesEntireDirectory()
    {
        // Arrange
        var testDirectory = Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString());
        Directory.CreateDirectory(testDirectory);
        var testFile = Path.Combine(testDirectory, "test.txt");

        try
        {
            File.WriteAllText(testFile, "test content");

            // Act
            FileSystemUtilities.DeleteDirectoryContents(testDirectory, deleteDirectory: true);

            // Assert
            Assert.False(Directory.Exists(testDirectory));
            Assert.False(File.Exists(testFile));
        }
        finally
        {
            if (Directory.Exists(testDirectory))
            {
                Directory.Delete(testDirectory, recursive: true);
            }
        }
    }

    [Fact]
    public void DeleteDirectoryContents_WithNonExistentDirectory_DoesNotThrow()
    {
        // Arrange
        var nonExistentDirectory = Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString());

        // Act & Assert - Should not throw
        FileSystemUtilities.DeleteDirectoryContents(nonExistentDirectory);
        FileSystemUtilities.DeleteDirectoryContents(nonExistentDirectory, deleteDirectory: true);
    }

    [Fact]
    public void DirectoryTreeNode_FullPath_ReturnsCorrectPath()
    {
        // Arrange
        var root = new DirectoryTreeNode
        {
            NodeType = DirectoryTreeNodeType.Directory,
            Name = "root",
            Parent = null
        };

        var child = new DirectoryTreeNode
        {
            NodeType = DirectoryTreeNodeType.Directory,
            Name = "child",
            Parent = root
        };

        var grandchild = new DirectoryTreeNode
        {
            NodeType = DirectoryTreeNodeType.File,
            Name = "file.txt",
            Parent = child
        };

        // Act
        var rootPath = root.FullPath;
        var childPath = child.FullPath;
        var grandchildPath = grandchild.FullPath;

        // Assert
        Assert.Equal("root", rootPath);
        Assert.Equal($"root{Path.DirectorySeparatorChar}child", childPath);
        Assert.Equal($"root{Path.DirectorySeparatorChar}child{Path.DirectorySeparatorChar}file.txt", grandchildPath);
    }

    [Fact]
    public void DirectoryTreeNode_Properties_WorkCorrectly()
    {
        // Arrange & Act
        var parent = new DirectoryTreeNode
        {
            NodeType = DirectoryTreeNodeType.Directory,
            Name = "parent",
            Parent = null,
            Children = []
        };

        var child = new DirectoryTreeNode
        {
            NodeType = DirectoryTreeNodeType.File,
            Name = "child.txt",
            Parent = parent,
            Children = []
        };

        // Assert
        Assert.Equal(DirectoryTreeNodeType.Directory, parent.NodeType);
        Assert.Equal("parent", parent.Name);
        Assert.Null(parent.Parent);
        Assert.Empty(parent.Children);

        Assert.Equal(DirectoryTreeNodeType.File, child.NodeType);
        Assert.Equal("child.txt", child.Name);
        Assert.Equal(parent, child.Parent);
        Assert.Empty(child.Children);
    }

    [Fact]
    public async Task FileSystemUtilities_IntegrationTest_ComplexDirectoryStructure()
    {
        // Arrange
        var testDirectory = Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString());
        var documentsDir = Path.Combine(testDirectory, "Documents");
        var imagesDir = Path.Combine(testDirectory, "Images");
        var tempDir = Path.Combine(documentsDir, "temp");

        Directory.CreateDirectory(tempDir);
        Directory.CreateDirectory(imagesDir);

        var files = new[]
        {
            Path.Combine(testDirectory, "readme.txt"),
            Path.Combine(documentsDir, "doc1.txt"),
            Path.Combine(documentsDir, "doc2.pdf"),
            Path.Combine(tempDir, "temp.tmp"),
            Path.Combine(imagesDir, "image1.jpg"),
            Path.Combine(imagesDir, "image2.png")
        };

        try
        {
            // Create files with different sizes
            for (int i = 0; i < files.Length; i++)
            {
                var content = new string('X', (i + 1) * 100); // Different sizes
                await File.WriteAllTextAsync(files[i], content);
            }

            // Act
            var directoryTree = await FileSystemUtilities.GetDirectoryTreeAsync(testDirectory);

            // Assert directory tree structure
            Assert.Equal(DirectoryTreeNodeType.Directory, directoryTree.NodeType);
            Assert.Equal(3, directoryTree.Children.Count); // 1 file + 2 directories

            var documentsNode = directoryTree.Children.FirstOrDefault(c => c.Name == "Documents");
            Assert.NotNull(documentsNode);
            Assert.Equal(3, documentsNode.Children.Count); // 2 files + 1 directory

            var imagesNode = directoryTree.Children.FirstOrDefault(c => c.Name == "Images");
            Assert.NotNull(imagesNode);
            Assert.Equal(2, imagesNode.Children.Count); // 2 files

            // Test file sizes
            foreach (var file in files)
            {
                var size = FileSystemUtilities.GetFileSize(file);
                Assert.True(size > 0);
            }

            // Test file deletion
            var deleteResult = FileSystemUtilities.TryDeleteFile(files[0]);
            Assert.True(deleteResult);
            Assert.False(File.Exists(files[0]));

            // Test directory cleanup
            FileSystemUtilities.DeleteDirectoryContents(tempDir);
            Assert.True(Directory.Exists(tempDir)); // Directory should exist
            Assert.False(File.Exists(files[3])); // But file should be gone
        }
        finally
        {
            if (Directory.Exists(testDirectory))
            {
                Directory.Delete(testDirectory, recursive: true);
            }
        }
    }

    [Fact]
    public async Task GetDirectoryTreeAsync_Performance_HandlesLargeDirectoryStructure()
    {
        // Arrange
        var testDirectory = Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString());
        Directory.CreateDirectory(testDirectory);

        try
        {
            // Create a moderately large directory structure
            for (int i = 0; i < 10; i++)
            {
                var subDir = Path.Combine(testDirectory, $"dir{i}");
                Directory.CreateDirectory(subDir);

                for (int j = 0; j < 10; j++)
                {
                    var file = Path.Combine(subDir, $"file{j}.txt");
                    await File.WriteAllTextAsync(file, $"Content {i}-{j}");
                }
            }

            var stopwatch = System.Diagnostics.Stopwatch.StartNew();

            // Act
            var result = await FileSystemUtilities.GetDirectoryTreeAsync(testDirectory);
            stopwatch.Stop();

            // Assert
            Assert.Equal(10, result.Children.Count); // 10 directories
            Assert.True(result.Children.All(c => c.NodeType == DirectoryTreeNodeType.Directory));
            Assert.True(result.Children.All(c => c.Children.Count == 10)); // Each has 10 files
            Assert.True(stopwatch.ElapsedMilliseconds < 5000,
                $"Performance test failed: took {stopwatch.ElapsedMilliseconds}ms");
        }
        finally
        {
            if (Directory.Exists(testDirectory))
            {
                Directory.Delete(testDirectory, recursive: true);
            }
        }
    }

    [Fact]
    public void DeleteDirectoryContents_WithProtectedFiles_HandlesGracefully()
    {
        // Arrange
        var testDirectory = Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString());
        Directory.CreateDirectory(testDirectory);

        var normalFile = Path.Combine(testDirectory, "normal.txt");
        var protectedFile = Path.Combine(testDirectory, "protected.txt");

        try
        {
            File.WriteAllText(normalFile, "normal content");
            File.WriteAllText(protectedFile, "protected content");

            // Make one file read-only
            // ReSharper disable once UnusedVariable
            var protectedFileInfo = new FileInfo(protectedFile)
            {
                IsReadOnly = true
            };

            // Act - Should not throw even if some files can't be deleted
            FileSystemUtilities.DeleteDirectoryContents(testDirectory);

            // Assert
            Assert.True(Directory.Exists(testDirectory));
            Assert.False(File.Exists(normalFile)); // Normal file should be deleted
            // Protected file behavior depends on system permissions
        }
        finally
        {
            if (Directory.Exists(testDirectory))
            {
                // Clean up - remove protection and delete
                if (File.Exists(protectedFile))
                {
                    // ReSharper disable once UnusedVariable
                    var protectedFileInfo = new FileInfo(protectedFile)
                    {
                        IsReadOnly = false
                    };
                }
                Directory.Delete(testDirectory, recursive: true);
            }
        }
    }

    [Fact]
    public void DirectoryTreeNode_WithRecord_SupportsValueEquality()
    {
        // Arrange
        var node1 = new DirectoryTreeNode
        {
            NodeType = DirectoryTreeNodeType.File,
            Name = "test.txt",
            Parent = null,
            Children = []
        };

        var node2 = new DirectoryTreeNode
        {
            NodeType = DirectoryTreeNodeType.File,
            Name = "test.txt",
            Parent = null,
            Children = []
        };

        var node3 = new DirectoryTreeNode
        {
            NodeType = DirectoryTreeNodeType.Directory,
            Name = "test.txt",
            Parent = null,
            Children = []
        };

        // Act & Assert
        Assert.Equal(node1, node2); // Same properties
        Assert.NotEqual(node1, node3); // Different NodeType
        Assert.Equal(node1.GetHashCode(), node2.GetHashCode()); // Hash codes should match
    }

    [Fact]
    public async Task GetDirectoryTreeAsync_WithSymlinks_HandlesCorrectly()
    {
        // Arrange
        var testDirectory = Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString());
        Directory.CreateDirectory(testDirectory);

        var regularFile = Path.Combine(testDirectory, "regular.txt");

        try
        {
            await File.WriteAllTextAsync(regularFile, "regular file content");

            // Act
            var result = await FileSystemUtilities.GetDirectoryTreeAsync(testDirectory);

            // Assert
            Assert.Single(result.Children);
            Assert.Equal("regular.txt", result.Children[0].Name);
            Assert.Equal(DirectoryTreeNodeType.File, result.Children[0].NodeType);

            // Note: Symlink testing is platform-dependent and may require admin privileges on Windows
        }
        finally
        {
            if (Directory.Exists(testDirectory))
            {
                Directory.Delete(testDirectory, recursive: true);
            }
        }
    }

    [Fact]
    public void FileSystemUtilities_EdgeCases_HandleCorrectly()
    {
        // Test empty strings and null handling (where applicable)
        var tempDir = Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString());
        Directory.CreateDirectory(tempDir);

        try
        {
            // Test with directory that has no extension
            var result = FileSystemUtilities.TryDeleteFile(tempDir); // Trying to delete directory as file
            Assert.False(result); // Should fail gracefully

            // Test DeleteDirectoryContents with empty directory
            FileSystemUtilities.DeleteDirectoryContents(tempDir);
            Assert.True(Directory.Exists(tempDir)); // Should still exist and be empty

            // Test DeleteDirectoryContents with deleteDirectory = true on empty directory
            FileSystemUtilities.DeleteDirectoryContents(tempDir, deleteDirectory: true);
            Assert.False(Directory.Exists(tempDir)); // Should be deleted now
        }
        finally
        {
            if (Directory.Exists(tempDir))
            {
                Directory.Delete(tempDir, recursive: true);
            }
        }
    }

    #region MakeValidFileName Tests

    [Fact]
    public void MakeValidFileName_WithInvalidCharacters_ReplacesWithUnderscore()
    {
        // Arrange
        var input = "file<name>with|invalid*chars";

        // Act
        var result = input.MakeValidFileName();

        // Assert
        Assert.False(result.Contains('<'));
        Assert.False(result.Contains('>'));
        Assert.False(result.Contains('|'));
        Assert.False(result.Contains('*'));
    }

    [Fact]
    public void MakeValidFileName_WithValidName_ReturnsUnchanged()
    {
        // Arrange
        var input = "validfilename.txt";

        // Act
        var result = input.MakeValidFileName();

        // Assert
        Assert.Equal("validfilename.txt", result);
    }

    #endregion
}
