// Copyright (c) 2022- Burak Kara, MIT License
// See LICENSE file in the project root for full license information.

using Cloud.Interfaces;
using FluentAssertions;
using Newtonsoft.Json.Linq;
using Utilities.Common;
using Xunit;

namespace Cloud.Database.Tests.Common;

/// <summary>
/// Base class for database service integration tests that covers all IDatabaseService functionality
/// </summary>
public abstract class DatabaseServiceTestBase
{
    protected abstract IDatabaseService CreateDatabaseService();
    protected abstract Task CleanupDatabaseAsync(string tableName);
    protected virtual string GetTestTableName() => $"test-table-{Guid.NewGuid():N}";

    #region Test Data Helpers

    protected static PrimitiveType CreateStringKey(string value = "test-key") => new(value);
    protected static PrimitiveType CreateIntegerKey(long value = 123) => new(value);
    protected static PrimitiveType CreateDoubleKey(double value = 123.456) => new(value);
    protected static PrimitiveType CreateByteArrayKey(byte[]? value = null) => new(value ?? [1, 2, 3, 4, 5]);

    protected static JObject CreateTestItem(string name = "TestItem", int value = 42)
    {
        return new JObject
        {
            ["Name"] = name,
            ["Value"] = value,
            ["Created"] = DateTime.UtcNow,
            ["Tags"] = new JArray { "tag1", "tag2", "tag3" }
        };
    }

    protected static JObject CreateUpdateData(string newName = "UpdatedItem", int newValue = 84)
    {
        return new JObject
        {
            ["Name"] = newName,
            ["Value"] = newValue,
            ["Updated"] = DateTime.UtcNow
        };
    }

    #endregion

    #region Condition Builder Tests

    [Fact]
    public virtual void BuildAttributeExistsCondition_ShouldReturnExistenceCondition()
    {
        // Arrange
        var service = CreateDatabaseService();
        const string attributeName = "TestAttribute";

        try
        {
            // Act
            var condition = service.BuildAttributeExistsCondition(attributeName);

            // Assert
            condition.Should().BeOfType<ExistenceCondition>();
            condition.ConditionType.Should().Be(DatabaseAttributeConditionType.AttributeExists);
            condition.AttributeName.Should().Be(attributeName);
        }
        finally
        {
            if (service is IDisposable disposable)
                disposable.Dispose();
        }
    }

    [Fact]
    public virtual void BuildAttributeNotExistsCondition_ShouldReturnExistenceCondition()
    {
        // Arrange
        var service = CreateDatabaseService();
        const string attributeName = "TestAttribute";

        try
        {
            // Act
            var condition = service.BuildAttributeNotExistsCondition(attributeName);

            // Assert
            condition.Should().BeOfType<ExistenceCondition>();
            condition.ConditionType.Should().Be(DatabaseAttributeConditionType.AttributeNotExists);
            condition.AttributeName.Should().Be(attributeName);
        }
        finally
        {
            if (service is IDisposable disposable)
                disposable.Dispose();
        }
    }

    #endregion

    #region Core CRUD Operations Tests - Simplified for now

    [Fact]
    public virtual async Task PutItemAsync_WithNewItem_ShouldSucceed()
    {
        // Arrange
        var service = CreateDatabaseService();
        service.IsInitialized.Should().BeTrue();
        
        var tableName = GetTestTableName();
        var keyName = "Id";
        var keyValue = CreateStringKey();
        var item = CreateTestItem();

        try
        {
            // Act
            var result = await service.PutItemAsync(tableName, keyName, keyValue, item);

            // Assert
            result.Should().NotBeNull();
            result.IsSuccessful.Should().BeTrue();
            result.ErrorMessage.Should().BeNull();
        }
        finally
        {
            await CleanupDatabaseAsync(tableName);
            if (service is IDisposable disposable)
                disposable.Dispose();
        }
    }

    [Fact]
    public virtual async Task GetItemAsync_WhenItemNotExists_ShouldReturnNull()
    {
        // Arrange
        var service = CreateDatabaseService();
        var tableName = GetTestTableName();
        var keyName = "Id";
        var keyValue = CreateStringKey("non-existent-key");

        try
        {
            // Act
            var result = await service.GetItemAsync(tableName, keyName, keyValue);

            // Assert
            result.Should().NotBeNull();
            result.IsSuccessful.Should().BeTrue();
            result.Data.Should().BeNull();
        }
        finally
        {
            await CleanupDatabaseAsync(tableName);
            if (service is IDisposable disposable)
                disposable.Dispose();
        }
    }

    [Fact]
    public virtual async Task ItemExistsAsync_WhenItemNotExists_ShouldReturnFalse()
    {
        // Arrange
        var service = CreateDatabaseService();
        var tableName = GetTestTableName();
        var keyName = "Id";
        var keyValue = CreateStringKey("non-existent-key");

        try
        {
            // Act
            var result = await service.ItemExistsAsync(tableName, keyName, keyValue);

            // Assert
            result.Should().NotBeNull();
            result.IsSuccessful.Should().BeTrue();
            result.Data.Should().BeFalse();
        }
        finally
        {
            await CleanupDatabaseAsync(tableName);
            if (service is IDisposable disposable)
                disposable.Dispose();
        }
    }

    #endregion

    #region Database Options Tests

    [Fact]
    public virtual void SetOptions_WithValidOptions_ShouldUpdateOptions()
    {
        // Arrange
        var service = CreateDatabaseService();
        var options = new DatabaseOptions
        {
            AutoSortArrays = AutoSortArrays.Yes,
            AutoConvertRoundableFloatToInt = AutoConvertRoundableFloatToInt.Yes
        };

        try
        {
            // Act & Assert (no exception should be thrown)
            service.SetOptions(options);
        }
        finally
        {
            if (service is IDisposable disposable)
                disposable.Dispose();
        }
    }

    [Fact]
    public virtual void SetOptions_WithNullOptions_ShouldThrowArgumentNullException()
    {
        // Arrange
        var service = CreateDatabaseService();

        try
        {
            // Act & Assert
            Assert.Throws<ArgumentNullException>(() => service.SetOptions(null!));
        }
        finally
        {
            if (service is IDisposable disposable)
                disposable.Dispose();
        }
    }

    #endregion
}
