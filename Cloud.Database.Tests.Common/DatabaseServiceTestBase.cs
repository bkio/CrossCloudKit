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

    protected virtual async Task CleanupDatabaseAsync(string tableName)
    {
        // For Google Cloud Datastore, we need to delete all entities of the given kind
        try
        {
            var service = CreateDatabaseService();
            if (service.IsInitialized)
            {
                // Query and delete all entities of this kind
                // Note: This is a simplified cleanup - in production you might want batch deletion
                var scanResult = await service.ScanTableAsync(tableName, ["Id"]);
                if (scanResult.IsSuccessful && scanResult.Data != null)
                {
                    foreach (var item in scanResult.Data)
                    {
                        if (item.TryGetValue("Id", out var idToken))
                        {
                            var keyValue = new Utilities.Common.PrimitiveType(idToken.ToString()!);
                            await service.DeleteItemAsync(tableName, "Id", keyValue);
                        }
                    }
                }
            }
        }
        catch (Exception)
        {
            // Ignore cleanup errors in tests - this is common practice for test cleanup
        }
    }
    protected virtual string GetTestTableName() => $"test-table-{Guid.NewGuid():N}";

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

    /// <summary>
    /// Override the base test to handle the case where service is not initialized due to missing credentials
    /// </summary>
    [Fact]
    public async Task PutItemAsync_WithNewItem_ShouldSucceed()
    {
        // Arrange
        var service = CreateDatabaseService();

        // Skip the test if service couldn't be initialized (no real credentials)
        if (!service.IsInitialized)
        {
            // In xUnit, we can't dynamically skip, so we just return early
            // This is the expected behavior when no credentials are available
            return;
        }

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

    /// <summary>
    /// Override the base test to handle the case where service is not initialized due to missing credentials
    /// </summary>
    [Fact]
    public async Task GetItemAsync_WhenItemNotExists_ShouldReturnNull()
    {
        // Arrange
        var service = CreateDatabaseService();

        // Skip the test if service couldn't be initialized (no real credentials)
        if (!service.IsInitialized)
        {
            return;
        }

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

    /// <summary>
    /// Override the base test to handle the case where service is not initialized due to missing credentials
    /// </summary>
    [Fact]
    public async Task ItemExistsAsync_WhenItemNotExists_ShouldReturnFalse()
    {
        // Arrange
        var service = CreateDatabaseService();

        // Skip the test if service couldn't be initialized (no real credentials)
        if (!service.IsInitialized)
        {
            return;
        }

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

    [Fact]
    public async Task ValueConditions_InItemExistsAsync_ShouldWorkCorrectly()
    {
        var service = CreateDatabaseService();
        if (!service.IsInitialized)
        {
            return;
        }

        var tableName = GetTestTableName();
        var keyName = "Id";
        var keyValue = new PrimitiveType($"value-condition-test-{Guid.NewGuid():N}");

        try
        {
            // Create test item with specific values for condition testing
            var item = CreateTestItem("ConditionTest", 42);
            item["Status"] = "active";
            item["Score"] = 85.5;
            await service.PutItemAsync(tableName, keyName, keyValue, item);

            // Test AttributeEquals condition - should return true
            var equalsCondition = service.BuildAttributeEqualsCondition("Value", new PrimitiveType(42.0));
            var existsWithEquals = await service.ItemExistsAsync(tableName, keyName, keyValue, equalsCondition);
            existsWithEquals.IsSuccessful.Should().BeTrue();
            existsWithEquals.Data.Should().BeTrue("Item exists and Value equals 42");

            // Test AttributeEquals condition - should return false
            var equalsConditionFalse = service.BuildAttributeEqualsCondition("Value", new PrimitiveType(99.0));
            var existsWithEqualsFalse = await service.ItemExistsAsync(tableName, keyName, keyValue, equalsConditionFalse);
            existsWithEqualsFalse.IsSuccessful.Should().BeTrue();
            existsWithEqualsFalse.Data.Should().BeFalse("Item exists but Value does not equal 99");

            // Test AttributeGreater condition
            var greaterCondition = service.BuildAttributeGreaterCondition("Value", new PrimitiveType(40.0));
            var existsWithGreater = await service.ItemExistsAsync(tableName, keyName, keyValue, greaterCondition);
            existsWithGreater.IsSuccessful.Should().BeTrue();
            existsWithGreater.Data.Should().BeTrue("Item exists and Value > 40");

            // Test AttributeLess condition
            var lessCondition = service.BuildAttributeLessCondition("Value", new PrimitiveType(50.0));
            var existsWithLess = await service.ItemExistsAsync(tableName, keyName, keyValue, lessCondition);
            existsWithLess.IsSuccessful.Should().BeTrue();
            existsWithLess.Data.Should().BeTrue("Item exists and Value < 50");

            // Test AttributeGreaterOrEqual condition
            var greaterOrEqualCondition = service.BuildAttributeGreaterOrEqualCondition("Value", new PrimitiveType(42.0));
            var existsWithGreaterOrEqual = await service.ItemExistsAsync(tableName, keyName, keyValue, greaterOrEqualCondition);
            existsWithGreaterOrEqual.IsSuccessful.Should().BeTrue();
            existsWithGreaterOrEqual.Data.Should().BeTrue("Item exists and Value >= 42");

            // Test AttributeLessOrEqual condition
            var lessOrEqualCondition = service.BuildAttributeLessOrEqualCondition("Value", new PrimitiveType(42.0));
            var existsWithLessOrEqual = await service.ItemExistsAsync(tableName, keyName, keyValue, lessOrEqualCondition);
            existsWithLessOrEqual.IsSuccessful.Should().BeTrue();
            existsWithLessOrEqual.Data.Should().BeTrue("Item exists and Value <= 42");

            // Test AttributeNotEquals condition
            var notEqualsCondition = service.BuildAttributeNotEqualsCondition("Status", new PrimitiveType("inactive"));
            var existsWithNotEquals = await service.ItemExistsAsync(tableName, keyName, keyValue, notEqualsCondition);
            existsWithNotEquals.IsSuccessful.Should().BeTrue();
            existsWithNotEquals.Data.Should().BeTrue("Item exists and Status != 'inactive'");
        }
        finally
        {
            await CleanupDatabaseAsync(tableName);
            if (service is IDisposable disposable)
                disposable.Dispose();
        }
    }

    [Fact]
    public async Task ExistenceConditions_InUpdateAsync_ShouldPreventOrAllowOperations()
    {
        var service = CreateDatabaseService();
        if (!service.IsInitialized)
        {
            return;
        }

        var tableName = GetTestTableName();
        var keyName = "Id";
        var keyValue = new PrimitiveType($"existence-test-{Guid.NewGuid():N}");

        try
        {
            // Create item with specific attributes
            var item = CreateTestItem("ExistenceTest", 100);
            item["OptionalField"] = "present";
            await service.PutItemAsync(tableName, keyName, keyValue, item);

            // Test AttributeExists condition - should succeed
            var existsCondition = service.BuildAttributeExistsCondition("OptionalField");
            var updateData1 = CreateUpdateData("UpdatedWithExists", 200);
            var updateResult1 = await service.UpdateItemAsync(tableName, keyName, keyValue, updateData1,
                ReturnItemBehavior.DoNotReturn, existsCondition);

            updateResult1.IsSuccessful.Should().BeTrue("Update should succeed when required field exists");

            // Test AttributeNotExists condition - should fail
            var notExistsCondition = service.BuildAttributeNotExistsCondition("OptionalField");
            var updateData2 = CreateUpdateData("ShouldNotUpdate", 300);
            var updateResult2 = await service.UpdateItemAsync(tableName, keyName, keyValue, updateData2,
                ReturnItemBehavior.DoNotReturn, notExistsCondition);

            updateResult2.IsSuccessful.Should().BeFalse("Update should fail when field exists but condition requires it not to exist");

            // Test AttributeNotExists condition for non-existent field - should succeed
            var notExistsCondition2 = service.BuildAttributeNotExistsCondition("NonExistentField");
            var updateData3 = CreateUpdateData("UpdatedWithNotExists", 400);
            var updateResult3 = await service.UpdateItemAsync(tableName, keyName, keyValue, updateData3,
                ReturnItemBehavior.DoNotReturn, notExistsCondition2);

            updateResult3.IsSuccessful.Should().BeTrue("Update should succeed when non-existent field is checked for non-existence");

            // Test AttributeExists condition for non-existent field - should fail
            var existsCondition2 = service.BuildAttributeExistsCondition("NonExistentField");
            var updateData4 = CreateUpdateData("ShouldNotUpdate2", 500);
            var updateResult4 = await service.UpdateItemAsync(tableName, keyName, keyValue, updateData4,
                ReturnItemBehavior.DoNotReturn, existsCondition2);

            updateResult4.IsSuccessful.Should().BeFalse("Update should fail when non-existent field is checked for existence");
        }
        finally
        {
            await CleanupDatabaseAsync(tableName);
            if (service is IDisposable disposable)
                disposable.Dispose();
        }
    }

    [Fact]
    public async Task ArrayElementConditions_InDeleteAsync_ShouldWorkWithArrays()
    {
        var service = CreateDatabaseService();
        if (!service.IsInitialized)
        {
            return;
        }

        var tableName = GetTestTableName();
        var keyName = "Id";
        var keyValue = new PrimitiveType($"array-condition-test-{Guid.NewGuid():N}");

        try
        {
            // Create item with array
            var item = new JObject
            {
                ["Name"] = "ArrayConditionTest",
                ["Tags"] = new JArray { "production", "critical", "database" },
                ["Numbers"] = new JArray { 1, 2, 3, 5, 8 }
            };
            await service.PutItemAsync(tableName, keyName, keyValue, item);

            // Test ArrayElementExists condition - should allow deletion
            var arrayExistsCondition = service.BuildArrayElementExistsCondition("Tags", new PrimitiveType("production"));
            var deleteResult1 = await service.DeleteItemAsync(tableName, keyName, keyValue,
                ReturnItemBehavior.ReturnOldValues, arrayExistsCondition);

            deleteResult1.IsSuccessful.Should().BeTrue("Delete should succeed when array contains 'production'");
            deleteResult1.Data.Should().NotBeNull("Should return old values");

            // Recreate item for next test
            await service.PutItemAsync(tableName, keyName, keyValue, item);

            // Test ArrayElementNotExists condition - should prevent deletion
            var arrayNotExistsCondition = service.BuildArrayElementNotExistsCondition("Tags", new PrimitiveType("production"));
            var deleteResult2 = await service.DeleteItemAsync(tableName, keyName, keyValue,
                ReturnItemBehavior.DoNotReturn, arrayNotExistsCondition);

            deleteResult2.IsSuccessful.Should().BeFalse("Delete should fail when array contains 'production' but condition requires it not to");

            // Test ArrayElementNotExists with element that doesn't exist - should allow deletion
            var arrayNotExistsCondition2 = service.BuildArrayElementNotExistsCondition("Tags", new PrimitiveType("nonexistent"));
            var deleteResult3 = await service.DeleteItemAsync(tableName, keyName, keyValue,
                ReturnItemBehavior.DoNotReturn, arrayNotExistsCondition2);

            deleteResult3.IsSuccessful.Should().BeTrue("Delete should succeed when array doesn't contain 'nonexistent'");
        }
        finally
        {
            await CleanupDatabaseAsync(tableName);
            if (service is IDisposable disposable)
                disposable.Dispose();
        }
    }

    [Fact]
    public async Task Conditions_InAddElementsToArrayAsync_ShouldControlArrayModification()
    {
        var service = CreateDatabaseService();
        if (!service.IsInitialized)
        {
            return;
        }

        var tableName = GetTestTableName();
        var keyName = "Id";
        var keyValue = new PrimitiveType($"array-add-condition-test-{Guid.NewGuid():N}");

        try
        {
            // Create item with initial state
            var item = new JObject
            {
                ["Name"] = "ArrayAddTest",
                ["Status"] = "active",
                ["Tags"] = new JArray { "initial" }
            };
            await service.PutItemAsync(tableName, keyName, keyValue, item);

            // Test condition that should allow adding elements
            var allowCondition = service.BuildAttributeEqualsCondition("Status", new PrimitiveType("active"));
            var elementsToAdd1 = new[]
            {
                new PrimitiveType("allowed1"),
                new PrimitiveType("allowed2")
            };

            var addResult1 = await service.AddElementsToArrayAsync(
                tableName, keyName, keyValue, "Tags", elementsToAdd1,
                ReturnItemBehavior.ReturnNewValues, allowCondition);

            addResult1.IsSuccessful.Should().BeTrue("Should add elements when Status equals 'active'");
            var tags1 = addResult1.Data!["Tags"] as JArray;
            tags1!.Count.Should().Be(3);
            tags1.Should().Contain(t => t.ToString() == "allowed1");
            tags1.Should().Contain(t => t.ToString() == "allowed2");

            // Test condition that should prevent adding elements
            var preventCondition = service.BuildAttributeEqualsCondition("Status", new PrimitiveType("inactive"));
            var elementsToAdd2 = new[]
            {
                new PrimitiveType("blocked1"),
                new PrimitiveType("blocked2")
            };

            var addResult2 = await service.AddElementsToArrayAsync(
                tableName, keyName, keyValue, "Tags", elementsToAdd2,
                ReturnItemBehavior.DoNotReturn, preventCondition);

            addResult2.IsSuccessful.Should().BeFalse("Should not add elements when Status doesn't equal 'inactive'");

            // Verify elements were not added
            var getResult = await service.GetItemAsync(tableName, keyName, keyValue);
            var tags2 = getResult.Data!["Tags"] as JArray;
            tags2!.Count.Should().Be(3); // Should still be 3, not 5
            tags2.Should().NotContain(t => t.ToString() == "blocked1");
            tags2.Should().NotContain(t => t.ToString() == "blocked2");
        }
        finally
        {
            await CleanupDatabaseAsync(tableName);
            if (service is IDisposable disposable)
                disposable.Dispose();
        }
    }

    [Fact]
    public async Task ComplexConditions_InRealScenarios_ShouldHandleBusinessLogic()
    {
        var service = CreateDatabaseService();
        if (!service.IsInitialized)
        {
            return;
        }

        var tableName = GetTestTableName();
        var keyName = "Id";

        try
        {
            // Scenario: User account management with conditions
            var userId = new PrimitiveType($"user-{Guid.NewGuid():N}");
            var userItem = new JObject
            {
                ["Username"] = "testuser",
                ["AccountBalance"] = 100.0,
                ["Status"] = "active",
                ["LoginAttempts"] = 0,
                ["Permissions"] = new JArray { "read", "write" }
            };
            await service.PutItemAsync(tableName, keyName, userId, userItem);

            // Business rule: Only deduct money if account is active
            var activeCondition = service.BuildAttributeEqualsCondition("Status", new PrimitiveType("active"));

            // Test deducting money - should work
            var deductResult = await service.IncrementAttributeAsync(
                tableName, keyName, userId, "AccountBalance", -30.0, activeCondition);
            deductResult.IsSuccessful.Should().BeTrue("Should deduct money from active account");
            deductResult.Data.Should().Be(70.0);

            // Business rule: Block account after too many login attempts
            await service.UpdateItemAsync(tableName, keyName, userId,
                new JObject { ["LoginAttempts"] = 5 });

            var tooManyAttemptsCondition = service.BuildAttributeGreaterOrEqualCondition("LoginAttempts", new PrimitiveType(5.0));
            var blockResult = await service.UpdateItemAsync(tableName, keyName, userId,
                new JObject { ["Status"] = "blocked" },
                ReturnItemBehavior.DoNotReturn, tooManyAttemptsCondition);

            blockResult.IsSuccessful.Should().BeTrue("Should block account with too many login attempts");

            // Business rule: Cannot deduct from blocked account
            var deductFromBlockedResult = await service.IncrementAttributeAsync(
                tableName, keyName, userId, "AccountBalance", -10.0, activeCondition); // Using active condition, should fail

            deductFromBlockedResult.IsSuccessful.Should().BeFalse("Should not deduct money from blocked account");

            // Business rule: Add permission only if user has basic permissions
            var hasReadPermissionCondition = service.BuildArrayElementExistsCondition("Permissions", new PrimitiveType("read"));
            var addPermissionResult = await service.AddElementsToArrayAsync(
                tableName, keyName, userId, "Permissions",
                [new PrimitiveType("admin")],
                ReturnItemBehavior.ReturnNewValues, hasReadPermissionCondition);

            addPermissionResult.IsSuccessful.Should().BeTrue("Should add admin permission when user has read permission");
            var permissions = addPermissionResult.Data!["Permissions"] as JArray;
            permissions.Should().Contain(p => p.ToString() == "admin");
        }
        finally
        {
            await CleanupDatabaseAsync(tableName);
            if (service is IDisposable disposable)
                disposable.Dispose();
        }
    }

    [Fact]
    public async Task ArrayOperations_WithRealCredentials_ShouldWorkCorrectly()
    {
        var service = CreateDatabaseService();
        if (!service.IsInitialized)
        {
            return;
        }

        var tableName = GetTestTableName();
        var keyName = "Id";
        var keyValue = new PrimitiveType($"array-test-{Guid.NewGuid():N}");

        try
        {
            // Create initial item
            var item = new JObject
            {
                ["Name"] = "ArrayTestItem",
                ["Tags"] = new JArray { "initial", "test" }
            };

            await service.PutItemAsync(tableName, keyName, keyValue, item);

            // Add elements to array
            var elementsToAdd = new[]
            {
                new PrimitiveType("added1"),
                new PrimitiveType("added2")
            };

            var addResult = await service.AddElementsToArrayAsync(
                tableName, keyName, keyValue, "Tags", elementsToAdd);
            addResult.IsSuccessful.Should().BeTrue("Add elements should succeed");

            // Verify elements were added
            var getResult = await service.GetItemAsync(tableName, keyName, keyValue);
            var tags = getResult.Data!["Tags"] as JArray;
            tags.Should().NotBeNull();
            tags!.Count.Should().Be(4);
            tags.Should().Contain(t => t.ToString() == "added1");
            tags.Should().Contain(t => t.ToString() == "added2");

            // Remove elements from array
            var elementsToRemove = new[]
            {
                new PrimitiveType("initial"),
                new PrimitiveType("added1")
            };

            var removeResult = await service.RemoveElementsFromArrayAsync(
                tableName, keyName, keyValue, "Tags", elementsToRemove);
            removeResult.IsSuccessful.Should().BeTrue("Remove elements should succeed");

            // Verify elements were removed
            var getFinalResult = await service.GetItemAsync(tableName, keyName, keyValue);
            var finalTags = getFinalResult.Data!["Tags"] as JArray;
            finalTags!.Count.Should().Be(2);
            finalTags.Should().Contain(t => t.ToString() == "test");
            finalTags.Should().Contain(t => t.ToString() == "added2");
            finalTags.Should().NotContain(t => t.ToString() == "initial");
            finalTags.Should().NotContain(t => t.ToString() == "added1");
        }
        finally
        {
            await CleanupDatabaseAsync(tableName);
            if (service is IDisposable disposable)
                disposable.Dispose();
        }
    }

    [Fact]
    public async Task PutItemAsync_WithComplexNestedDocument_ShouldPreserveStructure()
    {
        // Arrange
        var service = CreateDatabaseService();
        var tableName = GetTestTableName();
        var keyName = "Id";
        var keyValue = new PrimitiveType("complex-mongo-doc");

        var complexItem = new JObject
        {
            ["Name"] = "ComplexMongoDocument",
            ["Profile"] = new JObject
            {
                ["FirstName"] = "John",
                ["LastName"] = "Doe",
                ["Age"] = 30,
                ["Addresses"] = new JArray
                {
                    new JObject
                    {
                        ["Type"] = "Home",
                        ["Street"] = "123 Main St",
                        ["City"] = "Anytown",
                        ["Coordinates"] = new JObject
                        {
                            ["Latitude"] = 40.7128,
                            ["Longitude"] = -74.0060
                        }
                    },
                    new JObject
                    {
                        ["Type"] = "Work",
                        ["Street"] = "456 Business Ave",
                        ["City"] = "Worktown"
                    }
                }
            },
            ["Tags"] = new JArray { "vip", "customer", "active" },
            ["Metadata"] = new JObject
            {
                ["CreatedAt"] = DateTime.UtcNow,
                ["Version"] = "1.0"
            }
        };

        try
        {
            // Act
            var putResult = await service.PutItemAsync(tableName, keyName, keyValue, complexItem);
            var getResult = await service.GetItemAsync(tableName, keyName, keyValue);

            // Assert
            putResult.IsSuccessful.Should().BeTrue();
            getResult.IsSuccessful.Should().BeTrue();
            getResult.Data.Should().NotBeNull();

            var retrievedData = getResult.Data!;
            retrievedData["Profile"]?["FirstName"]?.ToString().Should().Be("John");
            retrievedData["Profile"]?["Addresses"]?.Should().BeOfType<JArray>();
            var addresses = (JArray)retrievedData["Profile"]!["Addresses"]!;
            addresses.Should().HaveCount(2);
            addresses[0]?["Coordinates"]?["Latitude"]?.ToObject<double>().Should().BeApproximately(40.7128, 0.0001);
        }
        finally
        {
            await CleanupDatabaseAsync(tableName);
            if (service is IDisposable disposable)
                disposable.Dispose();
        }
    }

    [Fact]
    public async Task BatchOperations_WithMultipleItems_ShouldHandleEfficiently()
    {
        // Arrange
        var service = CreateDatabaseService();
        var tableName = GetTestTableName();
        var keyName = "Id";

        // Create multiple items for batch testing
        var keyValues = Enumerable.Range(1, 25)
            .Select(i => new PrimitiveType($"batch-key-{i}"))
            .ToArray();

        try
        {
            // Setup - Put multiple items
            foreach (var keyValue in keyValues)
            {
                var item = new JObject
                {
                    ["Name"] = $"BatchItem-{keyValue.AsString}",
                    ["Value"] = keyValue.AsString.GetHashCode()
                };
                await service.PutItemAsync(tableName, keyName, keyValue, item);
            }

            // Act - Get multiple items
            var result = await service.GetItemsAsync(tableName, keyName, keyValues);

            // Assert
            result.IsSuccessful.Should().BeTrue();
            result.Data.Should().NotBeNull();
            result.Data!.Count.Should().Be(keyValues.Length);

            // Verify each item
            foreach (var item in result.Data)
            {
                item["Name"]?.ToString().Should().StartWith("BatchItem-");
                item[keyName]?.ToString().Should().StartWith("batch-key-");
            }
        }
        finally
        {
            await CleanupDatabaseAsync(tableName);
            if (service is IDisposable disposable)
                disposable.Dispose();
        }
    }

    [Fact]
    public async Task ConditionalOperations_WithAttributeNotExists_ShouldWorkCorrectly()
    {
        // Arrange
        var service = CreateDatabaseService();
        var tableName = GetTestTableName();
        var keyName = "Id";
        var keyValue = new PrimitiveType("conditional-test");

        try
        {
            var item = new JObject
            {
                ["Name"] = "ConditionalItem",
                ["Status"] = "active"
            };

            // Setup
            await service.PutItemAsync(tableName, keyName, keyValue, item);

            // Test with attribute not exists condition
            var condition = service.BuildAttributeNotExistsCondition("NewAttribute");
            var updateData = new JObject
            {
                ["NewAttribute"] = "NewValue"
            };

            // Act
            var result = await service.UpdateItemAsync(tableName, keyName, keyValue, updateData,
                condition: condition);

            // Assert
            result.IsSuccessful.Should().BeTrue();

            // Verify the update happened
            var getResult = await service.GetItemAsync(tableName, keyName, keyValue);
            getResult.Data!["NewAttribute"]?.ToString().Should().Be("NewValue");
        }
        finally
        {
            await CleanupDatabaseAsync(tableName);
            if (service is IDisposable disposable)
                disposable.Dispose();
        }
    }

    [Fact]
    public async Task ParallelOperations_ShouldHandleConcurrentRequests()
    {
        // Arrange
        var service = CreateDatabaseService();
        var tableName = GetTestTableName();
        var keyName = "Id";
        const int concurrentOperations = 10;

        try
        {
            // Act - Perform parallel put operations
            var tasks = Enumerable.Range(1, concurrentOperations)
                .Select(async i =>
                {
                    var keyValue = new PrimitiveType($"parallel-key-{i}");
                    var item = new JObject
                    {
                        ["Name"] = $"ParallelItem-{i}",
                        ["ThreadId"] = Environment.CurrentManagedThreadId
                    };
                    return await service.PutItemAsync(tableName, keyName, keyValue, item);
                });

            var results = await Task.WhenAll(tasks);

            // Assert
            results.Should().HaveCount(concurrentOperations);
            results.Should().OnlyContain(r => r.IsSuccessful);

            // Verify all items were created
            var scanResult = await service.ScanTableAsync(tableName, [keyName]);
            scanResult.Data!.Count.Should().Be(concurrentOperations);
        }
        finally
        {
            await CleanupDatabaseAsync(tableName);
            if (service is IDisposable disposable)
                disposable.Dispose();
        }
    }

    [Fact]
    public async Task Operations_OnNonExistentTable_ShouldHandleGracefully()
    {
        // Arrange
        var service = CreateDatabaseService();
        var nonExistentTable = "non-existent-table-" + Guid.NewGuid();
        var keyName = "Id";
        var keyValue = new PrimitiveType("test-key");

        try
        {
            // Act & Assert - Different operations should handle non-existent tables gracefully
            var getResult = await service.GetItemAsync(nonExistentTable, keyName, keyValue);
            getResult.IsSuccessful.Should().BeFalse();

            var scanResult = await service.ScanTableAsync(nonExistentTable, [keyName]);
            scanResult.IsSuccessful.Should().BeFalse();

            var existsResult = await service.ItemExistsAsync(nonExistentTable, keyName, keyValue);
            existsResult.IsSuccessful.Should().BeFalse();
        }
        finally
        {
            if (service is IDisposable disposable)
                disposable.Dispose();
        }
    }

    [Fact]
    public async Task LargeItem_ShouldHandleWithinDynamoDBLimits()
    {
        // Arrange
        var service = CreateDatabaseService();
        var tableName = GetTestTableName();
        var keyName = "Id";
        var keyValue = new PrimitiveType("large-item-key");

        // Create a large item (but within DynamoDB's 400KB limit)
        var largeData = new string('A', 100_000); // 100KB string
        var largeItem = new JObject
        {
            ["Name"] = "LargeItem",
            ["LargeData"] = largeData,
            ["Metadata"] = new JObject
            {
                ["Size"] = largeData.Length,
                ["Created"] = DateTime.UtcNow
            }
        };

        try
        {
            // Act
            var putResult = await service.PutItemAsync(tableName, keyName, keyValue, largeItem);
            var getResult = await service.GetItemAsync(tableName, keyName, keyValue);

            // Assert
            putResult.IsSuccessful.Should().BeTrue();
            getResult.IsSuccessful.Should().BeTrue();
            getResult.Data.Should().NotBeNull();
            getResult.Data!["LargeData"]?.ToString().Should().HaveLength(100_000);
        }
        finally
        {
            await CleanupDatabaseAsync(tableName);
            if (service is IDisposable disposable)
                disposable.Dispose();
        }
    }

    [Fact]
    public async Task PutItemAsync_WithObjectId_ShouldHandleMongoDBObjectId()
    {
        // Arrange
        var service = CreateDatabaseService();
        var tableName = GetTestTableName();
        var keyName = "Id";
        var keyValue = new Utilities.Common.PrimitiveType("507f1f77bcf86cd799439011"); // Valid ObjectId format

        var item = new Newtonsoft.Json.Linq.JObject
        {
            ["Name"] = "MongoItem",
            ["Value"] = 42,
            ["IsMongoTest"] = true
        };

        try
        {
            // Act
            var putResult = await service.PutItemAsync(tableName, keyName, keyValue, item);
            var getResult = await service.GetItemAsync(tableName, keyName, keyValue);

            // Assert
            putResult.IsSuccessful.Should().BeTrue();
            getResult.IsSuccessful.Should().BeTrue();
            getResult.Data.Should().NotBeNull();
            getResult.Data!["Name"]?.ToString().Should().Be("MongoItem");
            getResult.Data["IsMongoTest"]?.ToObject<bool>().Should().BeTrue();
        }
        finally
        {
            await CleanupDatabaseAsync(tableName);
            if (service is IDisposable disposable)
                disposable.Dispose();
        }
    }
}
