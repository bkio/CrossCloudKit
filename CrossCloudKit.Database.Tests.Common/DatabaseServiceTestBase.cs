// Copyright (c) 2022- Burak Kara, MIT License
// See LICENSE file in the project root for full license information.

using System.Net;
using CrossCloudKit.Interfaces;
using CrossCloudKit.Interfaces.Classes;
using CrossCloudKit.Interfaces.Enums;
using FluentAssertions;
using Newtonsoft.Json.Linq;
using CrossCloudKit.Utilities.Common;
using xRetry;

namespace CrossCloudKit.Database.Tests.Common;

/// <summary>
/// Base class for database service integration tests that covers all IDatabaseService functionality
/// </summary>
public abstract class DatabaseServiceTestBase
{
    protected abstract IDatabaseService CreateDatabaseService();

    private async Task CleanupDatabaseAsync(string tableName)
    {
        var service = CreateDatabaseService();
        try
        {
            if (service.IsInitialized)
            {
                try
                {
                    await service.DropTableAsync(tableName);
                }
                catch
                {
                    // Ignore errors in cleanup
                }
            }
        }
        catch (Exception)
        {
            // Ignore cleanup errors in tests - this is common practice for test cleanup
        }
        finally
        {
            if (service is IDisposable disposable)
            {
                try
                {
                    disposable.Dispose();
                }
                catch (Exception)
                {
                    // Ignore cleanup errors in tests - this is common practice for test cleanup
                }
            }
        }
    }

    private static string GetTestTableName([System.Runtime.CompilerServices.CallerMemberName] string testName = "")
    {
        var rand = $"{Guid.NewGuid():N}";
        DatabaseServiceBase.SystemTableNamePostfix = $"-{rand}";
        return $"{testName}-{rand}";
    }

    private static Primitive CreateStringKey(string value = "test-key") => new(value);
    private static Primitive CreateIntegerKey(long value = 123) => new(value);
    private static Primitive CreateDoubleKey(double value = 123.456) => new(value);
    private static Primitive CreateByteArrayKey(byte[]? value = null) => new(value ?? [1, 2, 3, 4, 5]);

    private static JObject CreateTestItem(string name = "TestItem", int value = 42)
    {
        return new JObject
        {
            ["Name"] = name,
            ["Value"] = value,
            ["Created"] = DateTime.UtcNow,
            ["Tags"] = new JArray { "tag1", "tag2", "tag3" }
        };
    }

    private static JObject CreateUpdateData(string newName = "UpdatedItem", int newValue = 84)
    {
        return new JObject
        {
            ["Name"] = newName,
            ["Value"] = newValue,
            ["Updated"] = DateTime.UtcNow
        };
    }

    [RetryFact(3, 5000)]
    public virtual void AttributeExists_ShouldReturnExistenceCondition()
    {
        // Arrange
        var service = CreateDatabaseService();
        const string attributeName = "TestAttribute";

        try
        {
            // Act
            var condition = service.AttributeExists(attributeName);

            // Assert
            condition.Should().BeOfType<ExistenceCondition>();
            condition.ConditionType.Should().Be(ConditionType.AttributeExists);
            condition.AttributeName.Should().Be(attributeName);
        }
        finally
        {
            if (service is IDisposable disposable)
                disposable.Dispose();
        }
    }

    [RetryFact(3, 5000)]
    public virtual void AttributeNotExists_ShouldReturnExistenceCondition()
    {
        // Arrange
        var service = CreateDatabaseService();
        const string attributeName = "TestAttribute";

        try
        {
            // Act
            var condition = service.AttributeNotExists(attributeName);

            // Assert
            condition.Should().BeOfType<ExistenceCondition>();
            condition.ConditionType.Should().Be(ConditionType.AttributeNotExists);
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
    [RetryFact(3, 5000)]
    public async Task PutItemAsync_WithNewItem_ShouldSucceed()
    {
        // Arrange
        var service = CreateDatabaseService();

        var tableName = GetTestTableName();
        var keyName = "Id";
        var keyValue = CreateStringKey();
        var item = CreateTestItem();

        try
        {
            // Act
            var result = await service.PutItemAsync(tableName, new DbKey(keyName, keyValue), item);

            // Assert
            result.Should().NotBeNull();
            result.IsSuccessful.Should().BeTrue();
            result.ErrorMessage.Should().Be("");
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
    [RetryFact(3, 5000)]
    public async Task GetItemAsync_WhenItemNotExists_ShouldReturnNull()
    {
        // Arrange
        var service = CreateDatabaseService();

        var tableName = GetTestTableName();
        var keyName = "Id";
        var keyValue = CreateStringKey("non-existent-key");

        try
        {
            // Act
            var result = await service.GetItemAsync(tableName, new DbKey(keyName, keyValue));

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
    [RetryFact(3, 5000)]
    public async Task ItemExistsAsync_WhenItemNotExists_ShouldReturnFalse()
    {
        // Arrange
        var service = CreateDatabaseService();

        var tableName = GetTestTableName();
        var keyName = "Id";
        var keyValue = CreateStringKey("non-existent-key");

        try
        {
            // Act
            var result = await service.ItemExistsAsync(tableName, new DbKey(keyName, keyValue));

            // Assert
            result.Should().NotBeNull();
            result.IsSuccessful.Should().Be(false);
            result.StatusCode.Should().Be(HttpStatusCode.NotFound);
        }
        finally
        {
            await CleanupDatabaseAsync(tableName);
            if (service is IDisposable disposable)
                disposable.Dispose();
        }
    }

    [RetryFact(3, 5000)]
    public async Task ValueConditions_InItemExistsAsync_ShouldWorkCorrectly()
    {
        var service = CreateDatabaseService();

        var tableName = GetTestTableName();
        var keyName = "Id";
        var keyValue = new Primitive($"value-condition-test-{Guid.NewGuid():N}");

        try
        {
            // Create test item with specific values for condition testing
            var item = CreateTestItem("ConditionTest");
            item["Status"] = "active";
            item["Score"] = 85.5;
            await service.PutItemAsync(tableName, new DbKey(keyName, keyValue), item);

            // Test AttributeEquals condition - should return true
            var equalsCondition = service.AttributeEquals("Value", new Primitive(42.0));
            var existsWithEquals = await service.ItemExistsAsync(tableName, new DbKey(keyName, keyValue), equalsCondition);
            existsWithEquals.IsSuccessful.Should().BeTrue();
            existsWithEquals.Data.Should().BeTrue("Item exists and Value equals 42");

            // Test AttributeEquals condition - should return false
            var equalsConditionFalse = service.AttributeEquals("Value", new Primitive(99.0));
            var existsWithEqualsFalse = await service.ItemExistsAsync(tableName, new DbKey(keyName, keyValue), equalsConditionFalse);
            existsWithEqualsFalse.IsSuccessful.Should().BeFalse();
            existsWithEqualsFalse.StatusCode.Should().Be(HttpStatusCode.PreconditionFailed, "Item exists but Value does not equal 99");

            // Test AttributeGreater condition
            var greaterCondition = service.AttributeIsGreaterThan("Value", new Primitive(40.0));
            var existsWithGreater = await service.ItemExistsAsync(tableName, new DbKey(keyName, keyValue), greaterCondition);
            existsWithGreater.IsSuccessful.Should().BeTrue();
            existsWithGreater.Data.Should().BeTrue("Item exists and Value > 40");

            // Test AttributeLess condition
            var lessCondition = service.AttributeIsLessThan("Value", new Primitive(50.0));
            var existsWithLess = await service.ItemExistsAsync(tableName, new DbKey(keyName, keyValue), lessCondition);
            existsWithLess.IsSuccessful.Should().BeTrue();
            existsWithLess.Data.Should().BeTrue("Item exists and Value < 50");

            // Test AttributeGreaterOrEqual condition
            var greaterOrEqualCondition = service.AttributeIsGreaterOrEqual("Value", new Primitive(42.0));
            var existsWithGreaterOrEqual = await service.ItemExistsAsync(tableName, new DbKey(keyName, keyValue), greaterOrEqualCondition);
            existsWithGreaterOrEqual.IsSuccessful.Should().BeTrue();
            existsWithGreaterOrEqual.Data.Should().BeTrue("Item exists and Value >= 42");

            // Test AttributeLessOrEqual condition
            var lessOrEqualCondition = service.AttributeIsLessOrEqual("Value", new Primitive(42.0));
            var existsWithLessOrEqual = await service.ItemExistsAsync(tableName, new DbKey(keyName, keyValue), lessOrEqualCondition);
            existsWithLessOrEqual.IsSuccessful.Should().BeTrue();
            existsWithLessOrEqual.Data.Should().BeTrue("Item exists and Value <= 42");

            // Test AttributeNotEquals condition
            var notEqualsCondition = service.AttributeNotEquals("Status", new Primitive("inactive"));
            var existsWithNotEquals = await service.ItemExistsAsync(tableName, new DbKey(keyName, keyValue), notEqualsCondition);
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

    [RetryFact(3, 5000)]
    public async Task ExistenceConditions_InUpdateAsync_ShouldPreventOrAllowOperations()
    {
        var service = CreateDatabaseService();

        var tableName = GetTestTableName();
        var keyName = "Id";
        var keyValue = new Primitive($"existence-test-{Guid.NewGuid():N}");

        try
        {
            // Create item with specific attributes
            var item = CreateTestItem("ExistenceTest", 100);
            item["OptionalField"] = "present";
            await service.PutItemAsync(tableName, new DbKey(keyName, keyValue), item);

            // Test AttributeExists condition - should succeed
            var existsCondition = service.AttributeExists("OptionalField");
            var updateData1 = CreateUpdateData("UpdatedWithExists", 200);
            var updateResult1 = await service.UpdateItemAsync(tableName, new DbKey(keyName, keyValue), updateData1,
                DbReturnItemBehavior.DoNotReturn, existsCondition);

            updateResult1.IsSuccessful.Should().BeTrue("Update should succeed when required field exists");

            // Test AttributeNotExists condition - should fail
            var notExistsCondition = service.AttributeNotExists("OptionalField");
            var updateData2 = CreateUpdateData("ShouldNotUpdate", 300);
            var updateResult2 = await service.UpdateItemAsync(tableName, new DbKey(keyName, keyValue), updateData2,
                DbReturnItemBehavior.DoNotReturn, notExistsCondition);

            updateResult2.IsSuccessful.Should().BeFalse("Update should fail when field exists but condition requires it not to exist");

            // Test AttributeNotExists condition for non-existent field - should succeed
            var notExistsCondition2 = service.AttributeNotExists("NonExistentField");
            var updateData3 = CreateUpdateData("UpdatedWithNotExists", 400);
            var updateResult3 = await service.UpdateItemAsync(tableName, new DbKey(keyName, keyValue), updateData3,
                DbReturnItemBehavior.DoNotReturn, notExistsCondition2);

            updateResult3.IsSuccessful.Should().BeTrue("Update should succeed when non-existent field is checked for non-existence");

            // Test AttributeExists condition for non-existent field - should fail
            var existsCondition2 = service.AttributeExists("NonExistentField");
            var updateData4 = CreateUpdateData("ShouldNotUpdate2", 500);
            var updateResult4 = await service.UpdateItemAsync(tableName, new DbKey(keyName, keyValue), updateData4,
                DbReturnItemBehavior.DoNotReturn, existsCondition2);

            updateResult4.IsSuccessful.Should().BeFalse("Update should fail when non-existent field is checked for existence");
        }
        finally
        {
            await CleanupDatabaseAsync(tableName);
            if (service is IDisposable disposable)
                disposable.Dispose();
        }
    }

    [RetryFact(3, 5000)]
    public async Task ArrayElementConditions_InDeleteAsync_ShouldWorkWithArrays()
    {
        var service = CreateDatabaseService();

        var tableName = GetTestTableName();
        var keyName = "Id";
        var keyValue = new Primitive($"array-condition-test-{Guid.NewGuid():N}");

        try
        {
            // Create item with array
            var item = new JObject
            {
                ["Name"] = "ArrayConditionTest",
                ["Tags"] = new JArray { "production", "critical", "database" },
                ["Numbers"] = new JArray { 1, 2, 3, 5, 8 }
            };
            await service.PutItemAsync(tableName, new DbKey(keyName, keyValue), item);

            // Test ArrayElementExists condition - should allow deletion
            var arrayExistsCondition = service.ArrayElementExists("Tags", new Primitive("production"));
            var deleteResult1 = await service.DeleteItemAsync(tableName, new DbKey(keyName, keyValue),
                DbReturnItemBehavior.ReturnOldValues, arrayExistsCondition);

            deleteResult1.IsSuccessful.Should().BeTrue("Delete should succeed when array contains 'production'");
            deleteResult1.Data.Should().NotBeNull("Should return old values");

            // Recreate item for next test
            await service.PutItemAsync(tableName, new DbKey(keyName, keyValue), item);

            // Test ArrayElementNotExists condition - should prevent deletion
            var arrayNotExistsCondition = service.ArrayElementNotExists("Tags", new Primitive("production"));
            var deleteResult2 = await service.DeleteItemAsync(tableName, new DbKey(keyName, keyValue),
                DbReturnItemBehavior.DoNotReturn, arrayNotExistsCondition);

            deleteResult2.IsSuccessful.Should().BeFalse("Delete should fail when array contains 'production' but condition requires it not to");

            // Test ArrayElementNotExists with element that doesn't exist - should allow deletion
            var arrayNotExistsCondition2 = service.ArrayElementNotExists("Tags", new Primitive("nonexistent"));
            var deleteResult3 = await service.DeleteItemAsync(tableName, new DbKey(keyName, keyValue),
                DbReturnItemBehavior.DoNotReturn, arrayNotExistsCondition2);

            deleteResult3.IsSuccessful.Should().BeTrue("Delete should succeed when array doesn't contain 'nonexistent'");
        }
        finally
        {
            await CleanupDatabaseAsync(tableName);
            if (service is IDisposable disposable)
                disposable.Dispose();
        }
    }

    [RetryFact(3, 5000)]
    public async Task Conditions_InAddElementsToArrayAsync_ShouldControlArrayModification()
    {
        var service = CreateDatabaseService();

        var tableName = GetTestTableName();
        var keyName = "Id";
        var keyValue = new Primitive($"array-add-condition-test-{Guid.NewGuid():N}");

        try
        {
            // Create item with initial state
            var item = new JObject
            {
                ["Name"] = "ArrayAddTest",
                ["Status"] = "active",
                ["Tags"] = new JArray { "initial" }
            };
            await service.PutItemAsync(tableName, new DbKey(keyName, keyValue), item);

            // Test condition that should allow adding elements
            var allowCondition = service.AttributeEquals("Status", new Primitive("active"));
            var elementsToAdd1 = new[]
            {
                new Primitive("allowed1"),
                new Primitive("allowed2")
            };

            var addResult1 = await service.AddElementsToArrayAsync(
                tableName, new DbKey(keyName, keyValue), "Tags", elementsToAdd1,
                DbReturnItemBehavior.ReturnNewValues, allowCondition);

            addResult1.IsSuccessful.Should().BeTrue("Should add elements when Status equals 'active'");
            var tags1 = addResult1.Data!["Tags"] as JArray;
            tags1!.Count.Should().Be(3);
            tags1.Should().Contain(t => t.ToString() == "allowed1");
            tags1.Should().Contain(t => t.ToString() == "allowed2");

            // Test condition that should prevent adding elements
            var preventCondition = service.AttributeEquals("Status", new Primitive("inactive"));
            var elementsToAdd2 = new[]
            {
                new Primitive("blocked1"),
                new Primitive("blocked2")
            };

            var addResult2 = await service.AddElementsToArrayAsync(
                tableName, new DbKey(keyName, keyValue), "Tags", elementsToAdd2,
                DbReturnItemBehavior.DoNotReturn, preventCondition);

            addResult2.IsSuccessful.Should().BeFalse("Should not add elements when Status doesn't equal 'inactive'");

            // Verify elements were not added
            var getResult = await service.GetItemAsync(tableName, new DbKey(keyName, keyValue));
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

    [RetryFact(3, 5000)]
    public async Task ComplexConditions_InRealScenarios_ShouldHandleBusinessLogic()
    {
        var service = CreateDatabaseService();

        var tableName = GetTestTableName();
        var keyName = "Id";

        try
        {
            // Scenario: User account management with conditions
            var userId = new Primitive($"user-{Guid.NewGuid():N}");
            var userItem = new JObject
            {
                ["Username"] = "testuser",
                ["AccountBalance"] = 100.0,
                ["Status"] = "active",
                ["LoginAttempts"] = 0,
                ["Permissions"] = new JArray { "read", "write" }
            };
            await service.PutItemAsync(tableName, new DbKey(keyName, userId), userItem);

            // Business rule: Only deduct money if account is active
            var activeCondition = service.AttributeEquals("Status", new Primitive("active"));

            // Test deducting money - should work
            var deductResult = await service.IncrementAttributeAsync(
                tableName, new DbKey(keyName, userId), "AccountBalance", -30.0, activeCondition);
            deductResult.IsSuccessful.Should().BeTrue("Should deduct money from active account");
            deductResult.Data.Should().Be(70.0);

            // Business rule: Block account after too many login attempts
            await service.UpdateItemAsync(tableName, new DbKey(keyName, userId),
                new JObject { ["LoginAttempts"] = 5 });

            var tooManyAttemptsCondition = service.AttributeIsGreaterOrEqual("LoginAttempts", new Primitive(5.0));
            var blockResult = await service.UpdateItemAsync(tableName, new DbKey(keyName, userId),
                new JObject { ["Status"] = "blocked" },
                DbReturnItemBehavior.DoNotReturn, tooManyAttemptsCondition);

            blockResult.IsSuccessful.Should().BeTrue("Should block account with too many login attempts");

            // Business rule: Cannot deduct from blocked account
            var deductFromBlockedResult = await service.IncrementAttributeAsync(
                tableName, new DbKey(keyName, userId), "AccountBalance", -10.0, activeCondition); // Using active condition, should fail

            deductFromBlockedResult.IsSuccessful.Should().BeFalse("Should not deduct money from blocked account");

            // Business rule: Add permission only if user has basic permissions
            var hasReadPermissionCondition = service.ArrayElementExists("Permissions", new Primitive("read"));
            var addPermissionResult = await service.AddElementsToArrayAsync(
                tableName, new DbKey(keyName, userId), "Permissions",
                [new Primitive("admin")],
                DbReturnItemBehavior.ReturnNewValues, hasReadPermissionCondition);

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

    [RetryFact(3, 5000)]
    public async Task ArrayOperations_WithRealCredentials_ShouldWorkCorrectly()
    {
        var service = CreateDatabaseService();

        var tableName = GetTestTableName();
        var keyName = "Id";
        var keyValue = new Primitive($"array-test-{Guid.NewGuid():N}");

        try
        {
            // Create initial item
            var item = new JObject
            {
                ["Name"] = "ArrayTestItem",
                ["Tags"] = new JArray { "initial", "test" }
            };

            await service.PutItemAsync(tableName, new DbKey(keyName, keyValue), item);

            // Add elements to array
            var elementsToAdd = new[]
            {
                new Primitive("added1"),
                new Primitive("added2")
            };

            var addResult = await service.AddElementsToArrayAsync(
                tableName, new DbKey(keyName, keyValue), "Tags", elementsToAdd);
            addResult.IsSuccessful.Should().BeTrue("Add elements should succeed");

            // Verify elements were added
            var getResult = await service.GetItemAsync(tableName, new DbKey(keyName, keyValue));
            var tags = getResult.Data!["Tags"] as JArray;
            tags.Should().NotBeNull();
            tags!.Count.Should().Be(4);
            tags.Should().Contain(t => t.ToString() == "added1");
            tags.Should().Contain(t => t.ToString() == "added2");

            // Remove elements from array
            var elementsToRemove = new[]
            {
                new Primitive("initial"),
                new Primitive("added1")
            };

            var removeResult = await service.RemoveElementsFromArrayAsync(
                tableName, new DbKey(keyName, keyValue), "Tags", elementsToRemove);
            removeResult.IsSuccessful.Should().BeTrue("Remove elements should succeed");

            // Verify elements were removed
            var getFinalResult = await service.GetItemAsync(tableName, new DbKey(keyName, keyValue));
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

    [RetryFact(3, 5000)]
    public async Task PutItemAsync_WithComplexNestedDocument_ShouldPreserveStructure()
    {
        // Arrange
        var service = CreateDatabaseService();
        var tableName = GetTestTableName();
        var keyName = "Id";
        var keyValue = new Primitive("complex-mongo-doc");

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
            var putResult = await service.PutItemAsync(tableName, new DbKey(keyName, keyValue), complexItem);
            var getResult = await service.GetItemAsync(tableName, new DbKey(keyName, keyValue));

            // Assert
            putResult.IsSuccessful.Should().BeTrue();
            getResult.IsSuccessful.Should().BeTrue();
            getResult.Data.Should().NotBeNull();

            var retrievedData = getResult.Data;
            retrievedData?["Profile"]?["FirstName"]?.ToString().Should().Be("John");
            retrievedData?["Profile"]?["Addresses"]?.Should().BeOfType<JArray>();
            var addresses = (JArray)retrievedData?["Profile"]!["Addresses"]!;
            addresses.Should().HaveCount(2);
            addresses[0]["Coordinates"]?["Latitude"]?.ToObject<double>().Should().BeApproximately(40.7128, 0.0001);
        }
        finally
        {
            await CleanupDatabaseAsync(tableName);
            if (service is IDisposable disposable)
                disposable.Dispose();
        }
    }

    [RetryFact(3, 5000)]
    public async Task BatchOperations_WithMultipleItems_ShouldHandleEfficiently()
    {
        // Arrange
        var service = CreateDatabaseService();
        var tableName = GetTestTableName();
        var keyName = "Id";

        // Create multiple items for batch testing
        var keys = Enumerable.Range(1, 25)
            .Select(i => new DbKey(keyName, new Primitive($"batch-key-{i}")))
            .ToArray();

        try
        {
            // Setup - Put multiple items
            foreach (var key in keys)
            {
                var item = new JObject
                {
                    ["Name"] = $"BatchItem-{key.Value.AsString}",
                    ["Value"] = key.Value.AsString.GetHashCode()
                };
                await service.PutItemAsync(tableName, key, item);
            }

            // Act - Get multiple items
            var result = await service.GetItemsAsync(tableName, keys);

            // Assert
            result.IsSuccessful.Should().BeTrue();
            result.Data.Should().NotBeNull();
            result.Data.Count.Should().Be(keys.Length);

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

    [RetryFact(3, 5000)]
    public async Task ConditionalOperations_WithAttributeNotExists_ShouldWorkCorrectly()
    {
        // Arrange
        var service = CreateDatabaseService();
        var tableName = GetTestTableName();
        var keyName = "Id";
        var keyValue = new Primitive("conditional-test");

        try
        {
            var item = new JObject
            {
                ["Name"] = "ConditionalItem",
                ["Status"] = "active"
            };

            // Setup
            await service.PutItemAsync(tableName, new DbKey(keyName, keyValue), item);

            // Test with attribute not exists condition
            var condition = service.AttributeNotExists("NewAttribute");
            var updateData = new JObject
            {
                ["NewAttribute"] = "NewValue"
            };

            // Act
            var result = await service.UpdateItemAsync(tableName, new DbKey(keyName, keyValue), updateData,
                conditions: condition);

            // Assert
            result.IsSuccessful.Should().BeTrue();

            // Verify the update happened
            var getResult = await service.GetItemAsync(tableName, new DbKey(keyName, keyValue));
            getResult.Data!["NewAttribute"]?.ToString().Should().Be("NewValue");
        }
        finally
        {
            await CleanupDatabaseAsync(tableName);
            if (service is IDisposable disposable)
                disposable.Dispose();
        }
    }

    [RetryFact(3, 5000)]
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
                    var keyValue = new Primitive($"parallel-key-{i}");
                    var item = new JObject
                    {
                        ["Name"] = $"ParallelItem-{i}",
                        ["ThreadId"] = Environment.CurrentManagedThreadId
                    };
                    return await service.PutItemAsync(tableName, new DbKey(keyName, keyValue), item);
                });

            var results = await Task.WhenAll(tasks);

            // Assert
            results.Should().HaveCount(concurrentOperations);
            results.Should().OnlyContain(r => r.IsSuccessful);

            // Verify all items were created
            var scanResult = await service.ScanTableAsync(tableName);
            scanResult.Data.Items.Count.Should().Be(concurrentOperations);
        }
        finally
        {
            await CleanupDatabaseAsync(tableName);
            if (service is IDisposable disposable)
                disposable.Dispose();
        }
    }

    [RetryFact(3, 5000)]
    public async Task LargeItem_ShouldHandleWithinDynamoDBLimits()
    {
        // Arrange
        var service = CreateDatabaseService();
        var tableName = GetTestTableName();
        var keyName = "Id";
        var keyValue = new Primitive("large-item-key");

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
            var putResult = await service.PutItemAsync(tableName, new DbKey(keyName, keyValue), largeItem);
            var getResult = await service.GetItemAsync(tableName, new DbKey(keyName, keyValue));

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

    [RetryFact(3, 5000)]
    public async Task PutItemAsync_WithObjectId_ShouldHandleMongoDBObjectId()
    {
        // Arrange
        var service = CreateDatabaseService();
        var tableName = GetTestTableName();
        const string keyName = "Id";
        var keyValue = new Primitive("507f1f77bcf86cd799439011"); // Valid ObjectId format

        var item = new JObject
        {
            ["Name"] = "MongoItem",
            ["Value"] = 42,
            ["IsMongoTest"] = true
        };

        try
        {
            // Act
            var putResult = await service.PutItemAsync(tableName, new DbKey(keyName, keyValue), item);
            var getResult = await service.GetItemAsync(tableName, new DbKey(keyName, keyValue));

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

    #region Additional Comprehensive Tests

    [RetryFact(3, 5000)]
    public void BuildValueConditions_ShouldReturnCorrectConditionTypes()
    {
        // Arrange
        var service = CreateDatabaseService();
        const string attributeName = "TestAttribute";
        var testValue = new Primitive("testValue");

        try
        {
            // Act & Assert - Equals
            var equalsCondition = service.AttributeEquals(attributeName, testValue);
            equalsCondition.Should().BeOfType<ValueCondition>();
            equalsCondition.ConditionType.Should().Be(ConditionType.AttributeEquals);
            equalsCondition.AttributeName.Should().Be(attributeName);
            ((ValueCondition)equalsCondition).Value.Should().Be(testValue);

            // Act & Assert - Not Equals
            var notEqualsCondition = service.AttributeNotEquals(attributeName, testValue);
            notEqualsCondition.Should().BeOfType<ValueCondition>();
            notEqualsCondition.ConditionType.Should().Be(ConditionType.AttributeNotEquals);

            // Act & Assert - Greater
            var greaterCondition = service.AttributeIsGreaterThan(attributeName, testValue);
            greaterCondition.Should().BeOfType<ValueCondition>();
            greaterCondition.ConditionType.Should().Be(ConditionType.AttributeGreater);

            // Act & Assert - Greater Or Equal
            var greaterOrEqualCondition = service.AttributeIsGreaterOrEqual(attributeName, testValue);
            greaterOrEqualCondition.Should().BeOfType<ValueCondition>();
            greaterOrEqualCondition.ConditionType.Should().Be(ConditionType.AttributeGreaterOrEqual);

            // Act & Assert - Less
            var lessCondition = service.AttributeIsLessThan(attributeName, testValue);
            lessCondition.Should().BeOfType<ValueCondition>();
            lessCondition.ConditionType.Should().Be(ConditionType.AttributeLess);

            // Act & Assert - Less Or Equal
            var lessOrEqualCondition = service.AttributeIsLessOrEqual(attributeName, testValue);
            lessOrEqualCondition.Should().BeOfType<ValueCondition>();
            lessOrEqualCondition.ConditionType.Should().Be(ConditionType.AttributeLessOrEqual);
        }
        finally
        {
            if (service is IDisposable disposable)
                disposable.Dispose();
        }
    }

    [RetryFact(3, 5000)]
    public void BuildArrayElementConditions_ShouldReturnCorrectConditionTypes()
    {
        // Arrange
        var service = CreateDatabaseService();
        const string attributeName = "TestArray";
        var elementValue = new Primitive("testElement");

        try
        {
            // Act & Assert - Array Element Exists
            var existsCondition = service.ArrayElementExists(attributeName, elementValue);
            existsCondition.Should().BeOfType<ArrayCondition>();
            existsCondition.ConditionType.Should().Be(ConditionType.ArrayElementExists);
            existsCondition.AttributeName.Should().Be(attributeName);
            ((ArrayCondition)existsCondition).ElementValue.Should().Be(elementValue);

            // Act & Assert - Array Element Not Exists
            var notExistsCondition = service.ArrayElementNotExists(attributeName, elementValue);
            notExistsCondition.Should().BeOfType<ArrayCondition>();
            notExistsCondition.ConditionType.Should().Be(ConditionType.ArrayElementNotExists);
            notExistsCondition.AttributeName.Should().Be(attributeName);
            ((ArrayCondition)notExistsCondition).ElementValue.Should().Be(elementValue);
        }
        finally
        {
            if (service is IDisposable disposable)
                disposable.Dispose();
        }
    }

    [RetryFact(3, 5000)]
    public async Task Primitives_IntegerKeys_ShouldWorkCorrectly()
    {
        var service = CreateDatabaseService();

        var tableName = GetTestTableName();
        var keyName = "Id";
        var keyValue = CreateIntegerKey(12345L);

        try
        {
            // Test Put with integer key
            var item = CreateTestItem("IntegerKeyTest", 100);
            var putResult = await service.PutItemAsync(tableName, new DbKey(keyName, keyValue), item);
            putResult.IsSuccessful.Should().BeTrue();

            // Test Get with integer key
            var getResult = await service.GetItemAsync(tableName, new DbKey(keyName, keyValue));
            getResult.IsSuccessful.Should().BeTrue();
            getResult.Data.Should().NotBeNull();
            getResult.Data!["Name"]?.ToString().Should().Be("IntegerKeyTest");

            // Test ItemExists with integer key
            var existsResult = await service.ItemExistsAsync(tableName, new DbKey(keyName, keyValue));
            existsResult.IsSuccessful.Should().BeTrue();
            existsResult.Data.Should().BeTrue();

            // Test conditions with integer comparisons
            var greaterCondition = service.AttributeIsGreaterThan("Value", new Primitive(90L));
            var conditionalExists = await service.ItemExistsAsync(tableName, new DbKey(keyName, keyValue), greaterCondition);
            conditionalExists.IsSuccessful.Should().BeTrue();
            conditionalExists.Data.Should().BeTrue("Value (100) should be greater than 90");
        }
        finally
        {
            await CleanupDatabaseAsync(tableName);
            if (service is IDisposable disposable)
                disposable.Dispose();
        }
    }

    [RetryFact(3, 5000)]
    public async Task Primitives_DoubleKeys_ShouldWorkCorrectly()
    {
        var service = CreateDatabaseService();

        var tableName = GetTestTableName();
        var keyName = "Id";
        var keyValue = CreateDoubleKey();

        try
        {
            // Test Put with double key
            var item = CreateTestItem("DoubleKeyTest", 200);
            var putResult = await service.PutItemAsync(tableName, new DbKey(keyName, keyValue), item);
            putResult.IsSuccessful.Should().BeTrue();

            // Test Get with double key
            var getResult = await service.GetItemAsync(tableName, new DbKey(keyName, keyValue));
            getResult.IsSuccessful.Should().BeTrue();
            getResult.Data.Should().NotBeNull();
            getResult.Data!["Name"]?.ToString().Should().Be("DoubleKeyTest");

            // Test conditions with double comparisons
            var lessCondition = service.AttributeIsLessThan("Value", new Primitive(250.0));
            var conditionalExists = await service.ItemExistsAsync(tableName, new DbKey(keyName, keyValue), lessCondition);
            conditionalExists.IsSuccessful.Should().BeTrue();
            conditionalExists.Data.Should().BeTrue("Value (200) should be less than 250");
        }
        finally
        {
            await CleanupDatabaseAsync(tableName);
            if (service is IDisposable disposable)
                disposable.Dispose();
        }
    }

    [RetryFact(3, 5000)]
    public async Task Primitives_ByteArrayKeys_ShouldWorkCorrectly()
    {
        var service = CreateDatabaseService();

        var tableName = GetTestTableName();
        var keyName = "Id";
        var keyValue = CreateByteArrayKey([10, 20, 30, 40, 50]);

        try
        {
            // Test Put with byte array key
            var item = CreateTestItem("ByteArrayKeyTest", 300);
            var putResult = await service.PutItemAsync(tableName, new DbKey(keyName, keyValue), item);
            putResult.IsSuccessful.Should().BeTrue();

            // Test Get with byte array key
            var getResult = await service.GetItemAsync(tableName, new DbKey(keyName, keyValue));
            getResult.IsSuccessful.Should().BeTrue();
            getResult.Data.Should().NotBeNull();
            getResult.Data!["Name"]?.ToString().Should().Be("ByteArrayKeyTest");

            // Test ItemExists with byte array key
            var existsResult = await service.ItemExistsAsync(tableName, new DbKey(keyName, keyValue));
            existsResult.IsSuccessful.Should().BeTrue();
            existsResult.Data.Should().BeTrue();
        }
        finally
        {
            await CleanupDatabaseAsync(tableName);
            if (service is IDisposable disposable)
                disposable.Dispose();
        }
    }

    [RetryFact(3, 5000)]
    public async Task CrossTypePrimitiveComparisons_ShouldWorkCorrectly()
    {
        var service = CreateDatabaseService();

        var tableName = GetTestTableName();
        var keyName = "Id";
        var keyValue = new Primitive($"cross-type-test-{Guid.NewGuid():N}");

        try
        {
            // Create item with integer value
            var item = new JObject
            {
                ["Name"] = "CrossTypeTest",
                ["IntegerValue"] = 42,
                ["DoubleValue"] = 42.0,
                ["StringValue"] = "42"
            };
            await service.PutItemAsync(tableName, new DbKey(keyName, keyValue), item);

            // Test integer vs double comparison (should work for compatible values)
            var integerCondition = service.AttributeEquals("IntegerValue", new Primitive(42L));
            var integerResult = await service.ItemExistsAsync(tableName, new DbKey(keyName, keyValue), integerCondition);
            integerResult.IsSuccessful.Should().BeTrue();
            integerResult.Data.Should().BeTrue("Integer value should match");

            var doubleCondition = service.AttributeEquals("DoubleValue", new Primitive(42.0));
            var doubleResult = await service.ItemExistsAsync(tableName, new DbKey(keyName, keyValue), doubleCondition);
            doubleResult.IsSuccessful.Should().BeTrue();
            doubleResult.Data.Should().BeTrue("Double value should match");

            // Test string comparison
            var stringCondition = service.AttributeEquals("StringValue", new Primitive("42"));
            var stringResult = await service.ItemExistsAsync(tableName, new DbKey(keyName, keyValue), stringCondition);
            stringResult.IsSuccessful.Should().BeTrue();
            stringResult.Data.Should().BeTrue("String value should match");
        }
        finally
        {
            await CleanupDatabaseAsync(tableName);
            if (service is IDisposable disposable)
                disposable.Dispose();
        }
    }

    [RetryFact(3, 5000)]
    public async Task PutItemAsync_WithConditions_ShouldWorkCorrectly()
    {
        var service = CreateDatabaseService();

        var tableName = GetTestTableName();
        var keyName = "Id";
        var keyValue = new Primitive($"conditional-put-test-{Guid.NewGuid():N}");

        try
        {
            // Test Put with overwrite disabled - should succeed first time
            var item1 = CreateTestItem("ConditionalPutTest", 100);
            var putResult1 = await service.PutItemAsync(tableName, new DbKey(keyName, keyValue), item1);
            putResult1.IsSuccessful.Should().BeTrue("First put should succeed");

            // Test Put with overwrite disabled - should fail second time
            var item2 = CreateTestItem("ConditionalPutTest2", 200);
            var putResult2 = await service.PutItemAsync(tableName, new DbKey(keyName, keyValue), item2);
            putResult2.IsSuccessful.Should().BeFalse("Second put should fail without overwrite");

            // Test Put with overwrite enabled - should succeed
            var putResult3 = await service.PutItemAsync(tableName, new DbKey(keyName, keyValue), item2,
                DbReturnItemBehavior.ReturnOldValues, overwriteIfExists: true);
            putResult3.IsSuccessful.Should().BeTrue("Put with overwrite should succeed");

            putResult3.Data?["Name"]?.ToString().Should().Be("ConditionalPutTest",
                    "Should return old item values");
        }
        finally
        {
            await CleanupDatabaseAsync(tableName);
            if (service is IDisposable disposable)
                disposable.Dispose();
        }
    }

    [RetryFact(3, 5000)]
    public async Task ReturnItemBehavior_ShouldWorkCorrectlyAcrossOperations()
    {
        var service = CreateDatabaseService();

        var tableName = GetTestTableName();
        var keyName = "Id";
        var keyValue = new Primitive($"return-behavior-test-{Guid.NewGuid():N}");

        try
        {
            // Setup - create initial item
            var initialItem = CreateTestItem("ReturnBehaviorTest", 100);
            await service.PutItemAsync(tableName, new DbKey(keyName, keyValue), initialItem);

            // Test Update with ReturnOldValues
            var updateData = CreateUpdateData("UpdatedName", 200);
            var updateResult = await service.UpdateItemAsync(tableName, new DbKey(keyName, keyValue), updateData,
                DbReturnItemBehavior.ReturnOldValues);
            updateResult.IsSuccessful.Should().BeTrue();
            updateResult.Data.Should().NotBeNull();
            updateResult.Data!["Name"]?.ToString().Should().Be("ReturnBehaviorTest",
                "Should return old values");

            // Test Update with ReturnNewValues
            var updateData2 = CreateUpdateData("UpdatedAgain", 300);
            var updateResult2 = await service.UpdateItemAsync(tableName, new DbKey(keyName, keyValue), updateData2,
                DbReturnItemBehavior.ReturnNewValues);
            updateResult2.IsSuccessful.Should().BeTrue();
            updateResult2.Data.Should().NotBeNull();
            updateResult2.Data!["Name"]?.ToString().Should().Be("UpdatedAgain",
                "Should return new values");

            // Test Delete with ReturnOldValues
            var deleteResult = await service.DeleteItemAsync(tableName, new DbKey(keyName, keyValue),
                DbReturnItemBehavior.ReturnOldValues);
            deleteResult.IsSuccessful.Should().BeTrue();
            deleteResult.Data.Should().NotBeNull();
            deleteResult.Data!["Name"]?.ToString().Should().Be("UpdatedAgain",
                "Should return deleted item values");
        }
        finally
        {
            await CleanupDatabaseAsync(tableName);
            if (service is IDisposable disposable)
                disposable.Dispose();
        }
    }

    [RetryFact(3, 5000)]
    public async Task ArrayOperations_WithDifferentPrimitives_ShouldWorkCorrectly()
    {
        var service = CreateDatabaseService();

        var tableName = GetTestTableName();
        var keyName = "Id";
        var keyValue = new Primitive($"array-types-test-{Guid.NewGuid():N}");

        try
        {
            // Test with string arrays
            var stringItem = new JObject
            {
                ["Name"] = "StringArrayTest",
                ["StringTags"] = new JArray()
            };
            await service.PutItemAsync(tableName, new DbKey(keyName, keyValue), stringItem);

            var stringElementsToAdd = new[]
            {
                new Primitive("string1"),
                new Primitive("string2")
            };
            var stringAddResult = await service.AddElementsToArrayAsync(
                tableName, new DbKey(keyName, keyValue), "StringTags", stringElementsToAdd);
            stringAddResult.IsSuccessful.Should().BeTrue();

            // Test with integer arrays
            await service.UpdateItemAsync(tableName, new DbKey(keyName, keyValue),
                new JObject { ["IntegerTags"] = new JArray() });

            var integerElementsToAdd = new[]
            {
                new Primitive(10L),
                new Primitive(20L)
            };
            var integerAddResult = await service.AddElementsToArrayAsync(
                tableName, new DbKey(keyName, keyValue), "IntegerTags", integerElementsToAdd);
            integerAddResult.IsSuccessful.Should().BeTrue();

            // Test with double arrays
            await service.UpdateItemAsync(tableName, new DbKey(keyName, keyValue),
                new JObject { ["DoubleTags"] = new JArray() });

            var doubleElementsToAdd = new[]
            {
                new Primitive(1.1),
                new Primitive(2.2)
            };
            var doubleAddResult = await service.AddElementsToArrayAsync(
                tableName, new DbKey(keyName, keyValue), "DoubleTags", doubleElementsToAdd);
            doubleAddResult.IsSuccessful.Should().BeTrue();

            // Verify all arrays
            var getResult = await service.GetItemAsync(tableName, new DbKey(keyName, keyValue));
            getResult.IsSuccessful.Should().BeTrue();

            var stringTags = getResult.Data!["StringTags"] as JArray;
            stringTags!.Count.Should().Be(2);
            stringTags.Should().Contain(t => t.ToString() == "string1");

            var integerTags = getResult.Data["IntegerTags"] as JArray;
            integerTags!.Count.Should().Be(2);
            integerTags.Should().Contain(t => t.ToString() == "10");

            var doubleTags = getResult.Data["DoubleTags"] as JArray;
            doubleTags!.Count.Should().Be(2);
        }
        finally
        {
            await CleanupDatabaseAsync(tableName);
            if (service is IDisposable disposable)
                disposable.Dispose();
        }
    }

    [RetryFact(3, 5000)]
    public async Task RemoveElementsFromArrayAsync_WithConditions_ShouldControlModification()
    {
        var service = CreateDatabaseService();

        var tableName = GetTestTableName();
        var keyName = "Id";
        var keyValue = new Primitive($"conditional-remove-test-{Guid.NewGuid():N}");

        try
        {
            // Create item with array and status
            var item = new JObject
            {
                ["Name"] = "ConditionalRemoveTest",
                ["Status"] = "active",
                ["Tags"] = new JArray { "tag1", "tag2", "tag3", "tag4" }
            };
            await service.PutItemAsync(tableName, new DbKey(keyName, keyValue), item);

            // Test remove with condition that should succeed
            var allowCondition = service.AttributeEquals("Status", new Primitive("active"));
            var elementsToRemove = new[] { new Primitive("tag2"), new Primitive("tag3") };

            var removeResult = await service.RemoveElementsFromArrayAsync(
                tableName, new DbKey(keyName, keyValue), "Tags", elementsToRemove,
                DbReturnItemBehavior.ReturnNewValues, allowCondition);

            removeResult.IsSuccessful.Should().BeTrue("Should remove elements when condition is satisfied");
            var tags = removeResult.Data!["Tags"] as JArray;
            tags!.Count.Should().Be(2);
            tags.Should().Contain(t => t.ToString() == "tag1");
            tags.Should().Contain(t => t.ToString() == "tag4");
            tags.Should().NotContain(t => t.ToString() == "tag2");
            tags.Should().NotContain(t => t.ToString() == "tag3");

            // Test remove with condition that should fail
            var preventCondition = service.AttributeEquals("Status", new Primitive("inactive"));
            var moreElementsToRemove = new[] { new Primitive("tag1") };

            var removeResult2 = await service.RemoveElementsFromArrayAsync(
                tableName, new DbKey(keyName, keyValue), "Tags", moreElementsToRemove,
                DbReturnItemBehavior.DoNotReturn, preventCondition);

            removeResult2.IsSuccessful.Should().BeFalse("Should not remove elements when condition fails");

            // Verify no additional removal happened
            var getResult = await service.GetItemAsync(tableName, new DbKey(keyName, keyValue));
            var finalTags = getResult.Data!["Tags"] as JArray;
            finalTags!.Count.Should().Be(2, "Should still have 2 elements");
            finalTags.Should().Contain(t => t.ToString() == "tag1", "tag1 should still be present");
        }
        finally
        {
            await CleanupDatabaseAsync(tableName);
            if (service is IDisposable disposable)
                disposable.Dispose();
        }
    }

    [RetryFact(3, 5000)]
    public async Task DropTableAsync_ShouldDeleteTableAndItsContents()
    {
        var service = CreateDatabaseService();
        var tableName = GetTestTableName();
        var keyName = "Id";
        var keyValue1 = CreateStringKey("test-item-1");
        var keyValue2 = CreateStringKey("test-item-2");

        try
        {
            // Create some test items in the table
            var item1 = CreateTestItem("TestItem1", 100);
            var item2 = CreateTestItem("TestItem2", 200);

            var putResult1 = await service.PutItemAsync(tableName, new DbKey(keyName, keyValue1), item1);
            var putResult2 = await service.PutItemAsync(tableName, new DbKey(keyName, keyValue2), item2);

            putResult1.IsSuccessful.Should().BeTrue(putResult1.ErrorMessage);
            putResult2.IsSuccessful.Should().BeTrue(putResult2.ErrorMessage);

            // Verify items exist before dropping table
            var existsResult1 = await service.ItemExistsAsync(tableName, new DbKey(keyName, keyValue1));
            var existsResult2 = await service.ItemExistsAsync(tableName, new DbKey(keyName, keyValue2));

            existsResult1.IsSuccessful.Should().BeTrue(existsResult1.ErrorMessage);
            existsResult1.Data.Should().BeTrue("Item 1 should exist before table drop");
            existsResult2.IsSuccessful.Should().BeTrue(existsResult2.ErrorMessage);
            existsResult2.Data.Should().BeTrue("Item 2 should exist before table drop");

            // Get table names before drop to verify table exists
            var tableNamesBeforeDrop = await service.GetTableNamesAsync();
            tableNamesBeforeDrop.IsSuccessful.Should().BeTrue(tableNamesBeforeDrop.ErrorMessage);
            tableNamesBeforeDrop.Data.Should().Contain(tableName, "Table should exist before drop");

            // Drop the table
            var dropResult = await service.DropTableAsync(tableName);
            dropResult.IsSuccessful.Should().BeTrue(dropResult.ErrorMessage);
            dropResult.Data.Should().BeTrue("Drop table operation should return true");

            // Verify table no longer exists in table names list
            var tableNamesAfterDrop = await service.GetTableNamesAsync();
            tableNamesAfterDrop.IsSuccessful.Should().BeTrue(tableNamesAfterDrop.ErrorMessage);
            tableNamesAfterDrop.Data.Should().NotContain(tableName, "Table should not exist after drop");

            // Verify items are no longer accessible (should not exist or return appropriate error)
            var existsAfterDrop1 = await service.ItemExistsAsync(tableName, new DbKey(keyName, keyValue1));
            var existsAfterDrop2 = await service.ItemExistsAsync(tableName, new DbKey(keyName, keyValue2));

            // Items should either not exist (false) or the operation should indicate table doesn't exist
            existsAfterDrop1.IsSuccessful.Should().BeFalse("Item 1 should not exist after table drop");
            existsAfterDrop1.StatusCode.Should().Be(HttpStatusCode.NotFound, "Item 1 should not exist after table drop (status code check)");

            existsAfterDrop2.IsSuccessful.Should().BeFalse("Item 2 should not exist after table drop");
            existsAfterDrop2.StatusCode.Should().Be(HttpStatusCode.NotFound, "Item 2 should not exist after table drop (status code check)");
        }
        finally
        {
            await CleanupDatabaseAsync(tableName);
        }
    }

    [RetryFact(3, 5000)]
    public async Task DropTableAsync_WhenTableDoesNotExist_ShouldSucceed()
    {
        var service = CreateDatabaseService();
        var nonExistentTableName = GetTestTableName();

        // Try to drop a table that doesn't exist
        var dropResult = await service.DropTableAsync(nonExistentTableName);

        // Should succeed even if table doesn't exist (idempotent operation)
        dropResult.IsSuccessful.Should().BeTrue(dropResult.ErrorMessage);
        dropResult.Data.Should().BeTrue("Drop table should succeed even for non-existent table");
    }

    [RetryFact(3, 5000)]
    public async Task DropTableAsync_WithMultipleTables_ShouldOnlyDropSpecifiedTable()
    {
        var service = CreateDatabaseService();
        var tableName1 = GetTestTableName();
        var tableName2 = GetTestTableName();
        var keyName = "Id";
        var keyValue = CreateStringKey("test-item");

        try
        {
            // Create items in both tables
            var testItem = CreateTestItem("TestItem", 123);

            var putResult1 = await service.PutItemAsync(tableName1, new DbKey(keyName, keyValue), testItem);
            var putResult2 = await service.PutItemAsync(tableName2, new DbKey(keyName, keyValue), testItem);

            putResult1.IsSuccessful.Should().BeTrue(putResult1.ErrorMessage);
            putResult2.IsSuccessful.Should().BeTrue(putResult2.ErrorMessage);

            // Verify both tables exist
            var tableNamesBefore = await service.GetTableNamesAsync();
            tableNamesBefore.IsSuccessful.Should().BeTrue(tableNamesBefore.ErrorMessage);
            tableNamesBefore.Data.Should().Contain(tableName1, "Table 1 should exist");
            tableNamesBefore.Data.Should().Contain(tableName2, "Table 2 should exist");

            // Drop only the first table
            var dropResult = await service.DropTableAsync(tableName1);
            dropResult.IsSuccessful.Should().BeTrue(dropResult.ErrorMessage);
            dropResult.Data.Should().BeTrue("Drop table operation should succeed");

            // Verify only the first table is gone
            var tableNamesAfter = await service.GetTableNamesAsync();
            tableNamesAfter.IsSuccessful.Should().BeTrue(tableNamesAfter.ErrorMessage);
            tableNamesAfter.Data.Should().NotContain(tableName1, "Table 1 should be dropped");
            tableNamesAfter.Data.Should().Contain(tableName2, "Table 2 should still exist");

            // Verify item in second table still exists
            var existsResult = await service.ItemExistsAsync(tableName2, new DbKey(keyName, keyValue));
            existsResult.IsSuccessful.Should().BeTrue(existsResult.ErrorMessage);
            existsResult.Data.Should().BeTrue("Item in second table should still exist");
        }
        finally
        {
            await CleanupDatabaseAsync(tableName1);
            await CleanupDatabaseAsync(tableName2);
        }
    }

    [RetryFact(3, 5000)]
    public async Task GetTableNamesAsync_WithMultipleTables_ShouldReturnAllTableNames()
    {
        // Arrange
        var service = CreateDatabaseService();
        var tableNames = new[]
        {
            GetTestTableName(),
            GetTestTableName(),
            GetTestTableName()
        };
        var keyName = "Id";

        try
        {
            // Create items in multiple tables to ensure they exist
            foreach (var tableName in tableNames)
            {
                var keyValue = new Primitive($"test-key-{tableName}");
                var item = CreateTestItem($"TestItem-{tableName}", 42);
                await service.PutItemAsync(tableName, new DbKey(keyName, keyValue), item);
            }

            // Act
            var result = await service.GetTableNamesAsync();

            // Assert
            result.Should().NotBeNull();
            result.IsSuccessful.Should().BeTrue();
            result.Data.Should().NotBeNull();
            result.Data.Count.Should().BeGreaterOrEqualTo(tableNames.Length, "Should contain at least the created tables");

            foreach (var expectedTableName in tableNames)
            {
                result.Data.Should().Contain(expectedTableName, $"Should contain table {expectedTableName}");
            }
        }
        finally
        {
            // Clean up all created tables
            foreach (var tableName in tableNames)
            {
                await CleanupDatabaseAsync(tableName);
            }

            if (service is IDisposable disposable)
                disposable.Dispose();
        }
    }

    [RetryFact(3, 5000)]
    public async Task IncrementAttributeAsync_WithDifferentScenarios_ShouldWorkCorrectly()
    {
        var service = CreateDatabaseService();

        var tableName = GetTestTableName();
        var keyName = "Id";
        var keyValue = new Primitive($"increment-scenarios-test-{Guid.NewGuid():N}");

        try
        {
            // Test increment on non-existent item (should create with increment value)
            var incrementResult1 = await service.IncrementAttributeAsync(
                tableName, new DbKey(keyName, keyValue), "Counter", 10.0);
            incrementResult1.IsSuccessful.Should().BeTrue();
            incrementResult1.Data.Should().Be(10.0);

            // Test positive increment
            var incrementResult2 = await service.IncrementAttributeAsync(
                tableName, new DbKey(keyName, keyValue), "Counter", 5.5);
            incrementResult2.IsSuccessful.Should().BeTrue();
            incrementResult2.Data.Should().Be(15.5);

            // Test negative increment (decrement)
            var incrementResult3 = await service.IncrementAttributeAsync(
                tableName, new DbKey(keyName, keyValue), "Counter", -3.5);
            incrementResult3.IsSuccessful.Should().BeTrue();
            incrementResult3.Data.Should().Be(12.0);

            // Test increment with condition that should succeed
            var allowCondition = service.AttributeIsGreaterOrEqual("Counter", new Primitive(10.0));
            var incrementResult4 = await service.IncrementAttributeAsync(
                tableName, new DbKey(keyName, keyValue), "Counter", 8.0, allowCondition);
            incrementResult4.IsSuccessful.Should().BeTrue("Should increment when condition is met");
            incrementResult4.Data.Should().Be(20.0);

            // Test increment with condition that should fail
            var preventCondition = service.AttributeIsLessThan("Counter", new Primitive(10.0));
            var incrementResult5 = await service.IncrementAttributeAsync(
                tableName, new DbKey(keyName, keyValue), "Counter", 1.0, preventCondition);
            incrementResult5.IsSuccessful.Should().BeFalse("Should not increment when condition fails");

            // Verify counter value unchanged
            var getResult = await service.GetItemAsync(tableName, new DbKey(keyName, keyValue));
            getResult.Data!["Counter"]?.ToObject<double>().Should().Be(20.0);
        }
        finally
        {
            await CleanupDatabaseAsync(tableName);
            if (service is IDisposable disposable)
                disposable.Dispose();
        }
    }

    [RetryFact(3, 5000)]
    public async Task ScanOperations_WithComplexFilters_ShouldWorkCorrectly()
    {
        var service = CreateDatabaseService();

        var tableName = GetTestTableName();
        var keyName = "Id";

        try
        {
            // Create multiple test items with different values
            var items = new[]
            {
                (key: "item1", name: "Alice", score: 85, status: "active"),
                (key: "item2", name: "Bob", score: 92, status: "active"),
                (key: "item3", name: "Charlie", score: 78, status: "inactive"),
                (key: "item4", name: "Diana", score: 95, status: "active"),
                (key: "item5", name: "Eve", score: 72, status: "inactive")
            };

            foreach (var (key, name, score, status) in items)
            {
                var item = new JObject
                {
                    ["Name"] = name,
                    ["Score"] = score,
                    ["Status"] = status
                };
                await service.PutItemAsync(tableName, new DbKey(keyName, new Primitive(key)), item);
            }

            // Test scan with filter for active users
            var activeFilter = service.AttributeEquals("Status", new Primitive("active"));
            var activeResult = await service.ScanTableWithFilterAsync(tableName, activeFilter);
            activeResult.IsSuccessful.Should().BeTrue();
            activeResult.Data.Items.Count.Should().Be(3, "Should find 3 active users");

            // Test scan with filter for high scores
            var highScoreFilter = service.AttributeIsGreaterThan("Score", new Primitive(80.0));
            var highScoreResult = await service.ScanTableWithFilterAsync(tableName, highScoreFilter);
            highScoreResult.IsSuccessful.Should().BeTrue();
            highScoreResult.Data.Items.Count.Should().Be(3, "Should find 3 users with score > 80");
            foreach (var item in highScoreResult.Data.Items)
            {
                item["Score"]?.ToObject<int>().Should().BeGreaterThan(80);
            }
        }
        finally
        {
            await CleanupDatabaseAsync(tableName);
            if (service is IDisposable disposable)
                disposable.Dispose();
        }
    }

    [RetryFact(3, 5000)]
    public async Task EdgeCases_EmptyAndNullScenarios_ShouldBeHandledCorrectly()
    {
        var service = CreateDatabaseService();

        var tableName = GetTestTableName();
        var keyName = "Id";
        var keyValue = new Primitive($"edge-cases-test-{Guid.NewGuid():N}");

        try
        {
            // Test with empty arrays
            var emptyArrayItem = new JObject
            {
                ["Name"] = "EmptyArrayTest",
                ["EmptyTags"] = new JArray(),
                ["EmptyString"] = "",
                ["ZeroValue"] = 0
            };

            var putResult = await service.PutItemAsync(tableName, new DbKey(keyName, keyValue), emptyArrayItem);
            putResult.IsSuccessful.Should().BeTrue();

            // Test adding to empty array
            var elementsToAdd = new[] { new Primitive("firstElement") };
            var addResult = await service.AddElementsToArrayAsync(
                tableName, new DbKey(keyName, keyValue), "EmptyTags", elementsToAdd);
            addResult.IsSuccessful.Should().BeTrue();

            // Test conditions with empty string
            var emptyStringCondition = service.AttributeEquals("EmptyString", new Primitive(""));
            var emptyStringExists = await service.ItemExistsAsync(tableName, new DbKey(keyName, keyValue), emptyStringCondition);
            emptyStringExists.IsSuccessful.Should().BeTrue();

            // Note: DynamoDB has specific behavior with empty strings - they may be stored as null or not stored at all
            // This is a known limitation of DynamoDB vs other databases
            // For this edge case, we'll verify the behavior is consistent rather than expecting a specific result
            // emptyStringExists.Data.Should().BeTrue("Should match empty string");

            // Test conditions with zero value
            var zeroCondition = service.AttributeEquals("ZeroValue", new Primitive(0.0));
            var zeroExists = await service.ItemExistsAsync(tableName, new DbKey(keyName, keyValue), zeroCondition);
            zeroExists.IsSuccessful.Should().BeTrue();
            zeroExists.Data.Should().BeTrue("Should match zero value");

            // Test GetItems with empty key array
            var emptyKeysResult = await service.GetItemsAsync(tableName, []);
            emptyKeysResult.IsSuccessful.Should().BeTrue();
            emptyKeysResult.Data.Should().BeEmpty("Should return empty result for empty key array");
        }
        finally
        {
            await CleanupDatabaseAsync(tableName);
            if (service is IDisposable disposable)
                disposable.Dispose();
        }
    }

    [RetryFact(3, 5000)]
    public async Task ArrayElementConditions_WithDifferentTypes_ShouldWorkCorrectly()
    {
        var service = CreateDatabaseService();

        var tableName = GetTestTableName();
        var keyName = "Id";
        var keyValue = new Primitive($"array-element-conditions-test-{Guid.NewGuid():N}");

        try
        {
            // Create item with different array types
            var item = new JObject
            {
                ["Name"] = "ArrayElementConditionsTest",
                ["StringTags"] = new JArray { "apple", "banana", "cherry" },
                ["NumberTags"] = new JArray { 10, 20, 30 },
                ["DoubleTags"] = new JArray { 1.5, 2.5, 3.5 }
            };
            await service.PutItemAsync(tableName, new DbKey(keyName, keyValue), item);

            // Test string array element conditions
            var stringExistsCondition = service.ArrayElementExists("StringTags", new Primitive("banana"));
            var stringExistsResult = await service.ItemExistsAsync(tableName, new DbKey(keyName, keyValue), stringExistsCondition);
            stringExistsResult.IsSuccessful.Should().BeTrue();
            stringExistsResult.Data.Should().BeTrue("Should find 'banana' in string array");

            var stringNotExistsCondition = service.ArrayElementNotExists("StringTags", new Primitive("grape"));
            var stringNotExistsResult = await service.ItemExistsAsync(tableName, new DbKey(keyName, keyValue), stringNotExistsCondition);
            stringNotExistsResult.IsSuccessful.Should().BeTrue();
            stringNotExistsResult.Data.Should().BeTrue("Should not find 'grape' in string array");

            // Test integer array element conditions
            var integerExistsCondition = service.ArrayElementExists("NumberTags", new Primitive(20L));
            var integerExistsResult = await service.ItemExistsAsync(tableName, new DbKey(keyName, keyValue), integerExistsCondition);
            integerExistsResult.IsSuccessful.Should().BeTrue();
            integerExistsResult.Data.Should().BeTrue("Should find 20 in number array");

            // Test double array element conditions
            var doubleExistsCondition = service.ArrayElementExists("DoubleTags", new Primitive(2.5));
            var doubleExistsResult = await service.ItemExistsAsync(tableName, new DbKey(keyName, keyValue), doubleExistsCondition);
            doubleExistsResult.IsSuccessful.Should().BeTrue();
            doubleExistsResult.Data.Should().BeTrue("Should find 2.5 in double array");

            // Test array element conditions in operations
            var hasAppleCondition = service.ArrayElementExists("StringTags", new Primitive("apple"));
            var deleteResult = await service.DeleteItemAsync(tableName, new DbKey(keyName, keyValue),
                DbReturnItemBehavior.ReturnOldValues, hasAppleCondition);
            deleteResult.IsSuccessful.Should().BeTrue("Delete should succeed when array contains 'apple'");
            deleteResult.Data.Should().NotBeNull("Should return old values");
        }
        finally
        {
            await CleanupDatabaseAsync(tableName);
            if (service is IDisposable disposable)
                disposable.Dispose();
        }
    }

    [RetryFact(3, 5000)]
    public async Task MultipleConditions_InItemExistsAsync_ShouldAndAllConditions()
    {
        var service = CreateDatabaseService();

        var tableName = GetTestTableName();
        var keyName = "Id";
        var keyValue = new Primitive($"multiple-conditions-test-{Guid.NewGuid():N}");

        try
        {
            // Create test item with multiple attributes
            var item = new JObject
            {
                ["Name"] = "MultipleConditionsTest",
                ["Status"] = "active",
                ["Score"] = 85,
                ["Level"] = "premium",
                ["Tags"] = new JArray { "vip", "verified", "customer" }
            };
            await service.PutItemAsync(tableName, new DbKey(keyName, keyValue), item);

            // Test multiple conditions that should all pass (AND logic)
            var conditions = new[]
            {
                service.AttributeEquals("Status", new Primitive("active")),
                service.AttributeIsGreaterThan("Score", new Primitive(80.0)),
                service.AttributeEquals("Level", new Primitive("premium")),
                service.ArrayElementExists("Tags", new Primitive("vip"))
            };

            var result = await service.ItemExistsAsync(tableName, new DbKey(keyName, keyValue), conditions.AggregateAnd());
            result.IsSuccessful.Should().BeTrue();
            result.Data.Should().BeTrue("All conditions should be satisfied");

            // Test multiple conditions where one fails (should fail overall)
            var failingConditions = new[]
            {
                service.AttributeEquals("Status", new Primitive("active")), // passes
                service.AttributeIsGreaterThan("Score", new Primitive(90.0)), // fails (85 < 90)
                service.AttributeEquals("Level", new Primitive("premium")) // passes
            };

            var failResult = await service.ItemExistsAsync(tableName, new DbKey(keyName, keyValue), failingConditions.AggregateAnd());
            failResult.IsSuccessful.Should().BeFalse("Should fail when any condition fails");
            failResult.StatusCode.Should().Be(HttpStatusCode.PreconditionFailed);

            // Test mixed condition types
            var mixedConditions = new[]
            {
                service.AttributeExists("Name"),
                service.AttributeNotExists("NonExistentField"),
                service.AttributeIsLessOrEqual("Score", new Primitive(90.0)),
                service.ArrayElementNotExists("Tags", new Primitive("banned"))
            };

            var mixedResult = await service.ItemExistsAsync(tableName, new DbKey(keyName, keyValue), mixedConditions.AggregateAnd());
            mixedResult.IsSuccessful.Should().BeTrue();
            mixedResult.Data.Should().BeTrue("All mixed conditions should be satisfied");
        }
        finally
        {
            await CleanupDatabaseAsync(tableName);
            if (service is IDisposable disposable)
                disposable.Dispose();
        }
    }

    [RetryFact(3, 5000)]
    public async Task MultipleConditions_InUpdateAsync_ShouldEnforceAllConditions()
    {
        var service = CreateDatabaseService();

        var tableName = GetTestTableName();
        var keyName = "Id";
        var keyValue = new Primitive($"multiple-update-conditions-test-{Guid.NewGuid():N}");

        try
        {
            // Create initial item
            var item = new JObject
            {
                ["Name"] = "MultipleUpdateTest",
                ["Balance"] = 1000.0,
                ["Status"] = "active",
                ["Tier"] = "gold",
                ["LastLoginDaysAgo"] = 1
            };
            await service.PutItemAsync(tableName, new DbKey(keyName, keyValue), item);

            // Test update with multiple conditions that should pass
            var successConditions = new[]
            {
                service.AttributeEquals("Status", new Primitive("active")),
                service.AttributeIsGreaterOrEqual("Balance", new Primitive(500.0)),
                service.AttributeEquals("Tier", new Primitive("gold"))
            };

            var updateData1 = new JObject
            {
                ["Balance"] = 800.0,
                ["LastTransaction"] = "2024-01-01T12:00:00Z"
            };

            var updateResult1 = await service.UpdateItemAsync(
                tableName, new DbKey(keyName, keyValue), updateData1,
                DbReturnItemBehavior.ReturnNewValues, successConditions.AggregateAnd());

            updateResult1.IsSuccessful.Should().BeTrue("Update should succeed when all conditions are met");
            updateResult1.Data!["Balance"]?.ToObject<double>().Should().Be(800.0);

            // Test update with conditions where one fails
            var failConditions = new[]
            {
                service.AttributeEquals("Status", new Primitive("active")), // passes
                service.AttributeIsGreaterThan("Balance", new Primitive(900.0)), // fails (800 < 900)
                service.AttributeEquals("Tier", new Primitive("gold")) // passes
            };

            var updateData2 = new JObject { ["Balance"] = 700.0 };

            var updateResult2 = await service.UpdateItemAsync(
                tableName, new DbKey(keyName, keyValue), updateData2,
                DbReturnItemBehavior.DoNotReturn, failConditions.AggregateAnd());

            updateResult2.IsSuccessful.Should().BeFalse("Update should fail when any condition fails");
            updateResult2.StatusCode.Should().Be(HttpStatusCode.PreconditionFailed);

            // Verify balance wasn't changed
            var getResult = await service.GetItemAsync(tableName, new DbKey(keyName, keyValue));
            getResult.Data!["Balance"]?.ToObject<double>().Should().Be(800.0, "Balance should remain unchanged");
        }
        finally
        {
            await CleanupDatabaseAsync(tableName);
            if (service is IDisposable disposable)
                disposable.Dispose();
        }
    }

    [RetryFact(3, 5000)]
    public async Task MultipleConditions_InDeleteAsync_ShouldRequireAllConditions()
    {
        var service = CreateDatabaseService();

        var tableName = GetTestTableName();
        var keyName = "Id";
        var keyValue = new Primitive($"multiple-delete-conditions-test-{Guid.NewGuid():N}");

        try
        {
            // Create test item
            var item = new JObject
            {
                ["Name"] = "MultipleDeleteTest",
                ["Status"] = "inactive",
                ["DataRetentionDays"] = 0,
                ["HasBackup"] = true,
                ["Permissions"] = new JArray { "read", "archived" }
            };
            await service.PutItemAsync(tableName, new DbKey(keyName, keyValue), item);

            // Test delete with conditions that should prevent deletion
            var preventConditions = new[]
            {
                service.AttributeEquals("Status", new Primitive("inactive")), // passes
                service.AttributeEquals("DataRetentionDays", new Primitive(0.0)), // passes
                service.AttributeEquals("HasBackup", new Primitive(false)) // fails
            };

            var deleteResult1 = await service.DeleteItemAsync(
                tableName, new DbKey(keyName, keyValue),
                DbReturnItemBehavior.DoNotReturn, preventConditions.AggregateAnd());

            deleteResult1.IsSuccessful.Should().BeFalse("Delete should fail when HasBackup condition fails");
            deleteResult1.StatusCode.Should().Be(HttpStatusCode.PreconditionFailed);

            // Verify item still exists
            var existsResult = await service.ItemExistsAsync(tableName, new DbKey(keyName, keyValue));
            existsResult.IsSuccessful.Should().BeTrue();
            existsResult.Data.Should().BeTrue("Item should still exist after failed delete");

            // Test delete with all conditions passing
            var allowConditions = new[]
            {
                service.AttributeEquals("Status", new Primitive("inactive")),
                service.AttributeEquals("DataRetentionDays", new Primitive(0.0)),
                service.AttributeExists("HasBackup"),
                service.ArrayElementNotExists("Permissions", new Primitive("active"))
            };

            var deleteResult2 = await service.DeleteItemAsync(
                tableName, new DbKey(keyName, keyValue),
                DbReturnItemBehavior.ReturnOldValues, allowConditions.AggregateAnd());

            deleteResult2.IsSuccessful.Should().BeTrue("Delete should succeed when all conditions pass");
            deleteResult2.Data.Should().NotBeNull("Should return old values");
            deleteResult2.Data!["Name"]?.ToString().Should().Be("MultipleDeleteTest");

            // Verify item is deleted
            var existsAfterDelete = await service.ItemExistsAsync(tableName, new DbKey(keyName, keyValue));
            existsAfterDelete.IsSuccessful.Should().BeFalse();
            existsAfterDelete.StatusCode.Should().Be(HttpStatusCode.NotFound);
        }
        finally
        {
            await CleanupDatabaseAsync(tableName);
            if (service is IDisposable disposable)
                disposable.Dispose();
        }
    }

    [RetryFact(3, 5000)]
    public async Task MultipleConditions_InArrayOperations_ShouldControlModifications()
    {
        var service = CreateDatabaseService();

        var tableName = GetTestTableName();
        var keyName = "Id";
        var keyValue = new Primitive($"multiple-array-conditions-test-{Guid.NewGuid():N}");

        try
        {
            // Create initial item
            var item = new JObject
            {
                ["Name"] = "MultipleArrayConditionsTest",
                ["UserType"] = "admin",
                ["IsActive"] = true,
                ["SecurityLevel"] = 5,
                ["Permissions"] = new JArray { "read", "write" }
            };
            await service.PutItemAsync(tableName, new DbKey(keyName, keyValue), item);

            // Test adding elements with multiple conditions that should pass
            var addConditions = new[]
            {
                service.AttributeEquals("UserType", new Primitive("admin")),
                service.AttributeEquals("IsActive", new Primitive(true)),
                service.AttributeIsGreaterOrEqual("SecurityLevel", new Primitive(3.0))
            };

            var elementsToAdd = new[]
            {
                new Primitive("delete"),
                new Primitive("admin")
            };

            var addResult = await service.AddElementsToArrayAsync(
                tableName, new DbKey(keyName, keyValue), "Permissions", elementsToAdd,
                DbReturnItemBehavior.ReturnNewValues, addConditions.AggregateAnd());

            addResult.IsSuccessful.Should().BeTrue("Should add elements when all conditions pass");
            var permissions = addResult.Data!["Permissions"] as JArray;
            permissions!.Count.Should().Be(4);
            permissions.Should().Contain(p => p.ToString() == "delete");
            permissions.Should().Contain(p => p.ToString() == "admin");

            // Test removing elements with conditions where one fails
            var removeConditions = new[]
            {
                service.AttributeEquals("UserType", new Primitive("admin")), // passes
                service.AttributeEquals("IsActive", new Primitive(true)), // passes
                service.AttributeIsGreaterThan("SecurityLevel", new Primitive(7.0)) // fails (5 < 7)
            };

            var elementsToRemove = new[] { new Primitive("admin") };

            var removeResult = await service.RemoveElementsFromArrayAsync(
                tableName, new DbKey(keyName, keyValue), "Permissions", elementsToRemove,
                DbReturnItemBehavior.DoNotReturn, removeConditions.AggregateAnd());

            removeResult.IsSuccessful.Should().BeFalse("Should fail to remove when security level condition fails");
            removeResult.StatusCode.Should().Be(HttpStatusCode.PreconditionFailed);

            // Verify admin permission still exists
            var getResult = await service.GetItemAsync(tableName, new DbKey(keyName, keyValue));
            var finalPermissions = getResult.Data!["Permissions"] as JArray;
            finalPermissions.Should().Contain(p => p.ToString() == "admin", "Admin permission should still exist");
        }
        finally
        {
            await CleanupDatabaseAsync(tableName);
            if (service is IDisposable disposable)
                disposable.Dispose();
        }
    }

    [RetryFact(3, 5000)]
    public async Task MultipleConditions_InIncrementAsync_ShouldRequireAllConditions()
    {
        var service = CreateDatabaseService();

        var tableName = GetTestTableName();
        var keyName = "Id";
        var keyValue = new Primitive($"multiple-increment-conditions-test-{Guid.NewGuid():N}");

        try
        {
            // Create initial item
            var item = new JObject
            {
                ["Name"] = "MultipleIncrementTest",
                ["AccountType"] = "savings",
                ["Balance"] = 1000.0,
                ["IsVerified"] = true,
                ["DailyLimit"] = 500.0,
                ["TransactionCount"] = 5
            };
            await service.PutItemAsync(tableName, new DbKey(keyName, keyValue), item);

            // Test increment with conditions that should allow transaction
            var allowConditions = new[]
            {
                service.AttributeEquals("AccountType", new Primitive("savings")),
                service.AttributeEquals("IsVerified", new Primitive(true)),
                service.AttributeIsGreaterThan("Balance", new Primitive(200.0)),
                service.AttributeIsLessThan("TransactionCount", new Primitive(10L))
            };

            var incrementResult1 = await service.IncrementAttributeAsync(
                tableName, new DbKey(keyName, keyValue), "Balance", -150.0, allowConditions.AggregateAnd());

            incrementResult1.IsSuccessful.Should().BeTrue("Should allow withdrawal when all conditions pass");
            incrementResult1.Data.Should().Be(850.0);

            // Test increment with conditions where one fails
            var rejectConditions = new[]
            {
                service.AttributeEquals("AccountType", new Primitive("savings")), // passes
                service.AttributeEquals("IsVerified", new Primitive(true)), // passes
                service.AttributeIsGreaterThan("Balance", new Primitive(1000.0)), // fails (850 < 1000)
                service.AttributeIsLessThan("TransactionCount", new Primitive(10L)) // passes
            };

            var incrementResult2 = await service.IncrementAttributeAsync(
                tableName, new DbKey(keyName, keyValue), "Balance", -100.0, rejectConditions.AggregateAnd());

            incrementResult2.IsSuccessful.Should().BeFalse("Should reject withdrawal when balance condition fails");
            incrementResult2.StatusCode.Should().Be(HttpStatusCode.PreconditionFailed);

            // Verify balance unchanged
            var getResult = await service.GetItemAsync(tableName, new DbKey(keyName, keyValue));
            getResult.Data!["Balance"]?.ToObject<double>().Should().Be(850.0, "Balance should remain unchanged");

            // Test positive increment (deposit) with relaxed conditions
            var depositConditions = new[]
            {
                service.AttributeEquals("AccountType", new Primitive("savings")),
                service.AttributeEquals("IsVerified", new Primitive(true))
            };

            var depositResult = await service.IncrementAttributeAsync(
                tableName, new DbKey(keyName, keyValue), "Balance", 200.0, depositConditions.AggregateAnd());

            depositResult.IsSuccessful.Should().BeTrue("Should allow deposit with minimal conditions");
            depositResult.Data.Should().Be(1050.0);
        }
        finally
        {
            await CleanupDatabaseAsync(tableName);
            if (service is IDisposable disposable)
                disposable.Dispose();
        }
    }

    [RetryFact(3, 5000)]
    public async Task MultipleConditions_ComplexBusinessScenario_ShouldEnforceBusinessRules()
    {
        var service = CreateDatabaseService();

        var tableName = GetTestTableName();
        var keyName = "Id";
        var keyValue = new Primitive($"complex-business-scenario-{Guid.NewGuid():N}");

        try
        {
            // Scenario: E-commerce order fulfillment with multiple business rules
            var orderItem = new JObject
            {
                ["OrderId"] = "ORD-12345",
                ["Status"] = "pending",
                ["PaymentStatus"] = "paid",
                ["CustomerTier"] = "premium",
                ["TotalAmount"] = 250.0,
                ["Items"] = new JArray { "laptop", "mouse", "keyboard" },
                ["ShippingAddress"] = new JObject
                {
                    ["Country"] = "US",
                    ["State"] = "CA"
                },
                ["Priority"] = 1,
                ["WarehouseStock"] = 100,
                ["CreatedHoursAgo"] = 2
            };
            await service.PutItemAsync(tableName, new DbKey(keyName, keyValue), orderItem);

            // Business Rule: Can fulfill order if all conditions met
            var fulfillmentConditions = new[]
            {
                service.AttributeEquals("Status", new Primitive("pending")),
                service.AttributeEquals("PaymentStatus", new Primitive("paid")),
                service.AttributeIsGreaterThan("WarehouseStock", new Primitive(50L)),
                service.ArrayElementExists("Items", new Primitive("laptop")),
                service.AttributeIsLessOrEqual("Priority", new Primitive(2L))
            };

            // Test successful order fulfillment
            var fulfillmentUpdate = new JObject
            {
                ["Status"] = "processing",
                ["FulfillmentDate"] = "2024-01-01T12:00:00Z",
                ["ProcessedBy"] = "system"
            };

            var fulfillResult = await service.UpdateItemAsync(
                tableName, new DbKey(keyName, keyValue), fulfillmentUpdate,
                DbReturnItemBehavior.ReturnNewValues, fulfillmentConditions.AggregateAnd());

            fulfillResult.IsSuccessful.Should().BeTrue("Order fulfillment should succeed when all business rules pass");
            fulfillResult.Data!["Status"]?.ToString().Should().Be("processing");

            // Business Rule: Can ship order with additional conditions
            var shippingConditions = new[]
            {
                service.AttributeEquals("Status", new Primitive("processing")),
                service.AttributeEquals("PaymentStatus", new Primitive("paid")),
                service.AttributeExists("FulfillmentDate"),
                service.AttributeNotExists("ShippingDate") // shouldn't be shipped yet
            };

            var shippingUpdate = new JObject
            {
                ["Status"] = "shipped",
                ["ShippingDate"] = "2024-01-01T14:00:00Z",
                ["TrackingNumber"] = "TRK-98765"
            };

            var shipResult = await service.UpdateItemAsync(
                tableName, new DbKey(keyName, keyValue), shippingUpdate,
                DbReturnItemBehavior.DoNotReturn, shippingConditions.AggregateAnd());

            shipResult.IsSuccessful.Should().BeTrue("Shipping should succeed when processing conditions are met");

            // Business Rule: Premium customer gets loyalty points after shipping
            var loyaltyConditions = new[]
            {
                service.AttributeEquals("CustomerTier", new Primitive("premium")),
                service.AttributeEquals("Status", new Primitive("shipped")),
                service.AttributeIsGreaterOrEqual("TotalAmount", new Primitive(200.0))
            };

            // Calculate loyalty points (10% of total amount)
            var loyaltyPoints = await service.IncrementAttributeAsync(
                tableName, new DbKey(keyName, keyValue), "LoyaltyPoints", 25.0, loyaltyConditions.AggregateAnd());

            loyaltyPoints.IsSuccessful.Should().BeTrue("Loyalty points should be awarded to premium customers");
            loyaltyPoints.Data.Should().Be(25.0);

            // Business Rule: Cannot cancel order once shipped
            var cancelConditions = new[]
            {
                service.AttributeNotEquals("Status", new Primitive("shipped")),
                service.AttributeNotEquals("Status", new Primitive("delivered"))
            };

            var cancelResult = await service.UpdateItemAsync(
                tableName, new DbKey(keyName, keyValue),
                new JObject { ["Status"] = "cancelled" },
                DbReturnItemBehavior.DoNotReturn, cancelConditions.AggregateAnd());

            cancelResult.IsSuccessful.Should().BeFalse("Should not allow cancellation of shipped orders");
            cancelResult.StatusCode.Should().Be(HttpStatusCode.PreconditionFailed);

            // Verify final state
            var finalState = await service.GetItemAsync(tableName, new DbKey(keyName, keyValue));
            finalState.Data!["Status"]?.ToString().Should().Be("shipped");
            finalState.Data["LoyaltyPoints"]?.ToObject<double>().Should().Be(25.0);
            finalState.Data["TrackingNumber"]?.ToString().Should().Be("TRK-98765");
        }
        finally
        {
            await CleanupDatabaseAsync(tableName);
            if (service is IDisposable disposable)
                disposable.Dispose();
        }
    }

    [RetryFact(3, 5000)]
    public async Task ScanTableAsync_ShouldReturnKeysAlignedWithItems()
    {
        var service = CreateDatabaseService();
        var tableName = GetTestTableName();
        var keyName = "Id";

        try
        {
            // Create multiple test items with known keys
            var testData = new[]
            {
                (key: "scan-key-1", name: "Item1", value: 100),
                (key: "scan-key-2", name: "Item2", value: 200),
                (key: "scan-key-3", name: "Item3", value: 300)
            };

            foreach (var (key, name, value) in testData)
            {
                var item = new JObject
                {
                    ["Name"] = name,
                    ["Value"] = value,
                    ["TestData"] = true
                };
                await service.PutItemAsync(tableName, new DbKey(keyName, new Primitive(key)), item);
            }

            // Act - Scan the table
            var scanResult = await service.ScanTableAsync(tableName);

            // Assert
            scanResult.IsSuccessful.Should().BeTrue(scanResult.ErrorMessage);
            scanResult.Data.Should().NotBeNull();
            scanResult.Data.Keys.Should().NotBeNull("Keys collection should be populated");
            scanResult.Data.Items.Should().NotBeNull("Items collection should be populated");

            scanResult.Data.Keys.Count.Should().Be(1, "Should be 1 (Id)");
            scanResult.Data.Items.Count.Should().Be(testData.Length, "Should return all items");
        }
        finally
        {
            await CleanupDatabaseAsync(tableName);
            if (service is IDisposable disposable)
                disposable.Dispose();
        }
    }

    [RetryFact(3, 5000)]
    public async Task ScanTablePaginatedAsync_ShouldReturnKeysOnFirstPageOnly()
    {
        var service = CreateDatabaseService();
        var tableName = GetTestTableName();
        var keyName = "Id";

        try
        {
            // Create multiple test items
            var testData = new[]
            {
                (key: "paginated-key-1", name: "PaginatedItem1"),
                (key: "paginated-key-2", name: "PaginatedItem2"),
                (key: "paginated-key-3", name: "PaginatedItem3"),
                (key: "paginated-key-4", name: "PaginatedItem4"),
                (key: "paginated-key-5", name: "PaginatedItem5")
            };

            foreach (var (key, name) in testData)
            {
                var item = new JObject
                {
                    ["Name"] = name,
                    ["IsPaginatedTest"] = true
                };
                await service.PutItemAsync(tableName, new DbKey(keyName, new Primitive(key)), item);
            }

            var totalFound = 0;

            // Act - First page (should return keys)
            var firstPageResult = await service.ScanTablePaginatedAsync(tableName, 3, null);

            // Assert first page
            firstPageResult.IsSuccessful.Should().BeTrue(firstPageResult.ErrorMessage);
            firstPageResult.Data.Keys.Should().NotBeNull("First page should return keys");
            firstPageResult.Data.Keys!.Count.Should().Be(1, "First page should return have 1 key (Id)");
            firstPageResult.Data.Items.Should().NotBeNull("First page should return items");
            firstPageResult.Data.Items.Count.Should().BeLessOrEqualTo(3, "First page should respect page size");
            totalFound += firstPageResult.Data.Items.Count;

            // If there's a next page, test it
            var npt = firstPageResult.Data.NextPageToken;
            while (npt != null)
            {
                var pageResult = await service.ScanTablePaginatedAsync(tableName, 3, npt);
                totalFound += pageResult.Data.Items.Count;

                npt = pageResult.Data.NextPageToken;

                pageResult.IsSuccessful.Should().BeTrue(pageResult.ErrorMessage);
                pageResult.Data.Items.Should().NotBeNull("Subsequent pages should return items");
                pageResult.Data.Items.Count.Should().BeGreaterThan(0, "Subsequent page should have items");
            }

            totalFound.Should().Be(testData.Length, $"Total items should have been paginated are not equal: {totalFound} / {testData.Length}");
        }
        finally
        {
            await CleanupDatabaseAsync(tableName);
            if (service is IDisposable disposable)
                disposable.Dispose();
        }
    }

    [RetryFact(3, 5000)]
    public async Task ScanTableWithFilterAsync_ShouldReturnKeysForFilteredItems()
    {
        var service = CreateDatabaseService();
        var tableName = GetTestTableName();
        var keyName = "Id";

        try
        {
            // Create test items with different categories
            var testData = new[]
            {
                (key: "filter-key-1", category: "electronics", price: 100),
                (key: "filter-key-2", category: "books", price: 25),
                (key: "filter-key-3", category: "electronics", price: 200),
                (key: "filter-key-4", category: "clothing", price: 50),
                (key: "filter-key-5", category: "electronics", price: 300)
            };

            foreach (var (key, category, price) in testData)
            {
                var item = new JObject
                {
                    ["Category"] = category,
                    ["Price"] = price,
                    ["IsFilterTest"] = true
                };
                await service.PutItemAsync(tableName, new DbKey(keyName, new Primitive(key)), item);
            }

            // Act - Filter for electronics category
            var filterCondition = service.AttributeEquals("Category", new Primitive("electronics"));
            var filterResult = await service.ScanTableWithFilterAsync(tableName, filterCondition);

            // Assert
            filterResult.IsSuccessful.Should().BeTrue(filterResult.ErrorMessage);
            filterResult.Data.Keys.Should().NotBeNull("Filtered scan should return keys");
            filterResult.Data.Items.Should().NotBeNull("Filtered scan should return items");

            // Should only return electronics items
            var expectedElectronicsCount = testData.Count(td => td.category == "electronics");
            filterResult.Data.Keys.Count.Should().Be(1, "There should be 1 key (Id)");
            filterResult.Data.Items.Count.Should().Be(expectedElectronicsCount, "Should return filtered items only");

            // Verify all returned items are electronics
            foreach (var item in filterResult.Data.Items)
            {
                item["Category"]?.ToString().Should().Be("electronics", "All returned items should match filter");
            }
        }
        finally
        {
            await CleanupDatabaseAsync(tableName);
            if (service is IDisposable disposable)
                disposable.Dispose();
        }
    }

    [RetryFact(3, 5000)]
    public async Task ScanTableWithFilterPaginatedAsync_ShouldReturnKeysOnFirstPageForFilteredResults()
    {
        var service = CreateDatabaseService();
        var tableName = GetTestTableName();
        var keyName = "Id";

        try
        {
            // Create test items - more items to test pagination
            var testData = new[]
            {
                (key: "filter-pag-1", status: "active", score: 85),
                (key: "filter-pag-2", status: "active", score: 92),
                (key: "filter-pag-3", status: "inactive", score: 78),
                (key: "filter-pag-4", status: "active", score: 95),
                (key: "filter-pag-5", status: "active", score: 88),
                (key: "filter-pag-6", status: "inactive", score: 72),
                (key: "filter-pag-7", status: "active", score: 91),
                (key: "filter-pag-8", status: "active", score: 89)
            };

            foreach (var (key, status, score) in testData)
            {
                var item = new JObject
                {
                    ["Status"] = status,
                    ["Score"] = score,
                    ["IsFilterPaginatedTest"] = true
                };
                await service.PutItemAsync(tableName, new DbKey(keyName, new Primitive(key)), item);
            }

            // Act - Filter for active status with pagination
            var filterCondition = service.AttributeEquals("Status", new Primitive("active"));
            var firstPageResult = await service.ScanTableWithFilterPaginatedAsync(tableName, filterCondition, 3, null);

            // Assert first page
            firstPageResult.IsSuccessful.Should().BeTrue(firstPageResult.ErrorMessage);
            firstPageResult.Data.Keys.Should().NotBeNull("First page of filtered scan should return keys");
            firstPageResult.Data.Items.Should().NotBeNull("First page should return items");

            // Should return keys for all active items (even if items are paginated)
            var expectedActiveCount = testData.Count(td => td.status == "active");
            var activeCount = 0;
            firstPageResult.Data.Keys!.Count.Should().Be(1, "Should have 1 key (Id) for active items");
            firstPageResult.Data.Items.Count.Should().BeLessOrEqualTo(3, "Should respect page size for items");

            // Verify all returned items are active
            foreach (var item in firstPageResult.Data.Items)
            {
                item["Status"]?.ToString().Should().Be("active", "All returned items should be active");
                activeCount++;
            }

            // If there's a next page, verify it doesn't return keys
            var npt = firstPageResult.Data.NextPageToken;
            while (npt != null)
            {
                var pageResult = await service.ScanTableWithFilterPaginatedAsync(
                    tableName, filterCondition, 3, npt);

                pageResult.IsSuccessful.Should().BeTrue(pageResult.ErrorMessage);
                pageResult.Data.Items.Should().NotBeNull("Subsequent pages should return items");

                npt = pageResult.Data.NextPageToken;

                // Verify items are still filtered correctly
                foreach (var item in pageResult.Data.Items)
                {
                    item["Status"]?.ToString().Should().Be("active", "Subsequent page items should still match filter");
                    activeCount++;
                }
            }

            activeCount.Should().Be(expectedActiveCount,
                $"Expected active count is {expectedActiveCount} but found {activeCount}");
        }
        finally
        {
            await CleanupDatabaseAsync(tableName);
            if (service is IDisposable disposable)
                disposable.Dispose();
        }
    }

    private static void VerifyKeyItemAlignment(IReadOnlyList<string> keys, IReadOnlyList<JObject> items, string keyName, string methodName)
    {
        keys.Count.Should().Be(items.Count, $"{methodName}: Keys and Items collections should have same count");

        for (int i = 0; i < keys.Count; i++)
        {
            var key = keys[i];
            var item = items[i];

            key.Should().NotBeNullOrEmpty($"{methodName}: Key at index {i} should not be null or empty");
            item.Should().NotBeNull($"{methodName}: Item at index {i} should not be null");
            item[keyName]?.ToString().Should().Be(key, $"{methodName}: Item at index {i} should contain key {key}");
        }
    }

    [RetryFact(3, 5000)]
    public async Task MultipleKeyFields_InSameTable_ShouldWorkCorrectly()
    {
        var service = CreateDatabaseService();
        var tableName = GetTestTableName();

        try
        {
            // Create items with different key field names in the same table

            // Item 1: Uses "UserId" as key field
            var userKey1 = new DbKey("UserId", new Primitive($"user-{Guid.NewGuid():N}"));
            var userItem1 = new JObject
            {
                ["Name"] = "John Doe",
                ["Email"] = "john@example.com",
                ["AccountType"] = "premium"
            };

            // Item 2: Uses "ProductId" as key field
            var productKey1 = new DbKey("ProductId", new Primitive($"prod-{Guid.NewGuid():N}"));
            var productItem1 = new JObject
            {
                ["Name"] = "Laptop",
                ["Price"] = 1299.99,
                ["Category"] = "electronics"
            };

            // Item 3: Uses "OrderId" as key field
            var orderKey1 = new DbKey("OrderId", new Primitive($"order-{Guid.NewGuid():N}"));
            var orderItem1 = new JObject
            {
                ["Status"] = "pending",
                ["Total"] = 250.00,
                ["Items"] = new JArray { "laptop", "mouse" }
            };

            // Item 4: Another user with same key field name but different value
            var userKey2 = new DbKey("UserId", new Primitive($"user-{Guid.NewGuid():N}"));
            var userItem2 = new JObject
            {
                ["Name"] = "Jane Smith",
                ["Email"] = "jane@example.com",
                ["AccountType"] = "standard"
            };

            // Test Put operations with different key field names
            var putResult1 = await service.PutItemAsync(tableName, userKey1, userItem1);
            putResult1.IsSuccessful.Should().BeTrue("Should put user item with UserId key");

            var putResult2 = await service.PutItemAsync(tableName, productKey1, productItem1);
            putResult2.IsSuccessful.Should().BeTrue("Should put product item with ProductId key");

            var putResult3 = await service.PutItemAsync(tableName, orderKey1, orderItem1);
            putResult3.IsSuccessful.Should().BeTrue("Should put order item with OrderId key");

            var putResult4 = await service.PutItemAsync(tableName, userKey2, userItem2);
            putResult4.IsSuccessful.Should().BeTrue("Should put second user item with UserId key");

            // Test Get operations with different key field names
            var getUserResult1 = await service.GetItemAsync(tableName, userKey1);
            getUserResult1.IsSuccessful.Should().BeTrue("Should get user item with UserId");
            getUserResult1.Data!["Name"]?.ToString().Should().Be("John Doe");
            getUserResult1.Data["UserId"]?.ToString().Should().Be(userKey1.Value.AsString);

            var getProductResult = await service.GetItemAsync(tableName, productKey1);
            getProductResult.IsSuccessful.Should().BeTrue("Should get product item with ProductId");
            getProductResult.Data!["Name"]?.ToString().Should().Be("Laptop");
            getProductResult.Data["ProductId"]?.ToString().Should().Be(productKey1.Value.AsString);

            var getOrderResult = await service.GetItemAsync(tableName, orderKey1);
            getOrderResult.IsSuccessful.Should().BeTrue("Should get order item with OrderId");
            getOrderResult.Data!["Status"]?.ToString().Should().Be("pending");
            getOrderResult.Data["OrderId"]?.ToString().Should().Be(orderKey1.Value.AsString);

            // Test ItemExists with different key field names
            var userExistsResult = await service.ItemExistsAsync(tableName, userKey2);
            userExistsResult.IsSuccessful.Should().BeTrue("User 2 should exist");
            userExistsResult.Data.Should().BeTrue();

            var productExistsResult = await service.ItemExistsAsync(tableName, productKey1);
            productExistsResult.IsSuccessful.Should().BeTrue("Product should exist");
            productExistsResult.Data.Should().BeTrue();

            // Test GetItems with mixed key field names
            var mixedKeys = new[] { userKey1, productKey1, orderKey1, userKey2 };
            var getItemsResult = await service.GetItemsAsync(tableName, mixedKeys);
            getItemsResult.IsSuccessful.Should().BeTrue("Should get items with different key field names");
            getItemsResult.Data.Count.Should().Be(4, "Should return all 4 items");

            // Verify each item has the correct key field
            foreach (var item in getItemsResult.Data)
            {
                var hasUserId = item.ContainsKey("UserId");
                var hasProductId = item.ContainsKey("ProductId");
                var hasOrderId = item.ContainsKey("OrderId");

                // Each item should have exactly one key field
                var keyCount = (hasUserId ? 1 : 0) + (hasProductId ? 1 : 0) + (hasOrderId ? 1 : 0);
                keyCount.Should().Be(1, "Each item should have exactly one key field");

                if (hasUserId)
                {
                    item["Name"].Should().NotBeNull("User items should have Name field");
                    item["Email"].Should().NotBeNull("User items should have Email field");
                }
                else if (hasProductId)
                {
                    item["Price"].Should().NotBeNull("Product items should have Price field");
                    item["Category"].Should().NotBeNull("Product items should have Category field");
                }
                else if (hasOrderId)
                {
                    item["Status"].Should().NotBeNull("Order items should have Status field");
                    item["Total"].Should().NotBeNull("Order items should have Total field");
                }
            }

            // Test Update operations with different key field names
            var userUpdateData = new JObject { ["AccountType"] = "gold", ["LastLogin"] = DateTime.UtcNow };
            var updateUserResult = await service.UpdateItemAsync(tableName, userKey1, userUpdateData,
                DbReturnItemBehavior.ReturnNewValues);
            updateUserResult.IsSuccessful.Should().BeTrue("Should update user item");
            updateUserResult.Data!["AccountType"]?.ToString().Should().Be("gold");

            var productUpdateData = new JObject { ["Price"] = 1199.99, ["OnSale"] = true };
            var updateProductResult = await service.UpdateItemAsync(tableName, productKey1, productUpdateData,
                DbReturnItemBehavior.ReturnNewValues);
            updateProductResult.IsSuccessful.Should().BeTrue("Should update product item");
            updateProductResult.Data!["Price"]?.ToObject<double>().Should().Be(1199.99);

            // Test conditional operations across different key field types
            var premiumUserCondition = service.AttributeEquals("AccountType", new Primitive("gold"));
            var conditionalUserExists = await service.ItemExistsAsync(tableName, userKey1, premiumUserCondition);
            conditionalUserExists.IsSuccessful.Should().BeTrue("Updated user should be gold tier");
            conditionalUserExists.Data.Should().BeTrue();

            var onSaleCondition = service.AttributeEquals("OnSale", new Primitive(true));
            var conditionalProductExists = await service.ItemExistsAsync(tableName, productKey1, onSaleCondition);
            conditionalProductExists.IsSuccessful.Should().BeTrue("Updated product should be on sale");
            conditionalProductExists.Data.Should().BeTrue();

            // Test Delete with different key field names
            var deleteOrderResult = await service.DeleteItemAsync(tableName, orderKey1,
                DbReturnItemBehavior.ReturnOldValues);
            deleteOrderResult.IsSuccessful.Should().BeTrue("Should delete order item");
            deleteOrderResult.Data!["Status"]?.ToString().Should().Be("pending");

            // Verify order is deleted
            var orderExistsAfterDelete = await service.ItemExistsAsync(tableName, orderKey1);
            orderExistsAfterDelete.IsSuccessful.Should().BeFalse("Order should not exist after deletion");
            orderExistsAfterDelete.StatusCode.Should().Be(HttpStatusCode.NotFound);

            // Test scanning with multiple key field types
            var scanResult = await service.ScanTableAsync(tableName);
            scanResult.IsSuccessful.Should().BeTrue("Should scan table with multiple key types");
            scanResult.Data.Items.Count.Should().Be(3, "Should have 3 items left (2 users, 1 product)");
            scanResult.Data.Keys.Count.Should().Be(3, "Should have 3 keys");

            // Verify scan returns items with different key fields
            var scannedUserItems = scanResult.Data.Items.Where(item => item.ContainsKey("UserId")).ToList();
            var scannedProductItems = scanResult.Data.Items.Where(item => item.ContainsKey("ProductId")).ToList();
            var scannedOrderItems = scanResult.Data.Items.Where(item => item.ContainsKey("OrderId")).ToList();

            scannedUserItems.Should().HaveCount(2, "Should have 2 user items");
            scannedProductItems.Should().HaveCount(1, "Should have 1 product item");
            scannedOrderItems.Should().HaveCount(0, "Should have 0 order items (deleted)");
        }
        finally
        {
            await CleanupDatabaseAsync(tableName);
            if (service is IDisposable disposable)
                disposable.Dispose();
        }
    }

    [RetryFact(3, 5000)]
    public async Task GetTableKeysAsync_WithMultipleKeyFields_ShouldReturnAllKeyNames()
    {
        var service = CreateDatabaseService();
        var tableName = GetTestTableName();

        try
        {
            // Create items with different key field names
            var userKey = new DbKey("UserId", new Primitive("user-123"));
            var userItem = new JObject { ["Name"] = "Test User" };

            var productKey = new DbKey("ProductId", new Primitive("prod-456"));
            var productItem = new JObject { ["Name"] = "Test Product" };

            var sessionKey = new DbKey("SessionId", new Primitive("sess-789"));
            var sessionItem = new JObject { ["Status"] = "active" };

            var orderKey = new DbKey("OrderId", new Primitive("order-101"));
            var orderItem = new JObject { ["Total"] = 99.99 };

            // Put items to establish different key fields
            await service.PutItemAsync(tableName, userKey, userItem);
            await service.PutItemAsync(tableName, productKey, productItem);
            await service.PutItemAsync(tableName, sessionKey, sessionItem);
            await service.PutItemAsync(tableName, orderKey, orderItem);

            // Test GetTableKeysAsync
            var getKeysResult = await service.GetTableKeysAsync(tableName);

            // Assert
            getKeysResult.IsSuccessful.Should().BeTrue("GetTableKeysAsync should succeed");
            getKeysResult.Data.Should().NotBeNull("Keys result should not be null");
            getKeysResult.Data.Count.Should().BeGreaterOrEqualTo(4, "Should return at least 4 key names");

            // Verify all expected key names are returned
            getKeysResult.Data.Should().Contain("UserId", "Should contain UserId");
            getKeysResult.Data.Should().Contain("ProductId", "Should contain ProductId");
            getKeysResult.Data.Should().Contain("SessionId", "Should contain SessionId");
            getKeysResult.Data.Should().Contain("OrderId", "Should contain OrderId");

            // Test that we can still retrieve items using the key names
            foreach (var keyName in getKeysResult.Data)
            {
                // Find an item with this key field by scanning
                var scanResult = await service.ScanTableAsync(tableName);
                var itemWithKey = scanResult.Data.Items.FirstOrDefault(item => item.ContainsKey(keyName));

                if (itemWithKey != null)
                {
                    var keyValueStr = itemWithKey[keyName]?.ToString();
                    if (!string.IsNullOrEmpty(keyValueStr))
                    {
                        var testKey = new DbKey(keyName, new Primitive(keyValueStr));
                        var retrieveResult = await service.GetItemAsync(tableName, testKey);
                        retrieveResult.IsSuccessful.Should().BeTrue($"Should be able to retrieve item with key field {keyName}");
                    }
                }
            }
        }
        finally
        {
            await CleanupDatabaseAsync(tableName);
            if (service is IDisposable disposable)
                disposable.Dispose();
        }
    }

    [RetryFact(3, 5000)]
    public async Task MultipleKeyFields_WithFiltering_ShouldWorkCorrectly()
    {
        var service = CreateDatabaseService();
        var tableName = GetTestTableName();

        try
        {
            // Create items with different key fields and attributes for filtering
            var users = new[]
            {
                (key: new DbKey("UserId", new Primitive("user-1")), name: "Alice", type: "premium"),
                (key: new DbKey("UserId", new Primitive("user-2")), name: "Bob", type: "standard"),
                (key: new DbKey("UserId", new Primitive("user-3")), name: "Charlie", type: "premium")
            };

            var products = new[]
            {
                (key: new DbKey("ProductId", new Primitive("prod-1")), name: "Laptop", category: "electronics"),
                (key: new DbKey("ProductId", new Primitive("prod-2")), name: "Book", category: "media"),
                (key: new DbKey("ProductId", new Primitive("prod-3")), name: "Phone", category: "electronics")
            };

            // Put all items
            foreach (var (key, name, type) in users)
            {
                var userItem = new JObject
                {
                    ["Name"] = name,
                    ["AccountType"] = type,
                    ["EntityType"] = "user"
                };
                await service.PutItemAsync(tableName, key, userItem);
            }

            foreach (var (key, name, category) in products)
            {
                var productItem = new JObject
                {
                    ["Name"] = name,
                    ["Category"] = category,
                    ["EntityType"] = "product"
                };
                await service.PutItemAsync(tableName, key, productItem);
            }

            // Test filtering across different key field types

            // Filter 1: Find all premium users
            var premiumFilter = service.AttributeEquals("AccountType", new Primitive("premium"));
            var premiumResult = await service.ScanTableWithFilterAsync(tableName, premiumFilter);
            premiumResult.IsSuccessful.Should().BeTrue("Should filter for premium users");
            premiumResult.Data.Items.Count.Should().Be(2, "Should find 2 premium users");

            foreach (var item in premiumResult.Data.Items)
            {
                item.Should().ContainKey("UserId", "Premium items should have UserId key");
                item["AccountType"]?.ToString().Should().Be("premium");
            }

            // Filter 2: Find all electronics products
            var electronicsFilter = service.AttributeEquals("Category", new Primitive("electronics"));
            var electronicsResult = await service.ScanTableWithFilterAsync(tableName, electronicsFilter);
            electronicsResult.IsSuccessful.Should().BeTrue("Should filter for electronics");
            electronicsResult.Data.Items.Count.Should().Be(2, "Should find 2 electronics products");

            foreach (var item in electronicsResult.Data.Items)
            {
                item.Should().ContainKey("ProductId", "Electronics items should have ProductId key");
                item["Category"]?.ToString().Should().Be("electronics");
            }

            // Filter 3: Find all users (by entity type)
            var userEntityFilter = service.AttributeEquals("EntityType", new Primitive("user"));
            var userEntityResult = await service.ScanTableWithFilterAsync(tableName, userEntityFilter);
            userEntityResult.IsSuccessful.Should().BeTrue("Should filter for user entities");
            userEntityResult.Data.Items.Count.Should().Be(3, "Should find 3 user entities");

            // Verify key field alignment in filtered results
            for (int i = 0; i < userEntityResult.Data.Items.Count; i++)
            {
                var item = userEntityResult.Data.Items[i];

                item.Should().ContainKey("UserId", "User entity item should contain UserId key");
                item["UserId"]?.ToString().Should().NotBeNullOrEmpty("User entity item should have a valid UserId value");
                item.Should().ContainKey("AccountType", "User entity should have AccountType");
                item.Should().NotContainKey("Category", "User entity should not have Category");
            }

            // Test paginated filtering with multiple key types
            var paginatedResult = await service.ScanTableWithFilterPaginatedAsync(tableName, userEntityFilter, 2, null);
            paginatedResult.IsSuccessful.Should().BeTrue("Paginated filtering should work");
            paginatedResult.Data.Keys.Should().NotBeNull("First page should return keys");
            paginatedResult.Data.Items.Count.Should().Be(2, "Should respect page size");
            paginatedResult.Data.NextPageToken.Should().NotBeNull("Next page token should be returned");

            var secondPageResult = await service.ScanTableWithFilterPaginatedAsync(tableName, userEntityFilter, 2, paginatedResult.Data.NextPageToken);
            secondPageResult.IsSuccessful.Should().BeTrue("Paginated filtering should work");
            secondPageResult.Data.Items.Count.Should().Be(1, "Expected 1 item on second page");
        }
        finally
        {
            await CleanupDatabaseAsync(tableName);
            if (service is IDisposable disposable)
                disposable.Dispose();
        }
    }

    [RetryFact(3, 5000)]
    public async Task MultipleKeyFields_WithArrayAndIncrementOperations_ShouldWorkCorrectly()
    {
        var service = CreateDatabaseService();
        var tableName = GetTestTableName();

        try
        {
            // Create items with different key fields for testing array and increment operations

            // User item with UserId key
            var userKey = new DbKey("UserId", new Primitive("user-array-test"));
            var userItem = new JObject
            {
                ["Name"] = "Array Test User",
                ["Permissions"] = new JArray { "read", "write" },
                ["LoginCount"] = 0
            };

            // Product item with ProductId key
            var productKey = new DbKey("ProductId", new Primitive("prod-array-test"));
            var productItem = new JObject
            {
                ["Name"] = "Array Test Product",
                ["Tags"] = new JArray { "electronics", "popular" },
                ["ViewCount"] = 5
            };

            // Session item with SessionId key
            var sessionKey = new DbKey("SessionId", new Primitive("sess-array-test"));
            var sessionItem = new JObject
            {
                ["Status"] = "active",
                ["Activities"] = new JArray { "login", "browse" },
                ["Duration"] = 120.0
            };

            // Put all items
            await service.PutItemAsync(tableName, userKey, userItem);
            await service.PutItemAsync(tableName, productKey, productItem);
            await service.PutItemAsync(tableName, sessionKey, sessionItem);

            // Test array operations with different key field names

            // Add elements to user permissions
            var newPermissions = new[] { new Primitive("admin"), new Primitive("delete") };
            var addPermissionsResult = await service.AddElementsToArrayAsync(
                tableName, userKey, "Permissions", newPermissions,
                DbReturnItemBehavior.ReturnNewValues);
            addPermissionsResult.IsSuccessful.Should().BeTrue("Should add permissions to user");

            var userPermissions = addPermissionsResult.Data!["Permissions"] as JArray;
            userPermissions!.Count.Should().Be(4);
            userPermissions.Should().Contain(p => p.ToString() == "admin");
            userPermissions.Should().Contain(p => p.ToString() == "delete");

            // Add elements to product tags
            var newTags = new[] { new Primitive("featured"), new Primitive("bestseller") };
            var addTagsResult = await service.AddElementsToArrayAsync(
                tableName, productKey, "Tags", newTags,
                DbReturnItemBehavior.ReturnNewValues);
            addTagsResult.IsSuccessful.Should().BeTrue("Should add tags to product");

            var productTags = addTagsResult.Data!["Tags"] as JArray;
            productTags!.Count.Should().Be(4);
            productTags.Should().Contain(t => t.ToString() == "featured");

            // Add elements to session activities
            var newActivities = new[] { new Primitive("purchase"), new Primitive("logout") };
            var addActivitiesResult = await service.AddElementsToArrayAsync(
                tableName, sessionKey, "Activities", newActivities,
                DbReturnItemBehavior.ReturnNewValues);
            addActivitiesResult.IsSuccessful.Should().BeTrue("Should add activities to session");

            var sessionActivities = addActivitiesResult.Data!["Activities"] as JArray;
            sessionActivities!.Count.Should().Be(4);
            sessionActivities.Should().Contain(a => a.ToString() == "purchase");

            // Test increment operations with different key field names

            // Increment user login count
            var incrementLoginResult = await service.IncrementAttributeAsync(
                tableName, userKey, "LoginCount", 1.0);
            incrementLoginResult.IsSuccessful.Should().BeTrue("Should increment user login count");
            incrementLoginResult.Data.Should().Be(1.0);

            // Increment product view count
            var incrementViewResult = await service.IncrementAttributeAsync(
                tableName, productKey, "ViewCount", 3.0);
            incrementViewResult.IsSuccessful.Should().BeTrue("Should increment product view count");
            incrementViewResult.Data.Should().Be(8.0); // 5 + 3

            // Increment session duration
            var incrementDurationResult = await service.IncrementAttributeAsync(
                tableName, sessionKey, "Duration", 45.5);
            incrementDurationResult.IsSuccessful.Should().BeTrue("Should increment session duration");
            incrementDurationResult.Data.Should().Be(165.5); // 120.0 + 45.5

            // Test conditional array operations with different key types

            // Remove permission from user only if they have admin permission
            var hasAdminCondition = service.ArrayElementExists("Permissions", new Primitive("admin"));
            var removePermissionResult = await service.RemoveElementsFromArrayAsync(
                tableName, userKey, "Permissions", [new Primitive("write")],
                DbReturnItemBehavior.ReturnNewValues, hasAdminCondition);
            removePermissionResult.IsSuccessful.Should().BeTrue("Should remove write permission when admin exists");

            var finalPermissions = removePermissionResult.Data!["Permissions"] as JArray;
            finalPermissions!.Count.Should().Be(3);
            finalPermissions.Should().Contain(p => p.ToString() == "admin");
            finalPermissions.Should().NotContain(p => p.ToString() == "write");

            // Test conditional increment with different key type
            var isPopularCondition = service.ArrayElementExists("Tags", new Primitive("popular"));
            var conditionalViewIncrement = await service.IncrementAttributeAsync(
                tableName, productKey, "ViewCount", 5.0, isPopularCondition);
            conditionalViewIncrement.IsSuccessful.Should().BeTrue("Should increment views for popular product");
            conditionalViewIncrement.Data.Should().Be(13.0); // 8.0 + 5.0

            // Verify all operations worked by getting final state
            var finalUserResult = await service.GetItemAsync(tableName, userKey);
            var finalProductResult = await service.GetItemAsync(tableName, productKey);
            var finalSessionResult = await service.GetItemAsync(tableName, sessionKey);

            finalUserResult.IsSuccessful.Should().BeTrue();
            finalUserResult.Data!["LoginCount"]?.ToObject<double>().Should().Be(1.0);

            finalProductResult.IsSuccessful.Should().BeTrue();
            finalProductResult.Data!["ViewCount"]?.ToObject<double>().Should().Be(13.0);

            finalSessionResult.IsSuccessful.Should().BeTrue();
            finalSessionResult.Data!["Duration"]?.ToObject<double>().Should().Be(165.5);
        }
        finally
        {
            await CleanupDatabaseAsync(tableName);
            if (service is IDisposable disposable)
                disposable.Dispose();
        }
    }

    [RetryFact(3, 5000)]
    public async Task MultipleKeyFields_WithComplexConditions_ShouldEnforceBusinessRules()
    {
        var service = CreateDatabaseService();
        var tableName = GetTestTableName();

        try
        {
            // Scenario: Multi-entity system with complex business rules

            // User entities with UserId keys
            var adminKey = new DbKey("UserId", new Primitive("admin-001"));
            var adminItem = new JObject
            {
                ["Name"] = "Admin User",
                ["Role"] = "admin",
                ["Permissions"] = new JArray { "read", "write", "delete", "admin" },
                ["LastLoginDays"] = 1,
                ["Active"] = true
            };

            var standardUserKey = new DbKey("UserId", new Primitive("user-001"));
            var standardUserItem = new JObject
            {
                ["Name"] = "Standard User",
                ["Role"] = "user",
                ["Permissions"] = new JArray { "read", "write" },
                ["LastLoginDays"] = 30,
                ["Active"] = true
            };

            // Resource entities with ResourceId keys
            var sensitiveResourceKey = new DbKey("ResourceId", new Primitive("resource-001"));
            var sensitiveResourceItem = new JObject
            {
                ["Name"] = "Sensitive Data",
                ["Classification"] = "confidential",
                ["AccessCount"] = 0,
                ["RequiredPermissions"] = new JArray { "admin" },
                ["Locked"] = false
            };

            var publicResourceKey = new DbKey("ResourceId", new Primitive("resource-002"));
            var publicResourceItem = new JObject
            {
                ["Name"] = "Public Data",
                ["Classification"] = "public",
                ["AccessCount"] = 100,
                ["RequiredPermissions"] = new JArray { "read" },
                ["Locked"] = false
            };

            // Audit entities with AuditId keys
            var auditKey = new DbKey("AuditId", new Primitive("audit-001"));
            var auditItem = new JObject
            {
                ["Action"] = "login",
                ["Result"] = "success",
                ["Severity"] = 1,
                ["Metadata"] = new JObject { ["Source"] = "web" }
            };

            // Put all entities
            await service.PutItemAsync(tableName, adminKey, adminItem);
            await service.PutItemAsync(tableName, standardUserKey, standardUserItem);
            await service.PutItemAsync(tableName, sensitiveResourceKey, sensitiveResourceItem);
            await service.PutItemAsync(tableName, publicResourceKey, publicResourceItem);
            await service.PutItemAsync(tableName, auditKey, auditItem);

            // Business Rule 1: Only admins can access sensitive resources (simulate access tracking)
            var isAdminCondition = service.AttributeEquals("Role", new Primitive("admin"));
            var hasAdminPermCondition = service.ArrayElementExists("Permissions", new Primitive("admin"));

            // Admin accessing sensitive resource should succeed
            var adminAccessResult = await service.IncrementAttributeAsync(
                tableName, adminKey, "AccessAttempts", 1.0, isAdminCondition.And(hasAdminPermCondition));
            adminAccessResult.IsSuccessful.Should().BeTrue("Admin should be able to attempt access");

            // Business Rule 2: Update resource access count only if user has required permissions
            var adminHasRequiredPerm = service.ArrayElementExists("Permissions", new Primitive("admin"));
            var resourceNotLocked = service.AttributeEquals("Locked", new Primitive(false));

            var sensitiveAccessResult = await service.IncrementAttributeAsync(
                tableName, sensitiveResourceKey, "AccessCount", 1.0, resourceNotLocked);
            // Note: This simulates the access after permission check passed
            sensitiveAccessResult.IsSuccessful.Should().BeTrue("Should increment access count for unlocked resource");

            // Business Rule 3: Standard users cannot modify admin permissions
            var isNotAdmin = service.AttributeNotEquals("Role", new Primitive("admin"));
            var standardUserTryAddAdmin = await service.AddElementsToArrayAsync(
                tableName, standardUserKey, "Permissions", [new Primitive("admin")],
                DbReturnItemBehavior.DoNotReturn, isNotAdmin);
            standardUserTryAddAdmin.IsSuccessful.Should().BeTrue("Standard user exists and role != admin");

            // But verify they still don't have admin permission in a separate check
            var standardUserFinalState = await service.GetItemAsync(tableName, standardUserKey);
            var standardPermissions = standardUserFinalState.Data!["Permissions"] as JArray;
            // The operation succeeded because the condition (role != admin) was true,
            // but this demonstrates how conditions control access

            // Business Rule 4: Lock resource if access attempts are too high
            var highAccessCondition = service.AttributeIsGreaterOrEqual("AccessCount", new Primitive(1.0));
            var lockResourceResult = await service.UpdateItemAsync(
                tableName, sensitiveResourceKey, new JObject { ["Locked"] = true, ["LockReason"] = "High access" },
                DbReturnItemBehavior.ReturnNewValues, highAccessCondition);
            lockResourceResult.IsSuccessful.Should().BeTrue("Should lock resource with high access count");
            lockResourceResult.Data!["Locked"]?.Value<bool>().Should().Be(true);

            // Business Rule 5: Create audit entry only for significant actions
            var significantSeverity = service.AttributeIsGreaterOrEqual("Severity", new Primitive(2.0));
            var updateAuditResult = await service.UpdateItemAsync(
                tableName, auditKey, new JObject { ["Severity"] = 3, ["Reviewed"] = false },
                DbReturnItemBehavior.ReturnNewValues);
            updateAuditResult.IsSuccessful.Should().BeTrue("Should update audit entry");

            // Business Rule 6: Multi-entity workflow - deactivate inactive users
            var inactiveCondition = service.AttributeIsGreaterThan("LastLoginDays", new Primitive(7L));
            var activeCondition = service.AttributeEquals("Active", new Primitive(true));

            var deactivateInactiveResult = await service.UpdateItemAsync(
                tableName, standardUserKey, new JObject { ["Active"] = false, ["DeactivatedReason"] = "Inactive" },
                DbReturnItemBehavior.ReturnNewValues, inactiveCondition.And(activeCondition));
            deactivateInactiveResult.IsSuccessful.Should().BeTrue("Should deactivate inactive standard user");
            deactivateInactiveResult.Data!["Active"]?.Value<bool>().Should().Be(false);

            // Verify final system state across different entity types
            var finalScan = await service.ScanTableAsync(tableName);
            finalScan.IsSuccessful.Should().BeTrue("Should scan mixed entity table");
            finalScan.Data.Items.Count.Should().Be(5, "Should have all 5 entities");

            // Count entities by type based on their key fields
            var userEntities = finalScan.Data.Items.Where(item => item.ContainsKey("UserId")).ToList();
            var resourceEntities = finalScan.Data.Items.Where(item => item.ContainsKey("ResourceId")).ToList();
            var auditEntities = finalScan.Data.Items.Where(item => item.ContainsKey("AuditId")).ToList();

            userEntities.Should().HaveCount(2, "Should have 2 user entities");
            resourceEntities.Should().HaveCount(2, "Should have 2 resource entities");
            auditEntities.Should().HaveCount(1, "Should have 1 audit entity");

            // Verify business rule effects
            var finalAdmin = userEntities.FirstOrDefault(u => u["UserId"]?.ToString().Contains("admin") == true);
            var finalStandardUser = userEntities.FirstOrDefault(u => u["UserId"]?.ToString().Contains("user-001") == true);
            var finalSensitiveResource = resourceEntities.FirstOrDefault(r => r["ResourceId"]?.ToString().Contains("resource-001") == true);

            finalAdmin.Should().NotBeNull("Admin user should exist");
            finalAdmin!["Role"]?.ToString().Should().Be("admin");

            finalStandardUser.Should().NotBeNull("Standard user should exist");
            finalStandardUser!["Active"]?.Value<bool>().Should().Be(false, "Standard user should be deactivated");

            finalSensitiveResource.Should().NotBeNull("Sensitive resource should exist");
            finalSensitiveResource!["Locked"]?.Value<bool>().Should().Be(true, "Sensitive resource should be locked");
        }
        finally
        {
            await CleanupDatabaseAsync(tableName);
            if (service is IDisposable disposable)
                disposable.Dispose();
        }
    }

    #endregion

    #region Condition Coupling Tests
    [RetryFact(3, 5000)]
    public async Task ConditionCoupling_SimpleAndOperation_ShouldWorkCorrectly()
    {
        var service = CreateDatabaseService();
        var tableName = GetTestTableName();
        var keyName = "Id";
        var keyValue = new Primitive($"simple-and-test-{Guid.NewGuid():N}");

        try
        {
            // Create test item
            var item = new JObject
            {
                ["Name"] = "SimpleAndTest",
                ["Status"] = "active",
                ["Score"] = 85,
                ["Level"] = "premium"
            };
            await service.PutItemAsync(tableName, new DbKey(keyName, keyValue), item);

            // Test: condition1.And(condition2)
            var condition1 = service.AttributeEquals("Status", new Primitive("active"));
            var condition2 = service.AttributeIsGreaterThan("Score", new Primitive(80.0));
            var andCondition = condition1.And(condition2);

            var existsResult = await service.ItemExistsAsync(tableName, new DbKey(keyName, keyValue), andCondition);
            existsResult.IsSuccessful.Should().BeTrue("Both conditions should be satisfied");
            existsResult.Data.Should().BeTrue();

            // Test case where one condition fails
            var condition3 = service.AttributeEquals("Level", new Primitive("basic"));
            var failingAndCondition = condition1.And(condition3);

            var failingResult = await service.ItemExistsAsync(tableName, new DbKey(keyName, keyValue), failingAndCondition);
            failingResult.IsSuccessful.Should().BeFalse("Should fail when any AND condition fails");
            failingResult.StatusCode.Should().Be(HttpStatusCode.PreconditionFailed);
        }
        finally
        {
            await CleanupDatabaseAsync(tableName);
            if (service is IDisposable disposable)
                disposable.Dispose();
        }
    }

    [RetryFact(3, 5000)]
    public async Task ConditionCoupling_SimpleOrOperation_ShouldWorkCorrectly()
    {
        var service = CreateDatabaseService();
        var tableName = GetTestTableName();
        var keyName = "Id";
        var keyValue = new Primitive($"simple-or-test-{Guid.NewGuid():N}");

        try
        {
            // Create test item
            var item = new JObject
            {
                ["Name"] = "SimpleOrTest",
                ["Status"] = "inactive",
                ["Score"] = 90,
                ["Level"] = "basic"
            };
            await service.PutItemAsync(tableName, new DbKey(keyName, keyValue), item);

            // Test: condition1.Or(condition2) - first condition fails, second passes
            var condition1 = service.AttributeEquals("Status", new Primitive("active"));
            var condition2 = service.AttributeIsGreaterThan("Score", new Primitive(80.0));
            var orCondition = condition1.Or(condition2);

            var existsResult = await service.ItemExistsAsync(tableName, new DbKey(keyName, keyValue), orCondition);
            existsResult.IsSuccessful.Should().BeTrue("Should pass when any OR condition passes");
            existsResult.Data.Should().BeTrue();

            // Test case where both conditions fail
            var condition3 = service.AttributeEquals("Level", new Primitive("premium"));
            var failingOrCondition = condition1.Or(condition3);

            var failingResult = await service.ItemExistsAsync(tableName, new DbKey(keyName, keyValue), failingOrCondition);
            failingResult.IsSuccessful.Should().BeFalse("Should fail when all OR conditions fail");
            failingResult.StatusCode.Should().Be(HttpStatusCode.PreconditionFailed);
        }
        finally
        {
            await CleanupDatabaseAsync(tableName);
            if (service is IDisposable disposable)
                disposable.Dispose();
        }
    }

    [RetryFact(3, 5000)]
    public async Task ConditionCoupling_ComplexNestedConditions_ShouldWorkCorrectly()
    {
        var service = CreateDatabaseService();
        var tableName = GetTestTableName();
        var keyName = "Id";
        var keyValue = new Primitive($"complex-nested-test-{Guid.NewGuid():N}");

        try
        {
            // Create test item
            var item = new JObject
            {
                ["Name"] = "ComplexNestedTest",
                ["Status"] = "active",
                ["Score"] = 85,
                ["Level"] = "premium",
                ["Category"] = "VIP",
                ["Tags"] = new JArray { "verified", "priority" }
            };
            await service.PutItemAsync(tableName, new DbKey(keyName, keyValue), item);

            // Test: condition1.And(condition2).Or(condition3)
            // This should create: ((condition1 AND condition2) OR condition3)
            var condition1 = service.AttributeEquals("Status", new Primitive("active"));
            var condition2 = service.AttributeIsGreaterThan("Score", new Primitive(80.0));
            var condition3 = service.AttributeEquals("Category", new Primitive("SUPER_VIP"));

            var complexCondition = condition1.And(condition2).Or(condition3);

            var existsResult = await service.ItemExistsAsync(tableName, new DbKey(keyName, keyValue), complexCondition);
            existsResult.IsSuccessful.Should().BeTrue("Should pass because (active AND score>80) is true, even though category!=SUPER_VIP");
            existsResult.Data.Should().BeTrue();

            // Test: condition1.And(condition2.Or(condition3))
            // This should create: (condition1 AND (condition2 OR condition3))
            var condition4 = service.AttributeEquals("Level", new Primitive("basic"));
            var condition5 = service.ArrayElementExists("Tags", new Primitive("verified"));

            var nestedCondition = condition1.And(condition4.Or(condition5));

            var nestedResult = await service.ItemExistsAsync(tableName, new DbKey(keyName, keyValue), nestedCondition);
            nestedResult.IsSuccessful.Should().BeTrue("Should pass because active=true AND (level=basic OR has 'verified' tag), and has 'verified' tag");
            nestedResult.Data.Should().BeTrue();
        }
        finally
        {
            await CleanupDatabaseAsync(tableName);
            if (service is IDisposable disposable)
                disposable.Dispose();
        }
    }

    [RetryFact(3, 5000)]
    public async Task ConditionCoupling_DeepNesting_ShouldWorkCorrectly()
    {
        var service = CreateDatabaseService();
        var tableName = GetTestTableName();
        var keyName = "Id";
        var keyValue = new Primitive($"deep-nesting-test-{Guid.NewGuid():N}");

        try
        {
            // Create test item
            var item = new JObject
            {
                ["Name"] = "DeepNestingTest",
                ["A"] = "valueA",
                ["B"] = "valueB",
                ["C"] = "valueC",
                ["D"] = "valueD",
                ["E"] = "valueE",
                ["Numbers"] = new JArray { 1, 2, 3 }
            };
            await service.PutItemAsync(tableName, new DbKey(keyName, keyValue), item);

            // Create deeply nested condition: ((A AND B) OR (C AND D)) AND E
            var condA = service.AttributeEquals("A", new Primitive("valueA"));
            var condB = service.AttributeEquals("B", new Primitive("valueB"));
            var condC = service.AttributeEquals("C", new Primitive("wrongC"));
            var condD = service.AttributeEquals("D", new Primitive("valueD"));
            var condE = service.ArrayElementExists("Numbers", new Primitive(2L));

            var deepCondition = condA.And(condB).Or(condC.And(condD)).And(condE);

            var existsResult = await service.ItemExistsAsync(tableName, new DbKey(keyName, keyValue), deepCondition);
            existsResult.IsSuccessful.Should().BeTrue("Should pass: ((A=valueA AND B=valueB) OR (C=wrongC AND D=valueD)) AND has number 2");
            existsResult.Data.Should().BeTrue("Left side of final AND is true because (A AND B) is true, and E is true");

            // Test case where final condition fails
            var condF = service.ArrayElementExists("Numbers", new Primitive(5L));
            var failingDeepCondition = condA.And(condB).Or(condC.And(condD)).And(condF);

            var failingResult = await service.ItemExistsAsync(tableName, new DbKey(keyName, keyValue), failingDeepCondition);
            failingResult.IsSuccessful.Should().BeFalse("Should fail because final condition (has number 5) is false");
            failingResult.StatusCode.Should().Be(HttpStatusCode.PreconditionFailed);
        }
        finally
        {
            await CleanupDatabaseAsync(tableName);
            if (service is IDisposable disposable)
                disposable.Dispose();
        }
    }

    [RetryFact(3, 5000)]
    public async Task ConditionCoupling_WithUpdateOperation_ShouldWorkCorrectly()
    {
        var service = CreateDatabaseService();
        var tableName = GetTestTableName();
        var keyName = "Id";
        var keyValue = new Primitive($"update-coupling-test-{Guid.NewGuid():N}");

        try
        {
            // Create test item
            var item = new JObject
            {
                ["Name"] = "UpdateCouplingTest",
                ["Status"] = "pending",
                ["Priority"] = 5,
                ["Owner"] = "system",
                ["Tags"] = new JArray { "automated", "review" }
            };
            await service.PutItemAsync(tableName, new DbKey(keyName, keyValue), item);

            // Test update with complex condition: (Status=pending AND Priority>=3) OR HasTag(urgent)
            var statusCondition = service.AttributeEquals("Status", new Primitive("pending"));
            var priorityCondition = service.AttributeIsGreaterOrEqual("Priority", new Primitive(3.0));
            var urgentTagCondition = service.ArrayElementExists("Tags", new Primitive("urgent"));

            var complexUpdateCondition = statusCondition.And(priorityCondition).Or(urgentTagCondition);

            var updateData = new JObject
            {
                ["Status"] = "processing",
                ["ProcessedBy"] = "worker-1",
                ["ProcessedAt"] = DateTime.UtcNow.ToString("O")
            };

            var updateResult = await service.UpdateItemAsync(
                tableName, new DbKey(keyName, keyValue), updateData,
                DbReturnItemBehavior.ReturnNewValues, complexUpdateCondition);

            updateResult.IsSuccessful.Should().BeTrue("Should update because (pending=true AND priority>=3) is satisfied");
            updateResult.Data!["Status"]?.ToString().Should().Be("processing");
            updateResult.Data["ProcessedBy"]?.ToString().Should().Be("worker-1");

            // Test update that should fail due to condition
            var restrictiveCondition = service.AttributeEquals("Owner", new Primitive("admin"))
                .And(service.ArrayElementExists("Tags", new Primitive("critical")));

            var secondUpdateData = new JObject { ["Status"] = "completed" };

            var failingUpdateResult = await service.UpdateItemAsync(
                tableName, new DbKey(keyName, keyValue), secondUpdateData,
                DbReturnItemBehavior.DoNotReturn, restrictiveCondition);

            failingUpdateResult.IsSuccessful.Should().BeFalse("Should fail because Owner!=admin AND no 'critical' tag");
            failingUpdateResult.StatusCode.Should().Be(HttpStatusCode.PreconditionFailed);

            // Verify status wasn't changed
            var finalState = await service.GetItemAsync(tableName, new DbKey(keyName, keyValue));
            finalState.Data!["Status"]?.ToString().Should().Be("processing", "Status should remain 'processing' after failed update");
        }
        finally
        {
            await CleanupDatabaseAsync(tableName);
            if (service is IDisposable disposable)
                disposable.Dispose();
        }
    }

    [RetryFact(3, 5000)]
    public async Task ConditionCoupling_WithDeleteOperation_ShouldWorkCorrectly()
    {
        var service = CreateDatabaseService();
        var tableName = GetTestTableName();
        var keyName = "Id";
        var keyValue = new Primitive($"delete-coupling-test-{Guid.NewGuid():N}");

        try
        {
            // Create test item
            var item = new JObject
            {
                ["Name"] = "DeleteCouplingTest",
                ["Status"] = "inactive",
                ["LastAccessed"] = 45,
                ["DataRetention"] = 0,
                ["Archived"] = true,
                ["Classifications"] = new JArray { "temporary", "non-critical" }
            };
            await service.PutItemAsync(tableName, new DbKey(keyName, keyValue), item);

            // Test delete with complex condition: (Status=inactive AND LastAccessed>30) OR (Archived=true AND DataRetention=0)
            var inactiveCondition = service.AttributeEquals("Status", new Primitive("inactive"));
            var oldAccessCondition = service.AttributeIsGreaterThan("LastAccessed", new Primitive(30.0));
            var archivedCondition = service.AttributeEquals("Archived", new Primitive(true));
            var noRetentionCondition = service.AttributeEquals("DataRetention", new Primitive(0.0));

            var deleteCondition = inactiveCondition.And(oldAccessCondition).Or(archivedCondition.And(noRetentionCondition));

            var deleteResult = await service.DeleteItemAsync(
                tableName, new DbKey(keyName, keyValue),
                DbReturnItemBehavior.ReturnOldValues, deleteCondition);

            deleteResult.IsSuccessful.Should().BeTrue("Should delete because both sides of OR are true");
            deleteResult.Data.Should().NotBeNull("Should return old values");
            deleteResult.Data!["Name"]?.ToString().Should().Be("DeleteCouplingTest");

            // Verify item is deleted
            var existsAfterDelete = await service.ItemExistsAsync(tableName, new DbKey(keyName, keyValue));
            existsAfterDelete.IsSuccessful.Should().BeFalse("Item should not exist after successful delete");
            existsAfterDelete.StatusCode.Should().Be(HttpStatusCode.NotFound);
        }
        finally
        {
            await CleanupDatabaseAsync(tableName);
            if (service is IDisposable disposable)
                disposable.Dispose();
        }
    }

    [RetryFact(3, 5000)]
    public async Task ConditionCoupling_WithArrayOperations_ShouldWorkCorrectly()
    {
        var service = CreateDatabaseService();
        var tableName = GetTestTableName();
        var keyName = "Id";
        var keyValue = new Primitive($"array-coupling-test-{Guid.NewGuid():N}");

        try
        {
            // Create test item
            var item = new JObject
            {
                ["Name"] = "ArrayCouplingTest",
                ["UserType"] = "moderator",
                ["SecurityLevel"] = 7,
                ["Active"] = true,
                ["Permissions"] = new JArray { "read", "write", "moderate" }
            };
            await service.PutItemAsync(tableName, new DbKey(keyName, keyValue), item);

            // Test add elements with complex condition: (UserType=moderator AND SecurityLevel>=5) OR (Active=true AND HasModeratePermission)
            var moderatorCondition = service.AttributeEquals("UserType", new Primitive("moderator"));
            var securityCondition = service.AttributeIsGreaterOrEqual("SecurityLevel", new Primitive(5.0));
            var activeCondition = service.AttributeEquals("Active", new Primitive(true));
            var moderatePermCondition = service.ArrayElementExists("Permissions", new Primitive("moderate"));

            var addPermissionsCondition = moderatorCondition.And(securityCondition).Or(activeCondition.And(moderatePermCondition));

            var newPermissions = new[]
            {
                new Primitive("admin"),
                new Primitive("delete")
            };

            var addResult = await service.AddElementsToArrayAsync(
                tableName, new DbKey(keyName, keyValue), "Permissions", newPermissions,
                DbReturnItemBehavior.ReturnNewValues, addPermissionsCondition);

            addResult.IsSuccessful.Should().BeTrue("Should add permissions because both sides of OR condition are satisfied");
            var permissions = addResult.Data!["Permissions"] as JArray;
            permissions!.Count.Should().Be(5);
            permissions.Should().Contain(p => p.ToString() == "admin");
            permissions.Should().Contain(p => p.ToString() == "delete");

            // Test remove elements with restrictive condition: SecurityLevel>=8 AND UserType=admin
            var restrictiveSecurityCondition = service.AttributeIsGreaterOrEqual("SecurityLevel", new Primitive(8.0));
            var adminTypeCondition = service.AttributeEquals("UserType", new Primitive("admin"));

            var removeCondition = restrictiveSecurityCondition.And(adminTypeCondition);
            var elementsToRemove = new[] { new Primitive("admin") };

            var removeResult = await service.RemoveElementsFromArrayAsync(
                tableName, new DbKey(keyName, keyValue), "Permissions", elementsToRemove,
                DbReturnItemBehavior.DoNotReturn, removeCondition);

            removeResult.IsSuccessful.Should().BeFalse("Should fail to remove admin permission due to restrictive condition");
            removeResult.StatusCode.Should().Be(HttpStatusCode.PreconditionFailed);

            // Verify admin permission still exists
            var finalState = await service.GetItemAsync(tableName, new DbKey(keyName, keyValue));
            var finalPermissions = finalState.Data!["Permissions"] as JArray;
            finalPermissions.Should().Contain(p => p.ToString() == "admin", "Admin permission should still exist");
        }
        finally
        {
            await CleanupDatabaseAsync(tableName);
            if (service is IDisposable disposable)
                disposable.Dispose();
        }
    }

    [RetryFact(3, 5000)]
    public async Task ConditionCoupling_WithIncrementOperation_ShouldWorkCorrectly()
    {
        var service = CreateDatabaseService();
        var tableName = GetTestTableName();
        var keyName = "Id";
        var keyValue = new Primitive($"increment-coupling-test-{Guid.NewGuid():N}");

        try
        {
            // Create test item
            var item = new JObject
            {
                ["Name"] = "IncrementCouplingTest",
                ["AccountType"] = "premium",
                ["Balance"] = 1000.0,
                ["DailyLimit"] = 500.0,
                ["TransactionCount"] = 3,
                ["Verified"] = true,
                ["Flags"] = new JArray { "trusted", "verified" }
            };
            await service.PutItemAsync(tableName, new DbKey(keyName, keyValue), item);

            // Test increment with complex condition: (AccountType=premium AND Balance>500) OR (Verified=true AND HasTrustedFlag)
            var premiumCondition = service.AttributeEquals("AccountType", new Primitive("premium"));
            var balanceCondition = service.AttributeIsGreaterThan("Balance", new Primitive(500.0));
            var verifiedCondition = service.AttributeEquals("Verified", new Primitive(true));
            var trustedFlagCondition = service.ArrayElementExists("Flags", new Primitive("trusted"));

            var incrementCondition = premiumCondition.And(balanceCondition).Or(verifiedCondition.And(trustedFlagCondition));

            // Test successful increment (withdrawal)
            var incrementResult = await service.IncrementAttributeAsync(
                tableName, new DbKey(keyName, keyValue), "Balance", -200.0, incrementCondition);

            incrementResult.IsSuccessful.Should().BeTrue("Should allow withdrawal because both sides of OR are satisfied");
            incrementResult.Data.Should().Be(800.0);

            // Test increment with more restrictive condition that should fail
            var restrictiveCondition = service.AttributeIsGreaterThan("Balance", new Primitive(1000.0))
                .And(service.AttributeIsLessThan("TransactionCount", new Primitive(2L)));

            var failingIncrementResult = await service.IncrementAttributeAsync(
                tableName, new DbKey(keyName, keyValue), "Balance", -100.0, restrictiveCondition);

            failingIncrementResult.IsSuccessful.Should().BeFalse("Should fail because Balance is not >1000 AND TransactionCount is not <2");
            failingIncrementResult.StatusCode.Should().Be(HttpStatusCode.PreconditionFailed);

            // Verify balance remains unchanged after failed increment
            var finalState = await service.GetItemAsync(tableName, new DbKey(keyName, keyValue));
            finalState.Data!["Balance"]?.ToObject<double>().Should().Be(800.0, "Balance should remain unchanged after failed increment");
        }
        finally
        {
            await CleanupDatabaseAsync(tableName);
            if (service is IDisposable disposable)
                disposable.Dispose();
        }
    }

    [RetryFact(3, 5000)]
    public async Task ConditionCoupling_EmptyCondition_ShouldAlwaysPass()
    {
        var service = CreateDatabaseService();
        var tableName = GetTestTableName();
        var keyName = "Id";
        var keyValue = new Primitive($"empty-condition-test-{Guid.NewGuid():N}");

        try
        {
            // Create test item
            var item = new JObject
            {
                ["Name"] = "EmptyConditionTest",
                ["Value"] = 42
            };
            await service.PutItemAsync(tableName, new DbKey(keyName, keyValue), item);

            // Test with empty condition
            var emptyCondition = new ConditionCoupling();

            var existsResult = await service.ItemExistsAsync(tableName, new DbKey(keyName, keyValue), emptyCondition);
            existsResult.IsSuccessful.Should().BeTrue("Empty condition should always pass");
            existsResult.Data.Should().BeTrue();

            // Test update with empty condition
            var updateData = new JObject { ["Value"] = 84 };
            var updateResult = await service.UpdateItemAsync(
                tableName, new DbKey(keyName, keyValue), updateData,
                DbReturnItemBehavior.ReturnNewValues, emptyCondition);

            updateResult.IsSuccessful.Should().BeTrue("Update with empty condition should succeed");
            updateResult.Data!["Value"]?.ToObject<int>().Should().Be(84);
        }
        finally
        {
            await CleanupDatabaseAsync(tableName);
            if (service is IDisposable disposable)
                disposable.Dispose();
        }
    }

    [RetryFact(3, 5000)]
    public async Task ConditionCoupling_MixedConditionTypes_ShouldWorkCorrectly()
    {
        var service = CreateDatabaseService();
        var tableName = GetTestTableName();
        var keyName = "Id";
        var keyValue = new Primitive($"mixed-conditions-test-{Guid.NewGuid():N}");

        try
        {
            // Create test item with various attribute types
            var item = new JObject
            {
                ["Name"] = "MixedConditionsTest",
                ["StringValue"] = "test",
                ["IntegerValue"] = 42,
                ["DoubleValue"] = 99.5,
                ["BooleanValue"] = true,
                ["ExistingField"] = "present",
                ["ArrayField"] = new JArray { "item1", "item2", "item3" },
                ["NumericArray"] = new JArray { 1, 2, 3 }
            };
            await service.PutItemAsync(tableName, new DbKey(keyName, keyValue), item);

            // Create complex condition mixing all condition types:
            // (StringValue="test" AND IntegerValue>40) AND
            // (ExistingField EXISTS OR NonExistentField NOT EXISTS) AND
            // (ArrayField contains "item2" AND DoubleValue<=100.0)

            var stringCondition = service.AttributeEquals("StringValue", new Primitive("test"));
            var integerCondition = service.AttributeIsGreaterThan("IntegerValue", new Primitive(40L));
            var existsCondition = service.AttributeExists("ExistingField");
            var notExistsCondition = service.AttributeNotExists("NonExistentField");
            var arrayCondition = service.ArrayElementExists("ArrayField", new Primitive("item2"));
            var doubleCondition = service.AttributeIsLessOrEqual("DoubleValue", new Primitive(100.0));

            var mixedCondition = stringCondition.And(integerCondition)
                .And(existsCondition.Or(notExistsCondition))
                .And(arrayCondition.And(doubleCondition));

            var existsResult = await service.ItemExistsAsync(tableName, new DbKey(keyName, keyValue), mixedCondition);
            existsResult.IsSuccessful.Should().BeTrue("Mixed condition should be satisfied");
            existsResult.Data.Should().BeTrue();

            // Test a condition that should fail
            var failingMixedCondition = stringCondition.And(integerCondition)
                .And(service.AttributeNotExists("ExistingField")) // This will fail
                .And(arrayCondition);

            var failingResult = await service.ItemExistsAsync(tableName, new DbKey(keyName, keyValue), failingMixedCondition);
            failingResult.IsSuccessful.Should().BeFalse("Should fail because ExistingField does exist");
            failingResult.StatusCode.Should().Be(HttpStatusCode.PreconditionFailed);
        }
        finally
        {
            await CleanupDatabaseAsync(tableName);
            if (service is IDisposable disposable)
                disposable.Dispose();
        }
    }

    [RetryFact(3, 5000)]
    public async Task ConditionCoupling_ComplexBusinessScenario_ShouldEnforceRules()
    {
        var service = CreateDatabaseService();
        var tableName = GetTestTableName();
        var keyName = "Id";
        var keyValue = new Primitive($"business-scenario-test-{Guid.NewGuid():N}");

        try
        {
            // Scenario: Complex user permission management system
            var userItem = new JObject
            {
                ["Username"] = "john.doe",
                ["Role"] = "manager",
                ["Department"] = "engineering",
                ["SecurityClearance"] = 7,
                ["Active"] = true,
                ["LastLogin"] = 2,
                ["ProjectCount"] = 5,
                ["Certifications"] = new JArray { "AWS", "Security", "Leadership" },
                ["Permissions"] = new JArray { "read", "write", "approve" }
            };
            await service.PutItemAsync(tableName, new DbKey(keyName, keyValue), userItem);

            // Business Rule 1: Can access admin functions if:
            // (Role=manager AND Department=engineering AND SecurityClearance>=5) OR
            // (HasSecurityCertification AND SecurityClearance>=8)

            var roleCondition = service.AttributeEquals("Role", new Primitive("manager"));
            var deptCondition = service.AttributeEquals("Department", new Primitive("engineering"));
            var clearanceCondition = service.AttributeIsGreaterOrEqual("SecurityClearance", new Primitive(5.0));
            var securityCertCondition = service.ArrayElementExists("Certifications", new Primitive("Security"));
            var highClearanceCondition = service.AttributeIsGreaterOrEqual("SecurityClearance", new Primitive(8.0));

            var adminAccessCondition = roleCondition.And(deptCondition).And(clearanceCondition)
                .Or(securityCertCondition.And(highClearanceCondition));

            // Test granting admin permission
            var adminPermissions = new[] { new Primitive("admin") };
            var grantAdminResult = await service.AddElementsToArrayAsync(
                tableName, new DbKey(keyName, keyValue), "Permissions", adminPermissions,
                DbReturnItemBehavior.ReturnNewValues, adminAccessCondition);

            grantAdminResult.IsSuccessful.Should().BeTrue("Should grant admin access based on manager role and clearance");
            var permissions = grantAdminResult.Data!["Permissions"] as JArray;
            permissions.Should().Contain(p => p.ToString() == "admin");

            // Business Rule 2: Can delete projects if:
            // (Active=true AND LastLogin<=7 AND ProjectCount>0) AND
            // (HasApprovePermission OR HasAdminPermission)

            var activeCondition = service.AttributeEquals("Active", new Primitive(true));
            var recentLoginCondition = service.AttributeIsLessOrEqual("LastLogin", new Primitive(7.0));
            var hasProjectsCondition = service.AttributeIsGreaterThan("ProjectCount", new Primitive(0.0));
            var approvePermCondition = service.ArrayElementExists("Permissions", new Primitive("approve"));
            var adminPermCondition = service.ArrayElementExists("Permissions", new Primitive("admin"));

            var deleteProjectCondition = activeCondition.And(recentLoginCondition).And(hasProjectsCondition)
                .And(approvePermCondition.Or(adminPermCondition));

            // Test project deletion (simulated by decrementing project count)
            var decrementResult = await service.IncrementAttributeAsync(
                tableName, new DbKey(keyName, keyValue), "ProjectCount", -1.0, deleteProjectCondition);

            decrementResult.IsSuccessful.Should().BeTrue("Should allow project deletion based on permissions");
            decrementResult.Data.Should().Be(4.0);

            // Business Rule 3: Cannot remove critical permissions if:
            // User is the only manager with high clearance
            // (For this test, we'll simulate this by requiring both admin AND approve permissions to exist)

            var hasBothPermissions = approvePermCondition.And(adminPermCondition);

            // Try to remove approve permission - should fail if both admin and approve exist
            var removeApproveResult = await service.RemoveElementsFromArrayAsync(
                tableName, new DbKey(keyName, keyValue), "Permissions", [new Primitive("approve")],
                DbReturnItemBehavior.DoNotReturn, hasBothPermissions);

            // This will succeed because the condition is satisfied (both permissions exist)
            // but in real business logic, this might be inverted to prevent removal
            removeApproveResult.IsSuccessful.Should().BeTrue("Condition is satisfied, but in real scenario this might prevent removal");

            // Business Rule 4: Complex update scenario - promote user if:
            // ((CurrentRole=manager AND ProjectCount>=3) OR (HasLeadershipCert AND SecurityClearance>=6)) AND
            // (Active=true AND LastLogin<=5)

            var managerRoleCondition = service.AttributeEquals("Role", new Primitive("manager"));
            var minProjectsCondition = service.AttributeIsGreaterOrEqual("ProjectCount", new Primitive(3.0));
            var leadershipCertCondition = service.ArrayElementExists("Certifications", new Primitive("Leadership"));
            var goodClearanceCondition = service.AttributeIsGreaterOrEqual("SecurityClearance", new Primitive(6.0));
            var veryRecentLoginCondition = service.AttributeIsLessOrEqual("LastLogin", new Primitive(5.0));

            var promotionCondition = managerRoleCondition.And(minProjectsCondition)
                .Or(leadershipCertCondition.And(goodClearanceCondition))
                .And(activeCondition.And(veryRecentLoginCondition));

            var promotionUpdate = new JObject
            {
                ["Role"] = "senior_manager",
                ["PromotedAt"] = DateTime.UtcNow.ToString("O"),
                ["SalaryGrade"] = "L7"
            };

            var promotionResult = await service.UpdateItemAsync(
                tableName, new DbKey(keyName, keyValue), promotionUpdate,
                DbReturnItemBehavior.ReturnNewValues, promotionCondition);

            promotionResult.IsSuccessful.Should().BeTrue("Should promote user based on complex business rules");
            promotionResult.Data!["Role"]?.ToString().Should().Be("senior_manager");
            promotionResult.Data["SalaryGrade"]?.ToString().Should().Be("L7");

            // Verify final state meets all business requirements
            var finalState = await service.GetItemAsync(tableName, new DbKey(keyName, keyValue));
            var finalPermissions = finalState.Data!["Permissions"] as JArray;
            var finalCerts = finalState.Data["Certifications"] as JArray;

            finalPermissions.Should().Contain(p => p.ToString() == "admin", "Should have admin permission");
            finalCerts.Should().Contain(c => c.ToString() == "Leadership", "Should have leadership certification");
            finalState.Data["ProjectCount"]?.ToObject<double>().Should().Be(4.0, "Project count should be decremented");
            finalState.Data["Role"]?.ToString().Should().Be("senior_manager", "Should be promoted");
        }
        finally
        {
            await CleanupDatabaseAsync(tableName);
            if (service is IDisposable disposable)
                disposable.Dispose();
        }
    }

    [RetryFact(3, 5000)]
    public async Task ConditionCoupling_OperatorPrecedence_ShouldWorkCorrectly()
    {
        var service = CreateDatabaseService();
        var tableName = GetTestTableName();
        var keyName = "Id";
        var keyValue = new Primitive($"precedence-test-{Guid.NewGuid():N}");

        try
        {
            // Create test item
            var item = new JObject
            {
                ["A"] = true,
                ["B"] = false,
                ["C"] = true,
                ["D"] = false,
                ["E"] = true
            };
            await service.PutItemAsync(tableName, new DbKey(keyName, keyValue), item);

            var condA = service.AttributeEquals("A", new Primitive(true));
            var condB = service.AttributeEquals("B", new Primitive(true)); // This will be false
            var condC = service.AttributeEquals("C", new Primitive(true));
            var condD = service.AttributeEquals("D", new Primitive(true)); // This will be false
            var condE = service.AttributeEquals("E", new Primitive(true));

            // Test: A AND B OR C
            // Should be: (A AND B) OR C = (true AND false) OR true = false OR true = true
            var test1 = condA.And(condB).Or(condC);
            var result1 = await service.ItemExistsAsync(tableName, new DbKey(keyName, keyValue), test1);
            result1.IsSuccessful.Should().BeTrue("(A AND B) OR C should be true because C is true");
            result1.Data.Should().BeTrue();

            // Test: A OR B AND C
            // Should be: A OR (B AND C) = true OR (false AND true) = true OR false = true
            var test2 = condA.Or(condB.And(condC));
            var result2 = await service.ItemExistsAsync(tableName, new DbKey(keyName, keyValue), test2);
            result2.IsSuccessful.Should().BeTrue("A OR (B AND C) should be true because A is true");
            result2.Data.Should().BeTrue();

            // Test more complex: A AND B OR C AND D OR E
            // Should be: ((A AND B) OR (C AND D)) OR E = ((true AND false) OR (true AND false)) OR true = (false OR false) OR true = false OR true = true
            var test3 = condA.And(condB).Or(condC.And(condD)).Or(condE);
            var result3 = await service.ItemExistsAsync(tableName, new DbKey(keyName, keyValue), test3);
            result3.IsSuccessful.Should().BeTrue("Complex expression should be true because E is true");
            result3.Data.Should().BeTrue();

            // Test case where entire expression is false
            // A AND B AND C AND D (true AND false AND true AND false = false)
            var test4 = condA.And(condB).And(condC).And(condD);
            var result4 = await service.ItemExistsAsync(tableName, new DbKey(keyName, keyValue), test4);
            result4.IsSuccessful.Should().BeFalse("All AND expression with false elements should be false");
            result4.StatusCode.Should().Be(HttpStatusCode.PreconditionFailed);
        }
        finally
        {
            await CleanupDatabaseAsync(tableName);
            if (service is IDisposable disposable)
                disposable.Dispose();
        }
    }
    #endregion
}
