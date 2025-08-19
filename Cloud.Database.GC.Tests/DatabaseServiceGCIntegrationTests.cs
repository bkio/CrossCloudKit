// Copyright (c) 2022- Burak Kara, MIT License
// See LICENSE file in the project root for full license information.

using Cloud.Database.GC;
using Cloud.Interfaces;
using FluentAssertions;
using Xunit;

namespace Cloud.Database.GC.Tests;

/// <summary>
/// Basic tests for DatabaseServiceGC - These tests focus on interface compliance
/// Full integration tests would require Google Cloud Datastore emulator setup
/// </summary>
public class DatabaseServiceGCBasicTests
{
    private const string TestProjectId = "test-project";

    [Fact]
    public void DatabaseServiceGC_WithValidProjectId_ShouldCreateInstance()
    {
        // Arrange & Act
        DatabaseServiceGC service = new(TestProjectId);

        // Assert
        service.Should().NotBeNull();

        // Cleanup
        if (service is IAsyncDisposable asyncDisposable)
        {
            _ = Task.Run(async () => await asyncDisposable.DisposeAsync());
        }
    }

    [Fact]
    public void DatabaseServiceGC_WithNullProjectId_ShouldThrowArgumentException()
    {
        // Arrange, Act & Assert
        Assert.Throws<ArgumentException>(() => new DatabaseServiceGC(null!));
        Assert.Throws<ArgumentException>(() => new DatabaseServiceGC(""));
        Assert.Throws<ArgumentException>(() => new DatabaseServiceGC("   "));
    }

    [Fact]
    public void DatabaseOptions_ShouldBeConfigurable()
    {
        // Arrange
        var service = new DatabaseServiceGC(TestProjectId);

        try
        {
            var options = new DatabaseOptions
            {
                AutoSortArrays = AutoSortArrays.Yes,
                AutoConvertRoundableFloatToInt = AutoConvertRoundableFloatToInt.Yes
            };

            // Act & Assert - Should not throw
            service.SetOptions(options);
        }
        finally
        {
            if (service is IAsyncDisposable asyncDisposable)
            {
                _ = Task.Run(async () => await asyncDisposable.DisposeAsync());
            }
        }
    }

    [Fact]
    public void ConditionBuilders_ShouldCreateCorrectConditions()
    {
        // Arrange
        var service = new DatabaseServiceGC(TestProjectId);

        try
        {
            // Act & Assert
            var existsCondition = service.BuildAttributeExistsCondition("TestAttribute");
            existsCondition.Should().BeOfType<ExistenceCondition>();
            existsCondition.ConditionType.Should().Be(DatabaseAttributeConditionType.AttributeExists);

            var equalsCondition = service.BuildAttributeEqualsCondition("Name", new Utilities.Common.PrimitiveType("test"));
            equalsCondition.Should().BeOfType<ValueCondition>();
            equalsCondition.ConditionType.Should().Be(DatabaseAttributeConditionType.AttributeEquals);

            var arrayCondition = service.BuildArrayElementExistsCondition("Tags", new Utilities.Common.PrimitiveType("tag1"));
            arrayCondition.Should().BeOfType<ArrayElementCondition>();
            arrayCondition.ConditionType.Should().Be(DatabaseAttributeConditionType.ArrayElementExists);
        }
        finally
        {
            if (service is IAsyncDisposable asyncDisposable)
            {
                _ = Task.Run(async () => await asyncDisposable.DisposeAsync());
            }
        }
    }
}
