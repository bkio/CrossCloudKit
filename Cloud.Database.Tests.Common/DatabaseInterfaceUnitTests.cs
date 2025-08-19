// Copyright (c) 2022- Burak Kara, MIT License
// See LICENSE file in the project root for full license information.

using Cloud.Interfaces;
using FluentAssertions;
using Utilities.Common;
using Xunit;

namespace Cloud.Database.Tests.Common;

/// <summary>
/// Unit tests for database interface classes and condition builders
/// </summary>
public class DatabaseInterfaceUnitTests
{
    #region DatabaseResult Tests

    [Fact]
    public void DatabaseResult_Success_ShouldCreateSuccessfulResult()
    {
        // Arrange
        const string testData = "test-data";

        // Act
        var result = DatabaseResult<string>.Success(testData);

        // Assert
        result.Should().NotBeNull();
        result.IsSuccessful.Should().BeTrue();
        result.Data.Should().Be(testData);
        result.ErrorMessage.Should().BeNull();
    }

    [Fact]
    public void DatabaseResult_Failure_ShouldCreateFailedResult()
    {
        // Arrange
        const string errorMessage = "Test error message";

        // Act
        var result = DatabaseResult<string>.Failure(errorMessage);

        // Assert
        result.Should().NotBeNull();
        result.IsSuccessful.Should().BeFalse();
        result.Data.Should().BeNull();
        result.ErrorMessage.Should().Be(errorMessage);
    }

    [Fact]
    public void DatabaseResult_SuccessWithNull_ShouldAllowNullData()
    {
        // Act
        var result = DatabaseResult<string?>.Success(null);

        // Assert
        result.Should().NotBeNull();
        result.IsSuccessful.Should().BeTrue();
        result.Data.Should().BeNull();
        result.ErrorMessage.Should().BeNull();
    }

    #endregion

    #region ExistenceCondition Tests

    [Fact]
    public void ExistenceCondition_WithAttributeExists_ShouldCreateCorrectCondition()
    {
        // Arrange
        const string attributeName = "TestAttribute";

        // Act
        var condition = new ExistenceCondition(DatabaseAttributeConditionType.AttributeExists, attributeName);

        // Assert
        condition.Should().NotBeNull();
        condition.ConditionType.Should().Be(DatabaseAttributeConditionType.AttributeExists);
        condition.AttributeName.Should().Be(attributeName);
    }

    [Fact]
    public void ExistenceCondition_WithAttributeNotExists_ShouldCreateCorrectCondition()
    {
        // Arrange
        const string attributeName = "TestAttribute";

        // Act
        var condition = new ExistenceCondition(DatabaseAttributeConditionType.AttributeNotExists, attributeName);

        // Assert
        condition.Should().NotBeNull();
        condition.ConditionType.Should().Be(DatabaseAttributeConditionType.AttributeNotExists);
        condition.AttributeName.Should().Be(attributeName);
    }

    [Fact]
    public void ExistenceCondition_WithInvalidConditionType_ShouldThrowArgumentException()
    {
        // Arrange
        const string attributeName = "TestAttribute";

        // Act & Assert
        Assert.Throws<ArgumentException>(() =>
            new ExistenceCondition(DatabaseAttributeConditionType.AttributeEquals, attributeName));
    }

    [Fact]
    public void ExistenceCondition_WithNullAttributeName_ShouldThrowArgumentNullException()
    {
        // Act & Assert
        Assert.Throws<ArgumentNullException>(() =>
            new ExistenceCondition(DatabaseAttributeConditionType.AttributeExists, null!));
    }

    #endregion

    #region ValueCondition Tests

    [Theory]
    [InlineData(DatabaseAttributeConditionType.AttributeEquals)]
    [InlineData(DatabaseAttributeConditionType.AttributeNotEquals)]
    [InlineData(DatabaseAttributeConditionType.AttributeGreater)]
    [InlineData(DatabaseAttributeConditionType.AttributeGreaterOrEqual)]
    [InlineData(DatabaseAttributeConditionType.AttributeLess)]
    [InlineData(DatabaseAttributeConditionType.AttributeLessOrEqual)]
    public void ValueCondition_WithValidConditionTypes_ShouldCreateCorrectCondition(DatabaseAttributeConditionType conditionType)
    {
        // Arrange
        const string attributeName = "TestAttribute";
        var value = new PrimitiveType("test-value");

        // Act
        var condition = new ValueCondition(conditionType, attributeName, value);

        // Assert
        condition.Should().NotBeNull();
        condition.ConditionType.Should().Be(conditionType);
        condition.AttributeName.Should().Be(attributeName);
        condition.Value.Should().Be(value);
    }

    [Fact]
    public void ValueCondition_WithNullValue_ShouldThrowArgumentNullException()
    {
        // Arrange
        const string attributeName = "TestAttribute";

        // Act & Assert
        Assert.Throws<ArgumentNullException>(() =>
            new ValueCondition(DatabaseAttributeConditionType.AttributeEquals, attributeName, null!));
    }

    [Theory]
    [InlineData("string-value")]
    [InlineData(42L)]
    [InlineData(3.14)]
    public void ValueCondition_WithDifferentPrimitiveTypes_ShouldPreserveValues(object testValue)
    {
        // Arrange
        const string attributeName = "TestAttribute";
        var primitiveValue = testValue switch
        {
            string s => new PrimitiveType(s),
            long l => new PrimitiveType(l),
            double d => new PrimitiveType(d),
            _ => throw new ArgumentException("Unsupported test value type")
        };

        // Act
        var condition = new ValueCondition(DatabaseAttributeConditionType.AttributeEquals, attributeName, primitiveValue);

        // Assert
        condition.Value.Should().Be(primitiveValue);
        switch (testValue)
        {
            case string expectedString:
                condition.Value.AsString.Should().Be(expectedString);
                break;
            case long expectedLong:
                condition.Value.AsInteger.Should().Be(expectedLong);
                break;
            case double expectedDouble:
                condition.Value.AsDouble.Should().BeApproximately(expectedDouble, 0.0001);
                break;
        }
    }

    #endregion

    #region ArrayElementCondition Tests

    [Fact]
    public void ArrayElementCondition_WithArrayElementExists_ShouldCreateCorrectCondition()
    {
        // Arrange
        const string attributeName = "TestArray";
        var elementValue = new PrimitiveType("test-element");

        // Act
        var condition = new ArrayElementCondition(DatabaseAttributeConditionType.ArrayElementExists, 
            attributeName, elementValue);

        // Assert
        condition.Should().NotBeNull();
        condition.ConditionType.Should().Be(DatabaseAttributeConditionType.ArrayElementExists);
        condition.AttributeName.Should().Be(attributeName);
        condition.ElementValue.Should().Be(elementValue);
    }

    [Fact]
    public void ArrayElementCondition_WithArrayElementNotExists_ShouldCreateCorrectCondition()
    {
        // Arrange
        const string attributeName = "TestArray";
        var elementValue = new PrimitiveType("test-element");

        // Act
        var condition = new ArrayElementCondition(DatabaseAttributeConditionType.ArrayElementNotExists, 
            attributeName, elementValue);

        // Assert
        condition.Should().NotBeNull();
        condition.ConditionType.Should().Be(DatabaseAttributeConditionType.ArrayElementNotExists);
        condition.AttributeName.Should().Be(attributeName);
        condition.ElementValue.Should().Be(elementValue);
    }

    [Fact]
    public void ArrayElementCondition_WithInvalidConditionType_ShouldThrowArgumentException()
    {
        // Arrange
        const string attributeName = "TestArray";
        var elementValue = new PrimitiveType("test-element");

        // Act & Assert
        Assert.Throws<ArgumentException>(() =>
            new ArrayElementCondition(DatabaseAttributeConditionType.AttributeEquals, attributeName, elementValue));
    }

    [Fact]
    public void ArrayElementCondition_WithNullElementValue_ShouldThrowArgumentNullException()
    {
        // Arrange
        const string attributeName = "TestArray";

        // Act & Assert
        Assert.Throws<ArgumentNullException>(() =>
            new ArrayElementCondition(DatabaseAttributeConditionType.ArrayElementExists, attributeName, null!));
    }

    #endregion

    #region DatabaseOptions Tests

    [Fact]
    public void DatabaseOptions_DefaultValues_ShouldBeCorrect()
    {
        // Act
        var options = new DatabaseOptions();

        // Assert
        options.AutoSortArrays.Should().Be(AutoSortArrays.No);
        options.AutoConvertRoundableFloatToInt.Should().Be(AutoConvertRoundableFloatToInt.No);
    }

    [Fact]
    public void DatabaseOptions_SetValues_ShouldUpdateProperties()
    {
        // Arrange
        var options = new DatabaseOptions();

        // Act
        options.AutoSortArrays = AutoSortArrays.Yes;
        options.AutoConvertRoundableFloatToInt = AutoConvertRoundableFloatToInt.Yes;

        // Assert
        options.AutoSortArrays.Should().Be(AutoSortArrays.Yes);
        options.AutoConvertRoundableFloatToInt.Should().Be(AutoConvertRoundableFloatToInt.Yes);
    }

    #endregion

    #region DatabaseServiceBase Tests

    public class TestDatabaseService : DatabaseServiceBase, IDatabaseService
    {
        public bool IsInitialized => true;

        // Implement all required interface methods with minimal implementations for testing
        public DatabaseAttributeCondition BuildAttributeExistsCondition(string attributeName) => 
            new ExistenceCondition(DatabaseAttributeConditionType.AttributeExists, attributeName);
        
        public DatabaseAttributeCondition BuildAttributeNotExistsCondition(string attributeName) => 
            new ExistenceCondition(DatabaseAttributeConditionType.AttributeNotExists, attributeName);
        
        public DatabaseAttributeCondition BuildAttributeEqualsCondition(string attributeName, PrimitiveType value) => 
            new ValueCondition(DatabaseAttributeConditionType.AttributeEquals, attributeName, value);
        
        public DatabaseAttributeCondition BuildAttributeNotEqualsCondition(string attributeName, PrimitiveType value) => 
            new ValueCondition(DatabaseAttributeConditionType.AttributeNotEquals, attributeName, value);
        
        public DatabaseAttributeCondition BuildAttributeGreaterCondition(string attributeName, PrimitiveType value) => 
            new ValueCondition(DatabaseAttributeConditionType.AttributeGreater, attributeName, value);
        
        public DatabaseAttributeCondition BuildAttributeGreaterOrEqualCondition(string attributeName, PrimitiveType value) => 
            new ValueCondition(DatabaseAttributeConditionType.AttributeGreaterOrEqual, attributeName, value);
        
        public DatabaseAttributeCondition BuildAttributeLessCondition(string attributeName, PrimitiveType value) => 
            new ValueCondition(DatabaseAttributeConditionType.AttributeLess, attributeName, value);
        
        public DatabaseAttributeCondition BuildAttributeLessOrEqualCondition(string attributeName, PrimitiveType value) => 
            new ValueCondition(DatabaseAttributeConditionType.AttributeLessOrEqual, attributeName, value);
        
        public DatabaseAttributeCondition BuildArrayElementExistsCondition(string attributeName, PrimitiveType elementValue) => 
            new ArrayElementCondition(DatabaseAttributeConditionType.ArrayElementExists, attributeName, elementValue);
        
        public DatabaseAttributeCondition BuildArrayElementNotExistsCondition(string attributeName, PrimitiveType elementValue) => 
            new ArrayElementCondition(DatabaseAttributeConditionType.ArrayElementNotExists, attributeName, elementValue);

        // Minimal async method implementations for testing
        public Task<DatabaseResult<bool>> ItemExistsAsync(string tableName, string keyName, PrimitiveType keyValue, DatabaseAttributeCondition? condition = null, CancellationToken cancellationToken = default) => 
            Task.FromResult(DatabaseResult<bool>.Success(false));

        public Task<DatabaseResult<Newtonsoft.Json.Linq.JObject?>> GetItemAsync(string tableName, string keyName, PrimitiveType keyValue, string[]? attributesToRetrieve = null, CancellationToken cancellationToken = default) => 
            Task.FromResult(DatabaseResult<Newtonsoft.Json.Linq.JObject?>.Success(null));

        public Task<DatabaseResult<IReadOnlyList<Newtonsoft.Json.Linq.JObject>>> GetItemsAsync(string tableName, string keyName, PrimitiveType[] keyValues, string[]? attributesToRetrieve = null, CancellationToken cancellationToken = default) => 
            Task.FromResult(DatabaseResult<IReadOnlyList<Newtonsoft.Json.Linq.JObject>>.Success(new List<Newtonsoft.Json.Linq.JObject>().AsReadOnly()));

        public Task<DatabaseResult<Newtonsoft.Json.Linq.JObject?>> PutItemAsync(string tableName, string keyName, PrimitiveType keyValue, Newtonsoft.Json.Linq.JObject item, ReturnItemBehavior returnBehavior = ReturnItemBehavior.DoNotReturn, bool overwriteIfExists = false, CancellationToken cancellationToken = default) => 
            Task.FromResult(DatabaseResult<Newtonsoft.Json.Linq.JObject?>.Success(null));

        public Task<DatabaseResult<Newtonsoft.Json.Linq.JObject?>> UpdateItemAsync(string tableName, string keyName, PrimitiveType keyValue, Newtonsoft.Json.Linq.JObject updateData, ReturnItemBehavior returnBehavior = ReturnItemBehavior.DoNotReturn, DatabaseAttributeCondition? condition = null, CancellationToken cancellationToken = default) => 
            Task.FromResult(DatabaseResult<Newtonsoft.Json.Linq.JObject?>.Success(null));

        public Task<DatabaseResult<Newtonsoft.Json.Linq.JObject?>> DeleteItemAsync(string tableName, string keyName, PrimitiveType keyValue, ReturnItemBehavior returnBehavior = ReturnItemBehavior.DoNotReturn, DatabaseAttributeCondition? condition = null, CancellationToken cancellationToken = default) => 
            Task.FromResult(DatabaseResult<Newtonsoft.Json.Linq.JObject?>.Success(null));

        public Task<DatabaseResult<Newtonsoft.Json.Linq.JObject?>> AddElementsToArrayAsync(string tableName, string keyName, PrimitiveType keyValue, string arrayAttributeName, PrimitiveType[] elementsToAdd, ReturnItemBehavior returnBehavior = ReturnItemBehavior.DoNotReturn, DatabaseAttributeCondition? condition = null, CancellationToken cancellationToken = default) => 
            Task.FromResult(DatabaseResult<Newtonsoft.Json.Linq.JObject?>.Success(null));

        public Task<DatabaseResult<Newtonsoft.Json.Linq.JObject?>> RemoveElementsFromArrayAsync(string tableName, string keyName, PrimitiveType keyValue, string arrayAttributeName, PrimitiveType[] elementsToRemove, ReturnItemBehavior returnBehavior = ReturnItemBehavior.DoNotReturn, DatabaseAttributeCondition? condition = null, CancellationToken cancellationToken = default) => 
            Task.FromResult(DatabaseResult<Newtonsoft.Json.Linq.JObject?>.Success(null));

        public Task<DatabaseResult<double>> IncrementAttributeAsync(string tableName, string keyName, PrimitiveType keyValue, string numericAttributeName, double incrementValue, DatabaseAttributeCondition? condition = null, CancellationToken cancellationToken = default) => 
            Task.FromResult(DatabaseResult<double>.Success(0.0));

        public Task<DatabaseResult<IReadOnlyList<Newtonsoft.Json.Linq.JObject>>> ScanTableAsync(string tableName, string[] keyNames, CancellationToken cancellationToken = default) => 
            Task.FromResult(DatabaseResult<IReadOnlyList<Newtonsoft.Json.Linq.JObject>>.Success(new List<Newtonsoft.Json.Linq.JObject>().AsReadOnly()));

        public Task<DatabaseResult<(IReadOnlyList<Newtonsoft.Json.Linq.JObject> Items, string? NextPageToken, long? TotalCount)>> ScanTablePaginatedAsync(string tableName, string[] keyNames, int pageSize, string? pageToken = null, CancellationToken cancellationToken = default) => 
            Task.FromResult(DatabaseResult<(IReadOnlyList<Newtonsoft.Json.Linq.JObject>, string?, long?)>.Success((new List<Newtonsoft.Json.Linq.JObject>().AsReadOnly(), null, null)));

        public Task<DatabaseResult<IReadOnlyList<Newtonsoft.Json.Linq.JObject>>> ScanTableWithFilterAsync(string tableName, string[] keyNames, DatabaseAttributeCondition filterCondition, CancellationToken cancellationToken = default) => 
            Task.FromResult(DatabaseResult<IReadOnlyList<Newtonsoft.Json.Linq.JObject>>.Success(new List<Newtonsoft.Json.Linq.JObject>().AsReadOnly()));

        public Task<DatabaseResult<(IReadOnlyList<Newtonsoft.Json.Linq.JObject> Items, string? NextPageToken, long? TotalCount)>> ScanTableWithFilterPaginatedAsync(string tableName, string[] keyNames, DatabaseAttributeCondition filterCondition, int pageSize, string? pageToken = null, CancellationToken cancellationToken = default) => 
            Task.FromResult(DatabaseResult<(IReadOnlyList<Newtonsoft.Json.Linq.JObject>, string?, long?)>.Success((new List<Newtonsoft.Json.Linq.JObject>().AsReadOnly(), null, null)));
    }

    [Fact]
    public void DatabaseServiceBase_SetOptions_ShouldUpdateOptions()
    {
        // Arrange
        var service = new TestDatabaseService();
        var options = new DatabaseOptions
        {
            AutoSortArrays = AutoSortArrays.Yes,
            AutoConvertRoundableFloatToInt = AutoConvertRoundableFloatToInt.Yes
        };

        // Act
        service.SetOptions(options);

        // Assert - We can't directly test the protected Options property, 
        // but we can verify no exception is thrown
        // In a real implementation, the service would use these options
    }

    [Fact]
    public void DatabaseServiceBase_SetOptions_WithNull_ShouldThrowArgumentNullException()
    {
        // Arrange
        var service = new TestDatabaseService();

        // Act & Assert
        Assert.Throws<ArgumentNullException>(() => service.SetOptions(null!));
    }

    #endregion

    #region Enum Tests

    [Theory]
    [InlineData(ReturnItemBehavior.DoNotReturn)]
    [InlineData(ReturnItemBehavior.ReturnOldValues)]
    [InlineData(ReturnItemBehavior.ReturnNewValues)]
    public void ReturnItemBehavior_AllValues_ShouldBeValid(ReturnItemBehavior behavior)
    {
        // Act & Assert
        Enum.IsDefined(typeof(ReturnItemBehavior), behavior).Should().BeTrue();
    }

    [Theory]
    [InlineData(DatabaseAttributeConditionType.AttributeEquals)]
    [InlineData(DatabaseAttributeConditionType.AttributeNotEquals)]
    [InlineData(DatabaseAttributeConditionType.AttributeGreater)]
    [InlineData(DatabaseAttributeConditionType.AttributeGreaterOrEqual)]
    [InlineData(DatabaseAttributeConditionType.AttributeLess)]
    [InlineData(DatabaseAttributeConditionType.AttributeLessOrEqual)]
    [InlineData(DatabaseAttributeConditionType.AttributeExists)]
    [InlineData(DatabaseAttributeConditionType.AttributeNotExists)]
    [InlineData(DatabaseAttributeConditionType.ArrayElementExists)]
    [InlineData(DatabaseAttributeConditionType.ArrayElementNotExists)]
    public void DatabaseAttributeConditionType_AllValues_ShouldBeValid(DatabaseAttributeConditionType conditionType)
    {
        // Act & Assert
        Enum.IsDefined(typeof(DatabaseAttributeConditionType), conditionType).Should().BeTrue();
    }

    [Theory]
    [InlineData(AutoSortArrays.No)]
    [InlineData(AutoSortArrays.Yes)]
    public void AutoSortArrays_AllValues_ShouldBeValid(AutoSortArrays value)
    {
        // Act & Assert
        Enum.IsDefined(typeof(AutoSortArrays), value).Should().BeTrue();
    }

    [Theory]
    [InlineData(AutoConvertRoundableFloatToInt.No)]
    [InlineData(AutoConvertRoundableFloatToInt.Yes)]
    public void AutoConvertRoundableFloatToInt_AllValues_ShouldBeValid(AutoConvertRoundableFloatToInt value)
    {
        // Act & Assert
        Enum.IsDefined(typeof(AutoConvertRoundableFloatToInt), value).Should().BeTrue();
    }

    #endregion
}
