# CrossCloudKit Database Service Test Suite

This comprehensive test suite covers all database service implementations in the CrossCloudKit project, ensuring that all functionality of the `IDatabaseService` interface is properly tested across AWS DynamoDB, MongoDB, and Google Cloud Datastore implementations.

## 🧪 Test Projects Overview

### Cloud.Database.Tests.Common
**Shared test infrastructure and base test classes**
- **Purpose**: Provides common test utilities and base test classes that can be shared across all database service implementations
- **Key Components**:
  - `DatabaseServiceTestBase`: Abstract base class with comprehensive test cases covering all `IDatabaseService` methods
  - `DatabaseInterfaceUnitTests`: Unit tests for interface classes, condition builders, and database result types
  - Common test data helpers and utilities

### Cloud.Database.AWS.Tests
**AWS DynamoDB integration tests using Testcontainers**
- **Purpose**: Tests `DatabaseServiceAWS` implementation against DynamoDB Local
- **Infrastructure**: Uses Testcontainers to spin up DynamoDB Local container for isolated testing
- **Key Features**:
  - Tests modern TableBuilder implementation
  - Complex schema testing with GSI support
  - Performance tests with concurrent operations
  - AWS-specific features and error scenarios

### Cloud.Database.Mongo.Tests
**MongoDB integration tests using Testcontainers**
- **Purpose**: Tests `DatabaseServiceMongoDB` implementation against real MongoDB instance
- **Infrastructure**: Uses Testcontainers MongoDB container
- **Key Features**:
  - MongoDB-specific document operations
  - Complex nested document testing
  - Geospatial data handling
  - Large document and bulk operation testing
  - Text search and filtering capabilities

### Cloud.Database.GC.Tests
**Google Cloud Datastore integration tests using emulator**
- **Purpose**: Tests `DatabaseServiceGC` implementation against Datastore emulator
- **Infrastructure**: Uses Google Cloud Datastore emulator
- **Key Features**:
  - Datastore entity and key management
  - Transactional operations
  - Native Datastore data types
  - Retry logic and error handling
  - Binary data and complex hierarchies

## 🚀 Test Coverage

### Core CRUD Operations
- ✅ **Create**: `PutItemAsync` with various scenarios (new items, overwrites, return behaviors)
- ✅ **Read**: `GetItemAsync`, `GetItemsAsync` with attribute filtering and batch operations  
- ✅ **Update**: `UpdateItemAsync` with conditional updates and return behaviors
- ✅ **Delete**: `DeleteItemAsync` with conditional deletes and return behaviors

### Advanced Operations
- ✅ **Item Existence**: `ItemExistsAsync` with optional conditions
- ✅ **Array Operations**: `AddElementsToArrayAsync`, `RemoveElementsFromArrayAsync`
- ✅ **Numeric Operations**: `IncrementAttributeAsync` for counters and numeric fields
- ✅ **Scanning**: `ScanTableAsync`, `ScanTablePaginatedAsync` with filtering support

### Condition System
- ✅ **Existence Conditions**: `AttributeExists`, `AttributeNotExists`
- ✅ **Value Conditions**: `AttributeEquals`, `AttributeNotEquals`, `AttributeGreater`, etc.
- ✅ **Array Conditions**: `ArrayElementExists`, `ArrayElementNotExists`
- ✅ **Conditional Operations**: Updates and deletes with condition validation

### Data Types & Edge Cases
- ✅ **Primitive Types**: String, Integer, Double, ByteArray support
- ✅ **Complex Objects**: Nested JSON structures, arrays, null values
- ✅ **Unicode Support**: International characters and emojis
- ✅ **Large Data**: Testing within database-specific size limits
- ✅ **Edge Cases**: Empty values, special characters, extreme numbers

### Error Handling
- ✅ **Invalid Input**: Null parameters, empty arrays, invalid table names
- ✅ **Conditional Failures**: Failed condition checks, optimistic locking
- ✅ **Resource Issues**: Non-existent tables, network failures
- ✅ **Concurrent Operations**: Race conditions and retry logic

### Performance & Scale
- ✅ **Batch Operations**: Bulk inserts, batch retrieval
- ✅ **Pagination**: Large dataset scanning with page tokens
- ✅ **Concurrent Access**: Multi-threaded operations
- ✅ **Large Items**: Testing database size limits

## 🛠️ Running the Tests

### Prerequisites
- .NET 10 SDK
- Docker (for Testcontainers)
- Google Cloud SDK (optional, for Datastore emulator)

### Running All Tests
```bash
# Run all database service tests
dotnet test

# Run tests for specific database service
dotnet test Cloud.Database.AWS.Tests
dotnet test Cloud.Database.Mongo.Tests  
dotnet test Cloud.Database.GC.Tests

# Run with verbose output
dotnet test --verbosity normal

# Generate coverage report
dotnet test --collect:"XPlat Code Coverage"
```

### Environment Setup

#### AWS Tests
- Uses Testcontainers to automatically start DynamoDB Local
- No AWS credentials required
- Automatically provisions and tears down test infrastructure

#### MongoDB Tests  
- Uses Testcontainers to automatically start MongoDB container
- No MongoDB installation required
- Automatically handles database cleanup between tests

#### Google Cloud Tests
- Requires Google Cloud SDK for Datastore emulator
- Tests will be skipped if emulator cannot start
- Alternative: Uses in-memory mock for unit testing

## 📊 Test Structure

### Base Test Class Pattern
```csharp
public abstract class DatabaseServiceTestBase
{
    protected abstract IDatabaseService CreateDatabaseService();
    protected abstract Task CleanupDatabaseAsync(string tableName);
    protected virtual string GetTestTableName() => $"test-table-{Guid.NewGuid():N}";
    
    // Common test methods covering all IDatabaseService functionality
}
```

### Implementation-Specific Tests
Each implementation extends the base class and adds provider-specific tests:
```csharp
public class DatabaseServiceAWSIntegrationTests : DatabaseServiceTestBase
{
    // AWS-specific tests (TableBuilder, GSI, DynamoDB features)
}
```

## 🎯 Test Categories

### Unit Tests
- **Scope**: Individual classes, methods, and components in isolation
- **Focus**: Interface contracts, condition builders, data structures
- **Speed**: Fast execution, no external dependencies

### Integration Tests  
- **Scope**: Full database service implementations against real database engines
- **Focus**: End-to-end functionality, database-specific behaviors
- **Speed**: Slower execution, requires external services

### Performance Tests
- **Scope**: Concurrent operations, large datasets, bulk operations
- **Focus**: Scalability, throughput, resource utilization
- **Speed**: Variable based on test scope

## 🔧 Configuration

### Test Settings
```json
{
  "TestSettings": {
    "TimeoutMinutes": 10,
    "MaxConcurrentOperations": 50,
    "LargeDatasetSize": 1000,
    "BulkOperationSize": 100
  }
}
```

### Database-Specific Configuration
- **DynamoDB**: Uses local endpoint, configurable table schemas
- **MongoDB**: Uses test database, automatic collection cleanup
- **Datastore**: Uses emulator with test project ID

## 🚨 Troubleshooting

### Common Issues

#### Docker/Testcontainers Issues
```bash
# Ensure Docker is running
docker --version
docker ps

# Clean up containers if needed
docker system prune
```

#### Datastore Emulator Issues  
```bash
# Install Google Cloud SDK
gcloud components install cloud-datastore-emulator

# Verify emulator can start
gcloud beta emulators datastore start --no-store-on-disk
```

#### Permission Issues
- Ensure Docker daemon has proper permissions
- Check that test user can create temporary directories
- Verify network access for container downloads

## 📈 Coverage Goals

- **Line Coverage**: > 95% for all database service implementations
- **Branch Coverage**: > 90% for conditional logic and error paths  
- **Method Coverage**: 100% for all `IDatabaseService` interface methods
- **Scenario Coverage**: All major use cases and edge cases covered

## 🔄 Continuous Integration

### Test Pipeline
1. **Unit Tests**: Fast feedback on code changes
2. **Integration Tests**: Validate against real database engines
3. **Performance Tests**: Ensure scalability requirements
4. **Coverage Analysis**: Maintain quality standards

### Test Data Management
- **Isolation**: Each test uses unique table/collection names
- **Cleanup**: Automatic cleanup after each test
- **Deterministic**: Tests are repeatable and independent

## 🎭 Mock vs Real Testing

### When to Use Mocks
- Unit testing internal logic
- Testing error conditions that are hard to reproduce
- Fast feedback during development

### When to Use Real Services  
- Integration testing end-to-end flows
- Validating database-specific behaviors
- Performance and scalability testing

This comprehensive test suite ensures that all database service implementations maintain consistent behavior while allowing for provider-specific optimizations and features.
