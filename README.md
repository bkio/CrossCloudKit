# CrossCloudKit

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![.NET 10](https://img.shields.io/badge/.NET-10-blue.svg)](https://dotnet.microsoft.com/download)

CrossCloudKit is a comprehensive .NET library that provides unified interfaces and implementations for working with multiple cloud database services. It enables developers to write cloud-agnostic database code that can seamlessly work across AWS DynamoDB, MongoDB, and Google Cloud Datastore with consistent APIs and behavior.

## 🚀 Features

- **Unified Database Interface**: Single `IDatabaseService` interface for all cloud database providers
- **Multi-Cloud Support**: 
  - **AWS DynamoDB** - Full NoSQL database support with automatic table creation
  - **MongoDB** - Complete MongoDB integration with native BSON support
  - **Google Cloud Datastore** - Native Google Cloud integration
- **Type-Safe Operations**: Strongly-typed primitive operations with `PrimitiveType` system
- **Modern Async/Await**: Full asynchronous API with cancellation token support
- **Advanced Querying**: Rich condition system with support for:
  - Attribute existence checks
  - Value comparisons (equals, greater than, less than, etc.)
  - Array element operations
- **Atomic Operations**: 
  - Atomic increment/decrement operations
  - Array element addition/removal
  - Conditional updates and deletes
- **Comprehensive Testing**: Extensive integration test suites for all providers
- **.NET 10 Ready**: Built for the latest .NET platform with nullable reference types

## 📦 Packages

| Package | Description |
|---------|-------------|
| `Cloud.Interfaces` | Core interfaces and base classes |
| `Cloud.Database.AWS` | AWS DynamoDB implementation |
| `Cloud.Database.Mongo` | MongoDB implementation |
| `Cloud.Database.GC` | Google Cloud Datastore implementation |
| `Utilities.Common` | Common utilities and primitive types |

## 🛠️ Installation

```bash
# Choose your cloud provider package(s)
dotnet add package Cloud.Database.AWS
dotnet add package Cloud.Database.Mongo  
dotnet add package Cloud.Database.GC

# Core interfaces (automatically included as dependency)
dotnet add package Cloud.Interfaces
```

## 🏗️ Quick Start

### AWS DynamoDB

```csharp
using Cloud.Database.AWS;
using Cloud.Interfaces;
using Utilities.Common;
using Newtonsoft.Json.Linq;

// Initialize service
var dbService = new DatabaseServiceAWS(
    accessKey: "your-access-key",
    secretKey: "your-secret-key", 
    region: "us-east-1"
);

// Create and store an item
var item = new JObject
{
    ["Name"] = "John Doe",
    ["Email"] = "john@example.com",
    ["Age"] = 30
};

var keyValue = new PrimitiveType("user-123");
await dbService.PutItemAsync("Users", "Id", keyValue, item);

// Retrieve the item
var result = await dbService.GetItemAsync("Users", "Id", keyValue);
if (result.IsSuccessful && result.Data != null)
{
    Console.WriteLine($"User: {result.Data["Name"]}");
}
```

### MongoDB

```csharp
using Cloud.Database.Mongo;

// Initialize with connection string
var dbService = new DatabaseServiceMongoDB(
    connectionString: "mongodb://localhost:27017",
    databaseName: "myapp"
);

// Same API as AWS DynamoDB!
var keyValue = new PrimitiveType("user-456");
await dbService.PutItemAsync("Users", "Id", keyValue, item);
```

### Google Cloud Datastore

```csharp
using Cloud.Database.GC;

// Initialize with service account
var dbService = new DatabaseServiceGC(
    projectId: "my-gcp-project",
    serviceAccountPath: "path/to/service-account.json"
);

// Same unified API
await dbService.PutItemAsync("Users", "Id", keyValue, item);
```

## 🔧 Advanced Features

### Conditional Operations

```csharp
// Create conditions
var condition = dbService.BuildAttributeEqualsCondition("Status", new PrimitiveType("active"));

// Conditional update
var updateData = new JObject { ["LastLogin"] = DateTime.UtcNow };
var result = await dbService.UpdateItemAsync(
    "Users", "Id", keyValue, updateData,
    condition: condition
);

// Check existence with conditions
var exists = await dbService.ItemExistsAsync("Users", "Id", keyValue, condition);
```

### Array Operations

```csharp
// Add elements to array
var elementsToAdd = new[]
{
    new PrimitiveType("admin"),
    new PrimitiveType("editor")
};

await dbService.AddElementsToArrayAsync(
    "Users", "Id", keyValue, "Roles", elementsToAdd
);

// Remove elements from array
var elementsToRemove = new[] { new PrimitiveType("editor") };
await dbService.RemoveElementsFromArrayAsync(
    "Users", "Id", keyValue, "Roles", elementsToRemove
);
```

### Atomic Increment

```csharp
// Atomically increment a counter
var newValue = await dbService.IncrementAttributeAsync(
    "Users", "Id", keyValue, "LoginCount", incrementValue: 1
);

if (newValue.IsSuccessful)
{
    Console.WriteLine($"New login count: {newValue.Data}");
}
```

### Scanning and Filtering

```csharp
// Scan all items
var allUsers = await dbService.ScanTableAsync("Users", new[] { "Id" });

// Scan with filter
var activeUsersFilter = dbService.BuildAttributeEqualsCondition("Status", new PrimitiveType("active"));
var activeUsers = await dbService.ScanTableWithFilterAsync("Users", new[] { "Id" }, activeUsersFilter);

// Paginated scan
var (items, nextToken, totalCount) = await dbService.ScanTablePaginatedAsync(
    "Users", new[] { "Id" }, pageSize: 10
);
```

## 📊 Supported Data Types

CrossCloudKit uses a unified `PrimitiveType` system that seamlessly maps across all cloud providers:

```csharp
// String values
var stringKey = new PrimitiveType("hello-world");

// Numeric values  
var integerKey = new PrimitiveType(12345L);
var doubleKey = new PrimitiveType(123.45);

// Binary data
var binaryKey = new PrimitiveType(new byte[] { 1, 2, 3, 4 });
```

## 🧪 Testing

The library includes comprehensive integration tests for all providers:

```bash
# Run all tests
dotnet test

# Run tests for specific provider
dotnet test Cloud.Database.AWS.Tests
dotnet test Cloud.Database.Mongo.Tests  
dotnet test Cloud.Database.GC.Tests
```

### Test Configuration

Tests support environment variables for real cloud service integration:

**AWS DynamoDB:**
```bash
AWS_ACCESS_KEY=your-key
AWS_SECRET_KEY=your-secret
AWS_REGION=us-east-1
```

**MongoDB:**
```bash
MONGODB_HOST=your-host
MONGODB_USER=your-user
MONGODB_PASSWORD=your-password
```

**Google Cloud:**
```bash
GOOGLE_CLOUD_PROJECT=your-project-id
GOOGLE_BASE64_CREDENTIALS=your-base64-encoded-service-account
```

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    Application Layer                        │
├─────────────────────────────────────────────────────────────┤
│                  Cloud.Interfaces                          │
│                 (IDatabaseService)                          │
├─────────────────┬─────────────────┬─────────────────────────┤
│ Cloud.Database. │ Cloud.Database. │ Cloud.Database.GC       │
│      AWS        │     Mongo       │                         │
│  (DynamoDB)     │   (MongoDB)     │ (Cloud Datastore)       │
├─────────────────┴─────────────────┴─────────────────────────┤
│                 Utilities.Common                            │
│              (PrimitiveType, etc.)                          │
└─────────────────────────────────────────────────────────────┘
```

## 🔒 Database-Specific Considerations

### AWS DynamoDB
- Automatic table creation with string partition keys for maximum flexibility
- Expression attribute names used to handle reserved keywords
- Proper type mapping for conditions (string keys, numeric condition values)
- Support for both managed AWS service and local DynamoDB

### MongoDB  
- Native BSON support with automatic ObjectId handling
- Flexible schema with dynamic collection creation
- Base64 encoding for binary keys to ensure consistency
- Support for MongoDB connection strings and advanced configurations

### Google Cloud Datastore
- Native entity and key support
- Automatic project and namespace handling  
- Support for service account authentication and Application Default Credentials
- Efficient batch operations

## 🤝 Contributing

Contributions are welcome! Please read our contributing guidelines and ensure all tests pass:

1. Fork the repository
2. Create a feature branch
3. Write tests for your changes
4. Ensure all tests pass: `dotnet test`
5. Submit a pull request

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 👥 Authors

- **Burak Kara** - Initial work and architecture

## 🙏 Acknowledgments

- Built for .NET 10 with modern C# features
- Extensive use of nullable reference types for safer code
- Comprehensive async/await support throughout
- Inspired by the need for truly cloud-agnostic database access

---

**CrossCloudKit** - Write once, run anywhere in the cloud. 🌐
