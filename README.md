# CrossCloudKit
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![.NET 10](https://img.shields.io/badge/.NET-10-blue.svg)](https://dotnet.microsoft.com/download)
![Tests](https://img.shields.io/badge/Tests-324%2F345%20failing-red)

CrossCloudKit is a comprehensive .NET library that provides unified interfaces and implementations for working with multiple cloud services. It enables developers to write cloud-agnostic code that can seamlessly work across AWS, Google Cloud, MongoDB, Redis, and S3-compatible storage providers with consistent APIs and behavior.

## 🧪 Test Results

**Last Updated:** $current_date

| Metric | Count |
|--------|-------|
| ✅ **Tests Passed** | **$PASSED_TESTS** |
| ❌ **Tests Failed** | **$FAILED_TESTS** |
| 📊 **Total Tests** | **$TOTAL_TESTS** |

## 🚀 Features

- **Unified Cloud Interfaces**: Single interfaces for all cloud service categories
- **Multi-Service Support**:
  - **Database Services**: AWS DynamoDB, MongoDB, Google Cloud Datastore
  - **File Storage Services**: AWS S3, Google Cloud Storage, S3-Compatible providers
  - **PubSub Messaging**: AWS SNS/SQS, Google Cloud Pub/Sub, Redis Pub/Sub
  - **Memory/Caching**: Redis with distributed locking and advanced data structures
- **Type-Safe Operations**: Strongly-typed primitive operations with `PrimitiveType` system- **Modern Async/Await**: Full asynchronous API with cancellation token support
- **Advanced Features**:
  - Database querying with rich condition system and atomic operations
  - File operations with signed URLs, metadata, notifications, and streaming
  - Message queuing with topic management, subscriptions, and error handling
  - Distributed memory operations with mutex locking and data structures
  - Pub/Sub integration with file services for file event notifications
- **Cloud-Agnostic Design**: Write once, deploy anywhere across cloud providers
- **Comprehensive Testing**: Extensive integration test suites for all services
- **.NET 10 Ready**: Built for the latest .NET platform with nullable reference types

## 📦 Packages
| Package | Description |
|---------|-------------|
| `CrossCloudKit.Interfaces` | Core interfaces and base classes |
| **Database Services** | |
| `CrossCloudKit.Database.AWS` | AWS DynamoDB implementation |
| `CrossCloudKit.Database.Mongo` | MongoDB implementation |
| `CrossCloudKit.Database.GC` | Google Cloud Datastore implementation |
| **File Storage Services** | |
| `CrossCloudKit.File.AWS` | AWS S3 file storage implementation |
| `CrossCloudKit.File.GC` | Google Cloud Storage implementation |
| `CrossCloudKit.File.S3Compatible` | S3-compatible storage providers |
| **PubSub Messaging Services** | |
| `CrossCloudKit.PubSub.AWS` | AWS SNS/SQS implementation |
| `CrossCloudKit.PubSub.GC` | Google Cloud Pub/Sub implementation |
| `CrossCloudKit.PubSub.Redis` | Redis Pub/Sub implementation |
| **Memory/Caching Services** | |
| `CrossCloudKit.Memory.Redis` | Redis memory and caching implementation |
| **Utilities** | |
| `CrossCloudKit.Utilities.Common` | Common utilities and primitive types |
| `CrossCloudKit.Utilities.Windows` | Windows-specific utilities |

## 🛠️ Installation
```bash
# Choose your cloud provider package(s)
# Database Services
dotnet add package CrossCloudKit.Database.AWS
dotnet add package CrossCloudKit.Database.Mongo
dotnet add package CrossCloudKit.Database.GC

# File Storage Services
dotnet add package CrossCloudKit.File.AWS
dotnet add package CrossCloudKit.File.GC
dotnet add package CrossCloudKit.File.S3Compatible

# PubSub Services
dotnet add package CrossCloudKit.PubSub.AWS
dotnet add package CrossCloudKit.PubSub.GC
dotnet add package CrossCloudKit.PubSub.Redis

# Memory Services
dotnet add package CrossCloudKit.Memory.Redis

# Core interfaces (automatically included as dependency)
dotnet add package CrossCloudKit.Interfaces
```
## 🏗️ Quick Start
### Database Services

#### AWS DynamoDB
```csharp
using CrossCloudKit.Database.AWS;
using CrossCloudKit.Interfaces;
using CrossCloudKit.Utilities.Common;
using Newtonsoft.Json.Linq;

// Initialize service
var dbService = new DatabaseServiceAWS(
    // Parameters here
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
#### MongoDB
```csharp
using CrossCloudKit.Database.Mongo;

// Initialize with connection string
var dbService = new DatabaseServiceMongoDB(
    // Parameters here
);

// Same API as AWS DynamoDB!
var keyValue = new PrimitiveType("user-456");
await dbService.PutItemAsync("Users", "Id", keyValue, item);
```
#### Google Cloud Datastore
```csharp
using CrossCloudKit.Database.GC;

// Initialize with service account
var dbService = new DatabaseServiceGC(
    // Parameters here
);

// Same unified API
await dbService.PutItemAsync("Users", "Id", keyValue, item);
```
### File Storage Services

#### AWS S3
```csharp
using CrossCloudKit.File.AWS;
using CrossCloudKit.Interfaces;

// Initialize service
var fileService = new FileServiceAWS(
    // Parameters here
);

// Upload a file
var content = StringOrStream.FromString("Hello, World!");
var uploadResult = await fileService.UploadFileAsync(
    content: content,
    bucketName: "my-bucket",
    keyInBucket: "files/hello.txt",
    accessibility: FileAccessibility.PublicRead
);

// Download a file
using var memoryStream = new MemoryStream();
var downloadResult = await fileService.DownloadFileAsync(
    bucketName: "my-bucket",
    keyInBucket: "files/hello.txt",
    destination: StringOrStream.FromStream(memoryStream)
);

// Create signed URLs
var signedUploadUrl = await fileService.CreateSignedUploadUrlAsync(
    bucketName: "my-bucket",
    keyInBucket: "uploads/new-file.txt",
    options: new SignedUploadUrlOptions
    {
        ValidFor = TimeSpan.FromHours(1),
        ContentType = "text/plain"
    }
);
```
#### Google Cloud Storage
```csharp
using CrossCloudKit.File.GC;

// Initialize service
var fileService = new FileServiceGC(
    // Parameters here
);

// Same unified API as AWS S3
await fileService.UploadFileAsync(content, "my-bucket", "files/hello.txt");
```
#### S3-Compatible Storage
```csharp
using CrossCloudKit.File.S3Compatible;

// Initialize for MinIO or other S3-compatible storage
var fileService = new FileServiceS3Compatible(
    // Parameters here
);

// Same API as AWS S3
await fileService.UploadFileAsync(content, "my-bucket", "files/hello.txt");
```
### Memory Services

#### Redis Memory Service
```csharp
using CrossCloudKit.Memory.Redis;
using CrossCloudKit.Interfaces;
using CrossCloudKit.Utilities.Common;

// Initialize service
var memoryService = new MemoryServiceRedis(
    // Parameters here
);

// Create a memory scope
var scope = new LambdaMemoryServiceScope(() => "user:123");

// Set key-value pairs
await memoryService.SetKeyValuesAsync(scope, new[]
{
    new KeyValuePair<string, PrimitiveType>("name", new PrimitiveType("John Doe")),
    new KeyValuePair<string, PrimitiveType>("age", new PrimitiveType(30L))
});

// Get values
var name = await memoryService.GetKeyValueAsync(scope, "name");
Console.WriteLine($"Name: {name.Data?.AsString}");

// Atomic increment
var newAge = await memoryService.IncrementKeyByValueAndGetAsync(scope, "age", 1);
Console.WriteLine($"New age: {newAge.Data}");

// Distributed mutex locking
using var mutex = await MemoryServiceScopeMutex.CreateScopeAsync(
    memoryService, scope, "user-lock", TimeSpan.FromMinutes(5)
);
await mutex.LockAsync();
// Critical section - only one process can execute this at a time
```
### PubSub Services

#### AWS SNS/SQS
```csharp
using CrossCloudKit.PubSub.AWS;
using CrossCloudKit.Interfaces;

// Initialize service
var pubSubService = new PubSubServiceAWS(
    // Parameters here
);

// Ensure topic exists
await pubSubService.EnsureTopicExistsAsync("user-events");

// Subscribe to messages
await pubSubService.SubscribeAsync(
    topic: "user-events",
    onMessage: async (topic, message) =>
    {
        Console.WriteLine($"Received from {topic}: {message}");
    },
    onError: error =>
    {
        Console.WriteLine($"Error: {error.Message}");
    }
);

// Publish a message
await pubSubService.PublishAsync("user-events", "User logged in");
```
#### Google Cloud Pub/Sub
```csharp
using CrossCloudKit.PubSub.GC;

// Initialize service
var pubSubService = new PubSubServiceGC(
    // Parameters here
);

// Same unified API as AWS
await pubSubService.EnsureTopicExistsAsync("user-events");
await pubSubService.PublishAsync("user-events", "User logged in");
```
#### Redis Pub/Sub
```csharp
using CrossCloudKit.PubSub.Redis;

// Initialize service
var pubSubService = new PubSubServiceRedis(
    // Parameters here
);

// Same API for Redis pub/sub
await pubSubService.SubscribeAsync("user-events", async (topic, message) =>
{
    Console.WriteLine($"Redis message from {topic}: {message}");
});
```
## 🔧 Advanced Features
### Database Operations

#### Conditional Operations
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
#### Array Operations
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
#### Atomic Increment
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
#### Scanning and Filtering
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
### File Storage Operations

#### File Operations with Metadata
```csharp
// Get file metadata
var metadata = await fileService.GetFileMetadataAsync("my-bucket", "files/document.pdf");
if (metadata.IsSuccessful)
{
    Console.WriteLine($"Size: {metadata.Data!.Size} bytes");
    Console.WriteLine($"Content-Type: {metadata.Data.ContentType}");
    Console.WriteLine($"Last Modified: {metadata.Data.LastModified}");
}

// Set file tags
var tags = new Dictionary<string, string>
{
    ["department"] = "engineering",
    ["classification"] = "internal"
};
await fileService.SetFileTagsAsync("my-bucket", "files/document.pdf", tags);
```
#### Bulk File Operations
```csharp
// List files with pagination
var listResult = await fileService.ListFilesAsync("my-bucket", new ListFilesOptions
{
    Prefix = "uploads/",
    MaxResults = 100
});

foreach (var fileKey in listResult.Data!.FileKeys)
{
    Console.WriteLine($"File: {fileKey}");
}

// Delete entire folder
var deletedCount = await fileService.DeleteFolderAsync("my-bucket", "temp/");
Console.WriteLine($"Deleted {deletedCount.Data} files");
```
### Memory Operations

#### List and Data Structure Operations
```csharp
// Work with Redis lists
await memoryService.PushToListTailAsync(scope, "queue", new[]
{
    new PrimitiveType("task1"),
    new PrimitiveType("task2")
});

// Pop from list
var task = await memoryService.PopFirstElementOfListAsync(scope, "queue");
Console.WriteLine($"Processing: {task.Data?.AsString}");

// Check if list contains value
var contains = await memoryService.ListContainsAsync(scope, "queue", new PrimitiveType("task2"));
```
#### Advanced Memory Operations
```csharp
// Set expiration time
await memoryService.SetKeyExpireTimeAsync(scope, TimeSpan.FromHours(1));

// Conditional set (only if not exists)
var wasSet = await memoryService.SetKeyValueConditionallyAsync(
    scope, "initialized", new PrimitiveType("true")
);

// Get all keys in scope
var keys = await memoryService.GetKeysAsync(scope);
Console.WriteLine($"Keys in scope: {string.Join(", ", keys.Data!)}");
```
### PubSub Integration with File Services

#### File Event Notifications
```csharp
// Set up file notifications using PubSub integration
var notificationId = await fileService.CreateNotificationAsync(
    bucketName: "my-bucket",
    topicName: "file-events",
    pathPrefix: "uploads/",
    eventTypes: new[] { FileNotificationEventType.ObjectCreated, FileNotificationEventType.ObjectDeleted },
    pubSubService: pubSubService
);

// Subscribe to file events
await pubSubService.SubscribeAsync("file-events", async (topic, message) =>
{
    Console.WriteLine($"File event: {message}");
    // Process file upload/deletion events
});
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

// Type-safe access
Console.WriteLine(stringKey.AsString);
Console.WriteLine(integerKey.AsInteger);
Console.WriteLine(doubleKey.AsDouble);
Console.WriteLine(Convert.ToBase64String(binaryKey.AsByteArray));
```
## 🧪 Testing

The library includes comprehensive integration tests for all providers:

```bash
# Run all tests
dotnet test

# Run tests for specific service type
dotnet test CrossCloudKit.Database.AWS.Tests
dotnet test CrossCloudKit.Database.Mongo.Tests
dotnet test CrossCloudKit.Database.GC.Tests

dotnet test CrossCloudKit.File.AWS.Tests
dotnet test CrossCloudKit.File.GC.Tests
dotnet test CrossCloudKit.File.S3Compatible.Tests

dotnet test CrossCloudKit.PubSub.AWS.Tests
dotnet test CrossCloudKit.PubSub.GC.Tests
dotnet test CrossCloudKit.PubSub.Redis.Tests

dotnet test CrossCloudKit.Memory.Redis.Tests
```

### Test Configuration

Tests support environment variables for real cloud service integration:


**AWS Services (DynamoDB, S3, SNS/SQS):**
```shell script
AWS_ACCESS_KEY=your-key
AWS_SECRET_KEY=your-secret
AWS_REGION=us-east-1
```


**MongoDB:**
```shell script
MONGODB_HOST=your-host
MONGODB_USER=your-user
MONGODB_PASSWORD=your-password
```


**Google Cloud Services (Datastore, Storage, Pub/Sub):**
```shell script
GOOGLE_CLOUD_PROJECT=your-project-id
GOOGLE_APPLICATION_CREDENTIALS_BASE64=your-base64-encoded-service-account
GOOGLE_CLOUD_TEST_BUCKET=test-bucket-name
```


**Redis (Memory & PubSub):**
```shell script
REDIS_ENDPOINT=localhost
REDIS_PORT=6379
REDIS_USER=your-redis-user
REDIS_PASSWORD=your-redis-password
```


## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────────────────────────────────┐
│                                Application Layer                                    │
├─────────────────────────────────────────────────────────────────────────────────────┤
│                     CrossCloudKit.Interfaces (Unified API Layer)                           │
│           IDatabaseService | IFileService | IPubSubService | IMemoryService        │
├──────────────────┬──────────────────┬──────────────────┬─────────────────────────────┤
│    Database      │   File Storage   │     PubSub       │        Memory               │
│    Services      │    Services      │    Services      │       Services              │
├──────────────────┼──────────────────┼──────────────────┼─────────────────────────────┤
│ AWS DynamoDB     │    AWS S3        │   AWS SNS/SQS    │     Redis Memory            │
│ MongoDB          │ Google Storage   │ Google Pub/Sub   │  (Lists, KV, Mutex)         │
│ Google Datastore │ S3-Compatible    │  Redis Pub/Sub   │                             │
├──────────────────┴──────────────────┴──────────────────┴─────────────────────────────┤
│                            CrossCloudKit.Utilities.Common                                         │
│                   (PrimitiveType, OperationResult, etc.)                           │
└─────────────────────────────────────────────────────────────────────────────────────┘
```


## 🔒 Service-Specific Considerations

### Database Services

#### AWS DynamoDB
- Automatic table creation with string partition keys for maximum flexibility
- Expression attribute names used to handle reserved keywords
- Proper type mapping for conditions (string keys, numeric condition values)
- Support for both managed AWS service and local DynamoDB

#### MongoDB
- Native BSON support with automatic ObjectId handling
- Flexible schema with dynamic collection creation
- Base64 encoding for binary keys to ensure consistency
- Support for MongoDB connection strings and advanced configurations

#### Google Cloud Datastore
- Native entity and key support
- Automatic project and namespace handling
- Support for service account authentication and Application Default Credentials
- Efficient batch operations

### File Storage Services

#### AWS S3
- Automatic bucket creation and configuration
- Support for multipart uploads for large files
- S3 event notifications integration with SNS/SQS
- Proper IAM role and policy management

#### Google Cloud Storage
- Native integration with Google Cloud IAM
- Support for signed URLs with custom expiration
- Automatic retry logic for transient errors
- Efficient streaming uploads and downloads
- Google Pub/Sub integration for file notifications

#### S3-Compatible Storage
- Generic S3 API compatibility layer
- Support for custom endpoints (MinIO, Wasabi, etc.)
- Path-style and virtual-hosted-style URL support
- Flexible authentication mechanisms
- Redis Pub/Sub integration for file notifications

### PubSub Services

#### AWS SNS/SQS
- Automatic topic and queue creation with proper permissions
- Dead letter queue support for failed messages
- Message filtering and fan-out patterns
- Integration with S3 bucket notifications
- Automatic retry logic and error handling

#### Google Cloud Pub/Sub
- Native push and pull subscription models
- Message ordering and delivery guarantees
- Automatic scaling and load balancing
- Integration with Cloud Storage notifications

#### Redis Pub/Sub
- High-performance in-memory messaging
- Pattern-based subscriptions
- Cluster support for high availability
- Integration with Redis memory operations
- Integration with all file service notifications (polling-based) (tested with MinIO)

### Memory Services

#### Redis Memory Service
- Distributed mutex locking with automatic expiration
- Advanced data structures (lists, sets, sorted sets)
- Atomic operations and transactions
- Memory-efficient operations with streaming support
- Cluster and sentinel support for high availability

## 🚀 Advanced Use Cases

### Multi-Cloud Deployment

```csharp
// Switch between providers seamlessly
IDatabaseService dbService = useAws
    ? new DatabaseServiceAWS(/*Parameters*/) : new DatabaseServiceMongoDB(/*Parameters*/));
IFileService fileService = useGcp
    ? new FileServiceGC(/*Parameters*/)) : new FileServiceAWS(/*Parameters*/));
```


### Microservices Integration

```csharp
// Service registry pattern
public class CloudServiceRegistry
{
    public IDatabaseService Database { get; }
    public IFileService FileStorage { get; }
    public IPubSubService Messaging { get; }
    public IMemoryService Cache { get; }
    public CloudServiceRegistry(IConfiguration config)
    {
        Database = CreateDatabaseService(config);
        FileStorage = CreateFileService(config);
        Messaging = CreatePubSubService(config);
        Cache = CreateMemoryService(config);
    }
}
```


### Event-Driven Architecture

```csharp
// Complete event-driven workflow
public class OrderProcessingService
{
    private readonly IDatabaseService _db;
    private readonly IFileService _files;
    private readonly IPubSubService _pubsub;
    private readonly IMemoryService _cache;
    public async Task ProcessOrderAsync(Order order) {
        // Store order in database
        await _db.PutItemAsync("orders", "id", new PrimitiveType(order.Id), JObject.FromObject(order));

        // Generate receipt and store in file storage
        var receipt = GenerateReceipt(order); await _files.UploadFileAsync(receipt, "receipts", $"{order.Id}.pdf");

        // Cache order status
        var cacheScope = new LambdaMemoryServiceScope(() => $"order:{order.Id}");

        await _cache.SetKeyValuesAsync(cacheScope, new[]
        {
            new KeyValuePair<string, PrimitiveType>("status", new PrimitiveType("processing"))
        });

        // Publish order event await
        _pubsub.PublishAsync("order-events", JsonConvert.SerializeObject(new { OrderId = order.Id, Status = "processing" })); }}
```

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

---

Made with ❤️ by the Burak Kara
