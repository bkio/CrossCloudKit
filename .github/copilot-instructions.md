# CrossCloudKit — Copilot Instructions

## What is CrossCloudKit?

CrossCloudKit is a **.NET 8+** cloud-agnostic abstraction library. Code against six interfaces; swap providers (AWS, GCP, MongoDB, Redis, local, etc.) without changing business logic. Item data uses **`JObject` (Newtonsoft.Json)**, not `System.Text.Json`.

## Core Interfaces

| Interface | Purpose | Providers |
|-----------|---------|-----------|
| `IDatabaseService` | Key-value / document CRUD, conditional updates, array ops, scan/filter | AWS DynamoDB, Google Datastore, MongoDB, Basic (file-based) |
| `IFileService` | Upload/download, signed URLs, metadata, tags, notifications | AWS S3, Google Cloud Storage, S3-compatible (MinIO), Basic (local) |
| `IMemoryService` | Key-value cache, lists, distributed mutex, TTL, atomic increments | Redis, Basic (memory-mapped files) |
| `IPubSubService` | Publish/subscribe messaging | AWS SNS+SQS, Google Pub/Sub, Redis, Basic (file-based) |
| `ILLMService` | Chat completion (streaming & non-streaming), embeddings, tool calling | OpenAI-compatible (OpenAI, Azure, Ollama, LM Studio), Basic (LLamaSharp) |
| `IVectorService` | Vector upsert, similarity search, metadata filtering | Qdrant, Basic (in-memory) |

## Critical Types

- **`Primitive`** — discriminated union: `string | long | double | bool | byte[]`. Use `new Primitive(value)` — implicit conversions supported. **No `int` overload — always use `L` suffix for integers.**
- **`OperationResult<T>`** — every method returns this. **Always check `IsSuccessful` before accessing `Data`.** Key status codes: `200` OK, `404` not found, `409` conflict (item already exists), `412` precondition failed (condition not met), `500` internal error.
- **`DbKey`** — `new DbKey("id", new Primitive("value"))` — identifies items in `IDatabaseService`.
- **`ConditionCoupling`** — compose conditions with `.And()` / `.Or()` extension methods. Never construct condition classes directly.
- **`IMemoryScope`** / **`MemoryScopeLambda`** — namespace keys for `IMemoryService`: `new MemoryScopeLambda("scope:key")`.
- **`StringOrStream`** — pass a file path (`string`) or a `Stream` to `IFileService` upload/download. Implements `IDisposable`/`IAsyncDisposable`.
- **`JObject`** — all database item data is `Newtonsoft.Json.Linq.JObject`, not `System.Text.Json`.

## Disposal — IAsyncDisposable

`ILLMService`, `IVectorService`, and `IMemoryService` implement `IAsyncDisposable`. Always use `await using`:

```csharp
await using var llmService = new LLMServiceOpenAI("http://localhost:11434/v1");
await using var vectorService = new VectorServiceBasic();
await using var memoryService = new MemoryServiceBasic();
```

## Patterns — Do This

### OperationResult Handling
```csharp
var result = await dbService.GetItemAsync("Users", key);
if (result.IsSuccessful && result.Data != null)
    Console.WriteLine(result.Data["Name"]);

// Error handling with status codes
if (!result.IsSuccessful)
{
    if (result.StatusCode == HttpStatusCode.NotFound)
        Console.WriteLine("Item not found.");
    else if (result.StatusCode == HttpStatusCode.PreconditionFailed)
        Console.WriteLine("Condition not met.");
    else
        Console.WriteLine($"Error: {result.ErrorMessage}");
}
```

### Primitive Construction
```csharp
new Primitive("text")     // string
new Primitive(42L)        // long  — use L suffix!
new Primitive(3.14)       // double
new Primitive(true)       // bool
new Primitive(bytes)      // byte[]
```

### Database Conditions (IDatabaseService)
```csharp
// Build conditions from the service instance, compose with .And() / .Or()
var condition = dbService.AttributeEquals("Status", new Primitive("active"))
    .And(dbService.AttributeIsGreaterThan("Age", new Primitive(18L)));

// Available condition builders:
// dbService.AttributeExists("Name")
// dbService.AttributeNotExists("Name")
// dbService.AttributeEquals("Name", value)
// dbService.AttributeNotEquals("Name", value)
// dbService.AttributeIsGreaterThan("Name", value)
// dbService.AttributeIsGreaterOrEqual("Name", value)
// dbService.AttributeIsLessThan("Name", value)
// dbService.AttributeIsLessOrEqual("Name", value)
// dbService.ArrayElementExists("Tags", value)      — check if array contains element
// dbService.ArrayElementNotExists("Tags", value)    — check if array does NOT contain element

// Nested object paths use dot notation:
dbService.AttributeEquals("User.Email", new Primitive("john@example.com"));

// Compose with .And() / .Or():
var complex = dbService.AttributeEquals("Dept", new Primitive("IT"))
    .And(dbService.AttributeIsGreaterThan("Exp", new Primitive(5L))
        .Or(dbService.ArrayElementExists("Certs", new Primitive("Senior"))));
```

### Database Options
```csharp
// Configure runtime behavior (auto-sort arrays, convert roundable floats to int)
dbService.SetOptions(new DbOptions(
    AutoSortArrays: DbAutoSortArrays.Yes,
    AutoConvertRoundableFloatToInt: DbAutoConvertRoundableFloatToInt.Yes));
```

### Database CRUD
```csharp
var key = new DbKey("id", new Primitive("user-123"));

// Put (insert new; fails if exists unless overwriteIfExists: true)
var item = new JObject { ["Name"] = "John", ["Email"] = "john@example.com", ["Age"] = 30 };
await dbService.PutItemAsync("Users", key, item);
await dbService.PutItemAsync("Users", key, item, overwriteIfExists: true); // upsert

// Get
var result = await dbService.GetItemAsync("Users", key);

// Update (merge partial data; upsert if item missing)
var update = new JObject { ["LastLogin"] = DateTime.UtcNow.ToString("o") };
await dbService.UpdateItemAsync("Users", key, update);

// Conditional update
var cond = dbService.AttributeEquals("Status", new Primitive("active"));
await dbService.UpdateItemAsync("Users", key, update, conditions: cond);

// Delete
await dbService.DeleteItemAsync("Users", key);

// Batch get
var keys = new[] { new DbKey("id", new Primitive("u1")), new DbKey("id", new Primitive("u2")) };
var batch = await dbService.GetItemsAsync("Users", keys);

// Atomic increment
var newCount = await dbService.IncrementAttributeAsync("Users", key, "LoginCount", 1);
```

### Database Array Operations
```csharp
var key = new DbKey("id", new Primitive("user-123"));
await dbService.AddElementsToArrayAsync("Users", key, "Tags",
    new[] { new Primitive("admin"), new Primitive("verified") });
await dbService.RemoveElementsFromArrayAsync("Users", key, "Tags",
    new[] { new Primitive("guest") });
```

### Database Scanning & Pagination
```csharp
// Full scan
var all = await dbService.ScanTableAsync("Users");

// Filtered scan
var filter = dbService.AttributeEquals("Status", new Primitive("active"));
var filtered = await dbService.ScanTableWithFilterAsync("Users", filter);

// Paginated scan
string? token = null;
do
{
    var page = await dbService.ScanTablePaginatedAsync("Users", pageSize: 10, pageToken: token);
    if (!page.IsSuccessful) break;
    foreach (var item in page.Data.Items)
        Console.WriteLine(item["Name"]);
    token = page.Data.NextPageToken;
} while (token != null);

// Paginated filtered scan
var filteredPage = await dbService.ScanTableWithFilterPaginatedAsync(
    "Users", filter, pageSize: 10, pageToken: null);
```

### Vector Conditions (IVectorService)
```csharp
// Same pattern as database but with Field* prefix:
// vectorService.FieldExists("category")
// vectorService.FieldNotExists("deprecated")
// vectorService.FieldEquals("language", new Primitive("en"))
// vectorService.FieldNotEquals("status", new Primitive("draft"))
// vectorService.FieldGreaterThan("timestamp", new Primitive(1700000000L))
// vectorService.FieldGreaterThanOrEqual("score", new Primitive(0.5))
// vectorService.FieldLessThan("cost", new Primitive(10.0))
// vectorService.FieldLessThanOrEqual("confidence", new Primitive(1.0))

// Compose with .And() / .Or() exactly like database conditions:
var filter = vectorService.FieldEquals("inStock", new Primitive(true))
    .And(vectorService.FieldLessThan("price", new Primitive(20.0)));
var results = await vectorService.QueryAsync("products", queryVector, topK: 5, filter: filter);
```

### Vector Operations
```csharp
// Create collection
await vectorService.EnsureCollectionExistsAsync("docs", vectorDimensions: 1536, VectorDistanceMetric.Cosine);

// Upsert
await vectorService.UpsertAsync("docs", new VectorPoint
{
    Id = "doc-1", Vector = embedding, Metadata = new JObject { ["title"] = "Intro" }
});

// Query (similarity search)
var results = await vectorService.QueryAsync("docs", queryVector, topK: 5,
    filter: vectorService.FieldEquals("lang", new Primitive("en")),
    includeMetadata: true);
foreach (var r in results.Data)
    Console.WriteLine($"{r.Id}: {r.Score}");

// Get single point
var point = await vectorService.GetAsync("docs", "doc-1");

// Delete
await vectorService.DeleteAsync("docs", "doc-1");
```

### LLM — Chat Completion
```csharp
var request = new LLMRequest
{
    Messages = new[]
    {
        new LLMMessage { Role = LLMRole.System, Content = "You are a helpful assistant." },
        new LLMMessage { Role = LLMRole.User, Content = "Explain crosscloud abstraction." }
    },
    MaxTokens = 256,
    Temperature = 0.7
};
var result = await llmService.CompleteAsync(request);
if (result.IsSuccessful)
    Console.WriteLine(result.Data.Content);
```

### LLM — Streaming Completion
```csharp
await foreach (var chunk in llmService.CompleteStreamingAsync(request))
{
    if (chunk.IsSuccessful)
    {
        Console.Write(chunk.Data.ContentDelta);
        if (chunk.Data.IsFinal)
            Console.WriteLine($"\nDone: {chunk.Data.FinishReason}");
    }
}
```

### LLM — Tool Calling
```csharp
// 1. Define tools
var weatherTool = new LLMToolDefinition
{
    Name = "get_weather",
    Description = "Gets weather for a city.",
    Parameters = JObject.Parse(@"{
        ""type"": ""object"",
        ""properties"": { ""city"": { ""type"": ""string"" } },
        ""required"": [""city""]
    }")
};

// 2. Send request with tools
var request = new LLMRequest
{
    Messages = new[] { new LLMMessage { Role = LLMRole.User, Content = "Weather in Berlin?" } },
    Tools = new[] { weatherTool }
};
var result = await llmService.CompleteAsync(request);

// 3. Handle tool calls (check FinishReason)
if (result.IsSuccessful && result.Data.FinishReason == LLMFinishReason.ToolCall)
{
    var messages = new List<LLMMessage>(request.Messages);
    // Add the assistant's response (with tool calls) to history
    messages.Add(new LLMMessage { Role = LLMRole.Assistant, Content = result.Data.Content });

    // Execute each tool call and send results back
    foreach (var tc in result.Data.ToolCalls!)
    {
        var toolResult = ExecuteTool(tc.Name, tc.Arguments); // your function
        messages.Add(new LLMMessage
        {
            Role = LLMRole.Tool,
            ToolCallId = tc.Id,
            Content = toolResult
        });
    }

    // Continue conversation with tool results
    var followUp = await llmService.CompleteAsync(new LLMRequest { Messages = messages });
}
```

### LLM — Embeddings
```csharp
// Single
var embedding = await llmService.CreateEmbeddingAsync("Hello, world!");
// embedding.Data is float[]

// Batch
var texts = new[] { "first doc", "second doc" };
var embeddings = await llmService.CreateEmbeddingsAsync(texts);
// embeddings.Data is IReadOnlyList<float[]>, same order as input
```

### LLM — Token Usage
```csharp
// LLMResponse and LLMStreamChunk include optional Usage stats
var result = await llmService.CompleteAsync(request);
if (result.IsSuccessful && result.Data.Usage != null)
{
    Console.WriteLine($"Prompt: {result.Data.Usage.PromptTokens}");
    Console.WriteLine($"Completion: {result.Data.Usage.CompletionTokens}");
    Console.WriteLine($"Total: {result.Data.Usage.TotalTokens}");
}
```

### LLM + Vector Bridge (LLMVectorExtensions)
```csharp
// Extension methods on IVectorService — namespace: CrossCloudKit.Interfaces
using CrossCloudKit.Interfaces;

// Embed text and upsert in one call
await vectorService.EmbedAndUpsertAsync(
    llmService, "docs", "doc-1", "Some text",
    metadata: new JObject { ["source"] = "readme" });

// Batch embed + upsert
var items = new List<(string Id, string Text, JObject? Metadata)>
{
    ("doc-1", "First document.", new JObject { ["page"] = 1 }),
    ("doc-2", "Second document.", null)
};
await vectorService.EmbedAndUpsertBatchAsync(llmService, "docs", items);

// Semantic search (embed query + QueryAsync in one call)
var hits = await vectorService.SemanticSearchAsync(
    llmService, "docs", "what is crosscloud?", topK: 5);
```

### Memory Service — Key-Value Operations
```csharp
var scope = new MemoryScopeLambda("user:123");

// Set key-values
var kvs = new Dictionary<string, Primitive>
{
    ["Name"] = new Primitive("John"),
    ["Age"] = new Primitive(30L)
};
await memoryService.SetKeyValuesAsync(scope, kvs);

// Get single / batch / all
var val = await memoryService.GetKeyValueAsync(scope, "Name");
if (val.IsSuccessful && val.Data != null)
    Console.WriteLine(val.Data.AsString);
var batch = await memoryService.GetKeyValuesAsync(scope, new[] { "Name", "Age" });
var all   = await memoryService.GetAllKeyValuesAsync(scope);

// Conditional set (only if key does NOT already exist)
var wasSet = await memoryService.SetKeyValueConditionallyAsync(
    scope, "leader", new Primitive("node-1"));

// Conditional set + get current value (leader election pattern)
var (newlySet, currentValue) = (await memoryService
    .SetKeyValueConditionallyAndReturnValueRegardlessAsync(
        scope, "leader", new Primitive("node-1"))).Data;

// Atomic increment (single key)
var newVal = await memoryService.IncrementKeyByValueAndGetAsync(scope, "Counter", 1);

// Atomic increment (batch)
var increments = new Dictionary<string, long> { ["Views"] = 1, ["Clicks"] = 3 };
await memoryService.IncrementKeyValuesAsync(scope, increments);

// TTL
await memoryService.SetKeyExpireTimeAsync(scope, TimeSpan.FromMinutes(30));
var ttl = await memoryService.GetKeyExpireTimeAsync(scope);

// Key introspection
var keys  = await memoryService.GetKeysAsync(scope);
var count = await memoryService.GetKeysCountAsync(scope);

// Delete single / all
await memoryService.DeleteKeyAsync(scope, "TempToken");
await memoryService.DeleteAllKeysAsync(scope);

// Scan for scopes matching a pattern
var scopes = await memoryService.ScanMemoryScopesWithPattern("session:*");
```

### Memory Service — List Operations
```csharp
var scope = new MemoryScopeLambda("queue:tasks");

// Push to tail / head
await memoryService.PushToListTailAsync(scope, "pending",
    new[] { new Primitive("task-1"), new Primitive("task-2") });
await memoryService.PushToListHeadAsync(scope, "pending",
    new[] { new Primitive("urgent-task") });

// Push only if values don't already exist in the list
await memoryService.PushToListTailIfValuesNotExistsAsync(
    scope, "unique-tags", new[] { new Primitive("A"), new Primitive("B") });

// Pop from head / tail
var first = await memoryService.PopFirstElementOfListAsync(scope, "pending");
var last  = await memoryService.PopLastElementOfListAsync(scope, "pending");

// Read / query
var allItems = await memoryService.GetAllElementsOfListAsync(scope, "pending");
var contains = await memoryService.ListContainsAsync(scope, "pending", new Primitive("task-1"));
var size     = await memoryService.GetListSizeAsync(scope, "pending");

// Remove specific elements
await memoryService.RemoveElementsFromListAsync(scope, "pending",
    new[] { new Primitive("task-1") });

// Clear entire list
await memoryService.EmptyListAsync(scope, "pending");
```

### Distributed Mutex (MemoryScopeMutex)
```csharp
// Preferred: use await using with MemoryScopeMutex wrapper
await using var mutex = await MemoryScopeMutex.CreateEntityScopeAsync(
    memoryService, scope, "order-42", TimeSpan.FromSeconds(30));
// ... critical section — lock auto-released on dispose ...

// Master lock (blocks ALL locks in the scope)
await using var master = await MemoryScopeMutex.CreateMasterScopeAsync(
    memoryService, scope, TimeSpan.FromSeconds(60));
```

### File Service
```csharp
// Upload from file path
await fileService.UploadFileAsync("/tmp/report.pdf", "bucket", "reports/report.pdf");

// Upload from stream with accessibility and tags
using var stream = new MemoryStream(bytes);
await fileService.UploadFileAsync(stream, "bucket", "data/file.bin",
    accessibility: FileAccessibility.PublicRead,
    tags: new Dictionary<string, string> { ["env"] = "prod" });

// Download to file (supports range downloads via FileDownloadOptions)
await fileService.DownloadFileAsync("bucket", "reports/report.pdf", "/tmp/downloaded.pdf");
await fileService.DownloadFileAsync("bucket", "large.bin", "/tmp/part.bin",
    options: new FileDownloadOptions { StartIndex = 1000, Size = 5000 });

// Copy
await fileService.CopyFileAsync("src-bucket", "original.pdf", "dst-bucket", "backup/original.pdf");

// Delete file / folder (prefix)
await fileService.DeleteFileAsync("bucket", "reports/old.pdf");
var deletedCount = await fileService.DeleteFolderAsync("bucket", "temp/uploads/");

// Existence & size checks
var exists = await fileService.FileExistsAsync("bucket", "file.pdf");
var size   = await fileService.GetFileSizeAsync("bucket", "file.pdf");
var md5    = await fileService.GetFileChecksumAsync("bucket", "file.pdf");

// Metadata
var meta = await fileService.GetFileMetadataAsync("bucket", "file.pdf");
// meta.Data has: Size, ContentType, Checksum, CreatedAt, LastModified, Properties, Tags

// Tags — get / set
var tags = await fileService.GetFileTagsAsync("bucket", "file.pdf");
await fileService.SetFileTagsAsync("bucket", "file.pdf",
    new Dictionary<string, string> { ["team"] = "backend" });

// Accessibility
await fileService.SetFileAccessibilityAsync("bucket", "public/logo.png",
    FileAccessibility.PublicRead);

// Signed URLs (with optional options for TTL, content type, resumable)
var uploadUrl = await fileService.CreateSignedUploadUrlAsync("bucket", "uploads/doc.pdf");
var downloadUrl = await fileService.CreateSignedDownloadUrlAsync("bucket", "file.pdf",
    new FileSignedDownloadUrlOptions { ValidFor = TimeSpan.FromMinutes(15) });
// Returns FileSignedUrl { Url, ExpiresAt }

// List files in bucket (with prefix filter and pagination)
var list = await fileService.ListFilesAsync("bucket",
    new FileListOptions { Prefix = "reports/", MaxResults = 100 });
foreach (var key in list.Data.FileKeys)
    Console.WriteLine(key);
// list.Data.NextContinuationToken for pagination

// File event notifications (requires IPubSubService)
await fileService.CreateNotificationAsync("bucket", "file-events", "uploads/",
    new[] { FileNotificationEventType.Uploaded }, pubSubService);
await fileService.DeleteNotificationsAsync(pubSubService, "bucket", topicName: "file-events");

// Reset bucket to empty state
await fileService.CleanupBucketAsync("temp-bucket");
```

### PubSub
```csharp
await pubSubService.EnsureTopicExistsAsync("order-events");
await pubSubService.SubscribeAsync("order-events",
    onMessage: async (topic, message) => Console.WriteLine($"[{topic}] {message}"),
    onError: ex => Console.Error.WriteLine(ex));
await pubSubService.PublishAsync("order-events", "{\"orderId\":\"123\"}");
await pubSubService.DeleteTopicAsync("order-events"); // cleanup
```

## Anti-Patterns — Do NOT Do This

```csharp
// ❌ Do NOT construct Condition/ConditionCoupling directly
new ValueCondition(...);                 // Use dbService.AttributeEquals(...)
new ConditionCoupledWithAnd(a, b);       // Use a.And(b)

// ❌ Do NOT use array index syntax in conditions
dbService.AttributeEquals("Tags[0]", val);  // Use dbService.ArrayElementExists("Tags", val)

// ❌ Do NOT access .Data without checking .IsSuccessful
var name = result.Data["Name"];  // Will throw if result failed

// ❌ Do NOT use Primitive(int) — there is no int overload
new Primitive(42);  // Compiler error — use new Primitive(42L)

// ❌ Do NOT use "master" as a mutexValue — it is reserved
await memoryService.MemoryMutexLock(scope, "master", ttl);  // Throws

// ❌ Do NOT forget to dispose ILLMService / IVectorService / IMemoryService
var llm = new LLMServiceOpenAI(...);  // WRONG — use: await using var llm = ...

// ❌ Do NOT use System.Text.Json for item data — this library uses Newtonsoft.Json (JObject)
```

## ASP.NET Core Integration

```csharp
// IDistributedCache adapter
builder.Services.AddSingleton<IDistributedCache>(sp =>
    new MemoryServiceDistributedCache(
        sp.GetRequiredService<IMemoryService>(),
        new MemoryScopeLambda("cache")));

// IFileProvider bridge (for UseStaticFiles, etc.)
var provider = new FileServiceFileProvider(fileService, "static-assets", "wwwroot");
app.UseStaticFiles(new StaticFileOptions { FileProvider = provider });
```

## Database Backup & Migration

```csharp
// Scheduled backup (daily at 1 AM UTC)
var backup = new DatabaseServiceBackup(
    databaseService, fileService, "backup-bucket",
    pubsubService: pubSubService, cronExpression: "0 1 * * *",
    timeZoneInfo: TimeZoneInfo.Utc, backupRootPath: "backups/");

// Manual one-time backup
var result = await backup.TakeBackup();

// Restore from backup
var cursors = await backup.GetBackupFileCursorsAsync().ToListAsync();
await backup.RestoreBackupAsync(cursors.Last());

// Cross-provider migration (e.g. AWS DynamoDB → Google Datastore)
await DatabaseServiceMigration.MigrateAsync(
    sourceDb, destinationDb, fileService, pubSubService,
    backupWorkBucketName: "tmp-bucket");
```

## Provider Constructors — Quick Reference

### Local Development (zero dependencies)
```csharp
IMemoryService mem   = new MemoryServiceBasic();
IPubSubService ps    = new PubSubServiceBasic();
IDatabaseService db  = new DatabaseServiceBasic("myapp", mem);
IFileService fs      = new FileServiceBasic();
IVectorService vs    = new VectorServiceBasic();
ILLMService llm      = new LLMCompletionServiceBasic();  // bundled SmolLM2-135M + MiniLM embeddings
ILLMService embedder = new LLMEmbeddingServiceBasic();    // embeddings only, lighter
```

### AWS
```csharp
IDatabaseService db = new DatabaseServiceAWS(accessKey, secretKey, region, memoryService);
IFileService fs     = new FileServiceAWS(accessKey, secretKey, region);
IPubSubService ps   = new PubSubServiceAWS(accessKey, secretKey, region);
```

### Google Cloud
```csharp
IDatabaseService db = new DatabaseServiceGC(projectId, serviceAccountKeyFilePath, memoryService);
IFileService fs     = new FileServiceGC(projectId, serviceAccountKeyFilePath);
IPubSubService ps   = new PubSubServiceGC(projectId, serviceAccountJson, isBase64Encoded: false);
```

### MongoDB
```csharp
IDatabaseService db = new DatabaseServiceMongo(connectionString, database, memoryService);
```

### Redis
```csharp
var opts = new RedisConnectionOptions { Host = "localhost", Port = 6379 };
IMemoryService mem = new MemoryServiceRedis(opts);
IPubSubService ps  = new PubSubServiceRedis(opts);
```

### S3-Compatible (MinIO, Wasabi, etc.)
```csharp
IFileService fs = new FileServiceS3Compatible(serverAddress, accessKey, secretKey, region);
```

### LLM (OpenAI-compatible)
```csharp
// Works with OpenAI, Ollama, Groq, Azure, LM Studio, etc.
ILLMService llm = new LLMServiceOpenAI(
    baseUrl: "http://localhost:11434/v1",
    apiKey: "",
    defaultModel: "gemma3:12b",
    embeddingModel: "nomic-embed-text:v1.5"  // optional; falls back to defaultModel
);
```

### Vector (Qdrant)
```csharp
IVectorService vs = new VectorServiceQdrant(host: "localhost", grpcPort: 6334);
```

## Key Notes

- **`DatabaseServiceAWS`/`GC`/`Mongo`/`Basic` all require an `IMemoryService` parameter** for internal caching and coordination.
- **`RedisConnectionOptions`** — `Host` and `Port` are required; optional: `Username`, `Password`, `SslEnabled`, `EnableRetryPolicy`, `RetryAttempts`, `RetryDelay`, `SyncTimeout`.
- **`LLMServiceOpenAI`** — `embeddingModel` is optional; if omitted, embeddings use `defaultModel`. Set it when your completion and embedding models differ (common with Ollama).
- **`LLMCompletionServiceBasic`** — bundles SmolLM2-135M (Q8_0, ~139 MB) + all-MiniLM-L6-v2 (384-dim). Zero config. Custom GGUF via `completionModelPath` parameter.
- **`LLMEmbeddingServiceBasic`** — embeddings only (384-dim). No LLamaSharp dependency. Lighter install.

## Project Structure

```
CrossCloudKit.Interfaces/          ← All interfaces, records, enums, conditions
CrossCloudKit.Utilities.Common/    ← Primitive, StringOrStream, shared utilities
CrossCloudKit.Database.{AWS,Basic,GC,Mongo}/     ← IDatabaseService implementations
CrossCloudKit.File.{AWS,Basic,GC,S3Compatible}/  ← IFileService implementations
CrossCloudKit.Memory.{Basic,Redis}/              ← IMemoryService implementations
CrossCloudKit.PubSub.{AWS,Basic,GC,Redis}/       ← IPubSubService implementations
CrossCloudKit.LLM.{OpenAI,Basic.Completion,Basic.Embeddings}/ ← ILLMService implementations
CrossCloudKit.Vector.{Qdrant,Basic}/             ← IVectorService implementations
```
