using CrossCloudKit.Interfaces;
using CrossCloudKit.Memory.Tests.Common;
using CrossCloudKit.PubSub.Basic;
using Xunit;
using Xunit.Abstractions;

[assembly: CollectionBehavior(DisableTestParallelization = false, MaxParallelThreads = 8)]

namespace CrossCloudKit.Memory.Basic.Tests;

public class MemoryServiceBasicIntegrationTests(ITestOutputHelper testOutputHelper) : MemoryServiceTestBase(testOutputHelper)
{
    protected override IMemoryService CreateMemoryService()
    {
        return new MemoryServiceBasic(CreatePubSubService());
    }
    protected override IPubSubService CreatePubSubService()
    {
        return new PubSubServiceBasic();
    }
}
