using CrossCloudKit.Interfaces;
using CrossCloudKit.Memory.Tests.Common;
using CrossCloudKit.PubSub.Basic;
using Xunit.Abstractions;

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
