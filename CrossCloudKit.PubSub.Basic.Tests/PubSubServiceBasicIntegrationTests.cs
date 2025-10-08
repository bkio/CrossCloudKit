using CrossCloudKit.Interfaces;
using CrossCloudKit.PubSub.Tests.Common;
using Xunit;
using Xunit.Abstractions;

[assembly: CollectionBehavior(DisableTestParallelization = false, MaxParallelThreads = 8)]

namespace CrossCloudKit.PubSub.Basic.Tests;

public class PubSubServiceBasicIntegrationTests(ITestOutputHelper testOutputHelper) : PubSubServiceTestBase(testOutputHelper)
{
    protected override IPubSubService CreatePubSubService()
    {
        return new PubSubServiceBasic();
    }
}
