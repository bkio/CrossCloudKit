using CrossCloudKit.Interfaces;
using CrossCloudKit.PubSub.Tests.Common;
using Xunit.Abstractions;

namespace CrossCloudKit.PubSub.Basic.Tests;

public class PubSubServiceBasicIntegrationTests(ITestOutputHelper testOutputHelper) : PubSubServiceTestBase(testOutputHelper)
{
    protected override IPubSubService CreatePubSubService()
    {
        return new PubSubServiceBasic();
    }
}
