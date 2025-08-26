// Copyright (c) 2022- Burak Kara, MIT License
// See LICENSE file in the project root for full license information.

using Cloud.Interfaces;
using Xunit.Abstractions;

namespace Cloud.Memory.Tests.Common;

public abstract class MemoryServiceTestBase(ITestOutputHelper testOutputHelper)
{
    protected abstract IMemoryService CreateMemoryService();
    protected abstract IPubSubService CreatePubSubService();
}
