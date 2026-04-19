using System.Runtime.CompilerServices;

namespace CrossCloudKit.Memory.Tests.Common;

internal static class ModuleInit
{
#pragma warning disable CA2255
    [ModuleInitializer]
#pragma warning restore CA2255
    internal static void DisableDebugPanel()
    {
        Environment.SetEnvironmentVariable("CROSSCLOUDKIT_DEBUG_PANEL_DISABLED", "true");
    }
}
