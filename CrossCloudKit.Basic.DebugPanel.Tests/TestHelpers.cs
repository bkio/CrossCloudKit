// Copyright (c) 2022- Burak Kara, MIT License
// See LICENSE file in the project root for full license information.

using System.Net;
using System.Net.Sockets;

namespace CrossCloudKit.Basic.DebugPanel.Tests;

/// <summary>
/// Helpers shared across debug panel test classes.
/// </summary>
internal static class TestHelpers
{
    /// <summary>
    /// Finds a random available TCP port on loopback by binding to port 0.
    /// </summary>
    public static int GetAvailablePort()
    {
        using var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        var port = ((IPEndPoint)listener.LocalEndpoint).Port;
        listener.Stop();
        return port;
    }

    /// <summary>
    /// Creates a unique temp directory for test isolation.
    /// </summary>
    public static string CreateTempDir()
    {
        var dir = Path.Combine(Path.GetTempPath(), "cck-test-" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(dir);
        return dir;
    }

    /// <summary>
    /// Deletes a directory and its contents (best-effort).
    /// </summary>
    public static void CleanupDir(string path)
    {
        try { if (Directory.Exists(path)) Directory.Delete(path, true); }
        catch { /* best-effort */ }
    }
}
