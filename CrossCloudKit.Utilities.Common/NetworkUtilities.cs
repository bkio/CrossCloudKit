// Copyright (c) 2022- Burak Kara, MIT License
// See LICENSE file in the project root for full license information.

using System.Net;
using System.Net.Sockets;

namespace CrossCloudKit.Utilities.Common;

/// <summary>
/// Provides utilities for network operations.
/// </summary>
public static class NetworkUtilities
{
    /// <summary>
    /// Converts a hostname or IP address string to an IPAddress object.
    /// </summary>
    /// <param name="hostNameOrAddress">The hostname or IP address</param>
    /// <param name="preferIPv6">Whether to prefer IPv6 addresses (defaults to IPv4)</param>
    /// <param name="cancellationToken">Cancellation token to observe</param>
    /// <returns>The resolved IP address</returns>
    public static async Task<IPAddress> ResolveHostnameAsync(string hostNameOrAddress, bool preferIPv6 = false, CancellationToken cancellationToken = default)
    {
        var preferredFamily = preferIPv6 ? AddressFamily.InterNetworkV6 : AddressFamily.InterNetwork;

        var addresses = await Dns.GetHostAddressesAsync(hostNameOrAddress, cancellationToken).ConfigureAwait(false);

        return addresses.FirstOrDefault(addr => addr.AddressFamily == preferredFamily)
               ?? addresses.FirstOrDefault()
               ?? throw new InvalidOperationException($"Could not resolve hostname: {hostNameOrAddress}");
    }

    /// <summary>
    /// Validates if a string is a properly formatted HTTP or HTTPS URL.
    /// </summary>
    /// <param name="url">The URL to validate</param>
    /// <returns>True if the URL is valid</returns>
    public static bool IsValidHttpUrl(string url)
    {
        if (string.IsNullOrWhiteSpace(url))
            return false;

        return Uri.TryCreate(url, UriKind.Absolute, out var result)
               && (result.Scheme == Uri.UriSchemeHttp || result.Scheme == Uri.UriSchemeHttps);
    }
}
