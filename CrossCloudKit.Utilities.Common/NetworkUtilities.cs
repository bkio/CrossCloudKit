// Copyright (c) 2022- Burak Kara, MIT License
// See LICENSE file in the project root for full license information.

using System.Globalization;
using System.Net;
using System.Net.Sockets;
using System.Text.RegularExpressions;

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
        if (string.IsNullOrWhiteSpace(hostNameOrAddress))
            throw new ArgumentException("Hostname or address cannot be null, empty, or whitespace.", nameof(hostNameOrAddress));

        var preferredFamily = preferIPv6 ? AddressFamily.InterNetworkV6 : AddressFamily.InterNetwork;

        try
        {
            var addresses = await Dns.GetHostAddressesAsync(hostNameOrAddress, cancellationToken);

            return addresses.FirstOrDefault(addr => addr.AddressFamily == preferredFamily)
                   ?? addresses.FirstOrDefault()
                   ?? throw new InvalidOperationException($"Could not resolve hostname: {hostNameOrAddress}");
        }
        catch (SocketException ex)
        {
            throw new InvalidOperationException($"Could not resolve hostname: {hostNameOrAddress}", ex);
        }
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

    /// <summary>
    /// Validates whether a string is a properly formatted email address.
    /// </summary>
    /// <param name="email">The email address to validate</param>
    /// <returns>True if the email address is valid; otherwise, false</returns>
    public static bool IsValidEmail(string? email)
    {
        if (string.IsNullOrWhiteSpace(email))
            return false;

        try
        {
            // Normalize the domain
            email = Regex.Replace(email, @"(@)(.+)$", DomainMapper, RegexOptions.None);

            // Examines the domain part of the email and normalizes it.
            string DomainMapper(Match match)
            {
                // Use IdnMapping class to convert Unicode domain names.
                var idn = new IdnMapping();

                // Pull out and process domain name (throws ArgumentException on invalid)
                var dName = idn.GetAscii(match.Groups[2].Value);

                return match.Groups[1].Value + dName;
            }
        }
        catch (RegexMatchTimeoutException)
        {
            return false;
        }
        catch (ArgumentException)
        {
            return false;
        }

        try
        {
            return Regex.IsMatch(email,
                @"^[^@\s]+@[^@\s]+\.[^@\s]+$",
                RegexOptions.IgnoreCase);
        }
        catch (RegexMatchTimeoutException)
        {
            return false;
        }
    }

    /// <summary>
    /// Validates whether a string is a properly formatted HTTP or HTTPS URL.
    /// </summary>
    /// <param name="url">The URL to validate</param>
    /// <returns>True if the URL is valid and uses HTTP or HTTPS scheme; otherwise, false</returns>
    public static bool IsValidUrl(string url)
    {
        return Uri.TryCreate(url, UriKind.Absolute, out var uri) && (uri.Scheme == Uri.UriSchemeHttp || uri.Scheme == Uri.UriSchemeHttps);
    }

    /// <summary>
    /// Retrieves a parameter value from URL parameters dictionary, handling both normal and amp-prefixed parameters.
    /// </summary>
    /// <param name="urlParameters">Dictionary containing URL parameters</param>
    /// <param name="parameter">The parameter name to retrieve</param>
    /// <param name="action">The parameter value if found; otherwise, null</param>
    /// <returns>True if the parameter was found; otherwise, false</returns>
    public static bool GetParameterFromUrlParameters(Dictionary<string, string> urlParameters, string parameter, out string? action)
    {
        action = null;

        if (!urlParameters.TryGetValue(parameter, out var urlParameter))
        {
            if (!urlParameters.ContainsKey($"amp;{parameter}"))
            {
                return false;
            }
            else
            {
                action = urlParameters[$"amp;{parameter}"];
            }
        }
        else
        {
            action = urlParameter;
        }
        return true;
    }
}
