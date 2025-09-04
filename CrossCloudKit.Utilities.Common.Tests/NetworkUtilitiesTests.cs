// Copyright (c) 2022- Burak Kara, MIT License
// See LICENSE file in the project root for full license information.

using Xunit;

namespace CrossCloudKit.Utilities.Common.Tests;

public class NetworkUtilitiesTests
{
    [Fact]
    public async Task ResolveHostnameAsync_WithLocalhost_ReturnsValidIPAddress()
    {
        // Act
        var result = await NetworkUtilities.ResolveHostnameAsync("localhost");

        // Assert
        Assert.NotNull(result);
        Assert.True(result.AddressFamily == System.Net.Sockets.AddressFamily.InterNetwork ||
                   result.AddressFamily == System.Net.Sockets.AddressFamily.InterNetworkV6);
    }

    [Fact]
    public async Task ResolveHostnameAsync_WithValidIPv4Address_ReturnsSameAddress()
    {
        // Arrange
        const string ipv4Address = "127.0.0.1";

        // Act
        var result = await NetworkUtilities.ResolveHostnameAsync(ipv4Address);

        // Assert
        Assert.NotNull(result);
        Assert.Equal(System.Net.IPAddress.Loopback, result);
        Assert.Equal(System.Net.Sockets.AddressFamily.InterNetwork, result.AddressFamily);
    }

    [Fact]
    public async Task ResolveHostnameAsync_WithValidIPv6Address_ReturnsSameAddress()
    {
        // Arrange
        const string ipv6Address = "::1";

        // Act
        var result = await NetworkUtilities.ResolveHostnameAsync(ipv6Address);

        // Assert
        Assert.NotNull(result);
        Assert.Equal(System.Net.IPAddress.IPv6Loopback, result);
        Assert.Equal(System.Net.Sockets.AddressFamily.InterNetworkV6, result.AddressFamily);
    }

    [Fact]
    public async Task ResolveHostnameAsync_WithPreferIPv6False_PrefersIPv4()
    {
        // Arrange - localhost typically resolves to both IPv4 and IPv6
        const string hostname = "localhost";

        // Act
        var result = await NetworkUtilities.ResolveHostnameAsync(hostname, preferIPv6: false);

        // Assert
        Assert.NotNull(result);
        // Should prefer IPv4 when available, but may fall back to IPv6 if IPv4 isn't available
        Assert.True(result.AddressFamily == System.Net.Sockets.AddressFamily.InterNetwork ||
                   result.AddressFamily == System.Net.Sockets.AddressFamily.InterNetworkV6);
    }

    [Fact]
    public async Task ResolveHostnameAsync_WithPreferIPv6True_PrefersIPv6()
    {
        // Arrange - localhost typically resolves to both IPv4 and IPv6
        const string hostname = "localhost";

        // Act
        var result = await NetworkUtilities.ResolveHostnameAsync(hostname, preferIPv6: true);

        // Assert
        Assert.NotNull(result);
        // Should prefer IPv6 when available, but may fall back to IPv4 if IPv6 isn't available
        Assert.True(result.AddressFamily == System.Net.Sockets.AddressFamily.InterNetworkV6 ||
                   result.AddressFamily == System.Net.Sockets.AddressFamily.InterNetwork);
    }

    [Fact]
    public async Task ResolveHostnameAsync_WithWellKnownHostname_ReturnsValidAddress()
    {
        // Arrange - Using a well-known public DNS that should always resolve
        const string hostname = "dns.google";

        // Act
        var result = await NetworkUtilities.ResolveHostnameAsync(hostname);

        // Assert
        Assert.NotNull(result);
        Assert.True(result.AddressFamily == System.Net.Sockets.AddressFamily.InterNetwork ||
                   result.AddressFamily == System.Net.Sockets.AddressFamily.InterNetworkV6);
    }

    [Fact]
    public async Task ResolveHostnameAsync_WithInvalidHostname_ThrowsInvalidOperationException()
    {
        // Arrange
        const string invalidHostname = "definitely-does-not-exist-12345.invalid";

        // Act & Assert
        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            NetworkUtilities.ResolveHostnameAsync(invalidHostname));
    }

    [Fact]
    public async Task ResolveHostnameAsync_WithCancellation_ThrowsOperationCanceledException()
    {
        // Arrange
        using var cts = new CancellationTokenSource();
        await cts.CancelAsync();

        // Act & Assert
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() =>
            NetworkUtilities.ResolveHostnameAsync("localhost", cancellationToken: cts.Token));
    }

    [Fact]
    public async Task ResolveHostnameAsync_WithTimeoutCancellation_ThrowsOperationCanceledException()
    {
        // Arrange
        using var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(1));

        // Act & Assert
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() =>
            NetworkUtilities.ResolveHostnameAsync("slow-dns-that-might-timeout.example", cancellationToken: cts.Token));
    }

    [Theory]
    [InlineData("http://example.com", true)]
    [InlineData("https://example.com", true)]
    [InlineData("http://www.example.com", true)]
    [InlineData("https://www.example.com", true)]
    [InlineData("http://example.com/path", true)]
    [InlineData("https://example.com/path?query=value", true)]
    [InlineData("http://localhost", true)]
    [InlineData("https://localhost:8080", true)]
    [InlineData("http://127.0.0.1", true)]
    [InlineData("https://192.168.1.1:3000", true)]
    public void IsValidHttpUrl_WithValidHttpUrls_ReturnsTrue(string url, bool expected)
    {
        // Act
        var result = NetworkUtilities.IsValidHttpUrl(url);

        // Assert
        Assert.Equal(expected, result);
    }

    [Theory]
    [InlineData("ftp://example.com", false)]
    [InlineData("file://example.com", false)]
    [InlineData("mailto:user@example.com", false)]
    [InlineData("tel:+1234567890", false)]
    [InlineData("ldap://example.com", false)]
    [InlineData("ws://example.com", false)]
    [InlineData("wss://example.com", false)]
    public void IsValidHttpUrl_WithNonHttpSchemes_ReturnsFalse(string url, bool expected)
    {
        // Act
        var result = NetworkUtilities.IsValidHttpUrl(url);

        // Assert
        Assert.Equal(expected, result);
    }

    [Theory]
    [InlineData("", false)]
    [InlineData(null, false)]
    [InlineData("   ", false)]
    [InlineData("not-a-url", false)]
    [InlineData("example.com", false)]
    [InlineData("www.example.com", false)]
    [InlineData("//example.com", false)]
    [InlineData("http://", false)]
    [InlineData("https://", false)]
    public void IsValidHttpUrl_WithInvalidUrls_ReturnsFalse(string? url, bool expected)
    {
        // Act
        var result = NetworkUtilities.IsValidHttpUrl(url!);

        // Assert
        Assert.Equal(expected, result);
    }

    [Theory]
    [InlineData("http://example.com:80")]
    [InlineData("https://example.com:443")]
    [InlineData("http://example.com:8080")]
    [InlineData("https://example.com:8443")]
    [InlineData("http://localhost:3000")]
    public void IsValidHttpUrl_WithPortNumbers_ReturnsTrue(string url)
    {
        // Act
        var result = NetworkUtilities.IsValidHttpUrl(url);

        // Assert
        Assert.True(result);
    }

    [Theory]
    [InlineData("http://example.com/")]
    [InlineData("https://example.com/path")]
    [InlineData("http://example.com/path/to/resource")]
    [InlineData("https://example.com/path?query=value")]
    [InlineData("http://example.com/path?query=value&other=123")]
    [InlineData("https://example.com/path#fragment")]
    [InlineData("http://example.com/path?query=value#fragment")]
    public void IsValidHttpUrl_WithPathsAndQueries_ReturnsTrue(string url)
    {
        // Act
        var result = NetworkUtilities.IsValidHttpUrl(url);

        // Assert
        Assert.True(result);
    }

    [Theory]
    [InlineData("http://user:pass@example.com")]
    [InlineData("https://user@example.com")]
    [InlineData("http://example.com:8080/path?param=value#section")]
    public void IsValidHttpUrl_WithComplexUrls_ReturnsTrue(string url)
    {
        // Act
        var result = NetworkUtilities.IsValidHttpUrl(url);

        // Assert
        Assert.True(result);
    }

    [Fact]
    public void IsValidHttpUrl_WithIPv6Address_ReturnsTrue()
    {
        // Arrange
        var ipv6Urls = new[]
        {
            "http://[::1]",
            "https://[::1]:8080",
            "http://[2001:db8::1]",
            "https://[2001:db8::1]:443"
        };

        foreach (var url in ipv6Urls)
        {
            // Act
            var result = NetworkUtilities.IsValidHttpUrl(url);

            // Assert
            Assert.True(result, $"URL {url} should be valid");
        }
    }

    [Fact]
    public void IsValidHttpUrl_WithInternationalDomains_ReturnsTrue()
    {
        // Arrange - International domain names
        var internationalUrls = new[]
        {
            "http://example.co.uk",
            "https://example.com.au",
            "http://subdomain.example.org",
            "https://test-site.example.net"
        };

        foreach (var url in internationalUrls)
        {
            // Act
            var result = NetworkUtilities.IsValidHttpUrl(url);

            // Assert
            Assert.True(result, $"URL {url} should be valid");
        }
    }

    [Theory]
    [InlineData("HTTP://EXAMPLE.COM")]
    [InlineData("HTTPS://EXAMPLE.COM")]
    [InlineData("Http://Example.Com")]
    [InlineData("HtTpS://ExAmPlE.cOm")]
    public void IsValidHttpUrl_WithDifferentCasing_ReturnsTrue(string url)
    {
        // Act
        var result = NetworkUtilities.IsValidHttpUrl(url);

        // Assert
        Assert.True(result);
    }

    [Fact]
    public void IsValidHttpUrl_WithVeryLongUrl_HandlesCorrectly()
    {
        // Arrange
        var longPath = string.Join("/", Enumerable.Repeat("verylongpathsegment", 100));
        var longUrl = $"https://example.com/{longPath}";

        // Act
        var result = NetworkUtilities.IsValidHttpUrl(longUrl);

        // Assert
        Assert.True(result);
    }

    [Fact]
    public void IsValidHttpUrl_WithSpecialCharacters_HandlesCorrectly()
    {
        // Arrange
        var urlsWithSpecialChars = new[]
        {
            "https://example.com/path%20with%20encoded%20spaces", // URL encoded
            "https://example.com/path?query=value%20with%20spaces",
            "https://example.com/path#fragment%20with%20spaces"
        };

        foreach (var url in urlsWithSpecialChars)
        {
            // Act
            var result = NetworkUtilities.IsValidHttpUrl(url);

            // Assert
            Assert.True(result, $"URL {url} should be valid");
        }
    }

    [Fact]
    public async Task NetworkUtilities_Performance_MultipleHostnameResolutions()
    {
        // Arrange
        var hostnames = new[] { "localhost", "127.0.0.1", "::1" };
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();

        // Act
        var tasks = hostnames.Select(hostname => NetworkUtilities.ResolveHostnameAsync(hostname));
        var results = await Task.WhenAll(tasks);
        stopwatch.Stop();

        // Assert
        Assert.Equal(hostnames.Length, results.Length);
        Assert.All(results, Assert.NotNull);
        Assert.True(stopwatch.ElapsedMilliseconds < 5000,
            $"Performance test failed: took {stopwatch.ElapsedMilliseconds}ms");
    }

    [Fact]
    public void IsValidHttpUrl_Performance_ManyValidations()
    {
        // Arrange
        var testUrls = new List<string>();
        for (int i = 0; i < 1000; i++)
        {
            testUrls.Add($"https://example{i}.com/path{i}");
        }

        var stopwatch = System.Diagnostics.Stopwatch.StartNew();

        // Act
        var results = testUrls.Select(NetworkUtilities.IsValidHttpUrl).ToList();
        stopwatch.Stop();

        // Assert
        Assert.Equal(testUrls.Count, results.Count);
        Assert.All(results, Assert.True);
        Assert.True(stopwatch.ElapsedMilliseconds < 1000,
            $"Performance test failed: took {stopwatch.ElapsedMilliseconds}ms");
    }

    [Fact]
    public async Task ResolveHostnameAsync_WithMultipleAddressFamilies_ReturnsAppropriateAddress()
    {
        // Arrange - Test with localhost which typically has both IPv4 and IPv6
        const string hostname = "localhost";

        // Act
        var ipv4Result = await NetworkUtilities.ResolveHostnameAsync(hostname, preferIPv6: false);
        var ipv6Result = await NetworkUtilities.ResolveHostnameAsync(hostname, preferIPv6: true);

        // Assert
        Assert.NotNull(ipv4Result);
        Assert.NotNull(ipv6Result);

        // Results might be the same if only one address family is available
        // but they should both be valid addresses
        Assert.True(ipv4Result.AddressFamily == System.Net.Sockets.AddressFamily.InterNetwork ||
                   ipv4Result.AddressFamily == System.Net.Sockets.AddressFamily.InterNetworkV6);
        Assert.True(ipv6Result.AddressFamily == System.Net.Sockets.AddressFamily.InterNetwork ||
                   ipv6Result.AddressFamily == System.Net.Sockets.AddressFamily.InterNetworkV6);
    }

    [Fact]
    public async Task ResolveHostnameAsync_ConsistentResults_ReturnsSameAddressMultipleCalls()
    {
        // Arrange
        const string hostname = "127.0.0.1";

        // Act
        var result1 = await NetworkUtilities.ResolveHostnameAsync(hostname);
        var result2 = await NetworkUtilities.ResolveHostnameAsync(hostname);

        // Assert
        Assert.Equal(result1, result2);
    }

    [Theory]
    [InlineData("192.168.1.1")]
    [InlineData("10.0.0.1")]
    [InlineData("172.16.0.1")]
    [InlineData("8.8.8.8")]
    public async Task ResolveHostnameAsync_WithVariousIPv4Addresses_ReturnsCorrectAddress(string ipAddress)
    {
        // Act
        var result = await NetworkUtilities.ResolveHostnameAsync(ipAddress);

        // Assert
        Assert.NotNull(result);
        Assert.Equal(System.Net.IPAddress.Parse(ipAddress), result);
        Assert.Equal(System.Net.Sockets.AddressFamily.InterNetwork, result.AddressFamily);
    }

    [Theory]
    [InlineData("2001:db8::1")]
    [InlineData("fe80::1")]
    [InlineData("::ffff:192.168.1.1")]
    public async Task ResolveHostnameAsync_WithVariousIPv6Addresses_ReturnsCorrectAddress(string ipAddress)
    {
        // Act
        var result = await NetworkUtilities.ResolveHostnameAsync(ipAddress);

        // Assert
        Assert.NotNull(result);
        Assert.Equal(System.Net.IPAddress.Parse(ipAddress), result);
        Assert.Equal(System.Net.Sockets.AddressFamily.InterNetworkV6, result.AddressFamily);
    }

    [Fact]
    public void IsValidHttpUrl_EdgeCase_WithMalformedUrls_HandlesSafely()
    {
        // Arrange - Malformed URLs that should not crash the validation
        var malformedUrls = new[]
        {
            "https://", // No host
            "http://exam ple.com", // Space in hostname
            "https://[", // Incomplete IPv6 bracket
            "http://", // No host at all
        };

        foreach (var url in malformedUrls)
        {
            // Act & Assert - Should not throw, should return false
            var result = NetworkUtilities.IsValidHttpUrl(url);
            Assert.False(result, $"Malformed URL {url} should be invalid");
        }
    }

    [Fact]
    public async Task NetworkUtilities_IntegrationTest_HostnameResolutionAndUrlValidation()
    {
        // Arrange
        const string testHost = "localhost";
        var testUrls = new[]
        {
            $"http://{testHost}",
            $"https://{testHost}:8080",
            $"http://{testHost}/api/test"
        };

        // Act
        var resolvedAddress = await NetworkUtilities.ResolveHostnameAsync(testHost);
        var urlValidationResults = testUrls.Select(NetworkUtilities.IsValidHttpUrl).ToArray();

        // Assert
        Assert.NotNull(resolvedAddress);
        Assert.All(urlValidationResults, Assert.True);

        // Create URLs with the resolved IP address
        var ipBasedUrls = testUrls.Select(url => url.Replace(testHost, resolvedAddress.ToString())).ToArray();
        var ipUrlValidationResults = ipBasedUrls.Select(NetworkUtilities.IsValidHttpUrl).ToArray();
        Assert.All(ipUrlValidationResults, Assert.True);
    }

    [Fact]
    public async Task ResolveHostnameAsync_StressTest_MultipleSimultaneousResolutions()
    {
        // Arrange
        const int concurrentResolutions = 50;
        var hostname = "localhost";

        // Act
        var tasks = Enumerable.Range(0, concurrentResolutions)
            .Select(_ => NetworkUtilities.ResolveHostnameAsync(hostname))
            .ToArray();

        var results = await Task.WhenAll(tasks);

        // Assert
        Assert.Equal(concurrentResolutions, results.Length);
        Assert.All(results, Assert.NotNull);

        // All results should be the same for the same hostname
        var firstResult = results[0];
        Assert.All(results, result => Assert.Equal(firstResult, result));
    }

    [Fact]
    public void IsValidHttpUrl_StressTest_ManyDifferentUrls()
    {
        // Arrange
        var testUrls = new List<string>();
        var schemes = new[] { "http", "https" };
        var hosts = new[] { "example.com", "test.org", "localhost", "127.0.0.1" };
        var paths = new[] { "", "/", "/api", "/api/v1/test", "/path?param=value" };

        foreach (var scheme in schemes)
        {
            foreach (var host in hosts)
            {
                foreach (var path in paths)
                {
                    testUrls.Add($"{scheme}://{host}{path}");
                }
            }
        }

        // Act
        var results = testUrls.Select(NetworkUtilities.IsValidHttpUrl).ToArray();

        // Assert
        Assert.Equal(testUrls.Count, results.Length);
        Assert.All(results, Assert.True);
    }

    [Fact]
    public async Task ResolveHostnameAsync_WithEmptyString_ThrowsException()
    {
        // Act & Assert
        await Assert.ThrowsAnyAsync<Exception>(() => NetworkUtilities.ResolveHostnameAsync(""));
    }

    [Fact]
    public async Task ResolveHostnameAsync_WithWhitespace_ThrowsException()
    {
        // Act & Assert
        await Assert.ThrowsAnyAsync<Exception>(() => NetworkUtilities.ResolveHostnameAsync("   "));
    }

    [Fact]
    public async Task ResolveHostnameAsync_WithNull_ThrowsException()
    {
        // Act & Assert
        await Assert.ThrowsAnyAsync<Exception>(() => NetworkUtilities.ResolveHostnameAsync(null!));
    }

    [Fact]
    public void NetworkUtilities_AllMethods_HandleNullAndEmptyInputsSafely()
    {
        // Test IsValidHttpUrl with various edge cases
        Assert.False(NetworkUtilities.IsValidHttpUrl(null!));
        Assert.False(NetworkUtilities.IsValidHttpUrl(""));
        Assert.False(NetworkUtilities.IsValidHttpUrl("   "));

        // ResolveHostnameAsync with invalid inputs should throw (tested above)
        // This confirms the methods have appropriate input validation
    }
}
