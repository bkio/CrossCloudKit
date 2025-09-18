// Copyright (c) 2022- Burak Kara, MIT License
// See LICENSE file in the project root for full license information.

using System.Net;
using CrossCloudKit.File.Tests.Common;
using CrossCloudKit.Interfaces;
using CrossCloudKit.Memory.Basic;
using CrossCloudKit.PubSub.Basic;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Hosting.Server;
using Microsoft.AspNetCore.Hosting.Server.Features;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;
using Xunit.Abstractions;

namespace CrossCloudKit.File.Basic.Tests;

public class FileServiceBasicIntegrationTests(ITestOutputHelper testOutputHelper) : FileServiceTestBase(testOutputHelper), IAsyncDisposable
{
    private WebApplication? _webApp;
    private readonly ITestOutputHelper _testOutputHelper = testOutputHelper;

    protected override IFileService CreateFileService()
    {
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10)); // Timeout for server startup

        var fileService = new FileServiceBasic(
            memoryService: new MemoryServiceBasic(),
            pubSubService: CreatePubSubService());

        // Start the web application and wait for it to be ready
        try
        {
            _webApp = CreateWebServer();

            fileService.RegisterSignedUrlEndpoints(_webApp);

            var testBaseUrl = StartWebServer(_webApp, cts.Token).GetAwaiter().GetResult();

            fileService.RegisterSignedUrlEndpointBase(testBaseUrl);

            // Verify the server is ready by polling the /health endpoint
            if (!PollHealthEndpoint(testBaseUrl, TimeSpan.FromSeconds(5)).GetAwaiter().GetResult())
            {
                throw new InvalidOperationException("Failed to verify server health.");
            }
        }
        catch (Exception ex)
        {
            _testOutputHelper.WriteLine($"Failed to start web application: {ex.Message}");
            _webApp = null;
            fileService.ResetSignedUrlSetup();
        }
        return fileService;
    }

    protected override IPubSubService CreatePubSubService()
    {
        return new PubSubServiceBasic();
    }

    protected override string GetTestBucketName()
    {
        return "cross-cloud-kit-tests-bucket";
    }

    private WebApplication CreateWebServer()
    {
        var builder = WebApplication.CreateBuilder();

        builder.WebHost.ConfigureKestrel(options =>
        {
            options.Listen(IPAddress.Loopback, 0);
        });

        // Add minimal services needed
        builder.Services.AddEndpointsApiExplorer();

        var app = builder.Build();

        // Add a simple health check endpoint for testing
        app.MapGet("/health", () => Results.Ok(new { status = "healthy", timestamp = DateTime.UtcNow }));

        return app;
    }

    private async Task<string> StartWebServer(WebApplication app, CancellationToken cancellationToken)
    {
        await app.StartAsync(cancellationToken);

        // Get the assigned port
        var server = app.Services.GetRequiredService<IServer>();
        var addressFeature = server.Features.Get<IServerAddressesFeature>();
        var address = addressFeature?.Addresses.FirstOrDefault()
            ?? throw new InvalidOperationException("No server address found.");
        var port = new Uri(address).Port;
        var testBaseUrl = $"http://localhost:{port}";

        _testOutputHelper.WriteLine($"Created web application listening on {testBaseUrl}");

        return testBaseUrl;
    }

    private static async Task<bool> PollHealthEndpoint(string baseUrl, TimeSpan timeout)
    {
        using var httpClient = new HttpClient();
        var deadline = DateTime.UtcNow + timeout;
        while (DateTime.UtcNow < deadline)
        {
            try
            {
                var response = await httpClient.GetAsync($"{baseUrl}/health");
                if (response.StatusCode == HttpStatusCode.OK)
                {
                    return true;
                }
            }
            catch
            {
                // Ignore transient errors and retry
            }
            await Task.Delay(100);
        }
        return false;
    }

    protected override bool IsFileServiceBasicAndHttpServerFailedToBeCreated()
    {
        return _webApp == null;
    }

    public async ValueTask DisposeAsync()
    {
        try
        {
            if (_webApp != null)
            {
                _testOutputHelper.WriteLine("Stopping web application...");
                using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
                await _webApp.StopAsync(cts.Token);
                _testOutputHelper.WriteLine("Web application stopped. Disposing web application...");
                await _webApp.DisposeAsync();
                _testOutputHelper.WriteLine("Web application stopped and disposed.");
            }
        }
        catch (Exception ex)
        {
            _testOutputHelper.WriteLine($"Error disposing web application: {ex.Message}");
        }
        finally
        {
            GC.SuppressFinalize(this);
        }
    }
}
