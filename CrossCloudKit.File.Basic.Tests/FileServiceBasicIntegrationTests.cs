// Copyright (c) 2022- Burak Kara, MIT License
// See LICENSE file in the project root for full license information.

using CrossCloudKit.File.Tests.Common;
using CrossCloudKit.Interfaces;
using CrossCloudKit.Memory.Basic;
using CrossCloudKit.PubSub.Basic;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;
using Xunit.Abstractions;

namespace CrossCloudKit.File.Basic.Tests;

public class FileServiceBasicIntegrationTests(ITestOutputHelper testOutputHelper) : FileServiceTestBase(testOutputHelper), IAsyncDisposable
{
    private WebApplication? _webApp;
    private readonly ITestOutputHelper _testOutputHelper = testOutputHelper;
    private const string TestBaseUrl = "http://localhost:57147";

    protected override IFileService CreateFileService()
    {
        _webApp = CreateWebApplication();

        // Start the web application in the background
        _ = Task.Run(async () =>
        {
            try
            {
                await _webApp.RunAsync();
            }
            catch (Exception ex)
            {
                _testOutputHelper.WriteLine($"Web application error: {ex.Message}");
            }
        });

        // Wait a bit for the server to start
        Thread.Sleep(1000);

        return new FileServiceBasic(
            memoryService: CreateMemoryService(),
            pubSubService: CreatePubSubService(),
            webApplicationForSignedUrls: _webApp,
            publicEndpointBaseForSignedUrls: TestBaseUrl);
    }

    private WebApplication CreateWebApplication()
    {
        var builder = WebApplication.CreateBuilder();

        // Configure to listen on specific port
        builder.WebHost.UseUrls(TestBaseUrl);

        // Add minimal services needed
        builder.Services.AddEndpointsApiExplorer();

        var app = builder.Build();

        // Add a simple health check endpoint for testing
        app.MapGet("/health", () => Results.Ok(new { status = "healthy", timestamp = DateTime.UtcNow }));

        _testOutputHelper.WriteLine($"Created web application listening on {TestBaseUrl}");

        return app;
    }

    private IMemoryService CreateMemoryService()
    {
        return new MemoryServiceBasic();
    }

    protected override IPubSubService CreatePubSubService()
    {
        return new PubSubServiceBasic();
    }

    protected override string GetTestBucketName()
    {
        return "cross-cloud-kit-tests-bucket";
    }

    public async ValueTask DisposeAsync()
    {
        try
        {
            if (_webApp != null)
            {
                _testOutputHelper.WriteLine("Stopping web application...");
                await _webApp.StopAsync(CancellationToken.None);
                _testOutputHelper.WriteLine(
                    "Web application stopped. Disposing web application..."
                );
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
