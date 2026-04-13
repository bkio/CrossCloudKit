// Copyright (c) 2022- Burak Kara, MIT License
// See LICENSE file in the project root for full license information.

using CrossCloudKit.Interfaces;
using CrossCloudKit.LLM.OpenAI;
using CrossCloudKit.LLM.Tests.Common;

[assembly: Xunit.CollectionBehavior(DisableTestParallelization = false, MaxParallelThreads = 4)]

namespace CrossCloudKit.LLM.OpenAI.Tests;

/// <summary>
/// Integration tests for <see cref="LLMServiceOpenAI"/> targeting the configured base URL.
/// Set <c>OPENAI_BASE_URL</c> (e.g. <c>http://ollama.ollama.svc.cluster.local:11434/v1</c>),
/// <c>OPENAI_MODEL</c>, and optionally <c>OPENAI_API_KEY</c> via environment variables
/// or <c>test.runsettings</c>.
/// </summary>
public class LLMServiceOpenAIIntegrationTests : LLMServiceTestBase
{
    private static string BaseUrl =>
        Environment.GetEnvironmentVariable("OPENAI_BASE_URL")
        ?? "http://localhost:11434/v1";

    private static string ApiKey =>
        Environment.GetEnvironmentVariable("OPENAI_API_KEY") ?? string.Empty;

    private static string Model =>
        Environment.GetEnvironmentVariable("OPENAI_MODEL") ?? "gemma3:12b";

    private static string EmbeddingModel =>
        Environment.GetEnvironmentVariable("OPENAI_EMBEDDING_MODEL") ?? "nomic-embed-text:v1.5";

    protected override ILLMService CreateLLMService()
        => new LLMServiceOpenAI(BaseUrl, ApiKey, Model, EmbeddingModel);
}
