// Copyright (c) 2022- Burak Kara, MIT License
// See LICENSE file in the project root for full license information.

using CrossCloudKit.Interfaces;
using CrossCloudKit.LLM.Basic;
using CrossCloudKit.LLM.Basic.Completion;
using CrossCloudKit.LLM.Basic.Embeddings;
using CrossCloudKit.LLM.Tests.Common;

[assembly: Xunit.CollectionBehavior(DisableTestParallelization = false, MaxParallelThreads = 2)]

namespace CrossCloudKit.LLM.Basic.Tests;

/// <summary>
/// Integration tests for <see cref="LLMServiceBasic"/>.
/// Tests require GGUF model files; set <c>LLM_BASIC_MODEL_PATH</c> / <c>LLM_BASIC_EMBEDDING_MODEL_PATH</c>
/// or place the files at <c>&lt;OutputDir&gt;/models/</c>.
/// </summary>
public class LLMServiceBasicIntegrationTests : LLMServiceTestBase
{
    // Probe the native backend once per process — file existence alone is not
    // enough because the LLamaSharp native library (libllama) may be missing.
    private static readonly Lazy<bool> CompletionAvailable = new(() =>
    {
        try
        {
            var svc = new LLMCompletionServiceBasic();
            var ok = svc.IsInitialized;
            try { svc.DisposeAsync().AsTask().GetAwaiter().GetResult(); } catch { /* disposal must not affect the probe result */ }
            return ok;
        }
        catch { return false; }
    });

    private static readonly Lazy<bool> EmbeddingAvailable = new(() =>
    {
        try
        {
            var svc = new LLMEmbeddingServiceBasic();
            var ok = svc.IsInitialized;
            try { svc.DisposeAsync().AsTask().GetAwaiter().GetResult(); } catch { /* disposal must not affect the probe result */ }
            return ok;
        }
        catch { return false; }
    });

    /// <summary>
    /// Each test gets a fresh <see cref="LLMServiceBasic"/> instance so that
    /// <c>await using</c> disposal inside the base class test template does not
    /// invalidate a shared object for subsequent tests.
    /// Both embedding and completion models are bundled GGUF files.
    /// </summary>
    protected override ILLMService CreateLLMService() => new LLMServiceBasic();

    protected override bool SupportsCompletion => CompletionAvailable.Value;

    protected override bool SupportsEmbedding => EmbeddingAvailable.Value;
}
