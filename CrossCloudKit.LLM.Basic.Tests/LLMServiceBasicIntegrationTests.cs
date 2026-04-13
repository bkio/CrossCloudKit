// Copyright (c) 2022- Burak Kara, MIT License
// See LICENSE file in the project root for full license information.

using CrossCloudKit.Interfaces;
using CrossCloudKit.LLM.Basic;
using CrossCloudKit.LLM.Tests.Common;

[assembly: Xunit.CollectionBehavior(DisableTestParallelization = false, MaxParallelThreads = 2)]

namespace CrossCloudKit.LLM.Basic.Tests;

/// <summary>
/// Integration tests for <see cref="LLMServiceBasic"/>.
/// Embedding tests always run (model bundled via SmartComponents.LocalEmbeddings).
/// Completion tests require a GGUF model file; set <c>LLM_BASIC_MODEL_PATH</c>
/// or place the file at <c>&lt;OutputDir&gt;/models/completion-model.gguf</c>.
/// </summary>
public class LLMServiceBasicIntegrationTests : LLMServiceTestBase
{
    /// <summary>
    /// Each test gets a fresh <see cref="LLMServiceBasic"/> instance so that
    /// <c>await using</c> disposal inside the base class test template does not
    /// invalidate a shared object for subsequent tests.
    /// The LocalEmbedder model is bundled as an embedded resource and loads quickly.
    /// </summary>
    protected override ILLMService CreateLLMService() => new LLMServiceBasic();

    protected override bool SupportsCompletion =>
        !string.IsNullOrEmpty(Environment.GetEnvironmentVariable("LLM_BASIC_MODEL_PATH")) ||
        File.Exists(Path.Combine(AppContext.BaseDirectory, "models", "completion-model.gguf"));
}
