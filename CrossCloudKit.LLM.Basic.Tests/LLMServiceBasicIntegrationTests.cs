// Copyright (c) 2022- Burak Kara, MIT License
// See LICENSE file in the project root for full license information.

using CrossCloudKit.Interfaces;
using CrossCloudKit.LLM.Basic;
using CrossCloudKit.LLM.Tests.Common;

[assembly: Xunit.CollectionBehavior(DisableTestParallelization = false, MaxParallelThreads = 2)]

namespace CrossCloudKit.LLM.Basic.Tests;

/// <summary>
/// Integration tests for <see cref="LLMServiceBasic"/>.
/// Completion tests require a GGUF model file; set <c>LLM_BASIC_MODEL_PATH</c>
/// or place the file at <c>&lt;OutputDir&gt;/models/SmolLM2-135M-Instruct-Q8_0.gguf</c>.
/// </summary>
public class LLMServiceBasicIntegrationTests : LLMServiceTestBase
{
    /// <summary>
    /// Each test gets a fresh <see cref="LLMServiceBasic"/> instance so that
    /// <c>await using</c> disposal inside the base class test template does not
    /// invalidate a shared object for subsequent tests.
    /// Both embedding and completion models are bundled GGUF files.
    /// </summary>
    protected override ILLMService CreateLLMService() => new LLMServiceBasic();

    protected override bool SupportsCompletion =>
        !string.IsNullOrEmpty(Environment.GetEnvironmentVariable("LLM_BASIC_MODEL_PATH")) ||
        File.Exists(Path.Combine(AppContext.BaseDirectory, "models", LLM.Basic.Completion.LLMCompletionServiceBasic.BundledModelFileName)) ||
        File.Exists(Path.Combine(AppContext.BaseDirectory, "models", "completion-model.gguf"));
}
