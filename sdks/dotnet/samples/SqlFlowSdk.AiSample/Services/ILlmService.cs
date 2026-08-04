// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using SqlFlowSdk.AiSample.Models;

namespace SqlFlowSdk.AiSample.Services;

public interface ILlmService
{
    Task<Solution> GenerateFixAsync(string log, string lastFeedback, CancellationToken ct);
}

public class LlmService : ILlmService
{
    private readonly ILogger<LlmService> _logger;
    public LlmService(ILogger<LlmService> logger) => _logger = logger;

    public async Task<Solution> GenerateFixAsync(string log, string lastFeedback, CancellationToken ct)
    {
        _logger.LogInformation("Agent is thinking: 'Learned from feedback: {feedback}'", lastFeedback);

        // Simulate a very expensive LLM call with a delay
        await Task.Delay(2500, ct);

        // Change Code based on human feedback
        string code = lastFeedback.Contains("error handling")
            ? "// AI: Improved Logging & Error-Handling added\nif(data == null) throw new ArgumentNullException();" 
            : "// AI: Simple Fix for the NullReferenceException\nif(data == null) return;";

        _logger.LogInformation("LLM has generated a potential fix: {PatchedCode}", code);

        return new Solution { PatchedCode = code };
    }
}