// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using SqlFlowSdk.AiSample.Models;

namespace SqlFlowSdk.AiSample.Services;

public interface IGitHubService
{
    Task<Issue> GetIssueDetailsAsync(string id, CancellationToken ct);

    Task<string> CreatePullRequestAsync(string id, string code, CancellationToken ct);

    Task RequestHumanReviewAsync(string issueId, Solution proposedFix, string correlationId, CancellationToken ct);

    Task EscalateToSeniorAsync(string id, string reason, CancellationToken ct);
}

public class GitHubService : IGitHubService
{
    private readonly ILogger<GitHubService> _logger;

    public GitHubService(ILogger<GitHubService> logger)
    {
        _logger = logger;
    }

    public async Task<Issue> GetIssueDetailsAsync(string issueId, CancellationToken ct)
    {
        _logger.LogInformation("GitHub: Gets Ticket #{id} details from the Repository...", issueId);

        await Task.Delay(800, ct);

        return new Issue { StackTrace = "NullReferenceException at PaymentGateway.cs:42" };
    }

    public async Task<string> CreatePullRequestAsync(string issueId, string code, CancellationToken ct)
    {
        _logger.LogInformation("GitHub: PR for Issue #{id} has been created...", issueId);

        await Task.Delay(1200, ct);

        return $"https://github.com/company/repo/pull/{new Random().Next(1000, 9999)}";
    }

    public async Task EscalateToSeniorAsync(string id, string reason, CancellationToken ct)
    {
        _logger.LogCritical("ESCALATION to Senior Developer: Issue #{id} - Grund: {reason}", id, reason);

        await Task.Delay(500, ct);
    }

    public async Task RequestHumanReviewAsync(string issueId, Solution proposedFix, string correlationId, CancellationToken ct)
    {
        _logger.LogInformation("ACTION REQUIRED: Solution for Issue #{id} with Correlation-ID {CorrelationId} has been created: {ProposedFix}...", issueId, correlationId, proposedFix.PatchedCode);

        await Task.Delay(1200, ct);
    }
}