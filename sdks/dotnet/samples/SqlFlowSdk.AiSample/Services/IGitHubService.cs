// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using SqlFlowSdk.AiSample.Models;

namespace SqlFlowSdk.AiSample.Services;

public interface IGitHubService
{
    Task<Issue> GetIssueDetailsAsync(string tenantId, string id, CancellationToken ct);

    Task<string> CreatePullRequestAsync(string tenantId, string id, string code, CancellationToken ct);

    Task RequestHumanReviewAsync(string tenantId, string issueId, Solution proposedFix, string correlationId, CancellationToken ct);

    Task EscalateToSeniorAsync(string tenantId, string id, string reason, CancellationToken ct);
}

public class GitHubService : IGitHubService
{
    private readonly ILogger<GitHubService> _logger;

    public GitHubService(ILogger<GitHubService> logger)
    {
        _logger = logger;
    }

    public async Task<Issue> GetIssueDetailsAsync(string tenantId, string issueId, CancellationToken ct)
    {
        _logger.LogInformation("GitHub [Tenant: {TenantId}]: Gets Ticket #{id} details...", tenantId, issueId);

        await Task.Delay(800, ct);

        return new Issue { StackTrace = "NullReferenceException at PaymentGateway.cs:42" };
    }

    public async Task<string> CreatePullRequestAsync(string tenantId, string issueId, string code, CancellationToken ct)
    {
        _logger.LogInformation("GitHub [Tenant: {TenantId}]: PR for Issue #{id} has been created...", tenantId, issueId);

        await Task.Delay(1200, ct);

        return $"https://github.com/{tenantId}/repo/pull/{new Random().Next(1000, 9999)}";
    }

    public async Task EscalateToSeniorAsync(string tenantId, string id, string reason, CancellationToken ct)
    {
        _logger.LogCritical("ESCALATION [Tenant: {TenantId}] to Senior Developer: Issue #{id} - Reason: {reason}", tenantId, id, reason);

        await Task.Delay(500, ct);
    }

    public async Task RequestHumanReviewAsync(string tenantId, string issueId, Solution proposedFix, string correlationId, CancellationToken ct)
    {
        _logger.LogInformation("ACTION REQUIRED [Tenant: {TenantId}]: Solution for Issue #{id} with Correlation-ID {CorrelationId}: {ProposedFix}...", tenantId, issueId, correlationId, proposedFix.PatchedCode);

        await Task.Delay(1200, ct);
    }
}