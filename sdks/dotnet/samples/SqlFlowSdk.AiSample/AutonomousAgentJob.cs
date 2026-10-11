
using SqlFlowSdk.AiSample.Models;
using SqlFlowSdk.AiSample.Services;
using SqlFlowSdk.Core;
using System.Text.Json.Nodes;

public class AutonomousAgentJob : IJob<AgentTask, AgentResult>
{
    private readonly ILogger<AutonomousAgentJob> _logger;

    private readonly ILlmService _llmService;
    private readonly IGitHubService _gitHubService;
    private readonly ILocalNotificationService _localNotificationService;

    public AutonomousAgentJob(ILogger<AutonomousAgentJob> logger, ILlmService llmService, IGitHubService gitHubService, ILocalNotificationService localNotificationService)
    {
        _logger = logger;
        _llmService = llmService;
        _gitHubService = gitHubService;
        _localNotificationService = localNotificationService;
    }

    public async Task<AgentResult> ExecuteAsync(TaskContext ctx, AgentTask task)
    {
        _logger.LogInformation("Agent starts researching ticket {IssueId} for tenant {TenantId}", task.IssueId, task.TenantId);

        // Load the Issue Context first, so the LLM has all relevant information
        var bugReport = await ctx.Step("fetch-issue-context", async () =>
            await _gitHubService.GetIssueDetailsAsync(task.TenantId, task.IssueId, ctx.CancellationToken));

        bool isApproved = false;
        int attempt = 0;

        string lastFeedback = "Initial Attempt";

        while (!isApproved && attempt < 3)
        {
            attempt++;

            // Generate CorrelationID for the Event
            string correlationId = $"{ctx.TaskId}-attempt-{attempt}";

            _logger.LogInformation("Attempt {attempt}/3: Generating a fix for tenant {TenantId} based on: {feedback}", attempt, task.TenantId, lastFeedback);

            Solution proposedFix = await ctx.Step($"generate-code-fix-{attempt}", async () =>
                await _llmService.GenerateFixAsync(task.TenantId, bugReport.StackTrace, lastFeedback, ctx.CancellationToken));

            await ctx.Step($"notify-reviewer-{attempt}", async () =>
            {
                // Notify the reviewer via GitHub service
                await _gitHubService.RequestHumanReviewAsync(task.TenantId, task.IssueId, proposedFix, correlationId, ctx.CancellationToken);
                // Notify the reviewer via local notification service
                await _localNotificationService.NotifyReviewerAsync(task.TenantId, task.IssueId, correlationId, ctx.CancellationToken);
            });

            _logger.LogInformation("Review for tenant {TenantId}, CorrelationId {CorrelationId} has been requested. Agent goes idle and waits...", task.TenantId, correlationId);

            // Wait for a human decision without blocking a thread
            JsonNode? review = await ctx.AwaitEvent(
                eventName: $"agent-approval:{task.TenantId}:{task.IssueId}:{correlationId}",
                stepName: $"wait-for-human-review-{attempt}"
            );

            isApproved = review["approved"]?.GetValue<bool>() ?? false;
            lastFeedback = review["reason"]?.GetValue<string>() ?? "No feedback has been given";

            if (!isApproved)
            {
                _logger.LogWarning("Attempt {attempt} for tenant {TenantId} has been rejected: {reason}", attempt, task.TenantId, lastFeedback);
            }
        }

        if (isApproved)
        {
            _logger.LogInformation("Fix approved for tenant {TenantId}. Creating Pull Request...", task.TenantId);

            string prUrl = await ctx.Step("create-pull-request", async () =>
            {
                return await _gitHubService.CreatePullRequestAsync(task.TenantId, task.IssueId, "apply-fix", ctx.CancellationToken);
            });

            _logger.LogInformation("Mission accomplished for tenant {TenantId}, the PR has been created: {Url}", task.TenantId, prUrl);

            return new AgentResult { Success = true, PullRequestUrl = prUrl };
        }
        else
        {
            _logger.LogError("Maximum attempts reached. Escalates ticket {IssueId} for tenant {TenantId} to a human.", task.IssueId, task.TenantId);

            await ctx.Step("notify-senior-developer", async () =>
            {
                await _gitHubService.EscalateToSeniorAsync(task.TenantId, task.IssueId, "Agent didn't find a solution after 3 attempts.", ctx.CancellationToken);
            });

            return new AgentResult { Success = false, Reason = "Escalated to human supervisor after 3 failures." };
        }
    }
}