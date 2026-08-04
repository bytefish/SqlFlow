// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using SqlFlowSdk.AiSample.Models;
using SqlFlowSdk.AiSample.Services;
using SqlFlowSdk.Core;
using System.Text.Json.Nodes;

namespace SqlFlowSdk.AiSample;

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
        _logger.LogInformation("Agent starts researching ticket {IssueId}", task.IssueId);

        // Load the Issue Context first, so the LLM has all relevant information
        var bugReport = await ctx.Step("fetch-issue-context", async () =>
            await _gitHubService.GetIssueDetailsAsync(task.IssueId, ctx.CancellationToken));

        bool isApproved = false;
        int attempt = 0;

        string lastFeedback = "Initial Attempt";

        while (!isApproved && attempt < 3)
        {
            attempt++;

            // Generate CorrelationID for the Event
            string correlationId = $"{ctx.TaskId}-attempt-{attempt}";

            _logger.LogInformation("Attempt {attempt}/3: Generating a fix based on: {feedback}", attempt, lastFeedback);

            Solution proposedFix = await ctx.Step($"generate-code-fix-{attempt}", async () =>
                await _llmService.GenerateFixAsync(bugReport.StackTrace, lastFeedback, ctx.CancellationToken));

            await ctx.Step($"notify-reviewer-{attempt}", async () => {
                // Notify the reviewer via GitHub service, which could post a link to a GitHub issue or PR for review
                await _gitHubService.RequestHumanReviewAsync(task.IssueId, proposedFix, correlationId, ctx.CancellationToken);
                // Notify the reviewer via local notification service
                await _localNotificationService.NotifyReviewerAsync(task.IssueId, correlationId, ctx.CancellationToken);
            });

            _logger.LogInformation("Review for {CorrelationId} has been requested. Agent goes idle and waits for the code review...", correlationId);

            // Wair for a human decision without blocking a thread
            JsonNode? review = await ctx.AwaitEvent(
                eventName: $"agent-approval:{task.IssueId}:{correlationId}",
                stepName: $"wait-for-human-review-{attempt}"
            );
            
            isApproved = review["approved"]?.GetValue<bool>() ?? false;
            lastFeedback = review["reason"]?.GetValue<string>() ?? "No feedback has been given";

            if (!isApproved)
            {
                _logger.LogWarning("Attempt {attempt} has been rejected: {reason}", attempt, lastFeedback);
            }
        }

        if (isApproved)
        {
            _logger.LogInformation("Fix approved. Creating Pull Request...");

            string prUrl = await ctx.Step("create-pull-request", async () =>
            {
                return await _gitHubService.CreatePullRequestAsync(task.IssueId, "apply-fix", ctx.CancellationToken);
            });

            _logger.LogInformation("Mission accomplished, the PR has been created: {Url}", prUrl);

            return new AgentResult { Success = true, PullRequestUrl = prUrl };
        }
        else
        {
            _logger.LogError("Maximum number of attempts reached. Escalates ticket {IssueId} to a human.", task.IssueId);

            await ctx.Step("notify-senior-developer", async () =>
            {
                await _gitHubService.EscalateToSeniorAsync(task.IssueId, "Agent didn't find a solution after 3 attempts.", ctx.CancellationToken);
            });

            return new AgentResult { Success = false, Reason = "Escalated to human supervisor after 3 failures." };
        }
    }
}