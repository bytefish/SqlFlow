// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System.Text;

namespace SqlFlowSdk.AiSample.Services
{
    public interface ILocalNotificationService
    {
        Task NotifyReviewerAsync(string issueId, string correlationId, CancellationToken ct);
    }

    public class LocalNotificationService : ILocalNotificationService
    {
        private readonly ILogger<LocalNotificationService> _logger;

        public LocalNotificationService(ILogger<LocalNotificationService> logger)
        {
            _logger = logger;
        }

        public Task NotifyReviewerAsync(string issueId, string correlationId, CancellationToken ct)
        {
            string notification = new StringBuilder()
            .AppendLine()
            .AppendLine("======================================================")
            .AppendLine("SqlServerFlow LLM AGENT PAUSED...")
            .AppendLine()
            .AppendLine($"The issue '{issueId}' requires human approval.")
            .AppendLine("To approve the code, execute the following request:")
            .AppendLine()
            .AppendLine($"POST http://localhost:5000/agent/review/{issueId}/{correlationId}")
            .AppendLine("Content-Type: application/json")
            .AppendLine()
            .AppendLine("{")
            .AppendLine("  \"approved\": true,")
            .AppendLine("  \"reason\": \"LGTM!\"")
            .AppendLine("}")
            .AppendLine("======================================================")
            .AppendLine()
            .ToString();

            _logger.LogInformation(notification);

            return Task.CompletedTask;
        }
    }
}
