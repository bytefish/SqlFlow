using System;
using System.Collections.Generic;
using System.Text;

namespace SqlFlowSdk.Management.Models;

public record CreateQueueCommand(string QueueName, string StorageMode = "unpartitioned");

public record CleanupCommand(string QueueName, int TtlSeconds, int Limit = 1000);

public record EmitEventCommand(string QueueName, string EventName, string? PayloadJson = null);

public record CompleteRunCommand(string QueueName, string RunId, string? StateJson = null);

public record FailRunCommand(string QueueName, string RunId, string ReasonJson, DateTimeOffset? RetryAt = null);

public record ScheduleWakeupCommand(string QueueName, string RunId, DateTime WakeAt);

public record SetCheckpointCommand(string QueueName, string TaskId, string OwnerRunId, string StepName, string StateJson, int? ExtendClaimBySeconds = null);

public record ExtendClaimCommand(string QueueName, string RunId, int ExtendBySeconds = 30);

public record CancelTaskCommand(string QueueName, string TaskId);

public record BulkCancelTasksCommand(string QueueName, List<string> TaskIds);

public record ReleaseWorkerClaimsCommand(string QueueName, string WorkerId);

