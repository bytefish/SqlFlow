using System;
using System.Collections.Generic;
using System.Text;

namespace SqlFlowSdk.Management.Models;

public record CreateQueueCommand(string TenantId, string QueueName, string StorageMode = "unpartitioned");

public record CleanupCommand(string TenantId, string QueueName, int TtlSeconds, int Limit = 1000);

public record EmitEventCommand(string TenantId, string QueueName, string EventName, string? PayloadJson = null);

public record CompleteRunCommand(string TenantId, string QueueName, string RunId, string? StateJson = null);

public record FailRunCommand(string TenantId, string QueueName, string RunId, string ReasonJson, DateTimeOffset? RetryAt = null);

public record ScheduleWakeupCommand(string TenantId, string QueueName, string RunId, DateTime WakeAt);

public record SetCheckpointCommand(string TenantId, string QueueName, string TaskId, string OwnerRunId, string StepName, string StateJson, int? ExtendClaimBySeconds = null);

public record ExtendClaimCommand(string TenantId, string QueueName, string RunId, int ExtendBySeconds = 30);

public record CancelTaskCommand(string TenantId, string QueueName, string TaskId);

public record BulkCancelTasksCommand(string TenantId, string QueueName, List<string> TaskIds);

public record ReleaseWorkerClaimsCommand(string TenantId, string QueueName, string WorkerId);