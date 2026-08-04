// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System.Text.Json.Nodes;

namespace SqlFlowSdk.Core;

/// <summary>
/// A CheckpointRow represents the state of a checkpoint for a specific task and run. It contains the checkpoint name, 
/// the current state of the checkpoint (as a JSON node), the status of the checkpoint (e.g. "pending", "completed", 
/// "failed"), the ID of the run that owns the checkpoint, and the timestamp of when the checkpoint was last updated.
/// </summary>
public class CheckpointRow
{
    /// <summary>
    /// Name of the checkpoint. This is used to identify different checkpoints within the same task and run. It must be 
    /// unique for each checkpoint within the same task and run, but can be reused across different tasks and runs.
    /// </summary>
    public required string CheckpointName { get; set; }

    /// <summary>
    /// State of the checkpoint. This is an arbitrary JSON node that can be used to store any information relevant 
    /// to the checkpoint.
    /// </summary>
    public JsonNode? State { get; set; }

    /// <summary>
    /// Status of the checkpoint. This is a string that represents the current status of the checkpoint, such as "pending", 
    /// "completed", "failed", etc. The specific values and their meanings are determined by the application using the 
    /// system.
    /// </summary>
    public required string Status { get; set; }

    /// <summary>
    /// Owner of the checkpoint. This is the ID of the run that currently owns the checkpoint. This is used to ensure that 
    /// only one run can update the checkpoint at a time, and to manage checkpoint leases. When a run claims a checkpoint, 
    /// it becomes the owner of that checkpoint until it releases it or until the lease expires.
    /// </summary>
    public string? OwnerRunId { get; set; }

    /// <summary>
    /// Timestamp the checkpoint was last updated. This is used to manage checkpoint leases and to determine when a 
    /// checkpoint should be considered expired. If a checkpoint has an owner and the current time exceeds the last 
    /// updated time plus the lease duration, the checkpoint is considered expired.
    /// </summary>
    public DateTime UpdatedAt { get; set; }
}
