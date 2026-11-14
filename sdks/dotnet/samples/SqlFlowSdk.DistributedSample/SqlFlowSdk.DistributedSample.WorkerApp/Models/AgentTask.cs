// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System.Text.Json.Serialization;

namespace SqlFlowSdk.DistributedSample.WorkerApp.Models;

public class AgentTask
{
    [JsonPropertyName("tenant_id")]
    public string TenantId { get; set; } = "";

    [JsonPropertyName("issue_id")]
    public string IssueId { get; set; } = "";
}