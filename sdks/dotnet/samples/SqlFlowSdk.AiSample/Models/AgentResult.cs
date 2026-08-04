// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System.Text.Json.Serialization;

namespace SqlFlowSdk.AiSample.Models;

public class AgentResult
{
    [JsonPropertyName("success")]
    public bool Success { get; set; }

    [JsonPropertyName("pull_request_url")]
    public string? PullRequestUrl { get; set; }

    [JsonPropertyName("reason")]
    public string? Reason { get; set; }
}