// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System.Text.Json.Serialization;

namespace SqlFlowSdk.AiSample.Models;

public class HumanApproval 
{
    [JsonPropertyName("approved")]
    public bool Approved { get; set; }

    [JsonPropertyName("reason")]
    public string? Reason { get; set; } 
}