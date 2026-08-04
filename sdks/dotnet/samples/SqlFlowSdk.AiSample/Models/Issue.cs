// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System.Text.Json.Serialization;

namespace SqlFlowSdk.AiSample.Models;

public class Issue 
{
    [JsonPropertyName("stack_trace")]
    public string StackTrace { get; set; } = ""; 
}