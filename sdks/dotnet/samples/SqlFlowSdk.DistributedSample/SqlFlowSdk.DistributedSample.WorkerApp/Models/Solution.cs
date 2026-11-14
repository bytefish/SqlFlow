// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System.Text.Json.Serialization;

namespace SqlFlowSdk.DistributedSample.WorkerApp.Models;

public class Solution 
{
    [JsonPropertyName("patched_code")]
    public string PatchedCode { get; set; } = ""; 
}