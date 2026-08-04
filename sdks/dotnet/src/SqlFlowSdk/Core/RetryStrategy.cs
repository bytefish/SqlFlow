// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System.Text.Json.Serialization;

namespace SqlFlowSdk.Core;

/// <summary>
/// The Retry Strategy defines how the SqlFlow system should handle retries for a task if it fails. It 
/// can specify the number of retry attempts, the delay between retries, and any backoff strategy to 
/// use.
/// </summary>
public class RetryStrategy
{
    /// <summary>
    /// The Kind of retry strategy to use.
    /// </summary>
    [JsonPropertyName("kind")]
    [JsonConverter(typeof(JsonStringEnumConverter))]
    public RetryStrategyKind Kind { get; set; }

    /// <summary>
    /// Base number of seconds to wait before retrying the task. The actual delay before the 
    /// next retry will be calculated differently depending on the Kind of retry strategy being 
    /// used.
    /// </summary>
    [JsonPropertyName("base_seconds")]
    public double? BaseSeconds { get; set; }

    /// <summary>
    /// Factor to multiply the delay by for each retry attempt. This is used for exponential 
    /// backoff strategies, where the delay increases with each retry attempt.
    /// </summary>
    [JsonPropertyName("factor")]
    public double? Factor { get; set; }

    /// <summary>
    /// Maximum number of seconds to wait before retrying the task. This is used to cap the 
    /// delay between retries.
    /// </summary>
    [JsonPropertyName("max_seconds")]
    public double? MaxSeconds { get; set; }
}
