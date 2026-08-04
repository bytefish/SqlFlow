// Licensed under the MIT license. See LICENSE file in the project root for full license information.

namespace SqlFlowSdk.Core;

/// <summary>
/// The RetryStrategyKind enum specifies the different types of retry strategies that can be 
/// used, such as fixed delay, exponential backoff, or no retries at all.
/// </summary>
public enum RetryStrategyKind
{
    /// <summary>
    /// Retries the task with a fixed delay between each attempt. The delay is determined by the BaseSeconds property 
    /// of the RetryStrategy. For example, if BaseSeconds is set to 10, the system will wait 10 seconds before retrying 
    /// the task after a failure, and will continue to wait 10 seconds between each subsequent retry attempt until 
    /// the maximum number of attempts is reached.
    /// </summary>
    Fixed,

    /// <summary>
    /// Retries the task with an exponentially increasing delay between each attempt. The delay is calculated using 
    /// the formula: delay = BaseSeconds * (Factor ^ (attempt - 1)), where attempt is the current retry attempt 
    /// number (starting at 1).
    /// </summary>
    Exponential,

    /// <summary>
    /// Use the default retry strategy specified in the RegisteredTask for this task type. If the RegisteredTask does not 
    /// define a default retry strategy, then the task will not be retried at all.
    /// </summary>
    None
}
