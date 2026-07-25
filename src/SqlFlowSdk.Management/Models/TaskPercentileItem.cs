// Licensed under the MIT license. See LICENSE file in the project root for full license information.

namespace SqlFlowSdk.Management.Models;

/// <summary>
/// Represents the percentile statistics for a specific task in the SQL Flow system, including the task name and the 50th, 95th, and 99th percentile execution times in milliseconds.
/// </summary>
/// <param name="TaskName">The name of the task.</param>
/// <param name="P50Ms">The 50th percentile execution time in milliseconds.</param>
/// <param name="P95Ms">The 95th percentile execution time in milliseconds.</param>
/// <param name="P99Ms">The 99th percentile execution time in milliseconds.</param>
public record TaskPercentileItem(string TaskName, double P50Ms, double P95Ms, double P99Ms);
