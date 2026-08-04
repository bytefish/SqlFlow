// Licensed under the MIT license. See LICENSE file in the project root for full license information.

namespace SqlFlowSdk.Configuration;

/// <summary>
/// Represents configuration options for a job, including its name and the maximum number of execution attempts.
/// </summary>
public class JobOptions
{
    /// <summary>
    /// Gets the name associated with the current instance.
    /// </summary>
    public string Name { get; }

    /// <summary>
    /// Gets or sets the maximum number of attempts allowed for the operation.
    /// </summary>
    /// <remarks>Set this property to control how many times the operation will be retried before failing. A
    /// value less than 1 disables retries.</remarks>
    public int MaxAttempts { get; set; } = 3;

    /// <summary>
    /// Initializes a new instance of the JobOptions class with the specified job name.
    /// </summary>
    /// <param name="name">The name of the job. Cannot be null, empty, or consist only of white-space characters.</param>
    /// <exception cref="ArgumentException">Thrown if name is null, empty, or consists only of white-space characters.</exception>
    public JobOptions(string name) => Name = string.IsNullOrWhiteSpace(name) ? throw new ArgumentException("Name is required", nameof(name)) : name;
}
