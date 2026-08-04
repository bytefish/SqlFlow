// Licensed under the MIT license. See LICENSE file in the project root for full license information.

namespace SqlFlowSdk.Configuration;

/// <summary>
/// Provides a fluent builder for configuring and modifying instances of the JobOptions class.
/// </summary>
/// <remarks>Use JobOptionsBuilder to set various options for a job in a chainable manner. This class is typically
/// used to simplify the construction and configuration of JobOptions before submitting or scheduling a job.</remarks>
public class JobOptionsBuilder
{
    /// <summary>
    /// Options being built and configured by this builder. This is the instance of JobOptions that will be modified
    /// </summary>
    private readonly JobOptions _options;

    /// <summary>
    /// Initializes a new instance of the JobOptionsBuilder class with the specified job options.
    /// </summary>
    /// <param name="options">The JobOptions instance to configure. Cannot be null.</param>
    public JobOptionsBuilder(JobOptions options) => _options = options;

    /// <summary>
    /// Sets the maximum number of times a job will be retried after failure.
    /// </summary>
    /// <param name="attempts">The maximum number of retry attempts. Must be greater than or equal to 1.</param>
    /// <returns>The current <see cref="JobOptionsBuilder"/> instance for method chaining.</returns>
    public JobOptionsBuilder WithMaxAttempts(int attempts) { _options.MaxAttempts = attempts; return this; }
}
