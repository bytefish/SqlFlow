package de.bytefish.sqlflow.core.infrastructure;


import com.fasterxml.jackson.databind.ObjectMapper;
import de.bytefish.sqlflow.core.ISqlFlow;
import de.bytefish.sqlflow.core.models.TaskRegistrationOptions;

/**
 * Provides elegant extension methods to bridge the core functional SDK
 * with strongly-typed Job interfaces and Dependency Injection.
 * This is the exact Java equivalent to the C# SqlFlowExtensions.UseJob method.
 */
public final class SqlFlowExtensions {

    private SqlFlowExtensions() {} // Prevent instantiation

    /**
     * Registers a strongly typed Job class by wrapping it in the functional TaskHandler.
     *
     * @param client             The core SqlFlow client.
     * @param provider           The JobFactory (equivalent to IServiceProvider in .NET) for DI resolution.
     * @param mapper             Jackson ObjectMapper for deserialization.
     * @param taskName           The logical name of the task to register.
     * @param defaultMaxAttempts The default max attempts for retries.
     * @param jobClass           The Class definition of the Job.
     * @param paramsClass        The Class definition of the Parameters expected by the Job.
     */
    public static <TParams, TResult> void useJob(
            ISqlFlow client,
            JobFactory provider,
            ObjectMapper mapper,
            String taskName,
            int defaultMaxAttempts,
            Class<? extends Job<TParams, TResult>> jobClass,
            Class<TParams> paramsClass) {

        TaskRegistrationOptions options = new TaskRegistrationOptions(taskName, defaultMaxAttempts);

        // This is the bridge! We pass a lambda to the core client which resolves the Job at runtime.
        client.registerTask(options, (ctx, jsonParams) -> {

            // 1. Resolve Job from DI Container (JobFactory)
            Job<TParams, TResult> job = provider.getJob(jobClass);

            // 2. Deserialize Parameters cleanly
            TParams typedParams = null;
            if (jsonParams != null && !jsonParams.isNull()) {
                typedParams = mapper.convertValue(jsonParams, paramsClass);
            }

            // 3. Execute Strongly-Typed Job
            return job.execute(ctx, typedParams);
        });
    }
}