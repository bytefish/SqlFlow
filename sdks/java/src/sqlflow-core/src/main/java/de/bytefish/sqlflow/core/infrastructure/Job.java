package de.bytefish.sqlflow.core.infrastructure;

@FunctionalInterface
public interface Job<TParams, TResult> {
    
    TResult execute(TaskContext ctx, TParams params) throws Exception;
}