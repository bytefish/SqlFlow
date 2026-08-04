package de.bytefish.sqlflow.core.exceptions;

public class SuspendTaskException extends SqlFlowException {
    public SuspendTaskException() {
        super("Task suspended.");
    }

    @Override
    public synchronized Throwable fillInStackTrace() {
        return this; // Do not generate stack trace
    }
}
