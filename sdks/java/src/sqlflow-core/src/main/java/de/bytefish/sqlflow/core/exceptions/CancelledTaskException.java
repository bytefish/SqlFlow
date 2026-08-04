package de.bytefish.sqlflow.core.exceptions;

public class CancelledTaskException extends SqlFlowException {
    public CancelledTaskException() {
        super("Task cancelled due to thread interruption.");
    }
}
