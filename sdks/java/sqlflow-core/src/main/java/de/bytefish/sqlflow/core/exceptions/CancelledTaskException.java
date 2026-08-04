package de.bytefish.sqlflow.core.exceptions;

public class CancelledTaskException extends RuntimeException {
        public CancelledTaskException() { super("Task cancelled."); }
    }