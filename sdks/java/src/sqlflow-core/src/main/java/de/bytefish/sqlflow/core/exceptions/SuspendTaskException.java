package de.bytefish.sqlflow.core.exceptions;

public class SuspendTaskException extends RuntimeException {
        public SuspendTaskException() { super("Task suspended."); }
    }
