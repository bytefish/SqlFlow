package de.bytefish.sqlflow.core.exceptions;

public class SqlFlowException extends RuntimeException {
    public SqlFlowException(String message) {
        super(message);
    }

    public SqlFlowException(String message, Throwable cause) {
        super(message, cause);
    }
}

