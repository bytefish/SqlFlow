package de.bytefish.sqlflow.core.exceptions;

public class TimeoutErrorException extends SqlFlowException {
    public TimeoutErrorException(String message) {
        super(message);
    }
}
