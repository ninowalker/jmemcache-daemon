package com.thimbleware.jmemcached.protocol.exceptions;

/**
 */
public class IncorrectlyTerminatedPayloadException extends ClientException {
    public IncorrectlyTerminatedPayloadException() {
    }

    public IncorrectlyTerminatedPayloadException(String s) {
        super(s);
    }

    public IncorrectlyTerminatedPayloadException(String s, Throwable throwable) {
        super(s, throwable);
    }

    public IncorrectlyTerminatedPayloadException(Throwable throwable) {
        super(throwable);
    }
}
