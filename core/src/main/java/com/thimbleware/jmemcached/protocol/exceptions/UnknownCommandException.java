package com.thimbleware.jmemcached.protocol.exceptions;

/**
 */
public class UnknownCommandException extends ClientException {

    public UnknownCommandException() {
    }

    public UnknownCommandException(String s) {
        super(s);
    }

    public UnknownCommandException(String s, Throwable throwable) {
        super(s, throwable);
    }

    public UnknownCommandException(Throwable throwable) {
        super(throwable);
    }
}
