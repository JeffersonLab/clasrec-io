package org.jlab.clas.std.services.base;

public class EventReaderException extends Exception {

    public EventReaderException() {
    }

    public EventReaderException(String message) {
        super(message);
    }

    public EventReaderException(Throwable cause) {
        super(cause);
    }

    public EventReaderException(String message, Throwable cause) {
        super(message, cause);
    }
}