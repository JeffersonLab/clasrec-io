package org.jlab.clas.std.services.base;

public class EventWriterException extends Exception {

    public EventWriterException() {
    }

    public EventWriterException(String message) {
        super(message);
    }

    public EventWriterException(Throwable cause) {
        super(cause);
    }

    public EventWriterException(String message, Throwable cause) {
        super(message, cause);
    }
}