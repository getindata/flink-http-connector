package com.getindata.connectors.http.internal.config;

// TODO EXP-98 add Javadoc
public class ConfigException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public ConfigException(String message) {
        super(message);
    }

    public ConfigException(String message, Throwable t) {
        super(message, t);
    }

    public ConfigException(String name, Object value, String message) {
        super("Invalid value " + value + " for configuration " + name + (message == null ? ""
            : ": " + message));
    }
}
