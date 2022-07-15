package com.getindata.connectors.http.internal.config;

/**
 * A Runtime exception throw when there is any issue with configuration properties for Http
 * Connector.
 */
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
