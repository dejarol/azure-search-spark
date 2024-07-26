package com.github.jarol.azure.search.spark.sql.connector.config;

import com.github.jarol.azure.search.spark.sql.connector.AzureSparkException;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;

import java.util.Objects;

/**
 * Exception related to datasource configuration issues
 */

public class ConfigException
        extends AzureSparkException {

    static final String MISSING_REQUIRED_OPTION_PREFIX = "Missing required option";
    static final String INVALID_VALUE_PREFIX = "Invalid value";

    /**
     * Create an instance
     * @param key configuration key
     * @param value configuration value
     * @param cause exception cause
     */

    public ConfigException(
            String key,
            Object value,
            Throwable cause
    ) {
        super(String.format(
                "%s (%s) for configuration %s%s",
                        INVALID_VALUE_PREFIX,
                        value,
                        key,
                        Objects.isNull(cause.getMessage()) ?
                                "" :
                                ". Reason: " + cause.getMessage()
                ),
                cause
        );
    }

    public ConfigException(
            String message
    ) {
        super(message);
    }

    @Contract("_ -> new")
    public static @NotNull ConfigException missingKey(
            String key
    ) {

        return new ConfigException(
                String.format("%s (%s)",
                        MISSING_REQUIRED_OPTION_PREFIX, key)
        );
    }
}
