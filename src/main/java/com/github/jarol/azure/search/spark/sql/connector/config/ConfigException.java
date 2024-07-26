package com.github.jarol.azure.search.spark.sql.connector.config;

import com.github.jarol.azure.search.spark.sql.connector.AzureSparkException;

import java.util.Objects;

public class ConfigException
        extends AzureSparkException {

    public ConfigException(
            String configKey,
            Object configValue,
            String message
    ) {
        this(String.format(
                "Invalid value (%s) for configuration %s%s",
                        configValue, configKey, Objects.isNull(message) ? "" : ". Reason: " + message
                )
        );
    }

    public ConfigException(
            String message
    ) {
        super(message);
    }
}
