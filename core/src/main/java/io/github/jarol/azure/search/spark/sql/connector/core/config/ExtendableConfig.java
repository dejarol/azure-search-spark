package io.github.jarol.azure.search.spark.sql.connector.core.config;

import org.jetbrains.annotations.NotNull;

/**
 * Mix-in interface for configuration objects that should be updated with some new key-value pairs
 * <br>
 * Concrete implementations should take care of the updating behavior
 * @param <T> configuration type
 */

@FunctionalInterface
public interface ExtendableConfig<T> {

    /**
     * Returns a copy of this configuration, updated by adding a new key-value pair
     * @param key key
     * @param value value
     * @return a copy of this configuration
     */

    @NotNull T withOption(
            @NotNull String key,
            @NotNull String value
    );
}
