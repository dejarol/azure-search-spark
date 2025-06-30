package io.github.dejarol.azure.search.spark.connector.core.config;

import org.jetbrains.annotations.NotNull;

import java.util.Map;

/**
 * Mix-in interface for configuration objects that should be updated with some new key-value pairs
 * <br>
 * Concrete implementations should take care of the updating behavior
 * @param <T> configuration type
 */

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

    /**
     * Returns a copy of this configuration, updated by adding multiple new key-value pairs
     * @param options map containing new key-value pairs
     * @return a copy of this configuration
     * @since 0.11.0
     */

    @NotNull T withOptions(
            @NotNull Map<String, String> options
    );
}
