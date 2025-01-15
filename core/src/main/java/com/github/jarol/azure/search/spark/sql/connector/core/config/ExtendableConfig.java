package com.github.jarol.azure.search.spark.sql.connector.core.config;

import org.jetbrains.annotations.NotNull;

@FunctionalInterface
public interface ExtendableConfig<T> {

    @NotNull T withOption(
            @NotNull String key,
            @NotNull String value
    );
}
