package com.github.jarol.azure.search.spark.sql.connector.core.utils;

import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;

public final class StringUtils {

    /**
     * Get a single quoted string
     * @param value string to quote
     * @return input string surrounded by single quote marks
     */

    @Contract(pure = true)
    public static @NotNull String singleQuoted(
            @NotNull String value
    ) {

        return '\'' + value + '\'';
    }
}
