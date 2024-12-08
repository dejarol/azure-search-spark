package com.github.jarol.azure.search.spark.sql.connector.core.utils;

import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;

public final class StringUtils {

    /**
     * Get a quoted string
     * @param value string to quote
     * @return the input value wrapped within double quotes
     */

    @Contract(pure = true)
    public static @NotNull String quoted(
            @NotNull String value
    ) {

        return "\"" + value + "\"";
    }

    /**
     * Get a single quoted string
     * @param value string to quote
     * @return input string wrapped within single quotes
     */

    @Contract(pure = true)
    public static @NotNull String singleQuoted(
            @NotNull String value
    ) {

        return '\'' + value + '\'';
    }
}
