package com.github.jarol.azure.search.spark.sql.connector.core.utils;

import org.apache.spark.unsafe.types.UTF8String;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Collection of utility methods for strings
 */

public final class StringUtils {

    /**
     * Convert an UTF8String to a simple Java string
     * @param string UTF8 instance
     * @return a simple Java string
     */

    @Contract("_ -> new")
    public static @NotNull String fromUTF8String(
            @NotNull UTF8String string
    ) {

        return new String(
                string.getBytes(),
                StandardCharsets.UTF_8
        );
    }

    /**
     * Return a new string, surrounded both ends by a character
     * @param input input string
     * @param c surrounding character
     * @return the input surrounded both ends by a character
     */

    @Contract(pure = true)
    private static @NotNull String surroundedBy(
            String input,
            char c
    ) {

        return c + input + c;
    }

    /**
     * Get a quoted string
     * @param value string to quote
     * @return the input value wrapped within double quotes
     */

    @Contract(pure = true)
    public static @NotNull String quoted(
            @NotNull String value
    ) {

        return surroundedBy(value, '"');
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

        return surroundedBy(value, '\'');
    }

    /**
     * Create an OData filter that combines other OData filters using logical AND
     * <br>
     * The behavior is
     * <ul>
     *     <li>for an empty list, null is returned</li>
     *     <li>for a single-item list, the first element will be returned as-is</li>
     *     <li>for a multiple-items list, the logical AND of the filters will be returned</li>
     * </ul>
     * @param filters filters to combine
     * @return the combined filter, or null
     */

    public static @Nullable String createODataFilter(
            @NotNull List<String> filters
    ) {

        if (filters.isEmpty()) {
            return null;
        } else {
            return filters.size() == 1 ?
                    filters.get(0) :
                    filters.stream().map(
                            filter -> String.format("(%s)", filter)
                    ).collect(Collectors.joining(" and "));
        }
    }
}
