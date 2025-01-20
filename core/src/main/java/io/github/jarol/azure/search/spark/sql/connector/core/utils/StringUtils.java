package io.github.jarol.azure.search.spark.sql.connector.core.utils;

import org.apache.spark.unsafe.types.UTF8String;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;

import java.nio.charset.StandardCharsets;

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

    @Contract(pure = true)
    public static @NotNull String removePrefix(
            @NotNull String s,
            @NotNull String prefix
    ) {

        return s.startsWith(prefix) ?
                s.substring(prefix.length()) :
                s;

    }

    public static @NotNull String removeSuffix(
            @NotNull String s,
            @NotNull String suffix
    ) {

        return s.endsWith(suffix) ?
                s.substring(0, s.length() - suffix.length()) :
                s;
    }
}
