package io.github.jarol.azure.search.spark.connector.core.utils;

import org.jetbrains.annotations.NotNull;

import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * Collection of utility methods for dealing with Java objects
 */

public final class JavaCollections {

    /**
     * Convert an iterator to a list
     * @param iterator iterator
     * @param <T> iterator type
     * @return a list
     */

    public static <T> List<T> iteratorToList(
            @NotNull Iterator<T> iterator
    ) {

        Iterable<T> iterable = () -> iterator;
        return StreamSupport.stream(
                iterable.spliterator(), false
        ).collect(Collectors.toList());
    }
}
