package com.github.jarol.azure.search.spark.sql.connector.core.schema.conversion.input;

import java.io.Serializable;

/**
 * An encoder from a Search data object to a Spark internal object
 */

@FunctionalInterface
public interface SearchEncoder
        extends Serializable {

    /**
     * Convert a Search data object to a Spark internal object
     * @param value search data object
     * @return a Spark internal object
     */

    Object apply(Object value);

    /**
     * Compose a new encoder by combining this instance with a second converter, so that
     * this converter is applied first, and the second right afterward (similarly to
     * {@link java.util.function.Function#compose})
     * @param after converter to apply after this instance
     * @return a combined converter
     */

    default SearchEncoder andThen(SearchEncoder after) {

        return (Object value) -> after.apply(this.apply(value));
    }
}
