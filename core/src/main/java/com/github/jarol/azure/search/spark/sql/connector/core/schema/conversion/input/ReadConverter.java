package com.github.jarol.azure.search.spark.sql.connector.core.schema.conversion.input;

import java.io.Serializable;

/**
 * A converter from a Search data object to a Spark internal object
 */

@FunctionalInterface
public interface ReadConverter
        extends Serializable {

    /**
     * Convert a Search data object to a Spark internal object
     * @param value search data object
     * @return a Spark internal object
     */

    Object apply(Object value);

    default ReadConverter andThen(ReadConverter after) {

        return (Object value) -> after.apply(apply(value));
    }
}
