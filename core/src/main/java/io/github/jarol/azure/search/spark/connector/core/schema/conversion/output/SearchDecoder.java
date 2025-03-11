package io.github.jarol.azure.search.spark.connector.core.schema.conversion.output;

import java.io.Serializable;

/**
 * Converter from a Spark internal object to Search document property
 */

@FunctionalInterface
public interface SearchDecoder extends Serializable {

    /**
     * Convert a Spark internal value to a Search document property
     * @param value internal value
     * @return a Search document property
     */

    Object apply(Object value);
}
