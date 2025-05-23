package io.github.dejarol.azure.search.spark.connector.write.decoding;

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
