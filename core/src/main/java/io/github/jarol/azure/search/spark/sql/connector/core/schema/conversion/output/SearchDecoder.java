package io.github.jarol.azure.search.spark.sql.connector.core.schema.conversion.output;

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

    /**
     * Combine this decoder with another
     * <br>
     * This instance's transformation will be applied first, and the <b>after</b> transformation later
     * @param after transformation to apply right after the one defined by this instance
     * @return a combined decoder
     */

    default SearchDecoder andThen(SearchDecoder after) {

        return (value -> after.apply(this.apply(value)));
    }
}
