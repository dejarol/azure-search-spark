package io.github.dejarol.azure.search.spark.connector.core.utils.json;

import com.fasterxml.jackson.databind.JsonNode;

/**
 * Interface for implementing custom JSON conversions, starting from a {@link com.fasterxml.jackson.databind.JsonNode}
 * @param <T> target deserialization type
 */

public interface JsonConversion<T> {

    /**
     * Gets a user-friendly description of the conversion type
     * @return a description of this instance's type
     */

    String typeDescription();

    /**
     * Defines if the conversion can be applied to the given node
     * @param node node to convert
     * @return true for nodes that support the conversion
     */

    Boolean isDefinedAt(JsonNode node);

    /**
     * Applies the conversion to a node
     * @param node node to convert
     * @return the converted value
     */

    T apply(JsonNode node);
}
