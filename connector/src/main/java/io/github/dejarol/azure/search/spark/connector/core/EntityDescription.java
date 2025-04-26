package io.github.dejarol.azure.search.spark.connector.core;

/**
 * An interface for describing entities
 */

public interface EntityDescription {

    /**
     * Provides a description of the entity.
     * @return a string representing the entity's description
     */

    String description();
}
