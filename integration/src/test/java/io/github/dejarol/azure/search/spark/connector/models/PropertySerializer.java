package io.github.dejarol.azure.search.spark.connector.models;

/**
 * Serializer for document properties
 * @param <TProperty> property type
 */

@FunctionalInterface
public interface PropertySerializer<TProperty> {

    /**
     * Serialize a property
     * @param v1 property
     * @return the serialized property
     */

    Object serialize(TProperty v1);
}
