package com.github.jarol.azure.search.spark.sql.connector;

/**
 * Deserializer for document properties
 * @param <TValue>> property type
 */

@FunctionalInterface
public interface PropertyDeserializer<TValue> {

    /**
     * Deserialize a property value
     * @param value property value
     * @return the deserialized property
     */

    TValue deserialize(
            Object value
    );
}
