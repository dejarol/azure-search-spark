package io.github.dejarol.azure.search.spark.connector.core.utils;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.jetbrains.annotations.Contract;

/**
 * Simple Java-based model for testing JSON deserialization
 */

public class JsonModel {

    private final String id;

    /**
     * Constructor
     * @param id id
     */

    @JsonCreator
    @Contract(pure = true)
    public JsonModel(
            @JsonProperty("id") String id
    ) {
        this.id = id;
    }

    /**
     * Gets the model id
     * @return model id
     */

    public String id() {
        return id;
    }
}
