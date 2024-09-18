package com.github.jarol.azure.search.spark.sql.connector.schema;

import com.github.jarol.azure.search.spark.sql.connector.IndexDoesNotExistException;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;

/**
 * Exception raised during schema inference. It may occur when
 * <ul>
 *     <li>a Search index does not exist</li>
 *     <li>a Search index has no retrievable (public) field</li>
 * </ul>
 */

public class InferSchemaException
        extends RuntimeException {

    public static final String COULD_NOT_INFER_SCHEMA_PREFIX = "Could not infer schema for index";

    /**
     * Create an instance, reporting also the cause
     * @param name  index name
     * @param cause cause
     */

    public InferSchemaException(
            String name,
            @NotNull Throwable cause
    ) {
        super(String.format("%s %s. Reason: (%s)",
                COULD_NOT_INFER_SCHEMA_PREFIX, name, cause.getMessage()),
                cause
        );
    }

    /**
     * Create an instance caused by a non-existing index
     * @param name index name
     * @return an instance
     */

    @Contract("_ -> new")
    public static @NotNull InferSchemaException forNonExistingIndex(
            String name
    ) {
        return new InferSchemaException(
                name,
                new IndexDoesNotExistException(name)
        );
    }

    /**
     * Create an instance caused by a Search index with no public fields
     * @param name index name
     * @return an instance
     */

    @Contract("_ -> new")
    public static @NotNull InferSchemaException forIndexWithNoRetrievableFields(
            String name
    ) {
        return new InferSchemaException(
                name,
                new IllegalStateException(
                        String.format("No retrievable field found for index %s", name)
                )
        );
    }
}
