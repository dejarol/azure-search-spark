package com.github.jarol.azure.search.spark.sql.connector.schema;

import org.jetbrains.annotations.NotNull;

/**
 * Exception raised when a Schema incompatibility is met
 * <p>
 * It may happen due to
 * <ul>
 *     <li>a non existing schema field (i.e. a schema field that does not exist on a Search index)</li>
 *     <li>an existing schema field with incompatible datatype with respect to its namesake Search field</li>
 * </ul>
 */

public class SchemaCompatibilityException
        extends IllegalStateException {

    /**
     * Create an instance
     * @param message exception message
     */

    public SchemaCompatibilityException(
            @NotNull String message
    ) {
        super(message);
    }
}
