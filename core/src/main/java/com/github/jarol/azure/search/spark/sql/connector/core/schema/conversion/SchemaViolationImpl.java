package com.github.jarol.azure.search.spark.sql.connector.core.schema.conversion;

import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;

/**
 * Parent class for all schema violations
 */

public class SchemaViolationImpl
        implements SchemaViolation {

    protected final String name;
    protected final Type type;

    /**
     * Create an instance
     * @param name field name
     * @param type violation type
     */

    @Contract(pure = true)
    protected SchemaViolationImpl(
            @NotNull String name,
            @NotNull SchemaViolation.Type type

    ) {
        this.name = name;
        this.type = type;
    }

    @Override
    public final String getFieldName() {

        return name;
    }

    @Override
    public final Type getType() {
        return type;
    }
}
