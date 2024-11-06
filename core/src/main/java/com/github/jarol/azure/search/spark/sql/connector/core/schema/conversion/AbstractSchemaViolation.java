package com.github.jarol.azure.search.spark.sql.connector.core.schema.conversion;

import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Optional;

/**
 * Parent class for all schema violations
 */

public abstract class AbstractSchemaViolation
        implements SchemaViolation {

    protected final String name;
    protected final Type type;

    /**
     * Create an instance
     * @param name field name
     * @param type violation type
     */

    @Contract(pure = true)
    protected AbstractSchemaViolation(
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

    @Override
    public final @NotNull String description() {

        String details = Optional.ofNullable(detailsDescription())
                .map(s -> String.format(", \"details\": %s", s))
                .orElse("");

        return String.format("{\"%s\": \"%s\"%s}",
                name, type.name(), details
        );
    }

    protected abstract @Nullable String detailsDescription();
}
