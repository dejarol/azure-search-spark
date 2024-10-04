package com.github.jarol.azure.search.spark.sql.connector.core.schema.conversion;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Objects;

public abstract class AbstractMappingViolation
        implements MappingViolation {

    protected final String name;
    protected final String prefix;
    protected final ViolationType violationType;

    protected AbstractMappingViolation(
            @NotNull String name,
            @Nullable String prefix,
            @NotNull ViolationType violationType

    ) {
        this.prefix = prefix;
        this.name = name;
        this.violationType = violationType;
    }

    @Override
    public final String getFieldName() {

        return Objects.isNull(prefix) ? name : prefix + "." + name;
    }

    @Override
    public final ViolationType getViolationType() {
        return violationType;
    }
}
