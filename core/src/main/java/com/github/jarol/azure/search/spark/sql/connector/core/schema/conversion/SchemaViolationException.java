package com.github.jarol.azure.search.spark.sql.connector.core.schema.conversion;

import java.util.List;

public class SchemaViolationException
        extends IllegalArgumentException {

    private final List<SchemaViolation> violations;

    public SchemaViolationException(
            List<SchemaViolation> violations
    ) {

        this.violations = violations;
    }

    @Override
    public String getMessage() {

        // TODO: working on message rendering
        return String.format(
                "Found %s violations. Description: %s",
                violations.size(),
                violations
        );
    }
}
