package io.github.jarol.azure.search.spark.sql.connector.core.schema.conversion;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Exception thrown in presence of schema violations
 */

public class SchemaViolationException
        extends IllegalArgumentException {

    private final List<SchemaViolation> violations;

    /**
     * Create an instance
     * @param violations schema violations
     */

    public SchemaViolationException(
            List<SchemaViolation> violations
    ) {

        this.violations = violations;
    }

    @Override
    public String getMessage() {

        return String.format(
                "Found %s violations. Description: [%s]",
                violations.size(),
                violations.stream()
                        .map(SchemaViolation::description)
                        .collect(Collectors.joining(", "))
        );
    }
}
