package com.github.jarol.azure.search.spark.sql.connector.core.schema.conversion;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.List;

public class MappingViolationException
        extends IllegalArgumentException {

    private final List<SchemaViolation> violations;
    private final String description;

    public MappingViolationException(
            List<SchemaViolation> violations
    ) throws JsonProcessingException {

        this.violations = violations;
        this.description = new ObjectMapper().writeValueAsString(violations);
    }

    @Override
    public String getMessage() {

        return String.format(
                "Found %s violations. Description: %s",
                violations.size(),
                description
        );
    }
}
