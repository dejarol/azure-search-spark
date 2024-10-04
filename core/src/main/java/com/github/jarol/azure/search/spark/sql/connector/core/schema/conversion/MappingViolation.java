package com.github.jarol.azure.search.spark.sql.connector.core.schema.conversion;

public interface MappingViolation {

    enum ViolationType {

        MISSING_FIELD,
        INCOMPATIBLE_TYPE,
        INCOMPATIBLE_ARRAY_TYPE,
        INCOMPATIBLE_NESTED_FIELD,
        NOT_SUITABLE_AS_GEOPOINT
    }

    String getFieldName();

    ViolationType getViolationType();
}
