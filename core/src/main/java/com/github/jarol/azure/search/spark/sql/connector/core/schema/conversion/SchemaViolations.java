package com.github.jarol.azure.search.spark.sql.connector.core.schema.conversion;

import com.azure.search.documents.indexes.models.SearchFieldDataType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructField;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;

import java.util.List;

/**
 * Collection of methods for creating instances of {@link SchemaViolation}(s)
 */

public final class SchemaViolations {

    /**
     * Violation for namesake fields with incompatible data types
     */

    private static class IncompatibleType
            extends SchemaViolationImpl {

        private final DataType sparkType;
        private final SearchFieldDataType searchType;

        /**
         * Create an instance
         * @param name field name
         * @param sparkType Spark type
         * @param searchType Search type
         */

        public IncompatibleType(
                String name,
                @NotNull DataType sparkType,
                @NotNull SearchFieldDataType searchType
        ) {

            super(name, Type.INCOMPATIBLE_TYPE);
            this.sparkType = sparkType;
            this.searchType = searchType;
        }
    }

    /**
     * Violation caused by an incompatible nested field
     */

    private static class ComplexFieldViolation
            extends SchemaViolationImpl {

        private final List<SchemaViolation> subFieldViolations;

        /**
         * Create an instance
         * @param name field name
         * @param subFieldViolations violations on subFields
         */

        @Contract(pure = true)
        public ComplexFieldViolation(
                @NotNull String name,
                List<SchemaViolation> subFieldViolations
        ) {
            super(name, Type.INCOMPATIBLE_NESTED_FIELD);
            this.subFieldViolations = subFieldViolations;
        }
    }

    /**
     * Violation for a collection field with incompatible inner type
     */

    private static class ArrayViolation
            extends SchemaViolationImpl {

        private final SchemaViolation subtypeViolation;

        /**
         * Create an instance
         * @param name field name
         * @param subtypeViolation inner type violation
         */

        public ArrayViolation(
                @NotNull String name,
                SchemaViolation subtypeViolation
        ) {
            super(name, Type.INCOMPATIBLE_ARRAY_TYPE);
            this.subtypeViolation = subtypeViolation;
        }

        /**
         * Get the inner type violation
         * @return inner type violation
         */

        public SchemaViolation getSubtypeViolation() {
            return subtypeViolation;
        }
    }

    /**
     * Create a schema violation for a missing field
     * @param field missing field
     * @return a violation instance
     */

    @Contract("_ -> new")
    public static @NotNull SchemaViolation forMissingField(
            @NotNull StructField field
    ) {

        return new SchemaViolationImpl(
                field.name(),
                SchemaViolation.Type.MISSING_FIELD
        );
    }

    /**
     * Create a schema violation for a Spark field which is not a good candidate for being Search GeoPoint
     * @param field Spark field (usually a StructType)
     * @return a violation instance
     */

    @Contract("_ -> new")
    public static @NotNull SchemaViolation forIncompatibleGeoPoint(
            @NotNull StructField field
    ) {

        return new SchemaViolationImpl(
                field.name(),
                SchemaViolation.Type.NOT_SUITABLE_AS_GEOPOINT
        );
    }

    /**
     * Create a new instance for namesake fields with incompatible data types
     * @param name field name
     * @param sparkType Spark type
     * @param searchType Search type
     * @return a violation instance
     */

    @Contract("_, _, _ -> new")
    public static @NotNull SchemaViolation forNamesakeButIncompatibleFields(
            String name,
            @NotNull DataType sparkType,
            @NotNull SearchFieldDataType searchType
    ) {

        return new IncompatibleType(name, sparkType, searchType);
    }

    /**
     * Create a new instance for a complex field
     * @param name name
     * @param subFieldViolations violations on subFields
     * @return a violation instance
     */

    @Contract(value = "_, _ -> new", pure = true)
    public static @NotNull SchemaViolation forComplexField(
            String name,
            List<SchemaViolation> subFieldViolations
    ) {

        return new ComplexFieldViolation(
                name,
                subFieldViolations
        );
    }

    /**
     * Create a new instance for an array field
     * @param name name
     * @param subtypeViolation violation for array inner type
     * @return a violation instance
     */

    @Contract("_, _ -> new")
    public static @NotNull SchemaViolation forArrayField(
            @NotNull String name,
            SchemaViolation subtypeViolation
    ) {

        return new ArrayViolation(name, subtypeViolation);
    }
}
