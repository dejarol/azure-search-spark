package com.github.jarol.azure.search.spark.sql.connector.core.schema.conversion;

import com.azure.search.documents.indexes.models.SearchField;
import org.apache.spark.sql.types.StructField;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;

import java.util.List;

/**
 * Collection of {@link SchemaViolation}(s)
 */

public final class SchemaViolations {

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
     * Instance for namesake fields with incompatible data types
     */

    @SuppressWarnings("unused")
    public static class IncompatibleType
            extends SchemaViolationImpl {

        private final String sparkType;
        private final String searchType;

        /**
         * Create an instance
         * @param schemaField Spark field
         * @param searchField Search field
         */

        public IncompatibleType(
                @NotNull StructField schemaField,
                @NotNull SearchField searchField
        ) {

            super(schemaField.name(), Type.INCOMPATIBLE_TYPE);
            this.sparkType = schemaField.dataType().sql();
            this.searchType = searchField.getType().toString();
        }

        /**
         * Get the data type of the Spark field
         * @return data type of the Spark field
         */

        public String getSparkType() {
            return sparkType;
        }

        /**
         * Get the data type of the Search field
         * @return data type of the Search field
         */

        public String getSearchType() {
            return searchType;
        }
    }

    /**
     * Violation caused by an incompatible nested field
     */

    public static class IncompatibleNestedField
            extends SchemaViolationImpl {

        private final List<SchemaViolation> subFieldViolations;

        /**
         * Create an instance
         * @param name field name
         * @param subFieldViolations violations on subFields
         */

        @Contract(pure = true)
        public IncompatibleNestedField(
                @NotNull String name,
                List<SchemaViolation> subFieldViolations
        ) {
            super(name, Type.INCOMPATIBLE_NESTED_FIELD);
            this.subFieldViolations = subFieldViolations;
        }

        public List<SchemaViolation> getSubFieldViolations() {
            return subFieldViolations;
        }
    }

    /**
     * Violation for a collection field with incompatible inner type
     */

    public static class ArrayViolation
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
}
