package io.github.jarol.azure.search.spark.sql.connector.core.schema.conversion;

import com.azure.search.documents.indexes.models.SearchFieldDataType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructField;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Collection of methods for creating instances of {@link SchemaViolation}(s)
 */

public final class SchemaViolations {

    /**
     * Violation for namesake fields with incompatible data types
     */

     static class IncompatibleType
            extends AbstractSchemaViolation {

        private final DataType sparkType;
        private final SearchFieldDataType searchType;

        /**
         * Create an instance
         * @param name field name
         * @param sparkType Spark type
         * @param searchType Search type
         */

        private IncompatibleType(
                String name,
                @NotNull DataType sparkType,
                @NotNull SearchFieldDataType searchType
        ) {

            super(name, Type.INCOMPATIBLE_TYPE);
            this.sparkType = sparkType;
            this.searchType = searchType;
        }

        @Override
        protected @Nullable String detailsDescription() {

            return String.format(
                    "{\"sparkType\": \"%s\", \"searchType\": \"%s\"}",
                    sparkType, searchType
            );
        }
    }

    /**
     * Violation caused by an incompatible nested field
     */

    static class ComplexFieldViolation
            extends AbstractSchemaViolation {

        private final List<SchemaViolation> subFieldViolations;

        /**
         * Create an instance
         * @param name field name
         * @param subFieldViolations violations on subFields
         */

        @Contract(pure = true)
        private ComplexFieldViolation(
                @NotNull String name,
                List<SchemaViolation> subFieldViolations
        ) {
            super(name, Type.INCOMPATIBLE_COMPLEX_FIELD);
            this.subFieldViolations = subFieldViolations;
        }

        /**
         * Get subField violations
         * @return subField violations
         */

        public List<SchemaViolation> getSubFieldViolations() {
            return subFieldViolations;
        }

        @Override
        protected @Nullable String detailsDescription() {
            return String.format(
                    "{\"subViolations\": [%s]}",
                    subFieldViolations.stream()
                            .map(SchemaViolation::description)
                            .collect(Collectors.joining(", "))
            );
        }
    }

    /**
     * Violation for a collection field with incompatible inner type
     */

    static class ArrayViolation
            extends AbstractSchemaViolation {

        private final SchemaViolation subtypeViolation;

        /**
         * Create an instance
         * @param name field name
         * @param subtypeViolation inner type violation
         */

        private ArrayViolation(
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

        @Override
        protected @Nullable String detailsDescription() {

            return String.format(
                    "{\"subTypeViolation\": %s}",
                    subtypeViolation.description()
            );
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

        return new AbstractSchemaViolation(
                field.name(),
                SchemaViolation.Type.MISSING_FIELD
        ) {
            @Override
            protected @Nullable String detailsDescription() {
                return null;
            }
        };
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

        return new AbstractSchemaViolation(
                field.name(),
                SchemaViolation.Type.NOT_SUITABLE_AS_GEOPOINT
        ) {
            @Override
            protected @Nullable String detailsDescription() {
                return null;
            }
        };
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
