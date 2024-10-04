package com.github.jarol.azure.search.spark.sql.connector.core.schema.conversion;

import com.azure.search.documents.indexes.models.SearchField;
import org.apache.spark.sql.types.StructField;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.List;

public final class MappingViolations {

    public static class MissingField
            extends AbstractMappingViolation {

        public MissingField(
                @NotNull StructField field,
                @Nullable String prefix
        ) {

            super(field.name(), prefix, ViolationType.MISSING_FIELD);
        }
    }

    public static class IncompatibleType
            extends AbstractMappingViolation {

        private final String sparkType;
        private final String searchType;

        public IncompatibleType(
                @NotNull StructField schemaField,
                @NotNull SearchField searchField,
                @Nullable String prefix
        ) {

            super(schemaField.name(), prefix, ViolationType.INCOMPATIBLE_TYPE);
            this.sparkType = schemaField.dataType().sql();
            this.searchType = searchField.getType().toString();
        }

        public String getSparkType() {
            return sparkType;
        }

        public String getSearchType() {
            return searchType;
        }
    }

    public static class IncompatibleNestedField
            extends AbstractMappingViolation {

        private final List<MappingViolation> subFieldViolations;

        @Contract(pure = true)
        public IncompatibleNestedField(
                @NotNull StructField field,
                List<MappingViolation> subFieldViolations,
                @Nullable String prefix
        ) {
            super(field.name(), prefix, ViolationType.INCOMPATIBLE_NESTED_FIELD);
            this.subFieldViolations = subFieldViolations;
        }

        public List<MappingViolation> getSubFieldViolations() {
            return subFieldViolations;
        }
    }

    public static class ArrayViolation
            extends AbstractMappingViolation {

        private final MappingViolation subtypeViolation;

        public ArrayViolation(
                @NotNull SearchField field,
                MappingViolation subtypeViolation,
                @Nullable String prefix
        ) {
            super(field.getName(), prefix, ViolationType.INCOMPATIBLE_ARRAY_TYPE);
            this.subtypeViolation = subtypeViolation;
        }

        public MappingViolation getSubtypeViolation() {
            return subtypeViolation;
        }
    }

    public static class NotSuitableAsGeoPoint
            extends AbstractMappingViolation {

        public NotSuitableAsGeoPoint(
                @NotNull StructField field,
                @Nullable String prefix
        ) {
            super(field.name(), prefix, ViolationType.NOT_SUITABLE_AS_GEOPOINT);
        }
    }
}
