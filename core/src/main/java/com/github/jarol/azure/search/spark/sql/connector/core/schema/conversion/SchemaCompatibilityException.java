package com.github.jarol.azure.search.spark.sql.connector.core.schema.conversion;

import com.azure.search.documents.indexes.models.SearchField;
import org.apache.spark.sql.types.StructField;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Exception raised when a Schema incompatibility is met
 * <p>
 * It may happen due to
 * <ul>
 *     <li>a non existing schema field (i.e. a schema field that does not exist on a Search index)</li>
 *     <li>an existing schema field with incompatible datatype with respect to its namesake Search field</li>
 * </ul>
 */

public class SchemaCompatibilityException
        extends IllegalStateException {

    /**
     * Create an instance
     * @param message exception message
     */

    public SchemaCompatibilityException(
            @NotNull String message
    ) {
        super(message);
    }

    /**
     * Create an instance due to missing schema fields
     * @param index index name
     * @param missingFields name of non-existing fields
     * @return an exception instance
     */

    public static @NotNull SchemaCompatibilityException forMissingFields(
            String index,
            @NotNull List<String> missingFields
    ) {

        String message = String.format("%s schema fields (%s) do not exist on index %s",
                missingFields.size(),
                String.join(", ", missingFields),
                index
        );
        return new SchemaCompatibilityException(message);
    }

    @Contract("_ -> new")
    public static @NotNull SchemaCompatibilityException forNonCompatibleFields(
            @NotNull Map<StructField, SearchField> fieldMap
    ) {

        String description = fieldMap.entrySet().stream().map(
                entry -> String.format(
                        "%s (%s, not compatible with %s)",
                        entry.getKey().name(),
                        entry.getKey().dataType(),
                        entry.getValue().getType()
                )
        ).collect(Collectors.joining(", "));

        String message = String.format(
                "%s fields have incompatible data types (%s)",
                fieldMap.size(),
                description
                );

        return new SchemaCompatibilityException(message);
    }

    @Contract("_ -> new")
    public static @NotNull SchemaCompatibilityException forSchemaFieldsWithoutConverter(
            @NotNull Map<StructField, SearchField> fieldMap
    ) {

        String description = fieldMap.entrySet().stream().map(
                entry -> String.format(
                        "%s [spark type %s, search type %s]",
                        entry.getKey().name(),
                        entry.getKey().dataType(),
                        entry.getValue().getType()
                        )
                ).collect(Collectors.joining(", "));

        String message = String.format(
                "Could not find a suitable converter for %s fields (%s)",
                fieldMap.size(),
                description
        );

        return new SchemaCompatibilityException(message);
    }
}
