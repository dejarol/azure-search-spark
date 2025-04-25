package io.github.dejarol.azure.search.spark.connector.core.schema;

import com.azure.search.documents.indexes.models.SearchFieldDataType;
import io.github.dejarol.azure.search.spark.connector.core.DataTypeException;
import org.apache.spark.sql.types.DataType;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class CodecFactoryException
        extends IllegalStateException {

    private CodecFactoryException(
            @NotNull String fieldName,
            @NotNull CodecType codecType,
            @NotNull Supplier<String> reason,
            @Nullable Throwable cause
    ) {

        super(
                String.format(
                        "Cannot build %s function for field %s (%s)",
                        codecType.description(),
                        fieldName,
                        reason.get()
                ), cause
        );
    }

    public static @NotNull CodecFactoryException forIncompatibleTypes(
            String fieldName,
            @NotNull CodecType codecType,
            @NotNull DataType dataType,
            @NotNull SearchFieldDataType searchFieldDataType
    ) {

        DataTypeException cause = DataTypeException.forIncompatibleField(
                fieldName,
                dataType,
                searchFieldDataType
        );

        return new CodecFactoryException(
                fieldName,
                codecType,
                cause::getMessage,
                cause
        );
    }

    public static @NotNull CodecFactoryException forArrays(
            @NotNull String fieldName,
            @NotNull CodecType codecType,
            @NotNull Throwable cause
    ) {

        return new CodecFactoryException(
                fieldName,
                codecType,
                cause::getMessage,
                cause
        );
    }

    public static @NotNull CodecFactoryException forComplexObjectDueToMissingSubfields(
            @NotNull String fieldName,
            @NotNull CodecType codecType,
            @NotNull List<String> subFieldNames
            ) {

        return new CodecFactoryException(
                fieldName,
                codecType,
                () -> String.format(
                        "subfields [%s] do not exist",
                        String.join(", ", subFieldNames)
                ),
                null
        );
    }

    public static @NotNull CodecFactoryException forComplexObjectDueToIncompatibleSubfields(
            @NotNull String fieldName,
            @NotNull CodecType codecType,
            @NotNull Map<String, String> subFieldsAndCauses
    ) {

        return new CodecFactoryException(
                fieldName,
                codecType,
                () -> String.format(
                        "cannot build subcodecs for fields [%s]",
                        subFieldsAndCauses.entrySet().stream().map(
                                entry -> String.format(
                                        "%s (%s)",
                                        entry.getKey(),
                                        entry.getValue()
                                )
                        ).collect(Collectors.joining(", "))
                ),
                null
        );
    }

    public static @NotNull CodecFactoryException forGeoPoint(
            @NotNull String fieldName,
            @NotNull CodecType codecType
    ) {

        return new CodecFactoryException(
                fieldName,
                codecType,
                () -> "not compatible as geopoint",
                null
        );
    }
}
