package io.github.dejarol.azure.search.spark.connector.core.schema;

import com.azure.search.documents.indexes.models.SearchFieldDataType;
import io.github.dejarol.azure.search.spark.connector.core.DataTypeException;
import org.apache.spark.sql.types.DataType;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * Exception thrown for illegal states met during codec creation.
 * <br>
 * Among the possible causes are
 * <ul>
 *     <li>incompatible data types</li>
 *     <li>array fields with incompatible internal data types</li>
 *     <li>complex (nested) objects not having all required fields</li>
 *     <li>complex (nested) objects not suitable as geopoints</li>
 * </ul>
 */

public class CodecFactoryException
        extends IllegalStateException {

     /**
     * Constructor
     * @param fieldName name of the field (null when the exception is not related to a specific field)
     * @param codecType instance of {@link CodecType} (should be either {@link CodecType#ENCODING} when the exception
     *                  arises during encoder creation or {@link CodecType#DECODING} when the exception arises during
     *                  decoder creation)
     * @param reason supplier with the reason
     * @param cause potential exception cause
     */

    private CodecFactoryException(
            @Nullable String fieldName,
            @NotNull CodecType codecType,
            @NotNull Supplier<String> reason,
            @Nullable Throwable cause
    ) {

        super(
                String.format(
                        "Cannot build %s function%s. Reason: (%s)",
                        codecType.description(),
                        Optional.ofNullable(fieldName).map(
                                name -> String.format(" for field %s", name)
                        ).orElse(""),
                        reason.get()
                ), cause
        );
    }

    /**
     * Returns a {@link CodecFactoryException} when the datatype of a field is incompatible with the Search field data
     * type
     * @param fieldName name of the field
     * @param codecType instance of {@link CodecType} (should be either {@link CodecType#ENCODING} when the exception
     *                  arises during encoder creation or {@link CodecType#DECODING} when the exception arises during
     *                  decoder creation)
     * @param dataType instance of {@link DataType} representing the data type of the field
     * @param searchFieldDataType instance of {@link SearchFieldDataType} representing the data type of the Search field
     * @return a new instance of {@link CodecFactoryException}
     */

    public static @NotNull CodecFactoryException forIncompatibleTypes(
            @NotNull String fieldName,
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

    /**
     * Returns a {@link CodecFactoryException} for array fields with incompatible internal data types.
     *
     * @param fieldName name of the field
     * @param codecType instance of {@link CodecType} (should be either {@link CodecType#ENCODING} or {@link CodecType#DECODING})
     * @param cause the underlying cause of the exception
     * @return a new instance of {@link CodecFactoryException}
     */

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

    /**
     * Returns a {@link CodecFactoryException} for complex fields that do not have all required subfields.
     *
     * @param fieldName name of the field
     * @param codecType instance of {@link CodecType} (should be either {@link CodecType#ENCODING} or {@link CodecType#DECODING})
     * @param subFieldNames names of the subfields
     * @return a new instance of {@link CodecFactoryException}
     */

    public static @NotNull CodecFactoryException forComplexObjectDueToMissingSubfields(
            @Nullable String fieldName,
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

    /**
     * Returns a {@link CodecFactoryException} for complex fields that have subfields that
     * cannot be used to build subcodecs.
     *
     * @param fieldName name of the field
     * @param codecType instance of {@link CodecType} (should be either {@link CodecType#ENCODING} or {@link CodecType#DECODING})
     * @param subFieldsAndCauses a mapping from subfield name to the cause of the subfield
     * @return a new instance of {@link CodecFactoryException}
     */

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

    /**
     * Returns a {@link CodecFactoryException} for fields that are not compatible as geopoint.
     *
     * @param fieldName name of the field
     * @param codecType instance of {@link CodecType} (should be either {@link CodecType#ENCODING} or {@link CodecType#DECODING})
     * @return a new instance of {@link CodecFactoryException}
     */
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
