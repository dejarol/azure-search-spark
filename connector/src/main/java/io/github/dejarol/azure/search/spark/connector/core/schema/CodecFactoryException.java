package io.github.dejarol.azure.search.spark.connector.core.schema;

import io.github.dejarol.azure.search.spark.connector.core.DataTypeException;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.json4s.jackson.JsonMethods$;

import java.util.function.Supplier;

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

    private CodecFactoryException(
            @NotNull Supplier<String> message,
            @Nullable Throwable cause
    ) {

        super(message.get(), cause);
    }

    /**
     * Returns a {@link CodecFactoryException} for codec creation errors related to a specific field
     * @param fieldName the name of the field for which the codec cannot be built
     * @param codecType the type of codec that cannot be built
     * @param cause the underlying cause of the exception
     * @return a new instance of {@link CodecFactoryException}
     */
    @Contract("_, _, _ -> new")
    private static @NotNull CodecFactoryException forFieldCodec(
            @NotNull String fieldName,
            @NotNull CodecType codecType,
            @NotNull Throwable cause
    ) {

        return new CodecFactoryException(
                () -> String.format(
                        "Cannot build %s codec for field %s. Reason: %s",
                        codecType.description(),
                        fieldName,
                        cause.getMessage()),
                cause
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

        return forFieldCodec(
                fieldName,
                codecType,
                DataTypeException.notAGeopoint(fieldName)
        );
    }
}
