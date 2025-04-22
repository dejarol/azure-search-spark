package io.github.dejarol.azure.search.spark.connector.core.schema;

import com.azure.search.documents.indexes.models.SearchFieldDataType;
import io.github.dejarol.azure.search.spark.connector.core.DataTypeException;
import org.apache.spark.sql.types.DataType;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.function.Supplier;

public class CodecFactoryException
        extends IllegalStateException {

    private CodecFactoryException(
            @NotNull Supplier<String> message,
            @Nullable Throwable cause
    ) {

        super(message.get(), cause);
    }

    private static @NotNull CodecFactoryException createInstance(
            String name,
            @Nullable Throwable cause,
            @NotNull Supplier<String> reason,
            boolean forEncoding
    ) {

        String message = String.format(
                "Cannot build %s function for field %s (%s)",
                forEncoding ? "encoding" : "decoding",
                name,
                reason.get()
        );

        return new CodecFactoryException(
                () -> message,
                cause
        );
    }

    public static @NotNull CodecFactoryException forIncompatibleTypes(
            String fieldName,
            @NotNull DataType dataType,
            @NotNull SearchFieldDataType searchFieldDataType,
            boolean forEncoding
    ) {

        DataTypeException cause = DataTypeException.forIncompatibleField(
                fieldName,
                dataType,
                searchFieldDataType
        );

        return createInstance(fieldName, cause, cause::getMessage, forEncoding);
    }

    public static @NotNull CodecFactoryException forArrays(
            String fieldName,
            @NotNull Throwable cause,
            boolean forEncoding
    ) {

        return createInstance(
                fieldName,
                cause,
                cause::getMessage,
                forEncoding
        );
    }

    public static @NotNull CodecFactoryException forComplex(
            String fieldName,
            boolean forEncoding
    ) {

        return createInstance(
                fieldName,
                null,
                () -> "complex",
                forEncoding
        );
    }

    public static @NotNull CodecFactoryException forGeoPoint(
            String fieldName,
            boolean forEncoding
    ) {

        return createInstance(
                fieldName,
                null,
                () -> "geopoint",
                forEncoding
        );
    }
}
