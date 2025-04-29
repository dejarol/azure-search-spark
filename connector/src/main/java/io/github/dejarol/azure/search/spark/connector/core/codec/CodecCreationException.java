package io.github.dejarol.azure.search.spark.connector.core.codec;

import org.jetbrains.annotations.NotNull;
import org.json4s.jackson.JsonMethods$;

import java.util.function.Supplier;

/**
 * Exception thrown for illegal states met during codec creation
 */

public class CodecCreationException
        extends IllegalStateException {

    /**
     * Constructor
     * @param message exception message supplier
     */

    private CodecCreationException(
            @NotNull Supplier<String> message
    ) {

        super(message.get());
    }

    /**
     * Creates a {@link CodecCreationException} from a {@link CodecError} and {@link CodecType}.
     * <br>
     * This method generates an exception with a message detailing the codec type and a pretty-printed
     * JSON representation of the codec error.
     *
     * @param codecType the type of codec operation (e.g., encoding or decoding) causing the exception
     * @param codecError the codec error encountered during the operation
     * @return a {@link CodecCreationException} with a formatted error message
     */

    public static @NotNull CodecCreationException fromCodedError(
            @NotNull CodecType codecType,
            @NotNull CodecError codecError
    ) {

        return new CodecCreationException(
                () -> String.format(
                        "Cannot build %s function. Reason: %s",
                        codecType.description(),
                        JsonMethods$.MODULE$.pretty(codecError.toJValue())
                )
        );
    }
}
