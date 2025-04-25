package io.github.dejarol.azure.search.spark.connector.core.schema;

import org.jetbrains.annotations.NotNull;

/**
 * An enum representing the types of codecs used for data processing
 * <ul>
 *     <li>{@link CodecType#ENCODING} is used for data encoding</li>
 *     <li>{@link CodecType#DECODING} is used for data decoding</li>
 * </ul>
 */

public enum CodecType {

    ENCODING("encoding"),
    DECODING("decoding");

    private final String description;

    /**
     * Constructor for this enum
     * @param description the description of this type.
     */

    CodecType(
            @NotNull String description
    ) {
        this.description = description;
    }

    /**
     * Returns the description of the codec type
     * @return a string representing the description of the codec type
     */

    public String description() {
        return description;
    }
}