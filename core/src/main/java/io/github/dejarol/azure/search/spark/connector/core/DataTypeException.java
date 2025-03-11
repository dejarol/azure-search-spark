package io.github.dejarol.azure.search.spark.connector.core;

import com.azure.search.documents.indexes.models.SearchFieldDataType;
import org.apache.spark.sql.types.DataType;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;

/**
 * Exception thrown for illegal data type states
 */

public class DataTypeException
        extends IllegalStateException {

    /**
     * Create an instance with a message
     * @param message message
     */

    public DataTypeException(
            String message
    ) {
        super(message);
    }

    /**
     * Create a new instance related to an unsupported {@link SearchFieldDataType}
     * @param type unsupported type
     * @return a new instance
     */

    @Contract("_ -> new")
    public static @NotNull DataTypeException forUnsupportedSearchType(
            SearchFieldDataType type
    ) {

        return new DataTypeException(
                String.format("Unsupported Search data type (%s)",
                        type
                )
        );
    }

    /**
     * Create a new instance related to an unsupported {@link DataType}
     * @param dataType unsupported type
     * @return a new instance
     */

    @Contract("_ -> new")
    public static @NotNull DataTypeException forUnsupportedSparkType(
            @NotNull DataType dataType
    ) {

        return new DataTypeException(
                String.format("Unsupported Spark data type (%s)",
                        dataType.typeName()
                )
        );
    }

    /**
     * Create a new instance for top-level Search SINGLE fields (not allowed by the Search REST API)
     * @return an instance for single fields
     */

    @Contract(" -> new")
    public static @NotNull DataTypeException forSingleSearchFieldDataType() {

        return new DataTypeException(
                String.format("%s are not supported. Only %s are",
                        SearchFieldDataType.SINGLE,
                        SearchFieldDataType.collection(
                                SearchFieldDataType.SINGLE
                        )
                )
        );
    }
}
