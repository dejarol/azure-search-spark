package io.github.dejarol.azure.search.spark.connector.core;

import com.azure.search.documents.indexes.models.SearchFieldDataType;
import org.apache.spark.sql.types.DataType;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;

import java.util.function.Supplier;

/**
 * Exception thrown for illegal states related to data types
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

    private DataTypeException(
            @NotNull Supplier<String> message
    ) {

        super(message.get());
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
                String.format(
                        "Unsupported Search data type (%s)",
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
                String.format(
                        "Unsupported Spark data type (%s)",
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

    // TODO: document
    public static @NotNull DataTypeException forNonComplexField(

    ) {

        return new DataTypeException(
                String.format(
                        "Could not retrieve subfields from a field of type %s",
                        "type" // TODO: replace with concrete type
                        )
        );
    }

    /**
     * Creates a new exception indicating that field has a Spark type
     * which is not compatible with the Search type of its homonymous counterpart
     * @param name field name
     * @param sparkType field's Spark type
     * @param searchFieldDataType field's Search type
     * @return a new exception indicating that field has a Spark type
     */

    @Contract("_, _, _ -> new")
    public static @NotNull DataTypeException forIncompatibleField(
            String name,
            DataType sparkType,
            SearchFieldDataType searchFieldDataType
    ) {

        Supplier<String> message = () -> String.format(
                "Field %s has Spark type %s and Search type %s, that are not compatible",
                name,
                sparkType.typeName(),
                searchFieldDataType.toString()
        );

        return new DataTypeException(message);
    }
}
