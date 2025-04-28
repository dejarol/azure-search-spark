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
     * Creates a new instance
     * @param message message supplier
     */

    private DataTypeException(
            @NotNull Supplier<String> message
    ) {

        super(message.get());
    }

    /**
     * Creates a new exception when a collection inner type is requested from a type that is not a collection.
     * @param description the type description
     * @return a new exception
     */

    public static @NotNull DataTypeException forNonCollectionType(
            @NotNull EntityDescription description
    ) {

        return new DataTypeException(
                () -> String.format(
                        "Cannot retrieve collection inner type from %s",
                        description.description()
                )

        );
    }

    /**
     * Creates a new exception when subFields are requested from an entity that is not complex.
     * @param description the entity description
     * @return a new exception
     */

    public static @NotNull DataTypeException forNonComplexEntity(
            @NotNull EntityDescription description
    ) {

        return new DataTypeException(
                () -> String.format(
                        "Cannot retrieve subFields from %s",
                        description.description()
                )
        );
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
                () -> String.format(
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
                () -> String.format(
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
                () -> String.format("%s are not supported. Only %s are",
                        SearchFieldDataType.SINGLE,
                        SearchFieldDataType.collection(
                                SearchFieldDataType.SINGLE
                        )
                )
        );
    }

    /**
     * Creates a new exception indicating that field has a Spark type
     * which is not compatible with the Search type of its homonymous counterpart
     * @param name field name
     * @param sparkType field's Spark type
     * @param searchFieldDataType field's Search type
     * @return a new exception indicating a field with incompatible Spark and Search types
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

    /**
     * Creates a new exception indicating that field is not compatible with a geopoint
     * @param name field name
     * @return a new exception indicating that field is not compatible with a geopoint
     */

    public static @NotNull DataTypeException notAGeopoint(
            String name
    ) {

        return new DataTypeException(
                () -> String.format(
                        "Field %s is not compatible with a geopoint",
                        name
                )
        );
    }
}
