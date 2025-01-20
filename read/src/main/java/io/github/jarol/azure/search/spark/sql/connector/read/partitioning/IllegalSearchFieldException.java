package io.github.jarol.azure.search.spark.sql.connector.read.partitioning;

import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;

import java.util.function.Supplier;

/**
 * Exception thrown for an illegal Search field
 */

public class IllegalSearchFieldException
        extends IllegalArgumentException {

    /**
     * Create an instance
     * @param supplier exception message supplier
     */

    protected IllegalSearchFieldException(
            @NotNull Supplier<String> supplier
    ) {
        super(supplier.get());
    }

    /**
     * Create an instance for a non-existing field
     * @param name field name
     * @return an instance
     */

    @Contract("_ -> new")
    public static @NotNull IllegalSearchFieldException nonExisting(
            String name
    ) {
        return new IllegalSearchFieldException(
                () -> String.format("Index field %s does not exist", name)
        );
    }
}
