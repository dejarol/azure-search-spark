package com.github.jarol.azure.search.spark.sql.connector.read;

import com.github.jarol.azure.search.spark.sql.connector.IndexDoesNotExistException;
import com.github.jarol.azure.search.spark.sql.connector.schema.SchemaCompatibilityException;
import org.apache.spark.sql.connector.read.Scan;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;

/**
 * Exception that may arise when building the dataSource Scan. It may occur when
 * <ul>
 *     <li>target Search index does not exist</li>
 *     <li>there's an incompatibility between Spark schema (either inferred or user-provided) and Search index schema</li>
 * </ul>
 */

public class ScanBuilderException
        extends RuntimeException {

  /**
   * Create an instance with a cause
   * @param cause cause
   */

    public ScanBuilderException(
            @NotNull Throwable cause
    ) {
        super(String.format("Failed to build dataSource %s. Reason: %s",
                Scan.class.getSimpleName(),
                cause.getMessage()),
                cause
        );
    }

  /**
   * Create an instance caused by a non-existing index
   * @param name index name
   * @return an instance caused by a non-existing index
   */

    @Contract("_ -> new")
    public static @NotNull ScanBuilderException causedByNonExistingIndex(
            String name
    ) {
      return new ScanBuilderException(
              new IndexDoesNotExistException(name)
      );
    }

  /**
   * Create an instance caused by a schema incompatibility
   * @param message detailed message
   * @return an instance caused by a schema incompatibility
   */

    @Contract("_ -> new")
    public static @NotNull ScanBuilderException causedBySchemaIncompatibility(
            String message
    ) {
      return new ScanBuilderException(
              new SchemaCompatibilityException(message)
      );
    }
}
