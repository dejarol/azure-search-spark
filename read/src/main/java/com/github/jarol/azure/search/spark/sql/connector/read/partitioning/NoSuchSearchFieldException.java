package com.github.jarol.azure.search.spark.sql.connector.read.partitioning;

import java.util.NoSuchElementException;

/**
 * Exception thrown for non-existing Search fields
 */

public class NoSuchSearchFieldException
        extends NoSuchElementException {

    /**
     * Create an instance
     * @param name field name
     */

    public NoSuchSearchFieldException(
            String name
    ) {
        super(String.format(
                "Field %s does not exist",
                name
                )
        );
    }
}
