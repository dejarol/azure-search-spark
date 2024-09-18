package com.github.jarol.azure.search.spark.sql.connector.read.partitioning;

import java.util.NoSuchElementException;

public class NoSuchSearchFieldException
        extends NoSuchElementException {

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
