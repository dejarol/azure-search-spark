package com.github.jarol.azure.search.spark.sql.connector;

public class IndexDoesNotExistException
        extends AzureSparkException {

    public IndexDoesNotExistException(
            String name
    ) {

        super(String.format(
                "Index %s does not exist",
                name)
        );
    }
}
