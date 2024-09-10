package com.github.jarol.azure.search.spark.sql.connector;

/**
 * Exception for non-existing Search indexes
 */

public class IndexDoesNotExistException
        extends AzureSparkException {

    /**
     * Create an instance for given index name
     * @param name index name
     */

    public IndexDoesNotExistException(
            String name
    ) {

        super(String.format(
                "Index %s does not exist",
                name)
        );
    }
}
