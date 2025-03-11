package io.github.dejarol.azure.search.spark.connector.core;

import java.util.NoSuchElementException;

/**
 * Exception for non-existing Search indexes
 */

public class IndexDoesNotExistException
        extends NoSuchElementException {

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
