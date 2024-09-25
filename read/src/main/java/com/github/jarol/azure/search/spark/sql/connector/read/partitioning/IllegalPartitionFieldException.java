package com.github.jarol.azure.search.spark.sql.connector.read.partitioning;

/**
 * Exception thrown for an illegal partition field.
 * <br>
 * A partition field is illegal if
 * <ul>
 *     <li>it does not exist</li>
 *     <li>its datatype is not numeric or datetime</li>
 * </ul>
 */

public class IllegalPartitionFieldException
        extends IllegalArgumentException {

    /**
     * Create an instance
     * @param name field name
     */

    public IllegalPartitionFieldException(
            String name
    ) {
        super(String.format(
                "Illegal partition field (%s). It should be an existing field with either numeric or datetime type",
                name
        ));
    }
}
