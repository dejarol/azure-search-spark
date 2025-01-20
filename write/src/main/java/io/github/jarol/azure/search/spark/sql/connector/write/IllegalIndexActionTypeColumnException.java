package io.github.jarol.azure.search.spark.sql.connector.write;

/**
 * Exception throw when the column name used for retrieving the {@link com.azure.search.documents.models.IndexActionType}
 * from an {@link org.apache.spark.sql.catalyst.InternalRow}
 * <ul>
 *     <li>does not exist</li>
 *     <li>is not a string column</li>
 * </ul>
 */

public class IllegalIndexActionTypeColumnException
        extends IllegalArgumentException {

  /**
   * Create an instance for a column name
   * @param columnName column name
   */

    public IllegalIndexActionTypeColumnException(
            String columnName
    ) {

        super(String.format(
                "Action column %s could not be found or it's not a string column",
                columnName
                )
        );
    }
}
