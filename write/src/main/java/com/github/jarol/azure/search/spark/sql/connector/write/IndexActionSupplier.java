package com.github.jarol.azure.search.spark.sql.connector.write;

import com.azure.search.documents.models.IndexActionType;
import org.apache.spark.sql.catalyst.InternalRow;

import java.io.Serializable;

/**
 * Interface for supplying an {@link IndexActionType} from an {@link InternalRow}
 */

@FunctionalInterface
public interface IndexActionSupplier
        extends Serializable {

    /**
     * Get the action type from a row
     * @param row row
     * @return the action type for this row
     */

    IndexActionType get(InternalRow row);
}
