package io.github.dejarol.azure.search.spark.connector.write;

import com.azure.search.documents.models.IndexActionType;
import org.apache.spark.sql.catalyst.InternalRow;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;

import java.io.Serializable;

/**
 * Interface for supplying an {@link com.azure.search.documents.models.IndexActionType}
 * from an {@link org.apache.spark.sql.catalyst.InternalRow}
 */

@FunctionalInterface
public interface IndexActionSupplier
        extends Serializable {

    /**
     * Create a constant action supplier instance (i.e. an instance that will supply the same action for all rows)
     * @param defaultAction default action
     * @return a constant supplier instance
     */

    @Contract(pure = true)
    static @NotNull IndexActionSupplier createConstantSupplier(
            IndexActionType defaultAction
    ) {
        return row -> defaultAction;
    }

    /**
     * Get the action type from a row
     * @param row row
     * @return the action type for this row
     */

    IndexActionType get(InternalRow row);
}
