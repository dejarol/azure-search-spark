package com.github.jarol.azure.search.spark.sql.connector.write;

import com.azure.search.documents.models.IndexActionType;
import org.apache.spark.sql.catalyst.InternalRow;
import org.jetbrains.annotations.Contract;

/**
 * Constant supplier (used when no index action column has been specified)
 */

public class ConstantActionSupplier
        implements IndexActionSupplier {

    private final IndexActionType defaultValue;

    /**
     * Create a new instance with a default value
     * @param defaultValue value
     */

    @Contract(pure = true)
    public ConstantActionSupplier(
            IndexActionType defaultValue
    ) {
        this.defaultValue = defaultValue;
    }

    @Override
    public IndexActionType get(InternalRow row) {
        return defaultValue;
    }
}
