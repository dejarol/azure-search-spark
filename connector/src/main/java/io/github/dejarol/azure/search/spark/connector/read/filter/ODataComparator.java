package io.github.dejarol.azure.search.spark.connector.read.filter;

import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;

/**
 * OData comparison operators
 */

public enum ODataComparator {

    GT(">", "gt"),
    GEQ(">=", "ge"),
    EQ("=", "eq"),
    NE("<>", "ne"),
    LT("<", "lt"),
    LEQ("<=", "le");

    private final String predicateName;
    private final String oDataValue;

    /**
     * Create an instance
     * @param oDataValue inner value
     */

    ODataComparator(
            @NotNull String predicateName,
            @NotNull String oDataValue
    ) {
        this.predicateName = predicateName;
        this.oDataValue = oDataValue;
    }

    /**
     * Get the name of the equivalent Spark predicate
     * @return name of the equivalent Spark predicate
     */

    @Contract(pure = true)
    public String predicateName() {
        return predicateName;
    }

    /**
     * Get the inner value
     * @return the inner value
     */

    @Contract(pure = true)
    public String oDataValue() {
        return oDataValue;
    }
}
