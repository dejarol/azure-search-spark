package com.github.jarol.azure.search.spark.sql.connector.read.filter;

import org.jetbrains.annotations.Contract;

/**
 * OData comparison operators
 */

public enum Comparator {

    GT(">", "gt"),
    GEQ(">=", "ge"),
    EQ("=", "eq"),
    NE("<>", "ne"),
    LT("<", "lt"),
    LEQ("<=", "le");

    private final String predicateName;
    private final String odataValue;

    /**
     * Create an instance
     * @param odataValue inner value
     */

    @Contract(pure = true)
    Comparator(
            String predicateName,
            String odataValue
    ) {
        this.predicateName = predicateName;
        this.odataValue = odataValue;
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
        return odataValue;
    }
}
