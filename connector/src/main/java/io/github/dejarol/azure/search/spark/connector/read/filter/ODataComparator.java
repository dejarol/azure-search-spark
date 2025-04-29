package io.github.dejarol.azure.search.spark.connector.read.filter;

import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;

/**
 * OData comparison operators
 */

public enum ODataComparator {

    GT("gt"),
    GEQ("ge"),
    EQ("eq"),
    NE("ne"),
    LT("lt"),
    LEQ("le");

    private final String oDataValue;

    /**
     * Create an instance
     * @param oDataValue inner value
     */

    ODataComparator(
            @NotNull String oDataValue
    ) {
        this.oDataValue = oDataValue;
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
