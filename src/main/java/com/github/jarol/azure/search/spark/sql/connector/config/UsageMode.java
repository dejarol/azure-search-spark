package com.github.jarol.azure.search.spark.sql.connector.config;

import org.jetbrains.annotations.Contract;

/**
 * Configuration usage modes
 */

public enum UsageMode {

    /**
     * Read mode
     */

    READ(SearchConfig.READ_PREFIX),

    /**
     * Write mode
     */

    WRITE(SearchConfig.WRITE_PREFIX);

    /**
     * Prefix of {@link org.apache.spark.SparkConf}'s options related to this mode
     */

    private final String prefix;

    /**
     * Construct a new instance
     * @param prefix prefix
     */

    @Contract(pure = true)
    UsageMode(String prefix) {
        this.prefix = prefix;
    }

    /**
     * Get the prefix for all {@link org.apache.spark.SparkConf}'s options related to this mode
     * @return prefix for spotting mode-related configurations
     */

    @Contract(pure = true)
    public String prefix() {
        return prefix;
    }
}
