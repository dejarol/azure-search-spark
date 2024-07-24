package com.github.jarol.azure.search.spark.sql.connector.config;

public enum UsageMode {

    READ(SearchConfig.READ_PREFIX),
    WRITE(SearchConfig.WRITE_PREFIX);

    private final String prefix;

    UsageMode(String prefix) {
        this.prefix = prefix;
    }

    public String getPrefix() {
        return prefix;
    }
}
