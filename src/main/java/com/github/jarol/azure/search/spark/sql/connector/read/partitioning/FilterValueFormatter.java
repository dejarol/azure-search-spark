package com.github.jarol.azure.search.spark.sql.connector.read.partitioning;

@FunctionalInterface
public interface FilterValueFormatter {

    String format(Object value);
}
