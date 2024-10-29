package com.github.jarol.azure.search.spark.sql.connector;

import java.util.Map;

@FunctionalInterface
public interface DocumentSerializer<T> {

    Map<String, Object> serialize(T document);
}
