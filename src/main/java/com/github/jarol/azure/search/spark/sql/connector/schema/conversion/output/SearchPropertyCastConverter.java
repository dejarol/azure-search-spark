package com.github.jarol.azure.search.spark.sql.connector.schema.conversion.output;

public class SearchPropertyCastConverter<T>
        implements SearchPropertyConverter {

    @Override
    public final T toSearchProperty(Object value) {
        //noinspection unchecked
        return (T) value;
    }
}
