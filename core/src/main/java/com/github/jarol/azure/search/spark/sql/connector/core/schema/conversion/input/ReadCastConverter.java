package com.github.jarol.azure.search.spark.sql.connector.core.schema.conversion.input;

/**
 * Read converters that will transform an object by casting it to a target Spark internal type
 * @param <T> target SPark internal type
 */

public final class ReadCastConverter<T>
        extends ReadTransformConverter<T> {

    /**
     * Converter for int32
     */

    public static final ReadCastConverter<Integer> INT32;

    /**
     * Converter for int64
     */

    public static final ReadCastConverter<Long> INT64;

    /**
     * Converter for doubles
     */

    public static final ReadCastConverter<Double> DOUBLE;

    /**
     * Converter for floats
     */

    public static final ReadCastConverter<Float> SINGLE;

    /**
     * Converter for booleans
     */

    public static final ReadCastConverter<Boolean> BOOLEAN;

    static {

        INT32 = new ReadCastConverter<>();
        INT64 = new ReadCastConverter<>();
        DOUBLE = new ReadCastConverter<>();
        SINGLE = new ReadCastConverter<>();
        BOOLEAN = new ReadCastConverter<>();
    }

    @SuppressWarnings("unchecked")
    @Override
    protected T transform(Object value) {
        return (T) value;
    }
}
