package com.github.jarol.azure.search.spark.sql.connector.read;

import java.util.Objects;

/**
 * Read converter that
 * <ul>
 *     <li>first checks that the value is not null</li>
 *     <li>then applies a transformation on such value</li>
 * </ul>
 *  Suitable for cases when the input value should be transformed by a function that does not accept null values
 * @param <T> output value type
 */

public abstract class ReadTransformConverter<T>
        implements ReadConverter<T> {

    @Override
    public T apply(Object obj) {

        return Objects.isNull(obj) ?
                null :
                transform(obj);
    }

    /**
     * Apply a conversion to an object
     * @param obj search document property
     * @return an instance of the output type
     */

    protected abstract T transform(Object obj);
}
