package com.github.jarol.azure.search.spark.sql.connector.read;

import java.util.Objects;

/**
 * Read converter that
 * <ul>
 *     <li>first checks that the value is not null</li>
 *     <li>then applies a casting to such value</li>
 * </ul>
 *  Suitable for cases when casting the input value is sufficient
 * @param <T> output value type
 */

public abstract class ReadCastingConverter<T>
        implements ReadConverter<T> {

    @SuppressWarnings("unchecked")
    @Override
    public T apply(Object obj) {

        return Objects.isNull(obj) ?
                null :
                (T) obj;
    }
}
