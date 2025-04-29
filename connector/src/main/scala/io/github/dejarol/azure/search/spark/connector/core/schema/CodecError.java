package io.github.dejarol.azure.search.spark.connector.core.schema;

import org.json4s.JsonAST;

/**
 * Interface for implementing codec errors
 * <br>
 * Subclasses should take care of implementing [[toJValue]], which should be responsible
 * for providing a JSON representation of the error
 */

@FunctionalInterface
public interface CodecError {

    /**
     * Converts this error to a JSON value
     * @return the JSON representation of this error
     */

    JsonAST.JValue toJValue();
}
