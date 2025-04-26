package io.github.dejarol.azure.search.spark.connector.core.schema;

/**
 * A simple interface for describing a dataSource field (either Spark or Search field)
 */

public interface FieldDescriptor {

    /**
     * Gets the field's name
     * @return the field's name
     */

    String name();

    /**
     * Gets the field's description
     * @return the field's description
     */

    String type();

    /**
     * Gets the field's datatype description
     * @return the field's datatype description
     */

    String dataTypeDescription();
}
