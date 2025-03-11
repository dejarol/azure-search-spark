package io.github.jarol.azure.search.spark.connector.core;

import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

/**
 * Project constants
 */

public final class Constants {

    /**
     * Datasource name
     */

    public static final String DATASOURCE_NAME = "azsearch";

    /**
     * Datasource key prefix
     */

    public static final String DATASOURCE_KEY_PREFIX = "azure.search";

    /**
     * Maximum number of documents that can be read per partition
     */

    public static final int DOCUMENTS_PER_PARTITION_LIMIT = 100000;

    /**
     * Default ZoneOffset for Search date times and Spark internal time types
     */

    public static final ZoneOffset UTC_OFFSET;

    /**
     * Formatter for parsing back and forth Search date times and Spark internal time types
     */

    public static final DateTimeFormatter DATETIME_OFFSET_FORMATTER;

    static {

        UTC_OFFSET = ZoneOffset.UTC;
        DATETIME_OFFSET_FORMATTER = DateTimeFormatter.ISO_DATE_TIME;
    }
}
