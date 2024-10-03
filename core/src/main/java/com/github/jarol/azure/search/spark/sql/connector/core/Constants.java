package com.github.jarol.azure.search.spark.sql.connector.core;

import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

/**
 * Project constants
 */

public final class Constants {

    public static final String DATASOURCE_NAME = "azure.search";

    /**
     * Maximum number of documents that can be read per partition.
     * See <a href="https://learn.microsoft.com/en-us/dotnet/api/azure.search.documents.searchoptions.skip?view=azure-dotnet">this</a>
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

    /**
     * Allowed formatters for converting a String to a OffsetDateTime
     */

    public static final DateTimeFormatter TIMESTAMP_FORMATTER;

    static {

        UTC_OFFSET = ZoneOffset.UTC;
        DATETIME_OFFSET_FORMATTER = DateTimeFormatter.ISO_DATE_TIME;
        TIMESTAMP_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss[.SSS]");
    }
}
