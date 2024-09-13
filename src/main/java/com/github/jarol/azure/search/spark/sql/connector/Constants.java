package com.github.jarol.azure.search.spark.sql.connector;

import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

/**
 * Project constants
 */

public final class Constants {

    /**
     * Formatter for parsing back and forth Search date times and Spark internal time types
     */

    public static final DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormatter.ISO_DATE_TIME;

    /**
     * Default ZoneOffset for Search date times and Spark internal time types
     */

    public static final ZoneOffset UTC_OFFSET = ZoneOffset.UTC;
}
