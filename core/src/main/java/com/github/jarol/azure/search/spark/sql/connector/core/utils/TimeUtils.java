package com.github.jarol.azure.search.spark.sql.connector.core.utils;

import com.github.jarol.azure.search.spark.sql.connector.core.Constants;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;

/**
 * Collector of utility methods for time and dates
 */

public final class TimeUtils {

    /**
     * Converts a string representation of a date into a {@link OffsetDateTime}
     * <br>
     * The string should have format <code>yyyy-MM-dd</code>
     * @param date date
     * @return an offset datetime with time at midnight and UTC offset
     */

    @Contract("_ -> new")
    public static @NotNull OffsetDateTime offsetDateTimeFromLocalDate(
            String date
        ) {

        return OffsetDateTime.of(
                LocalDate.parse(date, DateTimeFormatter.ISO_LOCAL_DATE),
                LocalTime.MIDNIGHT,
                Constants.UTC_OFFSET
        );
    }

    /**
     * Convert an integer representing epoch days to an {@link OffsetDateTime} object with time at midnight and UTC offset
     * @param epochDays epoch days
     * @return an offset datetime with time at midnight and UTC offset
     */

    @Contract("_ -> new")
    public static @NotNull OffsetDateTime offsetDateTimeFromEpochDays(
            @NotNull Integer epochDays
    ) {

        return OffsetDateTime.of(
                LocalDate.ofEpochDay(epochDays),
                LocalTime.MIDNIGHT,
                Constants.UTC_OFFSET
        );
    }

    /**
     * Convert a long representing epoch microseconds to an {@link OffsetDateTime} object with UTC offset
     * @param epochMicros epoch microseconds
     * @return an offset datetime with UTC offset
     */

    @Contract("_ -> new")
    public static @NotNull OffsetDateTime offsetDateTimeFromEpochMicros(
            @NotNull Long epochMicros
    ) {

        return Instant.EPOCH
                .plus(epochMicros, ChronoUnit.MICROS)
                .atOffset(Constants.UTC_OFFSET);
    }
}
