package com.github.jarol.azure.search.spark.sql.connector.core.utils;

import com.github.jarol.azure.search.spark.sql.connector.core.Constants;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.temporal.ChronoUnit;

/**
 * Collector of utility methods for time and dates
 */

public final class TimeUtils {

    /**
     * Convert an integer representing epoch days to an {@link OffsetDateTime} object with time at midnight and UTC offset
     * @param epochDays epoch days
     * @return an offset datetime with time at midnight and UTC offset
     */

    @Contract("_ -> new")
    public static @NotNull OffsetDateTime fromEpochDays(
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
    public static @NotNull OffsetDateTime fromEpochMicros(
            @NotNull Long epochMicros
    ) {

        return Instant.EPOCH
                .plus(epochMicros, ChronoUnit.MICROS)
                .atOffset(Constants.UTC_OFFSET);
    }
}
