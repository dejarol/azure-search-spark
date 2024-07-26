package com.github.jarol.azure.search.spark.sql.connector.config;

import org.jetbrains.annotations.Contract;

import java.time.LocalDate;

/**
 * A simple bean with a 2-args constructor
 */

public class MultipleArgsBean {

    private final Integer id;
    private final LocalDate date;

    /**
     * Create a new instance
     * @param id id
     * @param date date
     */

    @Contract(pure = true)
    public MultipleArgsBean(
            Integer id,
            LocalDate date
    ) {
        this.id = id;
        this.date = date;
    }

    /**
     * Get id
     * @return this instance's id
     */

    public Integer getId() {
        return id;
    }

    /**
     * Get date
     * @return this instance's date
     */

    public LocalDate getDate() {
        return date;
    }
}
