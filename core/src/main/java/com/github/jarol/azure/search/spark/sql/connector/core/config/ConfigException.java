package com.github.jarol.azure.search.spark.sql.connector.core.config;

import com.github.jarol.azure.search.spark.sql.connector.core.Constants;
import org.apache.spark.sql.DataFrameReader;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Objects;
import java.util.function.Supplier;

/**
 * Exception for datasource configuration issues
 */

public class ConfigException
        extends IllegalArgumentException {

    static final String INVALID_VALUE_PREFIX;
    static final Supplier<String> LOCAL_OR_SESSION_CONFIGURATION_MESSAGE_SUPPLIER;
    static final Supplier<String> PATH_OR_INDEX_SUPPLIER;

    static {

        INVALID_VALUE_PREFIX = "Illegal value";
        LOCAL_OR_SESSION_CONFIGURATION_MESSAGE_SUPPLIER = () -> String.format("This option should be provided either to Spark %s/Writer " +
                        "or configured at the session level prefixed by 'spark.datasource.%s.'",
                DataFrameReader.class.getSimpleName(),
                Constants.DATASOURCE_NAME
        );

        PATH_OR_INDEX_SUPPLIER = () -> String.format("The index name should be provided either as option 'path' " +
                        "or option '%s' to Spark %s/Writer or passed as argument to their load()/save() methods",
                IOConfig.INDEX_CONFIG,
                DataFrameReader.class.getSimpleName()
        );
    }

    /**
     * Create an instance
     * @param message message supplier
     * @param cause cause
     */

    private ConfigException(
            @NotNull Supplier<String> message,
            @Nullable Throwable cause
    ) {
        super(message.get(), cause);
    }

    /**
     * Create an instance for a missing option
     * @param key option key
     * @param prefix key prefix
     * @param extraInfoSupplier supplier for extending exception message
     * @return an instance
     */

   public static @NotNull ConfigException forMissingOption(
           String key,
           @Nullable String prefix,
           @Nullable Supplier<String> extraInfoSupplier
   ) {

        String prefixPlusKey = Objects.isNull(prefix) ? key : prefix.concat(key);
        String extraInfo = Objects.isNull(extraInfoSupplier) ? "": ". ".concat(extraInfoSupplier.get());
        Supplier<String> messageSupplier = () -> String.format(
                "Missing required option '%s'%s",
                prefixPlusKey,
                extraInfo
        );

        return new ConfigException(
                messageSupplier,
                null
        );
   }

    /**
     * Create an instance for an option with an illegal value
     * @param key configuration key
     * @param value configuration value
     * @param cause exception cause
     */

   public static @NotNull ConfigException forIllegalOptionValue(
           String key,
           String value,
           @NotNull Throwable cause
   ) {

       Supplier<String> supplier = () -> String.format(
               "Illegal value for option '%s' (%s). Reason: %s",
               key, value, cause.getMessage()
       );
       return new ConfigException(
               supplier,
               cause
       );
   }

    /**
     * Create an instance for an option with an illegal value
     * @param key configuration key
     * @param value configuration value
     * @param reason reason
     */

    public static @NotNull ConfigException forIllegalOptionValue(
            String key,
            String value,
            String reason
    ) {

        Supplier<String> supplier = () -> String.format(
                "Illegal value for option '%s' (%s). Reason: %s",
                key, value, reason
        );
        return new ConfigException(
                supplier,
                null
        );
    }
}
