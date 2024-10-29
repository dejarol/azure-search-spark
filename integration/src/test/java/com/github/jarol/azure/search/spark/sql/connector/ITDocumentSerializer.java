package com.github.jarol.azure.search.spark.sql.connector;

import com.github.jarol.azure.search.spark.sql.connector.models.ITDocument;
import org.jetbrains.annotations.NotNull;

import java.util.LinkedHashMap;
import java.util.Map;

public abstract class ITDocumentSerializer<T extends ITDocument>
        implements DocumentSerializer<T> {

    @Override
    public final @NotNull Map<String, Object> serialize(
            @NotNull T document
    ) {

        return extend(
                document,
                new LinkedHashMap<String, Object>() {{
                    put("id", document.id());
                }}
        );
    }

    protected abstract @NotNull Map<String, Object> extend(
            @NotNull T document,
            @NotNull Map<String, Object> map
    );
}
