package com.github.jarol.azure.search.spark.sql.connector.read.partitioning;

import com.azure.search.documents.models.SearchOptions;

import java.util.List;
import java.util.Objects;

public class SearchPartitionImpl
        implements SearchPartition {

    private final String filter;
    private final List<String> select;

    public SearchPartitionImpl(
            String filter,
            List<String> select
    ) {
        this.filter = filter;
        this.select = select;
    }

    @Override
    public SearchOptions getSearchOptions() {

        return new SearchOptions()
                .setFilter(filter)
                .setSelect(Objects.isNull(select) ? null : select.toArray(new String[0]));
    }
}
