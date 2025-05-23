package io.github.dejarol.azure.search.spark.connector.core.schema;

import com.azure.search.documents.indexes.models.SearchField;
import org.jetbrains.annotations.NotNull;

/**
 * Parent interface for actions (transformations)
 * to apply on {@link com.azure.search.documents.indexes.models.SearchField}(s)
 */

public interface SearchFieldAction {

    /**
     * Transform a {@link com.azure.search.documents.indexes.models.SearchField}
     * @param field field
     * @return the transformed version of the input field
     */

    @NotNull
    SearchField apply(
            @NotNull SearchField field
    );
}
