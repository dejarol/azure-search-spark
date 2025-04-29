package io.github.dejarol.azure.search.spark.connector.read.encoding;

import com.azure.search.documents.SearchDocument;
import org.apache.spark.sql.catalyst.InternalRow;

import java.io.Serializable;

/**
 * Interface for defining encoders that should convert {@link com.azure.search.documents.SearchDocument}(s)
 * to {@link org.apache.spark.sql.catalyst.InternalRow}(s). They must be serializable as they must be propagated
 * to each read partition
 */

@FunctionalInterface
public interface SearchDocumentEncoder
        extends Serializable {

    /**
     * Converts the given document to a Spark internal row
     * @param document a document retrieved from a Search index
     * @return a Spark internal row that contains document values
     */

    InternalRow apply(
            SearchDocument document
    );
}
