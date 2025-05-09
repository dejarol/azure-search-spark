package io.github.dejarol.azure.search.spark.connector.write.decoding;

import com.azure.search.documents.SearchDocument;
import org.apache.spark.sql.catalyst.InternalRow;

import java.io.Serializable;

/**
 * Interface for defining decoders that should convert {@link org.apache.spark.sql.catalyst.InternalRow}(s)
 * to {@link com.azure.search.documents.SearchDocument}(s) to be written later on.
 * They must be serializable as they must be propagated to each write partition
 */

@FunctionalInterface
public interface SearchDocumentDecoder
        extends Serializable {

    /**
     * Converts an internal row to a Search document
     * @param row the row to decode
     * @return a decoded Search document
     */

    SearchDocument apply(
            InternalRow row
    );
}
