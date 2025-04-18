package io.github.dejarol.azure.search.spark.connector.write;

import com.azure.search.documents.SearchDocument;
import org.apache.spark.sql.catalyst.InternalRow;

import java.io.Serializable;

/**
 * Interface for defining decoders that should convert {@link org.apache.spark.sql.catalyst.InternalRow}(s)
 * to {@link com.azure.search.documents.SearchDocument}(s). They must be serializable as they must be propagated
 * to each write partition
 */

@FunctionalInterface
public interface SearchDocumentDecoder
        extends Serializable {

    SearchDocument apply(
            InternalRow row
    );
}
