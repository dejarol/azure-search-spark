package io.github.dejarol.azure.search.spark.connector.write.config;

import com.azure.search.documents.indexes.models.LexicalAnalyzerName;
import com.azure.search.documents.indexes.models.SearchField;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * Types of Search field lexical analyzers.
 * <br>
 * According to the Azure Search API, a string field can define 3 types of lexical analyzers
 * <ul>
 *     <li>for both searching and indexing</li>
 *     <li>only for searching</li>
 *     <li>only for indexing</li>
 * </ul>
 */

public enum SearchFieldAnalyzerType {

    /**
     * Instance for both searching and indexing
     */

    ANALYZER(
            "analyzer",
            SearchField::setAnalyzerName,
            SearchField::getAnalyzerName
    ),

    /**
     * Instance for only searching
     */

    SEARCH_ANALYZER(
            "searchAnalyzer",
            SearchField::setSearchAnalyzerName,
            SearchField::getSearchAnalyzerName
    ),

    /**
     * Instance for only indexing
     */

    INDEX_ANALYZER(
            "indexAnalyzer",
            SearchField::setIndexAnalyzerName,
            SearchField::getIndexAnalyzerName
    );

    private final String description;
    private final BiFunction<SearchField, LexicalAnalyzerName, SearchField> setter;
    private final Function<SearchField, LexicalAnalyzerName> getter;

    /**
     * Create an instance
     * @param description description
     * @param setter function for setting the analyzer
     * @param getter function for getting an analyzer
     */

    SearchFieldAnalyzerType(
            @NotNull String description,
            @NotNull BiFunction<SearchField, LexicalAnalyzerName, SearchField> setter,
            @NotNull Function<SearchField, LexicalAnalyzerName> getter
    ) {
        this.description = description;
        this.setter = setter;
        this.getter = getter;
    }

    /**
     * Get the description of this instance
     * @return instance's description
     */

    @Contract(pure = true)
    public String description() {
        return description;
    }

    /**
     * Get a {@link  io.github.dejarol.azure.search.spark.connector.core.schema.SearchFieldAction} for setting an analyzer
     * @param name analyzer to set
     * @return an action for setting an analyzer on a field
     */

    public @NotNull SearchField setOnField(
            @NotNull SearchField field,
            @NotNull LexicalAnalyzerName name
    ) {

        return setter.apply(field, name);
    }

    /**
     * Get an analyzer instance from a field
     * @param field a Search field
     * @return an analyzer instance, or null if the analyzer was not set
     */

    public @Nullable LexicalAnalyzerName getFromField(
            @NotNull SearchField field
    ) {
        return getter.apply(field);
    }
}
