package com.github.jarol.azure.search.spark.sql.connector.write;

import com.azure.search.documents.indexes.models.LexicalAnalyzerName;
import com.azure.search.documents.indexes.models.SearchField;
import com.github.jarol.azure.search.spark.sql.connector.core.schema.SearchFieldAction;
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

    SEARCH_AND_INDEX(
            "searchAndIndex",
            SearchField::setAnalyzerName,
            SearchField::getAnalyzerName
    ),

    /**
     * Instance for only searching
     */

    SEARCH(
            "search",
            SearchField::setSearchAnalyzerName,
            SearchField::getSearchAnalyzerName
    ),

    /**
     * Instance for only indexing
     */

    INDEX(
            "index",
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

    @Contract(pure = true)
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
     * Get a {@link SearchFieldAction} for setting an analyzer
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
