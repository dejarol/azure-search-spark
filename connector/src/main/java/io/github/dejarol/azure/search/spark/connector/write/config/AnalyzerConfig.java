package io.github.dejarol.azure.search.spark.connector.write.config;

import com.azure.search.documents.indexes.models.LexicalAnalyzerName;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.node.ArrayNode;
import io.github.dejarol.azure.search.spark.connector.core.utils.JavaCollections;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.util.*;
import java.util.function.BiPredicate;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * Configuration for an analyzer
 */

@JsonDeserialize(using = AnalyzerConfig.Deserializer.class)
public class AnalyzerConfig {

    public static final String NAME_PROPERTY = "name";
    public static final String TYPE_PROPERTY = "type";
    public static final String FIELDS_PROPERTY = "fields";

    /**
     * Create a supplier that will provide a string stating that an analyzer property cannot be null
     * @param property property name
     * @return a supplier to use for generating error messages
     */

    @Contract(pure = true)
    protected static @NotNull Supplier<String> supplierForNonNullAnalyzerProperty(
            String property
    ) {

        return () -> String.format(
                "Analyzer property '%s' cannot be null",
                property
        );
    }

    /**
     * Custom deserializer for this class
     */

    protected static class Deserializer
            extends StdDeserializer<AnalyzerConfig> {

        /**
         * No-arg constructor
         */

        protected Deserializer() {
            super(AnalyzerConfig.class);
        }

        @Override
        public AnalyzerConfig deserialize(
                @NotNull JsonParser p,
                @NotNull DeserializationContext ctxt
        ) throws IOException {

            JsonNode node = p.getCodec().readTree(p);

            // All three properties should be not null
            String name = Objects.requireNonNull(node.get(NAME_PROPERTY), supplierForNonNullAnalyzerProperty(NAME_PROPERTY)).asText();
            String type = Objects.requireNonNull(node.get(TYPE_PROPERTY), supplierForNonNullAnalyzerProperty(TYPE_PROPERTY)).asText();
            JsonNode fieldsNode = Objects.requireNonNull(node.get(FIELDS_PROPERTY), supplierForNonNullAnalyzerProperty(FIELDS_PROPERTY));

            // Resolve properties and create an instance
            return new AnalyzerConfig(
                    resolveName(name),
                    resolveType(type),
                    getFields(fieldsNode)
            );
        }

        /**
         * Resolve a value by name
         * <br>
         * This method will look for the first instance that matches the name according to the given predicate.
         * Otherwise, a {@link NoSuchElementException} will be thrown
         * @param name name to resolve
         * @param values collection of values
         * @param predicate predicate to be match
         * @param messageProvider function for creating the message for the {@link NoSuchElementException}
         * @param valueToStringFunction function for converting a value to a string
         * @param <T> value type
         * @return the first value matching the name according to the predicate
         */

        private <T> T resolveValueUsingPredicate(
                @NotNull String name,
                @NotNull Collection<T> values,
                @NotNull BiPredicate<T, String> predicate,
                @NotNull Function<String, String> messageProvider,
                @NotNull Function<T, String> valueToStringFunction
        ) {

            return values.stream().filter(
                    value -> predicate.test(value, name)
            ).findFirst().orElseThrow(
                    () -> {

                        String allValuesList = values.stream()
                                .map(valueToStringFunction)
                                .collect(Collectors.joining("|"));

                        return new NoSuchElementException(
                                String.format("%s. Should be one among (case-insensitive) [%s]",
                                        messageProvider.apply(name),
                                        allValuesList
                                )
                        );
                    }
            );
        }

        /**
         * Resolve a {@link LexicalAnalyzerName} by name (case-insensitive)
         * @param name name to resolve
         * @return a {@link LexicalAnalyzerName}
         */

        private LexicalAnalyzerName resolveName(
                String name
        ) {

            return resolveValueUsingPredicate(
                    name,
                    LexicalAnalyzerName.values(),
                    (analyzerName, s) -> analyzerName.toString().equalsIgnoreCase(s),
                    s -> String.format("Analyzer '%s' does not exist", s),
                    LexicalAnalyzerName::toString
            );
        }

        /**
         * Resolve an analyzer type by name or description (case-insensitive)
         * @param nameOrDescription name or description to resolve
         * @return a {@link SearchFieldAnalyzerType}
         */

        private SearchFieldAnalyzerType resolveType(
                String nameOrDescription
        ) {

            return resolveValueUsingPredicate(
                    nameOrDescription,
                    Arrays.asList(SearchFieldAnalyzerType.values()),
                    (e, s)-> e.name().equalsIgnoreCase(s) || e.description().equalsIgnoreCase(s),
                    s -> String.format("Search analyzer type '%s' does not exist", s),
                    s -> String.format("%s|%s",
                            s.description(),
                            s.name()
                    )
            );
        }

        /**
         * Deserialize given node as a list of string representing config fields
         * @param node node
         * @return a collection of strings
         * @throws IOException if deserialization fails
         */

        private List<String> getFields(
                @NotNull JsonNode node
        ) throws IOException {

            // Check if node is an array and all elements are strings
            if (node.isArray()) {

                ArrayNode arrayNode = (ArrayNode) node;
                List<JsonNode> children = JavaCollections.iteratorToList(arrayNode.elements());
                boolean allChildrenAreStrings = children.stream().allMatch(JsonNode::isTextual);

                // If all elements are strings, convert them to a list and return
                if (allChildrenAreStrings) {
                    return children.stream()
                           .map(JsonNode::asText)
                           .collect(Collectors.toList());
                } else {
                    throw new IOException(
                            String.format(
                                    "Invalid format for %s property. All elements must be strings",
                                    FIELDS_PROPERTY
                            )
                    );
                }

            } else {
                throw new IOException(
                        String.format(
                                "Invalid format for %s property. Expected an array",
                                FIELDS_PROPERTY
                                )
                );
            }
        }
    }

    private final LexicalAnalyzerName name;
    private final SearchFieldAnalyzerType type;
    private final List<String> fields;

    /**
     * Create an instance
     * @param name name
     * @param type type
     * @param fields fields
     */

    public AnalyzerConfig(
            @NotNull LexicalAnalyzerName name,
            @NotNull SearchFieldAnalyzerType type,
            @NotNull List<String> fields
    ) {
        this.name = name;
        this.type = type;
        this.fields = fields;
    }

    /**
     * Get the name of the lexical analyzer
     * @return lexical analyzer name
     */

    public LexicalAnalyzerName getName() {
        return name;
    }

    /**
     * Get the analyzer type
     * @return analyzer type
     */

    public SearchFieldAnalyzerType getType() {
        return type;
    }

    /**
     * Get the list of fields on which the analyzer should be set
     * @return fields that should be enriched with this analyzer setting
     */

    public List<String> getFields() {
        return fields;
    }
}
