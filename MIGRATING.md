# Migration Guide

## From 0.9.0 to 0.10.0

### Breaking changes

- <code>fieldOptions.</code>-prefixed options have been heavily changed. Now the definition of Search field attributes 
resembles what is defined in the Search REST API. Example
  - for setting two fields to be non-filterable, in version 0.8.0 we had to specify this option
    ```
    .option("fieldOptions.nonFilterable", "field1,field2")  
    ```
    
  - now, for achieving the same effect, users should use
    ```
    .option("fieldOptions.field1", "{\"filterable\": false}")
    .option("fieldOptions.field2", "{\"filterable\": false}")
    ```
    
  - the same approach should be applied to other fields attributes, like
    - <code>nonFacetable</code> (replaced by a JSON object with <code>facetable</code> field se to false)
    - <code>nonSortable</code> (replaced by a JSON object with <code>sortable</code> field se to false)
    - <code>nonSearchable</code> (replaced by a JSON object with <code>searchable</code> field se to false)


  - in short, if users want to configure the properties of a Search index field <code>field1</code>, they must define
    an option <code> fieldOptions.field1</code>, whose values should be a JSON object defining the following properties

    - <code>analyzer</code> (string, optional) - the name of the analyzer to use for the field
    - <code>facetable</code> (boolean, optional) - if the field should be facetable
    - <code>filterable</code> (boolean, optional) - if the field should be filterable
    - <code>indexAnalyzer</code> (string, optional) - the name of the analyzer to use for indexing the field
    - <code>key</code> (boolean, optional) - if the field should be the index key field
    - <code>retrievable</code> (boolean, optional) - if the field should be retrievable
    - <code>searchAnalyzer</code> (string, optional) - the name of the analyzer to use for searching
    - <code>searchable</code> (boolean, optional) - if the field should be searchable
    - <code>sortable</code> (boolean, optional) - if the field should be sortable
    - <code>vectorSearchProfile</code> (string, optional) - the name of the vector search profile


  - example
    ```
    .option("fieldOptions.field1", "{\"analyzer\": \"standard.lucene\", \"sortable\": true}")
    ```

## From 0.8.0 to 0.9.0

### Breaking changes

- For read option <code>partitioner</code>, the user does not have to always specify the partitioner class name. Type
    - <code>faceted</code> for using a <code>FacetedPartitioner</code> 
    - <code>range</code> for using <code>RangePartitioner</code>
    - the FQ name of a class that extends <code>PartitionerFactory</code> for using a custom partitioner