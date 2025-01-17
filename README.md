# Azure Search Spark Connector

---

Unofficial Azure Search - Spark connector

## Downloading

----

## Documentation

### Batch Read

Here is the list of available datasource options for reading

<table>
    <tr>
        <th>Key</th>
        <th>Description</th>
        <th>Required</th>
        <th>Default</th>
    </tr>
    <tr>
        <td>endpoint</td>
        <td>URL of target Azure Search service</td>
        <td>&#9989</td>
        <td></td>
    </tr>
    <tr>
        <td>apiKey</td>
        <td>API key for target Azure Search service</td>
        <td>&#9989</td>
        <td></td>
    </tr>
    <tr>
        <td>index</td>
        <td>Name of target Azure Search index</td>
        <td>&#9989</td>
        <td></td>
    </tr>
    <tr>
        <td>searchOptions.*</td>
        <td>Options for querying documents. See the <b>Search options</b> section for more details</td>
        <td></td>
        <td></td>
    </tr>
    <tr>
        <td>partitioner</td>
        <td>The partitioner full class name. 
You can specify a custom implementation that must implement the <code>com.github.jarol.azure.search.spark.sql.connector.read.partitioning.SearchPartitioner</code> interface.
Have a look at the <b>Partitioners</b> section for more information about partitioners.
</td>
        <td></td>
        <td>com.github.jarol.azure.search.spark.sql.connector.read.partitioning.DefaultPartitioner</td>
    </tr>
    <tr>
        <td>partitioner.options.*</td>
        <td>Options that will be propagated to selected partitioner</td>
        <td></td>
        <td></td>
    </tr>
</table>

### Search options

Here is the list of available options for refining documents search

<table>
    <tr>
        <th>Key</th>
        <th>Description</th>
        <th>Default</th>
    </tr>
    <tr>
        <td>searchText</td>
        <td>A full-text search query expression</td>
        <td></td>
    </tr>
    <tr>
        <td>filter</td>
        <td>OData $filter expression to apply to the search query</td>
        <td></td>
    </tr>
    <tr>
        <td>select</td>
        <td>Comma-separated list of top-level fields to select</td>
        <td></td>
    </tr>
    <tr>
        <td>queryType</td>
        <td>Query type. Should be a valid value of enum <code>com.azure.search.documents.models.QueryType</code></td>
        <td></td>
    </tr>
    <tr>
        <td>searchMode</td>
        <td>Search mode. Should be a valid value of enum <code>com.azure.search.documents.models.SearchMode</code></td>
        <td></td>
    </tr>
    <tr>
        <td>searchFields</td>
        <td>Comma-separated list of field names to which to scope the full-text search</td>
        <td></td>
    </tr>
</table>

### Partitioners

Since Azure Search Service does not allow to retrieve more than 100K documents per read operation 
(have a look at <a href="https://learn.microsoft.com/en-us/dotnet/api/microsoft.azure.search.models.searchparameters.skip?view=azure-dotnet-legacy">this</a>, 
or just google <b>"azure search skip limit"</b>), we need to address parallel read operations very carefully.
Partitioners are components for handling the generation of partitions for parallel read operations.
<br>
<br>
Extending the <code>com.github.jarol.azure.search.spark.sql.connector.read.partitioning.SearchPartitioner</code> interface, each partitioner should 
create a collection of non-overlapping read partitions, and each of these partitions should retrieve a maximum of 100K documents.

Here's a summary on available partitioners and how to define a custom partitioner

#### FacetedPartitioner

Partitioner options
<table>
    <tr>
        <th>Key</th>
        <th>Description</th>
        <th>Required</th>
        <th>Default</th>
    </tr>
    <tr>
        <td>facetField</td>
        <td>Name of a facetable index field</td>
        <td>&#9989</td>
        <td></td>
    </tr>
    <tr>
        <td>numPartitions</td>
        <td>Number of partitions to generate</td>
        <td></td>
        <td>Default number of facets configured by the Search service</td>
    </tr>
</table>
Partitioner that generates partitions depending on facets. Given a facetable field named <code>f1</code> and <code>n</code> partitions,
it will generate <code>n-1</code> partitions related to the <code>n-1</code> most frequent values of <code>f1</code> and one partition for 
values of <code>f1</code> that are null or not equal to any of the <code>n-1</code> most frequent values
<br>
As an example, given a facetable field named <code>category</code>, 4 partitions and values
<code>A</code>, <code>B</code>, <code>C</code> being the 3 most frequent category values, it will generate 4 partitions
where 
<ul>
    <li><code>p0</code> (the first partition) will hold documents where <code>category = A</code></li>
    <li><code>p1</code> will hold documents where <code>category = B</code></li>
    <li><code>p2</code> will hold documents where <code>category = C</code></li>
    <li><code>p3</code> will hold documents where <code>category is null or category not in (A, B, C)</code></li>
</ul>

##### Usage

In order to use such partitioner, provide the following options to the Spark reader
```
.option('partitioner', 'com.github.jarol.azure.search.spark.sql.connector.read.partitioning.FacetedPartitioner')
.option('partitioner.options.facetField', 'nameOfTheFacetField')
.option('partitioner.options.numPartitions', 'numOfPartitions')
```  

According to the previous example
```
.option('partitioner', 'com.github.jarol.azure.search.spark.sql.connector.read.partitioning.FacetedPartitioner')
.option('partitioner.options.facetField', 'category')
.option('partitioner.options.numPartitions', '4')
```

#### RangePartitioner

Partitioner options

<table>
    <tr>
        <th>Key</th>
        <th>Description</th>
        <th>Required</th>
        <th>Default</th>
    </tr>
    <tr>
        <td>partitionField</td>
        <td>Name of a filterable numeric or date index field </td>
        <td>&#9989</td>
        <td></td>
    </tr>
    <tr>
        <td>lowerBound</td>
        <td>Lower bound for partition generation</td>
        <td>&#9989</td>
        <td></td>
    </tr>
    <tr>
        <td>upperBound</td>
        <td>Upper bound for partition generation</td>
        <td>&#9989</td>
        <td></td>
    </tr>
    <tr>
        <td>numPartitions</td>
        <td>Number of partitions to generate</td>
        <td>&#9989</td>
        <td></td>
    </tr>
</table>

Partitioner that exploits a mechanism similar to the one adopted by Spark JDBC datasource.
Indeed, <code>partitionField</code>, <code>lowerBound</code>, <code>upperBound</code> and <code>numPartitions</code> represent
the Search-related equivalent of, respectively, <code>partitionColumn</code>, <code>lowerBound</code>, <code>upperBound</code> 
and <code>numPartitions</code> JDBC datasource options

##### Usage

In order to use such partitioner, provide the following options to the Spark reader
```
.option('partitioner', 'com.github.jarol.azure.search.spark.sql.connector.read.partitioning.RangePartitioner')
.option('partitioner.options.partitionField', 'numericOrDateFieldName')
.option('partitioner.options.lowerBound', 'lb')
.option('partitioner.options.upperBound', 'ub')
.option('partitioner.options.numPartitions', 'numOfPartitions')
```  

#### SinglePartitionPartitioner

Class name <code>com.github.jarol.azure.search.spark.sql.connector.read.partitioning.SinglePartitionPartitioner</code>.
<br>
The simplest partitioner: retrieves all documents within a single partition. Suitable only for scenarios where the total number of documents 
to retrieve is smaller than 100K

#### Custom

Of course, you can create your own partitioner implementation, given that
<ul>
    <li>it implements the interface <code>com.github.jarol.azure.search.spark.sql.connector.read.partitioning.SearchPartitioner</code></li>
    <li>it provides a single, one-arg constructor accepting an instance of <code>com.github.jarol.azure.search.spark.sql.connector.read.ReadConfig</code></li>
</ul>

---

### Batch Write

Here is the list of available datasource options for writing

<table>
    <tr>
        <th>Key</th>
        <th>Description</th>
        <th>Required</th>
        <th>Default</th>
    </tr>
    <tr>
        <td>endpoint</td>
        <td>URL of target Azure Search service</td>
        <td>&#9989</td>
        <td></td>
    </tr>
    <tr>
        <td>apiKey</td>
        <td>API key for target Azure Search service</td>
        <td>&#9989</td>
        <td></td>
    </tr>
    <tr>
        <td>index</td>
        <td>Name of target Azure Search index</td>
        <td>&#9989</td>
        <td></td>
    </tr>
    <tr>
        <td>batchSize</td>
        <td>The maximum number of index operations to batch in a write operation</td>
        <td></td>
        <td>1000</td>
    </tr>
    <tr>    
        <td>action</td>
        <td>Action for indexing all documents within the input DataFrame.
It should be a valid index action according to the Azure Search API 
(see <a href="https://learn.microsoft.com/en-us/rest/api/searchservice/documents/?view=rest-searchservice-2024-07-01&tabs=HTTP#indexactiontype">this</a>)</td>
        <td></td>
        <td>mergeOrUpload</td>
    </tr>
    <tr>    
        <td>actionColumn</td>
        <td>Name of a string column within the input DataFrame that contains a row-specific index action. 
Values within such column should be valid index actions according to the Azure Search API</td>
        <td></td>
        <td></td>
    </tr>
    <tr>
        <td>fieldOptions.*</td>
        <td>Options for defining field properties. Used only at index creation time. See the <b>Field options</b> section for more information about available options</td>
        <td></td>
        <td></td>
    </tr>
    <tr>
        <td>indexAttributes.*</td>
        <td>Options for defining Search index properties. Used only at index creation time. See the <b>Index attributes</b> section for more information about supported attributes</td>
        <td></td>
        <td></td>
    </tr>
</table>

### Field options

Here is the list of supported field attributes

<table>
    <tr>
        <th>Key</th>
        <th>Description</th>
        <th>Required</th>
    </tr>
    <tr>
        <td>key</td>
        <td>Name of key field</td>
        <td>&#9989</td>
    </tr>
    <tr>
        <td>nonFilterable</td>
        <td>Comma-separated list of fields that should be marked as non-filterable</td>
        <td></td>
    </tr>
    <tr>
        <td>nonSortable</td>
        <td>Comma-separated list of fields that should be marked as non-sortable</td>
        <td></td>
    </tr>
    <tr>
        <td>hidden</td>
        <td>Comma-separated list of fields that should be marked as non-retrievable</td>
        <td></td>
    </tr>
    <tr>
        <td>nonSearchable</td>
        <td>Comma-separated list of string fields that should be marked as non-searchable</td>
        <td></td>
    </tr>
    <tr>
        <td>nonFacetable</td>
        <td>Comma-separated list of fields that should be marked as non-facetable</td>
        <td></td>
    </tr>
    <tr>
        <td>analyzers</td>
        <td>List of field analyzer configurations</td>
        <td></td>
    </tr>
</table>

### Index attributes

Here is the list of supported index attributes. They resemble the same definition stated by the Search Service REST API
(see <a href="#api_index_creation">this reference</a> for description and concrete examples for each attribute)
<ul>
    <li>similarity</li>
    <li>tokenizers</li>
    <li>suggesters</li>
    <li>analyzers</li>
    <li>charFilters</li>
    <li>scoringProfiles</li>
    <li>tokenFilters</li>
    <li>corsOptions</li>
    <li>defaultScoringProfile</li>
</ul>

---

## Azure Search References

<ol type="1">
    <li><a id="api_index_creation" href="https://learn.microsoft.com/en-us/rest/api/searchservice/indexes?view=rest-searchservice-2023-11-01">API for index creation</a></li>
</ol>