# Azure Search Spark Connector

A custom implementation of a Spark datasource that supports batch read and write operations
targeting Azure AI Search (formerly known as Azure Cognitive Search). 

## Downloading

The connector is available at Maven central repository

Runtime requirements
<ul>
    <li>8 <= Java version <= 11</li>
    <li>scala-2.12</li>
    <li>Spark version 3.1.1+</li>
</ul>

Maven
```
<dependency>
    <groupId>io.github.dejarol</groupId>
    <artifactId>azure-search-spark_2.12</artifactId>
    <version>x.y.z</version>
</dependency>
```

sbt
```
libraryDependencies += "io.github.dejarol" %% "azure-search-spark" % "x.y.z"
```

gradle
```
implementation("io.github.dejarol:azure-search-spark_2.12:x.y.z")
```

spark-submit
```
spark-submit --packages io.github.dejarol:azure-search-spark_2.12:x.y.z ...
```

----

## Documentation

### Data types conversions

Here's a list of the conversions between Azure Search datatypes and Spark datatypes 

<table>
    <tr>
        <th>Search Data Type</th>
        <th>Spark Data Type</th>
        <th>Scope</th>
        <th>Notes</th>
    </tr>
    <tr>
        <td>Edm.String</td>
        <td>StringType</td>
        <td>Read/Write</td>
        <td></td>
    </tr>
    <tr>
        <td>Edm.Boolean</td>
        <td>BooleanType</td>
        <td>Read/Write</td>
        <td></td>
    </tr>
    <tr>
        <td>Edm.Int32</td>
        <td>IntegerType</td>
        <td>Read/Write</td>
        <td></td>
    </tr>
    <tr>
        <td>Edm.Int64</td>
        <td>LongType</td>
        <td>Read/Write</td>
        <td></td>
    </tr>
    <tr>
        <td>Edm.Double</td>
        <td>DoubleType</td>
        <td>Read/Write</td>
        <td></td>
    </tr>
    <tr>
        <td>Edm.DateTimeOffset</td>
        <td>TimestampType</td>
        <td>Read/Write</td>
        <td></td>
    </tr>
    <tr>
        <td>Edm.DateTimeOffset</td>
        <td>DateType</td>
        <td>Write</td>
        <td>Dates will be stored as with datetimes with time set to midnight at UTC</td>
    </tr>
    <tr>
        <td>Edm.GeographyPoint</td>
        <td>StructType</td>
        <td>Read/Write</td>
        <td>When reading, a Geopoint is converted into a StructType with the following structure
            <ul>
                <li><code>Type</code> of type StringType</li>
                <li><code>Coordinates</code> of type ArrayType(DoubleType)</li>
            </ul>
            When writing, every Structype, at every level (both top-level or nested) that matches
            the previous structure, will be stored as Geopoint
    </tr>
    <tr>
        <td>Edm.ComplexType</td>
        <td>StructType</td>
        <td>Read/Write</td>
        <td></td>
    </tr>
    <tr>
        <td>Collection(*)</td>
        <td>ArrayType(*)</td>
        <td>Read/Write</td>
        <td>The inner collection type is inferred applying these rules recursively</td>
    </tr>
</table>

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
        <td>Name of target Azure Search index. 
It can also be provided as option <code>path</code> or passed as argument to Spark <code>load()</code> method</td>
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
        <td>The partitioner type. It should be one among <code>range</code>, <code>faceted</code> or the fully  
qualified name of a class that implements 
<code>io.github.dejarol.azure.search.spark.connector.read.partitioning.PartitionerFactory</code> interface and
provides a no-arg constructor
Have a look at the <b>Partitioners</b> section for more information about partitioners.
</td>
        <td></td>
        <td></td>
    </tr>
    <tr>
        <td>partitioner.options.*</td>
        <td>Options that will be propagated to selected partitioner</td>
        <td></td>
        <td></td>
    </tr>
    <tr>
        <td>pushDownPredicate</td>
        <td>Flag for enabling Spark's predicate pushdown for this datasource scan</td>
        <td></td>
        <td>true</td>
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
(have a look at <a href="#skip_limit">this reference</a>, 
or just google <b>"azure search skip limit"</b>), we need to address parallel read operations very carefully.
Partitioners are components for handling the generation of partitions for parallel read operations.
The inspiration behind partitioners comes from the MongoDB Spark connector, where a similar concept
has been adopted.
<br>

Up to now, 3 built-in partitioners are available:
<ul>
    <li><code>FacetedPartitioner</code></li>
    <li><code>RangePartitioner</code></li>
    <li><code>SinglePartitioner</code></li>
</ul>

The connector allows users to provided their own partitioner implementation, if needed.
In general, each partitioner should create a collection of non-overlapping read partitions, 
and each of these partitions should retrieve a maximum of 100K documents.

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
.option("partitioner", "faceted")
.option("partitioner.options.facetField", "nameOfTheFacetField")
.option("partitioner.options.numPartitions", "numOfPartitions")
```  

According to the previous example
```
.option("partitioner", "faceted")
.option("partitioner.options.facetField", "category")
.option("partitioner.options.numPartitions", "4")
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
        <td>Name of a filterable numeric (<code>Edm.Int32</code>, <code>Edm.Int64</code>, <code>Edm.Double</code>) or date 
(<code>Edm.DateTimeOffset</code>) index field </td>
        <td>&#9989</td>
        <td></td>
    </tr>
    <tr>
        <td>lowerBound</td>
        <td>Lower bound for partition generation. Should be an integer for <code>Edm.Int32</code> or <code>Edm.Int64</code> fields,
a double for a <code>Edm.Double</code> field, a date with pattern <code>yyyy-MM-dd</code> for<code>Edm.DateTimeOffset</code> fields </td>
        <td>&#9989</td>
        <td></td>
    </tr>
    <tr>
        <td>upperBound</td>
        <td>Upper bound for partition generation. Should be an integer for <code>Edm.Int32</code> or <code>Edm.Int64</code> fields,
a double for a <code>Edm.Double</code> field, a date with pattern <code>yyyy-MM-dd</code> for<code>Edm.DateTimeOffset</code> fields</td>
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
.option("partitioner", "range")
.option("partitioner.options.partitionField", "numericOrDateFieldName")
.option("partitioner.options.lowerBound", "lb")
.option("partitioner.options.upperBound", "ub")
.option("partitioner.options.numPartitions", "numOfPartitions")
```

<ul>
    <li>For <code>Edm.Int32</code> or <code>Edm.Int64</code> fields, the bounds should be values that can be parsed to Integers, 
like <code>"1"</code></li>
        <li>For <code>Edm.Double</code> fields, the bounds should be values that can be parsed to Doubles, 
like <code>"3.14"</code> </li>
    <li>For <code>Edm.DateTimeOffset</code> fields, the bounds should be strings with pattern <code>yyyy-MM-dd</code>, 
like <code>"2024-12-31"</code> </li>
</ul>

#### DefaultPartitioner

The simplest partitioner: retrieves all documents within a single partition. No options are required

> ⚠️ **Warning** Suitable only for scenarios where the total number of documents to retrieve is smaller than 100K.


#### Custom

As mentioned before, the connector allows users to provide their own partitioner implementation. 
In order to do so, users must
<ul>
    <li>
        a class that implements interface <code>io.github.dejarol.azure.search.spark.connector.read.partitioning.SearchPartitioner</code>
        (i.e. the partitioner itself)
    </li>
    <li>
        a class that implements interface <code>io.github.dejarol.azure.search.spark.connector.read.partitioning.PartitionerFactory</code>,
        that provides a single no-arg constructor and returns an instance of the custom partitioner implementation
    </li>
    <li>
        provide the fully-qualified name of the factory class to option <code>partitioner</code>
    </li>
</ul>

Users can access partitioner options by means of attribute <code>partitionerOptions</code> of the 
<code>ReadConfig</code> instance passed as first parameter to the <code>createPartitioner</code> method

##### Usage

Define your custom partitioner

```
package your.own.package

class CustomPartitioner(prival val x: Option[String], private val y: Option[String]) 
    extends SearchPartitioner {
    
    override def createPartitions(): java.util.List[SearchPartition] = {
    
        // your own custom logic for creating partitions
    }
}
```

Define your custom partitioner factory

```
package your.own.package

class CustomPartitionerFactory extends PartitionerFactory {

    override def createPartitioner(readConfig: ReadConfig): SearchPartitioner = {
    
        val partitionerOptions = readConfig.partitionerOptions
        new CustomPartitioner(
            partitionerOptions.get("x"),
            partitionerOptions.get("y")
        )
    }
}
```

and then provide the following options
```
.option("partitioner", "your.own.package.CustomPartitionerFactory")
.option("partitioner.options.x", "1")
.option("partitioner.options.y", "2")
```

### A concrete read example

```
val df = spark.read.format("azsearch")
    .option("endPoint", "yourEndpoint")
    .option("apiKey", "yourApiKey")
    .option("partitioner", "faceted")
    .option("partitioner.options.facetField", "category")
    .option("partitioner.options.numPartitions", "4")
    .option("searchOptions.searchText", "hotel")
    .option("searchOptions.filter", "value eq 10")
    .option("searchOptions.searchFields", "id,value")
    .load("yourIndex")
```
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
        <td>Name of target Azure Search index.
It can also be provided as option <code>path</code> or passed as argument to Spark <code>save()</code> method</td>
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
(see <a href="#index_action_type">this reference</a>)</td>
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

The user should define a <code>fieldOptions.</code> - prefixed property for each field that should be customized.
- for a top-level field named <code>f1</code>, the key should be <code>fieldOptions.f1</code>
- for a nested field named <code>address.city</code> (where <code>address</code> is a struct), the key should be <code>fieldOptions.address.city</code>.

The values for each of these properties should be a JSON object with the following attributes (all optional)

<table>
    <tr>
        <th>Key</th>
        <th>Description</th>
    </tr>
    <tr>
        <td>analyzer</td>
        <td>Name of the lexical analyzer to use for both searching and indexing</td>
    </tr>
    <tr>
        <td>facetable</td>
        <td>Flag for enabling faceting for this field</td>
    </tr>
    <tr>
        <td>filterable</td>
        <td>Flag for enabling filtering for this field</td>
    </tr>
    <tr>
        <td>indexAnalyzer</td>
        <td>Name of the lexical analyzer to use for indexing</td>
    </tr>
    <tr>
        <td>key</td>
        <td>Flag for marking this field as the key field. 
            Unnecessary if the field name is <code>id</code>, as it will be
            be automatically set by the connector
        </td>
    </tr>
    <tr>
        <td>retrievable</td>
        <td>Flag for marking the field as retrievable</td>
    </tr>
    <tr>
        <td>searchAnalyzer</td>
        <td>Name of the lexical analyzer to use for searching</td>
    </tr>
    <tr>
        <td>searchable</td>
        <td>Flag for enabling searching for this field</td>
    </tr>
    <tr>
        <td>sortable</td>
        <td>Flag for enabling sorting for this field</td>
    </tr>
    <tr>
        <td>vectorSearchProfile</td>
        <td>Name of the vector search profile to use for this field</td>
    </tr>
    <tr>
        <td>synonymMaps</td>
        <td>JSON array of synonyms map</td>
    </tr>
    <tr>
        <td>dimensions</td>
        <td>Number of vector search dimensions</td>
    </tr>
</table>

### Index attributes

Here is the list of supported index attributes. Most of the supported attributes should be expressed in JSON form and match 
the definition stated by the Search Service REST API
(see <a href="#api_index_creation">this reference</a> for description and concrete examples for each attribute)
<table>
    <tr>
        <th>Key</th>
        <th>Description</th>
    </tr>
    <tr>
        <td>similarity</td>
        <td>JSON object matching the <code>similarity</code> attribute of an index</td>
    </tr>
    <tr>
        <td>tokenizers</td>
        <td>JSON array of objects matching the <code>tokenizers</code> attribute of an index</td>
    </tr>
    <tr>
        <td>suggesters</td>
        <td>JSON array of objects matching the <code>suggesters</code> attribute of an index</td>
    </tr>
    <tr>
        <td>analyzers</td>
        <td>JSON array of objects matching the <code>analyzers</code> attribute of an index</td>
    </tr>
    <tr>
        <td>charFilters</td>
        <td>JSON array of objects matching the <code>charFilters</code> attribute of an index</td>
    </tr>
    <tr>
        <td>scoringProfiles</td>
        <td>JSON array of objects matching the <code>scoringProfiles</code> attribute of an index</td>
    </tr>
    <tr>
        <td>tokenFilters</td>
        <td>JSON array of objects matching the <code>tokenFilters</code> attribute of an index</td>
    </tr>
    <tr>
        <td>corsOptions</td>
        <td>JSON object matching the <code>corsOptions</code> attribute of an index</td>
    </tr>
    <tr>
        <td>defaultScoringProfile</td>
        <td>A simple string representing the <code>defaultScoringProfile</code> attribute of an index</td>
    </tr>
    <tr>
        <td>vectorSearch</td>
        <td>JSON object matching the <code>vectorSearch</code> attribute of an index</td>
    </tr>
    <tr>
        <td>semanticSearch</td>
        <td>JSON object matching the <code>semanticSearch</code> attribute of an index</td>
    </tr>
    <tr>
        <td>encryptionKey</td>
        <td>JSON object matching the <code>encryptionKey</code> attribute of an index</td>
    </tr>
    <tr>
        <td>etag</td>
        <td>A simple string representing the <code>etag</code> attribute of an index</td>
    </tr>
</table>

### A concrete write example

```
df.write.format("azsearch")
    .option("endPoint", "yourEndpoint")
    .option("apiKey", "yourApiKey")
    .option("action", "mergeOrUpload")
    .option("fieldOptions.id", "{\"key\": true}")   // can be omitted, specify it only if the key field's name <> "id"
    .option("fieldOptions.name", "{\"searchable\": true, \"sortable\": true}")
    .option("indexAttributes.similarity", "{"@odata.type": "#Microsoft.Azure.Search.BM25Similarity", "k": 1.2, "b": 0.75}")
    .mode("append")
    .save("yourIndex")
```
---
### Search catalog

Starting from version 0.11.0, the connector defines its own catalog implementation for Spark SQL. To
use the catalog, you need to set the following options in your application's Spark configuration

```
spark.sql.catalog.search = io.github.dejarol.azure.search.spark.connector.SearchCatalog
spark.sql.catalog.search.endpoint = <yourAzureSearchEndpoint>
spark.sql.catalog.search.apiKey = <yourAzureSearchApiKey>
```

However, due to some limitations in the Azure Search service, the connector does not support some operations like
<ul>
    <li>altering the schema of an index (<code>ALTER TABLE</code>)</li>
    <li>renaming an index (<code>RENAME TABLE</code>)</li>
</ul>

---

## Azure Search References

<ol type="1">
    <li><a id="api_index_creation" href="https://learn.microsoft.com/en-us/rest/api/searchservice/indexes?view=rest-searchservice-2023-11-01">Search API for index creation</a></li>
    <li><a id="index_action_type" href="https://learn.microsoft.com/en-us/rest/api/searchservice/documents/?view=rest-searchservice-2024-07-01&tabs=HTTP#indexactiontype">Index Action types</a></li>
    <li><a id="skip_limit" href="https://learn.microsoft.com/en-us/dotnet/api/microsoft.azure.search.models.searchparameters.skip?view=azure-dotnet-legacy">Skip Limit</a></li>
</ol>