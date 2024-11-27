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
        <th>Option</th>
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
        <td>filter</td>
        <td>OData filter for restricting the set of documents to retrieve</td>
        <td></td>
        <td></td>
    </tr>
    <tr>
        <td>select</td>
        <td>Comma-separated list of index fields to retrieve</td>
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
        <td>com.github.jarol.azure.search.spark.sql.connector.read.partitioning.SinglePartitionPartitioner</td>
    </tr>
    <tr>
        <td>partitioner.options.*</td>
        <td>Options that will be propagated to selected partitioner</td>
        <td></td>
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

Class name <code>com.github.jarol.azure.search.spark.sql.connector.read.partitioning.FacetedPartitioner</code></li>.
<br>
// TODO: add description

#### RangePartitioner

Class name <code>com.github.jarol.azure.search.spark.sql.connector.read.partitioning.RangePartitioner</code></li>.
<br>
// TODO: add description

#### SinglePartitionPartitioner

Class name <code>com.github.jarol.azure.search.spark.sql.connector.read.partitioning.SinglePartitionPartitioner</code></li>.
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

### Write options

Here is the list of available datasource options for writing

<table>
    <tr>
        <th>Configuration</th>
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
        <td>Options for defining field properties. See the <b>Field options</b> section for more information about available options</td>
        <td></td>
        <td></td>
    </tr>
</table>

### Field options

// TODO: add description