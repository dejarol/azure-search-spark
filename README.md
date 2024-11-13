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
    </tr>
    <tr>
        <td>apiKey</td>
        <td>API key for target Azure Search service</td>
        <td>&#9989</td>
    </tr>
    <tr>
        <td>index</td>
        <td>Name of target Azure Search index</td>
        <td>&#9989</td>
    </tr>
    <tr>
        <td>filter</td>
        <td>OData filter for restricting the set of documents to retrieve</td>
        <td></td>
    </tr>
    <tr>
        <td>select</td>
        <td>Comma-separated list of index fields to retrieve</td>
        <td></td>
    </tr>
    <tr>
        <td>partitioner</td>
        <td>The partitioner full class name. 
You can specify a custom implementation that must implement the <code>com.github.jarol.azure.search.spark.sql.connector.read.partitioning.SearchPartitioner</code> interface.
See the <b>Partitioner options</b> section for more information about partitioners.
</td>
        <td></td>
    </tr>
    <tr>
        <td>partitioner.options.*</td>
        <td>Options that will be propagated to selected partitioner</td>
        <td></td>
    </tr>
</table>

### Partitioner options

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
    </tr>
    <tr>
        <td>apiKey</td>
        <td>API key for target Azure Search service</td>
        <td>&#9989</td>
    </tr>
    <tr>
        <td>index</td>
        <td>Name of target Azure Search index</td>
        <td>&#9989</td>
    </tr>
    <tr>
        <td>batchSize</td>
        <td></td>
        <td></td>
        <td>1000</td>
    </tr>
    <tr>    
        <td>action</td>
        <td>Action for indexing documents</td>
        <td></td>
        <td>mergeOrUpload</td>
    </tr>
    <tr>    
        <td>actionColumn</td>
        <td></td>
        <td></td>
        <td></td>
    </tr>
    <tr>
        <td>createIndex.*</td>
        <td>Options for index creation. See the <b>Index creation</b> section for more information about available options</td>
        <td></td>
        <td></td>
    </tr>
</table>

### Index creation

---
