# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.12.0]

### Added

- write option <code>excludeFromGeoConversion</code>

## [0.11.0]

### Added

- catalog implementation for Spark SQL

### Changed 

- logic for setting the index key field at index creation. If the user hasn't specified the index key field,
the connector will set the key field to be <code>id</code>. Thus, the user can avoid specifying
```
.option("fieldOptions.id", "{\"key\": true}")
```


## [0.10.2]

### Added

- support for defining a synonym maps at field creation
- support for defining vector search dimensions at field creation

## [0.10.1]

### Added

- support for defining a semantic search at index creation
- support for defining encryption at index creation

## [0.10.0]

### Added

- support for defining a vector search at index creation
- support for defining a vector search profile at field creation

### Changed
- altered <code>fieldOptions.*</code> write options for enriching Search fields. 
Look at migration guide for details

## [0.9.0]

### Changed

- altered <code>partitioner</code> read option so that the user does not have to specify the partitioner class name.
Look at migration guide for details

## [0.8.0]

- First release