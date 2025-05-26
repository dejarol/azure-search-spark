# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.10.1]

### Added

- support for defining a semantic search at index creation
- support for defining encryption at index creation

## [0.10.0]

### Added

- support for defining a vector search at index creation
- support for defining a vector search profile at field creation

### Changed
- altered <code>fieldOptions.*</code> write options for enriching Search fields

## [0.9.0]

### Changed

- altered <code>partitioner</code> read option so that the user does not have to specify the partitioner class name.
  - for using <code>FacetedPartitioner</code>

## [0.8.0]

- First release