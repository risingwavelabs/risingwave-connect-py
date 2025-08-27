# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.1] - 2025-08-26

### Fixed
- Fixed package structure issue where Python modules were not included in PyPI distribution
- Corrected hatch build configuration to properly package source code
- Improved package installation reliability

### Changed
- Removed incorrect wheel target configuration from pyproject.toml
- Updated build system to use proper src-layout auto-discovery

## [0.1.0] - 2025-08-26

### Added
- Initial release of RisingWave Pipeline SDK
- PostgreSQL CDC source integration with table discovery
- Iceberg sink support with comprehensive configuration options
- S3, PostgreSQL, and template-based sink implementations
- Pipeline builder for creating complex data pipelines
- Pydantic-based configuration validation
- Support for multiple catalog types (REST, Glue, JDBC, Storage)
- Comprehensive error handling and validation
- Type hints and documentation throughout codebase

### Features
- **Sources**: PostgreSQL CDC with SSL configuration
- **Sinks**: Iceberg, S3, PostgreSQL with advanced features
- **Discovery**: Automated table discovery with pattern matching
- **Validation**: Runtime configuration validation with detailed error messages
- **SQL Generation**: Automatic RisingWave SQL generation for sources and sinks
