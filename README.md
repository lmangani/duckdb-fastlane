# DuckDB FastLanes Extension

This extension adds support for reading and writing FastLanes (`.fls`) files in DuckDB. FastLanes is a high-performance columnar data format that provides 40% better compression and 40× faster decoding compared to Parquet.

## Features

- **Read FastLanes files**: Use `read_fastlane()` function to read `.fls` files
- **Write FastLanes files**: Use `write_fastlane()` function to write data to FastLanes format
- **COPY TO support**: Use `COPY TO` with FastLanes format for efficient file writing
- **Automatic file detection**: Files with `.fls` or `.fastlane` extensions are automatically detected
- **Projection pushdown**: Efficient column selection support
- **Multi-file support**: Read multiple files with glob patterns or file lists

## Installation

### Building from Source

1. Clone this repository:
```bash
git clone <repository-url>
cd duckdb-fastlane
```

2. Build the extension:
```bash
make
```

3. Load the extension in DuckDB:
```sql
LOAD fastlane;
```

## Usage

### Reading FastLanes Files

```sql
-- Load the extension
LOAD fastlane;

-- Read a FastLanes file
SELECT * FROM read_fastlane('data.fls');

-- Read multiple files
SELECT * FROM read_fastlane(['file1.fls', 'file2.fls']);

-- Read files with glob pattern
SELECT * FROM read_fastlane('*.fls');

-- Automatic detection (files with .fls extension)
SELECT * FROM 'data.fls';
```

### Writing FastLanes Files

#### Using Table Functions
```sql
-- Convert a table to FastLanes format
SELECT * FROM write_fastlane(SELECT * FROM my_table);

-- Convert query results to FastLanes
SELECT * FROM write_fastlane(
    SELECT id, name, value 
    FROM source_table 
    WHERE value > 100
);
```

#### Using COPY TO (Recommended)
```sql
-- Basic COPY TO (equivalent to nanoarrow's COPY TO "test.arrows")
COPY (SELECT 42 as foofy, 'string' as stringy) TO "test.fls";

-- COPY TO with format specification
COPY (SELECT 42 as foofy, 'string' as stringy) TO "test_2.fls" (FORMAT FLS);

-- COPY TO with FastLanes format
COPY (SELECT 42 as foofy, 'string' as stringy) TO "test_3.fastlane" (FORMAT FASTLANE);

-- COPY TO with options
COPY (SELECT 42 as foofy, 'string' as stringy) TO "test_4.fls" (FORMAT FLS, ROW_GROUP_SIZE 1000);
```

### Version Information

```sql
-- Get extension version
SELECT fastlane_version();
```

## API Reference

### Table Functions

#### `read_fastlane(file_path)`
Reads a FastLanes file and returns the data as a table.

**Parameters:**
- `file_path` (VARCHAR): Path to the FastLanes file (supports glob patterns and file lists)

**Returns:** Table with the data from the FastLanes file

**Features:**
- Automatic schema detection
- Projection pushdown
- Multi-file support
- Error handling

#### `write_fastlane(query)`
Converts query results to FastLanes format.

**Parameters:**
- `query`: Any valid SQL query

**Returns:** Table with columns:
- `fastlane_data` (BLOB): The serialized FastLanes data
- `is_header` (BOOLEAN): Whether this row contains header information

**Features:**
- Type conversion
- Metadata generation
- Support for complex queries

### COPY Functions

#### `COPY TO` with FastLanes Format
Writes query results directly to FastLanes files.

**Syntax:**
```sql
COPY (SELECT ...) TO "filename.fls" (FORMAT FLS [, options]);
```

**Supported Options:**
- `ROW_GROUP_SIZE`: Number of rows per rowgroup (default: 65536)
- `ROW_GROUP_SIZE_BYTES`: Maximum bytes per rowgroup
- `ROW_GROUPS_PER_FILE`: Maximum rowgroups per file

**Examples:**
```sql
-- Basic usage
COPY (SELECT * FROM my_table) TO "data.fls";

-- With options
COPY (SELECT * FROM my_table) TO "data.fls" (FORMAT FLS, ROW_GROUP_SIZE 10000);
```

### Scalar Functions

#### `fastlane_version()`
Returns the version of the FastLanes extension.

**Returns:** VARCHAR containing the extension version

## Implementation Details

The extension integrates with the FastLanes C++ library to provide:

1. **Schema Detection**: Automatically detects column types and names from FastLanes files
2. **Rowgroup Support**: Handles FastLanes rowgroup structure for efficient reading
3. **Type Conversion**: Maps between DuckDB and FastLanes data types
4. **Error Handling**: Graceful handling of missing or corrupted files
5. **COPY Integration**: Full COPY TO/FROM support with FastLanes format

### Data Type Mapping

| DuckDB Type | FastLanes Type |
|-------------|----------------|
| INTEGER     | INT32/INT64    |
| BIGINT      | INT64          |
| DOUBLE      | DOUBLE         |
| VARCHAR     | STR            |
| BOOLEAN     | BOOLEAN        |
| DATE        | DATE           |

## Dependencies

- DuckDB (built against)
- FastLanes C++ library (automatically fetched via CMake)
- OpenSSL (for cryptographic operations)

## Development

### Project Structure

```
src/
├── include/
│   ├── table_function/     # Table function headers
│   ├── writer/            # Writer headers
│   └── type_mapping.hpp   # Type conversion utilities
├── scanner/               # Reading implementations
├── writer/                # Writing implementations
└── fastlane_extension.cpp # Main extension file
```

### Building

The extension uses CMake and automatically fetches the FastLanes library:

```bash
mkdir build
cd build
cmake ..
make
```

### Testing

Run the test suite:

```bash
make test
```

## License

This extension is licensed under the same terms as DuckDB.

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests
5. Submit a pull request

## References

- [FastLanes Documentation](https://github.com/cwida/FastLanes)
- [DuckDB Extensions Guide](https://duckdb.org/docs/extensions/overview)
