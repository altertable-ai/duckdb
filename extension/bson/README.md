# BSON Extension

The BSON extension adds support for the [BSON (Binary JSON)](http://bsonspec.org/) format to DuckDB.

## Features

### BSON Type

The extension adds a `BSON` logical type that is internally stored as `BLOB`. This allows efficient storage and manipulation of BSON documents without converting to/from JSON text.

```sql
-- Cast from JSON string to BSON (easiest way!)
SELECT '{"name": "Alice", "age": 30}'::BSON;

-- Cast from BLOB to BSON (validates the document)
SELECT '\x05\x00\x00\x00\x00'::BLOB::BSON;

-- Cast from BSON to BLOB (free operation)
SELECT bson_column::BLOB FROM my_table;
```

### JSON to BSON Casting

The easiest way to create BSON values is by casting from JSON strings. This makes tests and examples much more readable:

```sql
-- Instead of writing raw hex bytes:
-- SELECT '\x14\x00\x00\x00\x02name\x00\x05\x00\x00\x00John\x00\x00'::BLOB::BSON;

-- You can write readable JSON:
SELECT '{"name": "John"}'::BSON;

-- This works in all contexts:
CREATE TABLE users (id INT, data BSON);
INSERT INTO users VALUES (1, '{"name": "Alice", "email": "alice@example.com"}'::BSON);
```

### Functions

#### `bson_valid(bson) → BOOLEAN`

Check if a BSON value is a valid BSON document.

```sql
SELECT bson_valid('\x05\x00\x00\x00\x00'::BSON);
-- true
```

#### `bson_exists(bson, path) → BOOLEAN`

Check if a field exists at the given path.

```sql
SELECT bson_exists(bson_col, '$.field.subfield');
```

#### `bson_type(bson, path) → VARCHAR`

Get the BSON type of the value at the given path.

```sql
SELECT bson_type(bson_col, '$.count');
-- 'int32'
```

Possible return values:

- `'double'`, `'string'`, `'document'`, `'array'`, `'binary'`, `'objectid'`
- `'boolean'`, `'datetime'`, `'null'`, `'regex'`, `'javascript'`
- `'int32'`, `'timestamp'`, `'int64'`, `'decimal128'`
- `'minkey'`, `'maxkey'`

#### `bson_extract(bson, path) → BSON`

Extract a sub-document or array at the given path.

```sql
SELECT bson_extract(bson_col, '$.nested.document');
```

Note: Currently only returns documents and arrays. Scalar values return NULL.

#### `bson_extract_string(bson, path) → VARCHAR`

Extract a string value at the given path (fast path for string fields).

```sql
SELECT bson_extract_string(bson_col, '$.name');
```

### Path Syntax

The extension supports JSONPath-like syntax for accessing fields:

- `$` - Root document
- `$.field` - Access object field
- `$."field"` - Access object field with special characters
- `$.field[0]` - Access array element
- `$.field.nested` - Nested field access

## Performance

BSON extraction is typically faster than JSON extraction because:

1. **No UTF-8 validation**: BSON stores string lengths, avoiding character-by-character validation
2. **No escape sequence parsing**: Strings are stored raw
3. **Binary format**: Direct memory access without text parsing
4. **Pre-computed lengths**: All values have explicit lengths, enabling fast skipping

Best performance gains when:

- Data is already in BSON format (e.g., from MongoDB)
- Performing many extractions on the same data
- Extracting from deeply nested structures

## Limitations (v1)

- No file I/O functions (`read_bson`, `write_bson`) yet
- `bson_extract` only returns documents/arrays, not scalar values
- No wildcard path support yet
- No conversion functions between BSON and JSON yet

## Examples

```sql
-- Load the extension
LOAD bson;

-- Create a table with BSON column
CREATE TABLE users (
    id INTEGER,
    data BSON
);

-- Insert BSON data (from application or conversion)
INSERT INTO users VALUES (1, '\x20\x00\x00\x00...'::BLOB::BSON);

-- Query BSON data
SELECT
    id,
    bson_extract_string(data, '$.name') as name,
    bson_type(data, '$.age') as age_type,
    bson_exists(data, '$.email') as has_email
FROM users;

-- Filter by BSON field existence
SELECT * FROM users WHERE bson_exists(data, '$.premium');
```

## Building

The BSON extension is built as part of the standard DuckDB build process:

```bash
make
```

To build just the BSON extension:

```bash
BUILD_BSON=1 make
```

## Testing

Run the BSON tests:

```bash
make test_bson
```

Or run specific tests:

```bash
./build/release/test/unittest "test/sql/bson/*"
```
