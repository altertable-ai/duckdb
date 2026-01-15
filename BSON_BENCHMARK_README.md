# JSON vs BSON Performance Benchmark

A comprehensive benchmark script comparing JSON and BSON performance in DuckDB across multiple workloads.

## Quick Start

```bash
# Basic usage (auto-detects shell)
python3 scripts/bson_json_bench.py --extension-dir build/reldebug/extension

# With all options
python3 scripts/bson_json_bench.py \
    --shell build/reldebug/duckdb \
    --extension-dir build/reldebug/extension \
    --rows 200000 \
    --runs 7 \
    --warmup 1 \
    --threads 4
```

## Prerequisites

1. **Built DuckDB CLI**: The benchmark requires a locally built DuckDB shell binary
2. **BSON Extension**: The BSON extension must be built and available

### Building the Requirements

```bash
# Build DuckDB with the BSON extension
make reldebug

# Or for release build
make release
```

## Benchmark Workloads

The script measures performance across several dimensions:

### 1. Conversion (`10_convert_json_to_bson`)
- Measures the cost of converting JSON strings to BSON format
- Uses `json_to_bson()` function

### 2. String Extraction (`20_*`, `21_*`)
- Compares extracting nested string fields
- JSON: `json_extract_string(data, '$.user.name')`
- BSON: `bson_extract_string(data, '$.user.name')`

### 3. Existence Checks (`30_*`, `31_*`)
- Tests checking if a field exists
- JSON: `json_extract(data, '$.flag') IS NOT NULL`
- BSON: `bson_exists(data, '$.flag')`

### 4. Group By Operations (`40_*`, `41_*`)
- Groups data by an extracted field
- Tests both JSON and BSON with aggregations

### 5. Storage Size (`50_*`, `51_*`)
- Compares storage overhead
- Measures bytes using `octet_length()`

## Command-Line Options

| Option | Description | Default |
|--------|-------------|---------|
| `--shell` | Path to DuckDB CLI binary | Auto-detect |
| `--extension-dir` | Directory containing extensions | Required* |
| `--bson-extension` | Direct path to `bson.duckdb_extension` | Required* |
| `--rows` | Number of rows to generate | 100,000 |
| `--runs` | Number of measured runs per query | 5 |
| `--warmup` | Number of warmup runs | 1 |
| `--threads` | Number of threads to use | DuckDB default |
| `--db` | Path for temporary database | Auto (temp) |
| `--keep-db` | Keep database after benchmark | false |

\* Either `--extension-dir` or `--bson-extension` must be specified

## Output

The script produces:
1. **Timing table**: Shows median, mean, and standard deviation for each workload
2. **Comparison ratios**: Direct JSON vs BSON performance ratios
3. **Relative speedups**: Percentage faster/slower for each workload

Example output:
```
================================================================================
BENCHMARK RESULTS (100,000 rows)
================================================================================
Benchmark                                Median (s)   Mean (s)     StdDev (s)  
--------------------------------------------------------------------------------
10_convert_json_to_bson                  0.1234       0.1245       0.0015      
20_extract_string_json                   0.0567       0.0571       0.0008      
21_extract_string_bson                   0.0234       0.0238       0.0005      
...
================================================================================

COMPARISON (JSON baseline = 1.0x):
--------------------------------------------------------------------------------
Extract String                 JSON: 0.0567s  BSON: 0.0234s  (0.41x, BSON is 58.7% faster)
...
```

## Implementation Details

- Uses DuckDB's built-in `.timer on` for accurate timing
- Parses `Run Time (s): real X.XXX` output from shell
- Generates deterministic synthetic data for reproducibility
- Each document contains nested objects, strings, numbers, and booleans
- Database is created in a temporary location and cleaned up automatically

## Sample Data Structure

Each row contains a document like:
```json
{
  "user": {
    "name": "user_123",
    "age": 35
  },
  "country": "US",
  "flag": true,
  "score": 184.5
}
```

This structure tests:
- Nested field access (`$.user.name`)
- Simple field extraction (`$.country`)
- Boolean fields (`$.flag`)
- Numeric fields (`$.score`)

## Tips for Accurate Benchmarking

1. **Use release builds** for production comparisons: `--shell build/release/duckdb`
2. **Increase row count** for stable results: `--rows 500000` or higher
3. **More runs** reduce variance: `--runs 10`
4. **Set thread count** for consistency: `--threads 4`
5. **Close other applications** to reduce system noise
6. **Run multiple times** and compare results

## Troubleshooting

### "Could not find DuckDB shell"
Specify the shell explicitly: `--shell path/to/duckdb`

### "Must specify either --extension-dir or --bson-extension"
The BSON extension must be available. Build it first with `make` and then provide:
- `--extension-dir build/reldebug/extension` (for build output), or
- `--bson-extension /path/to/bson.duckdb_extension` (direct path)

### "Setup failed" or "Query failed"
- Ensure the BSON extension is built correctly
- Check that the shell version matches the extension build
- Try `--keep-db` to inspect the database after failure

### Timing seems inconsistent
- Increase `--runs` and `--warmup`
- Use larger `--rows` values
- Ensure the system isn't under heavy load
