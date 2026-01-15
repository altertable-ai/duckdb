#!/usr/bin/env python3
"""
JSON vs BSON Performance Benchmark for DuckDB

This script creates comparable JSON and BSON tables and measures performance across:
- Conversion (JSON -> BSON)
- Field extraction
- Existence checks
- Scan + filter + groupby operations
- Storage size comparisons

Usage:
    python3 scripts/bson_json_bench.py --shell build/reldebug/duckdb \\
        --extension-dir build/reldebug/extension --rows 100000 --runs 5

The script leverages DuckDB's built-in .timer to measure real wall-clock time.
"""

import argparse
import os
import re
import subprocess
import sys
import tempfile
import statistics
from pathlib import Path
from typing import List, Tuple, Optional, Dict

print = __import__('functools').partial(print, flush=True)


def find_default_shell() -> Optional[str]:
    """Try to find a built DuckDB shell in common build directories."""
    candidates = [
        'build/reldebug/duckdb',
        'build/release/duckdb',
        'build/debug/duckdb',
        './duckdb',
    ]
    for candidate in candidates:
        if os.path.isfile(candidate) and os.access(candidate, os.X_OK):
            return candidate
    return None


def generate_setup_sql(rows: int, extension_dir: Optional[str] = None, 
                       bson_extension: Optional[str] = None) -> str:
    """Generate SQL to set up the benchmark database with JSON and BSON tables."""
    setup = []
    
    # Set extension directory if provided
    if extension_dir:
        setup.append(f"SET extension_directory='{extension_dir}';")
    
    # Load extensions
    if bson_extension:
        setup.append(f"LOAD '{bson_extension}';")
    else:
        setup.append("LOAD bson;")
    setup.append("LOAD json;")
    
    # Create tables
    setup.append("""
-- Create JSON table with both VARCHAR and JSON columns
CREATE TABLE json_data (
    id INTEGER PRIMARY KEY,
    raw_json VARCHAR,
    data_json JSON
);
""")
    
    setup.append("""
-- Create BSON table
CREATE TABLE bson_data (
    id INTEGER PRIMARY KEY,
    data_bson BSON
);
""")
    
    # Generate deterministic data
    # Sample document: {"user": {"name": "user_<id>", "age": <20-60>}, "country": "US"/"UK"/"FR"/"DE"/"JP", "flag": true/false, "score": <id*1.5>}
    setup.append(f"""
-- Insert JSON data ({rows} rows)
INSERT INTO json_data
SELECT 
    i AS id,
    '{{' ||
    '"user":{{' ||
    '"name":"user_' || i::VARCHAR || '",' ||
    '"age":' || (20 + (i % 40))::VARCHAR ||
    '}},' ||
    '"country":"' || (CASE (i % 5) 
        WHEN 0 THEN 'US' 
        WHEN 1 THEN 'UK' 
        WHEN 2 THEN 'FR' 
        WHEN 3 THEN 'DE' 
        ELSE 'JP' END) || '",' ||
    '"flag":' || (CASE WHEN i % 2 = 0 THEN 'true' ELSE 'false' END) || ',' ||
    '"score":' || (i * 1.5)::VARCHAR ||
    '}}' AS raw_json,
    ('{{' ||
    '"user":{{' ||
    '"name":"user_' || i::VARCHAR || '",' ||
    '"age":' || (20 + (i % 40))::VARCHAR ||
    '}},' ||
    '"country":"' || (CASE (i % 5) 
        WHEN 0 THEN 'US' 
        WHEN 1 THEN 'UK' 
        WHEN 2 THEN 'FR' 
        WHEN 3 THEN 'DE' 
        ELSE 'JP' END) || '",' ||
    '"flag":' || (CASE WHEN i % 2 = 0 THEN 'true' ELSE 'false' END) || ',' ||
    '"score":' || (i * 1.5)::VARCHAR ||
    '}}')::JSON AS data_json
FROM range({rows}) t(i);
""")
    
    setup.append(f"""
-- Convert JSON to BSON and populate BSON table
INSERT INTO bson_data
SELECT 
    id,
    json_to_bson(raw_json) AS data_bson
FROM json_data;
""")
    
    return '\n'.join(setup)


def generate_workload_sqls() -> Dict[str, str]:
    """Generate all benchmark workload SQL queries."""
    workloads = {}
    
    # Conversion workload
    workloads['10_convert_json_to_bson'] = """
-- Benchmark: Convert JSON to BSON
SELECT COUNT(*) FROM (
    SELECT json_to_bson(raw_json) AS bson_doc FROM json_data
) sub;
"""
    
    # Extraction workloads
    workloads['20_extract_string_json'] = """
-- Benchmark: Extract string field from JSON
SELECT COUNT(*) FROM (
    SELECT json_extract_string(data_json, '$.user.name') AS name FROM json_data
) sub;
"""
    
    workloads['21_extract_string_bson'] = """
-- Benchmark: Extract string field from BSON
SELECT COUNT(*) FROM (
    SELECT bson_extract_string(data_bson, '$.user.name') AS name FROM bson_data
) sub;
"""
    
    # Existence check workloads
    workloads['30_exists_json'] = """
-- Benchmark: Check field existence in JSON
SELECT COUNT(*) FROM (
    SELECT json_extract(data_json, '$.flag') IS NOT NULL AS has_flag FROM json_data
) sub;
"""
    
    workloads['31_exists_bson'] = """
-- Benchmark: Check field existence in BSON
SELECT COUNT(*) FROM (
    SELECT bson_exists(data_bson, '$.flag') AS has_flag FROM bson_data
) sub;
"""
    
    # Group by workloads
    workloads['40_groupby_country_json'] = """
-- Benchmark: Group by extracted field (JSON)
SELECT json_extract_string(data_json, '$.country') AS country, COUNT(*) AS cnt
FROM json_data
GROUP BY country
ORDER BY country;
"""
    
    workloads['41_groupby_country_bson'] = """
-- Benchmark: Group by extracted field (BSON)
SELECT bson_extract_string(data_bson, '$.country') AS country, COUNT(*) AS cnt
FROM bson_data
GROUP BY country
ORDER BY country;
"""
    
    # Size comparison workloads
    workloads['50_size_json'] = """
-- Benchmark: Total storage size (JSON as VARCHAR)
SELECT SUM(octet_length(raw_json::BLOB)) AS total_bytes FROM json_data;
"""
    
    workloads['51_size_bson'] = """
-- Benchmark: Total storage size (BSON as BLOB)
SELECT SUM(octet_length(data_bson::BLOB)) AS total_bytes FROM bson_data;
"""
    
    return workloads


def run_query_with_timer(shell: str, db_path: str, sql: str, warmup: int, runs: int, 
                        threads: Optional[int] = None) -> Tuple[List[float], Optional[str]]:
    """
    Run a SQL query with .timer on and parse the timing results.
    
    Returns:
        (timings, error_message) - timings is a list of run times in seconds, 
                                   error_message is None on success
    """
    commands = []
    
    # Add thread configuration if specified
    if threads:
        commands.append(f'SET threads={threads}')
    
    # Enable timer
    commands.append('.timer on')
    
    # Run warmup + actual runs
    total_runs = warmup + runs
    for _ in range(total_runs):
        commands.append(sql.strip())
    
    # Build command line
    cmd = [shell, db_path]
    for command in commands:
        cmd.extend(['-c', command])
    
    # Execute
    try:
        proc = subprocess.run(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=600  # 10 minute timeout
        )
        stdout = proc.stdout.decode('utf8')
        stderr = proc.stderr.decode('utf8')
        returncode = proc.returncode
    except subprocess.TimeoutExpired:
        return ([], 'Query timeout (>600s)')
    
    if returncode != 0:
        return ([], f'Query failed with code {returncode}:\n{stderr}\n{stdout}')
    
    # Parse timing results from stdout
    # Format: "Run Time (s): real 0.123 user 0.100 sys 0.020"
    timing_pattern = r'Run Time \(s\): real (\d+\.\d+)'
    matches = re.findall(timing_pattern, stdout)
    
    if len(matches) != total_runs:
        return ([], f'Expected {total_runs} timing results, got {len(matches)}')
    
    # Convert to floats and skip warmup runs
    all_timings = [float(t) for t in matches]
    measured_timings = all_timings[warmup:]
    
    return (measured_timings, None)


def setup_database(shell: str, db_path: str, setup_sql: str) -> Optional[str]:
    """
    Initialize the database with setup SQL.
    
    Returns:
        None on success, error message on failure
    """
    print(f"Setting up database at {db_path}...")
    
    cmd = [shell, db_path, '-c', setup_sql]
    
    try:
        proc = subprocess.run(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=1800  # 30 minute timeout for setup
        )
        stdout = proc.stdout.decode('utf8')
        stderr = proc.stderr.decode('utf8')
        returncode = proc.returncode
    except subprocess.TimeoutExpired:
        return 'Setup timeout (>30 minutes)'
    
    if returncode != 0:
        return f'Setup failed with code {returncode}:\n{stderr}\n{stdout}'
    
    print("Database setup complete.")
    return None


def format_timing_stats(timings: List[float]) -> Dict[str, float]:
    """Calculate statistics from timing results."""
    if not timings:
        return {}
    
    return {
        'min': min(timings),
        'max': max(timings),
        'mean': statistics.mean(timings),
        'median': statistics.median(timings),
        'stdev': statistics.stdev(timings) if len(timings) > 1 else 0.0,
    }


def print_results(results: Dict[str, Dict[str, float]], rows: int):
    """Print benchmark results in a readable format."""
    print("\n" + "="*80)
    print(f"BENCHMARK RESULTS ({rows:,} rows)")
    print("="*80)
    print(f"{'Benchmark':<40} {'Median (s)':<12} {'Mean (s)':<12} {'StdDev (s)':<12}")
    print("-"*80)
    
    for name in sorted(results.keys()):
        stats = results[name]
        if stats:
            print(f"{name:<40} {stats['median']:<12.4f} {stats['mean']:<12.4f} {stats['stdev']:<12.4f}")
        else:
            print(f"{name:<40} {'FAILED':<12}")
    
    print("="*80)
    
    # Print comparison ratios for paired benchmarks
    print("\nCOMPARISON (JSON baseline = 1.0x):")
    print("-"*80)
    
    comparisons = [
        ('20_extract_string_json', '21_extract_string_bson', 'Extract String'),
        ('30_exists_json', '31_exists_bson', 'Exists Check'),
        ('40_groupby_country_json', '41_groupby_country_bson', 'GroupBy Country'),
    ]
    
    for json_key, bson_key, label in comparisons:
        if json_key in results and bson_key in results:
            json_time = results[json_key].get('median', 0)
            bson_time = results[bson_key].get('median', 0)
            if json_time > 0 and bson_time > 0:
                ratio = bson_time / json_time
                faster = "faster" if ratio < 1.0 else "slower"
                print(f"{label:<30} JSON: {json_time:.4f}s  BSON: {bson_time:.4f}s  "
                      f"({ratio:.2f}x, BSON is {abs(1-ratio)*100:.1f}% {faster})")
    
    # Print size comparison
    print("\n" + "="*80)


def main():
    parser = argparse.ArgumentParser(
        description='Benchmark JSON vs BSON performance in DuckDB',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Basic usage with auto-detected shell
  python3 scripts/bson_json_bench.py --extension-dir build/reldebug/extension
  
  # Full specification
  python3 scripts/bson_json_bench.py \\
      --shell build/reldebug/duckdb \\
      --extension-dir build/reldebug/extension \\
      --rows 200000 \\
      --runs 7 \\
      --warmup 1 \\
      --threads 4
  
  # Using explicit BSON extension path
  python3 scripts/bson_json_bench.py \\
      --shell ./duckdb \\
      --bson-extension build/reldebug/extension/bson/bson.duckdb_extension \\
      --rows 50000

Notes:
  - The BSON extension must be built and available (via --extension-dir or --bson-extension)
  - Higher --rows values give more stable timings but take longer to set up
  - The script creates a temporary database which is deleted on exit
"""
    )
    
    parser.add_argument('--shell', type=str, help='Path to DuckDB CLI binary (default: auto-detect)')
    parser.add_argument('--db', type=str, help='Path for temporary database (default: temp dir)')
    parser.add_argument('--rows', type=int, default=100000, help='Number of rows to generate (default: 100000)')
    parser.add_argument('--runs', type=int, default=5, help='Number of measured runs per query (default: 5)')
    parser.add_argument('--warmup', type=int, default=1, help='Number of warmup runs (default: 1)')
    parser.add_argument('--threads', type=int, help='Number of threads (default: DuckDB default)')
    parser.add_argument('--extension-dir', type=str, help='Extension directory path (for SET extension_directory)')
    parser.add_argument('--bson-extension', type=str, help='Direct path to bson.duckdb_extension file')
    parser.add_argument('--keep-db', action='store_true', help='Keep database after benchmark (for debugging)')
    
    args = parser.parse_args()
    
    # Find shell
    shell = args.shell
    if not shell:
        shell = find_default_shell()
        if not shell:
            print("ERROR: Could not find DuckDB shell. Please specify --shell", file=sys.stderr)
            sys.exit(1)
        print(f"Using shell: {shell}")
    
    if not os.path.isfile(shell):
        print(f"ERROR: Shell not found: {shell}", file=sys.stderr)
        sys.exit(1)
    
    # Validate extension arguments
    if not args.extension_dir and not args.bson_extension:
        print("ERROR: Must specify either --extension-dir or --bson-extension", file=sys.stderr)
        sys.exit(1)
    
    # Create temporary database path
    temp_db = None
    db_path = args.db
    if not db_path:
        # Create a temporary file name but delete the file itself
        # DuckDB will create a fresh database
        temp_db = tempfile.NamedTemporaryFile(suffix='.duckdb', delete=False)
        db_path = temp_db.name
        temp_db.close()
        # Remove the empty file that was created
        os.unlink(db_path)
    
    try:
        # Generate setup SQL
        setup_sql = generate_setup_sql(
            args.rows,
            extension_dir=args.extension_dir,
            bson_extension=args.bson_extension
        )
        
        # Setup database
        error = setup_database(shell, db_path, setup_sql)
        if error:
            print(f"ERROR: Setup failed:\n{error}", file=sys.stderr)
            sys.exit(1)
        
        # Generate workloads
        workloads = generate_workload_sqls()
        
        # Run benchmarks
        results = {}
        print(f"\nRunning benchmarks (warmup={args.warmup}, runs={args.runs})...")
        
        for name in sorted(workloads.keys()):
            sql = workloads[name]
            print(f"  {name}...", end=' ')
            
            timings, error = run_query_with_timer(
                shell, db_path, sql, args.warmup, args.runs, args.threads
            )
            
            if error:
                print(f"FAILED: {error}")
                results[name] = {}
            else:
                stats = format_timing_stats(timings)
                results[name] = stats
                print(f"OK (median: {stats['median']:.4f}s)")
        
        # Print results
        print_results(results, args.rows)
        
    finally:
        # Cleanup
        if temp_db and not args.keep_db:
            try:
                os.unlink(db_path)
                # Also remove WAL files if they exist
                for suffix in ['.wal', '-wal', '-shm']:
                    wal_path = db_path + suffix
                    if os.path.exists(wal_path):
                        os.unlink(wal_path)
            except Exception as e:
                print(f"Warning: Could not clean up temp database: {e}", file=sys.stderr)
        elif args.keep_db:
            print(f"\nDatabase kept at: {db_path}")


if __name__ == '__main__':
    main()
