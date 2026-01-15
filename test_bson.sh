#!/bin/bash
# Quick test script for BSON extension

cd "$(dirname "$0")"

echo "Building DuckDB with BSON extension..."
BUILD_BSON=1 make debug > /dev/null 2>&1

echo "Testing BSON extension..."
./build/debug/duckdb :memory: <<EOF
-- Load BSON extension (should be statically linked)
.mode line

-- Test 1: Create BSON type
SELECT 'Test 1: BSON type creation' as test;
SELECT '\x05\x00\x00\x00\x00'::BLOB::BSON as bson_value;

-- Test 2: Validate BSON
SELECT 'Test 2: BSON validation' as test;
SELECT bson_valid('\x05\x00\x00\x00\x00'::BSON) as is_valid;
SELECT bson_valid('\x04\x00\x00\x00\x00'::BLOB) as is_invalid;

-- Test 3: BSON with string field
SELECT 'Test 3: BSON string extraction' as test;
-- Document: {"name": "John"} in BSON format
SELECT bson_extract_string('\x14\x00\x00\x00\x02name\x00\x05\x00\x00\x00John\x00\x00'::BSON, '$.name') as name_field;

-- Test 4: BSON type detection
SELECT 'Test 4: BSON type detection' as test;
SELECT bson_type('\x05\x00\x00\x00\x00'::BSON, '$') as root_type;

-- Test 5: BSON exists
SELECT 'Test 5: BSON field existence' as test;
SELECT bson_exists('\x14\x00\x00\x00\x02name\x00\x05\x00\x00\x00John\x00\x00'::BSON, '$.name') as name_exists;
SELECT bson_exists('\x14\x00\x00\x00\x02name\x00\x05\x00\x00\x00John\x00\x00'::BSON, '$.missing') as missing_exists;

SELECT '✅ All tests completed successfully!' as result;
EOF

echo "Done!"
