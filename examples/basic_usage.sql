-- FastLanes Extension Basic Usage Example
-- This script demonstrates how to use the FastLanes extension for reading and writing data

-- Load the FastLanes extension
LOAD fastlane;

-- Check the extension version
SELECT fastlane_version() as extension_version;

-- Create some sample data
CREATE TABLE sample_data AS 
SELECT 
    generate_series(1, 1000) as id,
    'user_' || generate_series(1, 1000) as username,
    random() * 1000 as score,
    CASE WHEN random() > 0.5 THEN true ELSE false END as is_active,
    DATE '2023-01-01' + (generate_series(1, 1000) % 365) as join_date;

-- Display the sample data
SELECT * FROM sample_data LIMIT 10;

-- Convert the data to FastLanes format using table function
-- This creates a BLOB containing the serialized FastLanes data
SELECT * FROM write_fastlane(SELECT * FROM sample_data) LIMIT 5;

-- Convert data to FastLanes format using COPY TO (equivalent to nanoarrow's COPY TO "test.arrows")
COPY (SELECT * FROM sample_data) TO "sample_data.fls";

-- COPY TO with format specification
COPY (SELECT * FROM sample_data) TO "sample_data_2.fls" (FORMAT FLS);

-- COPY TO with FastLanes format
COPY (SELECT * FROM sample_data) TO "sample_data_3.fastlane" (FORMAT FASTLANE);

-- COPY TO with options
COPY (SELECT * FROM sample_data) TO "sample_data_4.fls" (FORMAT FLS, ROW_GROUP_SIZE 5000);

-- Read from a FastLanes file (if it exists)
-- Note: This will return empty results if the file doesn't exist
SELECT * FROM scan_fastlanes('sample_data.fls') LIMIT 10;

-- Test with different data types
CREATE TABLE mixed_types AS 
SELECT 
    42 as small_int,
    1234567890123456789 as big_int,
    3.14159265359 as pi,
    'Hello, FastLanes!' as greeting,
    true as flag,
    DATE '2023-12-25' as christmas,
    BLOB 'binary data' as binary_field;

-- Convert mixed types to FastLanes using COPY TO
COPY (SELECT * FROM mixed_types) TO "mixed_types.fls";

-- Convert mixed types to FastLanes using table function
SELECT * FROM write_fastlane(SELECT * FROM mixed_types);

-- Test projection pushdown (only select specific columns)
SELECT small_int, greeting, flag FROM scan_fastlanes('mixed_types.fls') LIMIT 5;

-- Test with complex queries using COPY TO
COPY (
    SELECT 
        id,
        username,
        score,
        CASE 
            WHEN score > 500 THEN 'High'
            WHEN score > 200 THEN 'Medium'
            ELSE 'Low'
        END as performance_level
    FROM sample_data 
    WHERE is_active = true
    ORDER BY score DESC
    LIMIT 100
) TO "complex_query.fls";

-- Test glob pattern (equivalent to nanoarrow's FROM read_arrow('*.arrows'))
SELECT * FROM scan_fastlanes('*.fls') LIMIT 10;

-- Test list of files (equivalent to nanoarrow's FROM read_arrow(['test.arrows','test_2.arrows']))
SELECT * FROM scan_fastlanes(['sample_data.fls','mixed_types.fls']) LIMIT 10;

-- Test automatic file detection (files with .fls extension)
-- SELECT * FROM 'sample_data.fls' LIMIT 10;

-- Clean up
DROP TABLE sample_data;
DROP TABLE mixed_types; 