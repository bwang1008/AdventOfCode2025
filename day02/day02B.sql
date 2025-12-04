CREATE TEMPORARY TABLE input (
    lines TEXT
);

\copy input FROM 'day02/input.txt' WITH (FORMAT text);

WITH raw_ranges AS (
    -- split input by comma
    SELECT
        UNNEST(STRING_TO_ARRAY(lines, ',')) AS lines
    FROM input
),
ranges AS (
    -- split "123-456" into two columns, one with "123", other with "456"
    SELECT
        ROW_NUMBER() OVER () AS range_id,
        SPLIT_PART(lines, '-', 1) AS lower_range,
        SPLIT_PART(lines, '-', 2) AS upper_range
    FROM raw_ranges
),
ranges_and_split AS (
    -- assign every possible "split_size" from 2 to LENGTH(upper_range)
    SELECT
        range_id,
        lower_range,
        upper_range,
        GENERATE_SERIES(2, LENGTH(upper_range)) AS split_size
    FROM ranges
),
candidate_brackets AS (
    -- get closest prefix: if perfectly divisible, use first few digits,
    -- otherwise round up so prefix looks like 100...0.
    -- Similarly, for upper range, round down so prefix looks like 999...9
    SELECT
        range_id,
        lower_range,
        CASE
            WHEN LENGTH(lower_range) % split_size != 0
            THEN '1' || REPEAT('0', LENGTH(lower_range) / split_size)
            ELSE SUBSTRING(lower_range, 1, LENGTH(lower_range) / split_size)
        END AS lower_bracket,
        upper_range,
        CASE
            WHEN LENGTH(upper_range) % split_size != 0
            THEN REPEAT('9', LENGTH(upper_range) / split_size)
            ELSE SUBSTRING(upper_range, 1, LENGTH(upper_range) / split_size)
        END AS upper_bracket,
        split_size
    FROM ranges_and_split
),
prevent_outside_ranges AS (
    SELECT
        range_id,
        lower_range,
        CASE
            WHEN CAST(REPEAT(lower_bracket, split_size) AS BIGINT) < CAST(lower_range AS BIGINT)
            THEN CAST((CAST(lower_bracket AS BIGINT) + 1) AS TEXT)
            ELSE lower_bracket
        END AS lower_bracket,
        upper_range,
        CASE
            WHEN CAST(REPEAT(upper_bracket, split_size) AS BIGINT) > CAST(upper_range AS BIGINT)
            THEN CAST((CAST(upper_bracket AS BIGINT) - 1) AS TEXT)
            ELSE upper_bracket
        END AS upper_bracket,
        split_size
    FROM candidate_brackets
),
prevent_inside_ranges AS (
    -- if the repeated bracket falls outside the range, assign bracket to outside of range
    -- to make an invalid interval
    SELECT
        range_id,
        lower_range,
        CASE
            WHEN CAST(REPEAT(lower_bracket, split_size) AS BIGINT) > CAST(upper_range AS BIGINT)
            THEN (CAST(upper_range AS BIGINT) + 1)
            ELSE CAST(lower_bracket AS BIGINT)
        END AS lower_bracket,
        upper_range,
        CASE
            WHEN CAST(REPEAT(upper_bracket, split_size) AS BIGINT) < CAST(lower_range AS BIGINT)
            THEN (CAST(lower_range AS BIGINT) - 1)
            ELSE CAST(upper_bracket AS BIGINT)
        END AS upper_bracket,
        split_size
    FROM prevent_outside_ranges
),
invalid_id_prefixes AS (
    -- invalid intervals (no invalid_ids in this range)
    -- results in no rows from GENERATE_SERIES
    SELECT
        range_id,
        lower_range,
        upper_range,
        lower_bracket,
        upper_bracket,
        split_size,
        GENERATE_SERIES(lower_bracket, upper_bracket) AS prefix
    FROM prevent_inside_ranges
),
invalid_ids AS (
    -- prevent duplicates, where say invalid_id 222222 could be split
    -- into 2, 3, or 6 pieces
    SELECT DISTINCT
        range_id,
        lower_range,
        upper_range,
        CAST(REPEAT(CAST(prefix AS TEXT), split_size) AS BIGINT) AS invalid_id
    FROM invalid_id_prefixes
    ORDER BY range_id, invalid_id
)
SELECT SUM(invalid_id) FROM invalid_ids;
