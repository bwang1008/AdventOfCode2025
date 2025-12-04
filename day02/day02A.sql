CREATE TEMPORARY TABLE input (
    lines TEXT
);

\copy input FROM 'day02/input.txt' WITH (FORMAT text);

WITH raw_ranges AS (
    SELECT
        UNNEST(STRING_TO_ARRAY(lines, ',')) AS lines
    FROM input
),
ranges AS (
    SELECT
        SPLIT_PART(lines, '-', 1) AS lower_range,
        SPLIT_PART(lines, '-', 2) AS upper_range
    FROM raw_ranges
),
candidate_brackets AS (
    SELECT
        lower_range,
        CASE
            WHEN LENGTH(lower_range) % 2 = 1
            THEN '1' || REPEAT('0', LENGTH(lower_range) / 2)
            ELSE SUBSTRING(lower_range, 1, LENGTH(lower_range) / 2)
        END AS lower_bracket,
        upper_range,
        CASE
            WHEN LENGTH(upper_range) % 2 = 1
            THEN REPEAT('9', LENGTH(upper_range) / 2)
            ELSE SUBSTRING(upper_range, 1, LENGTH(upper_range) / 2)
        END AS upper_bracket
    FROM ranges
),
prevent_outside_ranges AS (
    SELECT
        lower_range,
        CASE
            WHEN CAST(REPEAT(lower_bracket, 2) AS BIGINT) < CAST(lower_range AS BIGINT)
            THEN CAST((CAST(lower_bracket AS BIGINT) + 1) AS TEXT)
            ELSE lower_bracket
        END AS lower_bracket,
        upper_range,
        CASE
            WHEN CAST(REPEAT(upper_bracket, 2) AS BIGINT) > CAST(upper_range AS BIGINT)
            THEN CAST((CAST(upper_bracket AS BIGINT) - 1) AS TEXT)
            ELSE upper_bracket
        END AS upper_bracket
    FROM candidate_brackets
),
prevent_inside_ranges AS (
    SELECT
        lower_range,
        CASE
            WHEN CAST(REPEAT(lower_bracket, 2) AS BIGINT) > CAST(upper_range AS BIGINT)
            THEN (CAST(upper_range AS BIGINT) + 1)
            ELSE CAST(lower_bracket AS BIGINT)
        END AS lower_bracket,
        upper_range,
        CASE
            WHEN CAST(REPEAT(upper_bracket, 2) AS BIGINT) < CAST(lower_range AS BIGINT)
            THEN (CAST(lower_range AS BIGINT) - 1)
            ELSE CAST(upper_bracket AS BIGINT)
        END AS upper_bracket
    FROM prevent_outside_ranges
),
invalid_id_prefixes AS (
    SELECT
        lower_range,
        upper_range,
        lower_bracket,
        upper_bracket,
        GENERATE_SERIES(lower_bracket, upper_bracket) AS prefix
    FROM prevent_inside_ranges
),
invalid_ids AS (
    SELECT
        CAST(REPEAT(CAST(prefix AS TEXT), 2) AS BIGINT) AS invalid_id
    FROM invalid_id_prefixes
)
SELECT SUM(invalid_id) FROM invalid_ids;
