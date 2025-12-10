CREATE TEMPORARY TABLE input (
    lines TEXT
);

\copy input FROM 'day05/input.txt' WITH (FORMAT text);

WITH RECURSIVE lines_with_row AS (
    SELECT ROW_NUMBER() OVER () AS row_index,
    lines
    FROM input
),
blank_line_index AS (
    SELECT
        row_index
    FROM lines_with_row
    WHERE LENGTH(lines) = 0
),
fresh_ranges_input AS (
    SELECT
        lines
    FROM lines_with_row
    WHERE row_index < (SELECT row_index FROM blank_line_index)
),
fresh_ranges AS (
    SELECT
        CAST(SPLIT_PART(lines, '-', 1) AS BIGINT) AS lower_range,
        CAST(SPLIT_PART(lines, '-', 2) AS BIGINT) AS upper_range
    FROM fresh_ranges_input
),
fresh_ranges_ordered AS (
    SELECT DISTINCT
        lower_range,
        upper_range
    FROM fresh_ranges
    ORDER BY lower_range
),
fresh_ranges_ordered_with_row AS (
    SELECT
        ROW_NUMBER() OVER () AS row_index,
        lower_range,
        upper_range
    FROM fresh_ranges_ordered
),
distinct_ranges_buildup AS (
    SELECT
        row_index,
        lower_range,
        upper_range
    FROM fresh_ranges_ordered_with_row
    WHERE row_index = 1
    UNION
    (
        WITH current_range_with_prev AS (
            SELECT
                distinct_ranges_buildup.lower_range AS prev_lower_range,
                distinct_ranges_buildup.upper_range AS prev_upper_range,
                fresh_ranges_ordered_with_row.lower_range,
                fresh_ranges_ordered_with_row.upper_range,
                1 + distinct_ranges_buildup.row_index AS row_index
            FROM distinct_ranges_buildup
            INNER JOIN fresh_ranges_ordered_with_row
                ON (fresh_ranges_ordered_with_row.row_index = distinct_ranges_buildup.row_index + 1)
        )
        SELECT
            row_index,
            CASE
                WHEN lower_range <= prev_upper_range
                THEN LEAST(prev_lower_range, lower_range)
                ELSE lower_range
            END AS lower_range,
            CASE
                WHEN lower_range <= prev_upper_range
                THEN GREATEST(prev_upper_range, upper_range)
                ELSE upper_range
            END AS upper_range
        FROM current_range_with_prev
    )
),
distinct_ranges AS (
    SELECT DISTINCT ON (lower_range)
        lower_range,
        upper_range
    FROM distinct_ranges_buildup
    ORDER BY lower_range, upper_range DESC
)
SELECT
    SUM(upper_range - lower_range + 1) AS answer
FROM distinct_ranges;
