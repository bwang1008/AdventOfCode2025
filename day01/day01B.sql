CREATE TEMPORARY TABLE input01 (
    lines TEXT
);

\copy input01 FROM 'day01/input.txt' WITH (FORMAT text);

WITH turns AS (
    SELECT
        CAST(
            REPLACE(REPLACE(input01.lines, 'L', '-'), 'R', '')
            AS INTEGER
        ) AS number,
        ROW_NUMBER() OVER () AS row_number
    FROM input01
),
cumulative_sum AS (
    SELECT
        number,
        row_number,
        50 + SUM(number) OVER (
            ORDER BY row_number
            RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS current_sum
    FROM turns
),
lagged AS (
    SELECT
        LAG(current_sum, 1, 50::bigint) OVER (ORDER BY row_number) AS prev_sum,
        number,
        current_sum
    FROM cumulative_sum
),
brackets AS (
    SELECT
        CASE
            WHEN prev_sum > current_sum
            THEN CEILING((prev_sum / 100.0) - 1)
            ELSE FLOOR((prev_sum / 100.0) + 1)
        END AS prev_bracket,
        CASE
            WHEN prev_sum > current_sum
            THEN CEILING((current_sum / 100.0) - 1)
            ELSE FLOOR((current_sum / 100.0) + 1)
        END AS current_bracket
    FROM lagged
)
SELECT
    SUM(ABS(prev_bracket - current_bracket)) AS total_clicks
FROM brackets;
