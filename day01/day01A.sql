CREATE TABLE input01 (
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
        SUM(number) OVER (
            ORDER BY row_number
            RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS sums
    FROM turns
)
SELECT
    COUNT(*)
FROM cumulative_sum
WHERE ABS(sums % 100) = 50;
