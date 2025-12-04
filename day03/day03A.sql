CREATE TEMPORARY TABLE input (
    lines TEXT
);

\copy input FROM 'day03/input.txt' WITH (FORMAT text);

WITH input_row AS (
    SELECT
        ROW_NUMBER() OVER () AS row_index,
        UNNEST(STRING_TO_ARRAY(lines, NULL)) AS value
    FROM input
),
board AS (
    SELECT
        row_index,
        ROW_NUMBER() OVER (PARTITION BY row_index) AS col_index,
        value
    FROM input_row
),
second_max AS (
    SELECT
        row_index,
        col_index,
        value,
        MAX(value) OVER (PARTITION BY row_index ORDER BY col_index RANGE BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS next_max
    FROM board
),
battery_values AS (
    SELECT
        row_index,
        col_index,
        CAST(value || (LEAD(next_max) OVER (PARTITION BY row_index)) AS INTEGER) AS battery
    FROM second_max
),
bank_values AS (
    SELECT DISTINCT ON (row_index)
        row_index,
        battery
    FROM battery_values
    WHERE battery IS NOT NULL
    ORDER BY row_index, battery DESC
)
SELECT SUM(battery) FROM bank_values;
