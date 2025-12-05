CREATE TEMPORARY TABLE input (
    lines TEXT
);

\copy input FROM 'day03/input.txt' WITH (FORMAT text);

WITH RECURSIVE input_row AS (
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
built_digits AS (
    SELECT
        row_index,
        col_index,
        value,
        value AS accumulation,
        1 AS depth
    FROM board
    UNION
    (
        WITH second_max AS (
            SELECT
                row_index,
                col_index,
                value,
                MAX(accumulation) OVER (PARTITION BY row_index ORDER BY col_index RANGE BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS accumulation,
                depth + 1 AS depth
            FROM built_digits
            WHERE accumulation IS NOT NULL
        )
        SELECT
            row_index,
            col_index,
            value,
            value || (LEAD(accumulation) OVER (PARTITION BY row_index)) AS accumulation,
            depth
        FROM second_max
        WHERE depth <= 12
        AND accumulation IS NOT NULL
    )
),
best_joltages AS (
    SELECT DISTINCT ON (row_index)
        row_index,
        CAST(accumulation AS BIGINT) AS joltage
    FROM built_digits
    WHERE depth = (
        SELECT MAX(depth) FROM built_digits
    )
      AND accumulation IS NOT NULL
    ORDER BY row_index, joltage DESC
)
SELECT SUM(joltage) FROM best_joltages;
