CREATE TEMPORARY TABLE input (
    lines TEXT
);

\copy input FROM 'day06/input.txt' WITH (FORMAT text);

WITH RECURSIVE formatted_input AS (
    SELECT ROW_NUMBER() OVER () AS row_index,
    REGEXP_REPLACE(TRIM(lines), ' +', ',', 'g') AS lines
    FROM input
),
expanded AS (
    SELECT
        row_index,
        UNNEST(STRING_TO_ARRAY(lines, ',')) AS value
    FROM formatted_input
),
expanded_with_col AS (
    SELECT
        row_index,
        ROW_NUMBER() OVER (PARTITION BY row_index) AS col_index,
        value
    FROM expanded
),
operations AS (
    SELECT
        col_index,
        value
    FROM expanded_with_col
    WHERE row_index = (SELECT MAX(row_index) FROM expanded_with_col)
),
numeric_values AS (
    SELECT
        row_index,
        col_index,
        CAST(value AS BIGINT)
    FROM expanded_with_col
    WHERE row_index < (SELECT MAX(row_index) FROM expanded_with_col)
),
summed_values AS (
    SELECT
        col_index,
        SUM(value) AS summed_value
    FROM numeric_values
    GROUP BY col_index
    ORDER BY col_index
),
multiplied_values_buildup AS (
    SELECT
        row_index,
        col_index,
        value AS multiplied_value
    FROM numeric_values
    WHERE row_index = 1
    UNION
    SELECT
        numeric_values.row_index,
        numeric_values.col_index,
        numeric_values.value * multiplied_values_buildup.multiplied_value AS multiplied_value
    FROM numeric_values
    INNER JOIN multiplied_values_buildup
        ON (
            numeric_values.row_index = multiplied_values_buildup.row_index + 1
            AND numeric_values.col_index = multiplied_values_buildup.col_index
        )
),
multiplied_values AS (
    SELECT
        col_index,
        multiplied_value
    FROM multiplied_values_buildup
    WHERE row_index = (SELECT MAX(row_index) FROM multiplied_values_buildup)
    ORDER BY col_index
),
operation_results AS (
    SELECT
        col_index,
        operations.value AS operator,
        summed_values.summed_value,
        multiplied_values.multiplied_value
    FROM operations
    JOIN summed_values USING (col_index)
    JOIN multiplied_values USING (col_index)
    ORDER BY col_index
),
calculation_answers AS (
    SELECT
        col_index,
        CASE
            WHEN operator = '*'
            THEN multiplied_value
            ELSE summed_value
        END AS result
    FROM operation_results
)
SELECT
    SUM(result) AS answer
FROM calculation_answers;
