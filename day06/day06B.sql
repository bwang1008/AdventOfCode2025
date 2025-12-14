CREATE TEMPORARY TABLE input (
    lines TEXT
);

\copy input FROM 'day06/input.txt' WITH (FORMAT text);

-- row and col of problems
-- replace blanks in problems with 0s
-- get transposed row + col
-- reconstruct lines
-- feed into A

WITH RECURSIVE lines_with_rows AS (
    SELECT
        ROW_NUMBER() OVER () AS row_index,
        lines
    FROM input
),
expanded_lines AS (
    SELECT
        row_index,
        UNNEST(STRING_TO_ARRAY(lines, NULL)) AS value
    FROM lines_with_rows
),
board AS (
    -- row_index + col_index for each character in lines
    SELECT
        row_index,
        ROW_NUMBER() OVER (PARTITION BY row_index) AS col_index,
        value
    FROM expanded_lines
),
blank_columns AS (
    -- [4, 8, 12] in sample input: column indices of separators between vertical problems
    SELECT
        col_index - 1 AS col_index
    FROM board
    WHERE row_index = (SELECT MAX(row_index) FROM board)
    AND col_index > 1
    AND value != ' '
),
problem_groups AS (
    -- for each column, assign a problem_id: first few are problem_id 1, then next few are problem_id 2, ...
    SELECT
        col_index,
        1 + SUM(
            CASE
                WHEN col_index IN (SELECT col_index FROM blank_columns)
                THEN 1
                ELSE 0
            END
        ) OVER (ORDER BY col_index RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS problem_id
    FROM board
    WHERE row_index = (SELECT MAX(row_index) FROM board)
),
problem_operators AS (
    -- for each problem_id, which operator ('+', '*') is it?
    -- helps inform what the "default" value should be for missing rows 
    SELECT
        problem_groups.problem_id,
        board.value AS calculation_operator
    FROM board
    INNER JOIN problem_groups
    USING (col_index)
    WHERE board.row_index = (SELECT MAX(row_index) FROM board)
    AND board.value != ' '
),
reinterpreted_numbers AS (
    -- concat characters of columns togethers (only the numerical values)
    SELECT
        col_index,
        STRING_AGG(value, '' ORDER BY row_index) AS value
    FROM board
    WHERE row_index < (SELECT MAX(row_index) FROM board)
    AND col_index NOT IN (SELECT col_index FROM blank_columns)
    GROUP BY 1
    ORDER BY 1
),
reinterpreted_problems AS (
    -- join information together: column, concatted vertical values, problem_id,
    -- "which" column within problem
    SELECT
        reinterpreted_numbers.col_index,
        reinterpreted_numbers.value,
        problem_groups.problem_id,
        ROW_NUMBER() OVER (PARTITION BY problem_id) AS problem_col_index
    FROM reinterpreted_numbers
    INNER JOIN problem_groups
    USING (col_index)
),
default_numbers_helper AS (
    SELECT
        (SELECT MAX(col_index) FROM reinterpreted_problems) AS col_index,
        GENERATE_SERIES(1, (SELECT MAX(problem_id) FROM reinterpreted_problems)) AS problem_id
),
default_numbers AS (
    -- some problems involve 4 numbers, some only 3 numbers.
    -- pad out those with 3 numbers with the "default" value of the operator
    -- (1 for multiplication, 0 for addition)
    -- this is so we can reconstruct the input to be fed into solution A
    -- (every problem now has 4 lines of input)
    SELECT
        col_index,
        CASE
            WHEN problem_operators.calculation_operator = '+'
            THEN '0'
            ELSE '1'
        END AS value,
        problem_id,
        GENERATE_SERIES(1, (SELECT MAX(problem_col_index) FROM reinterpreted_problems)) AS problem_col_index
    FROM default_numbers_helper
    INNER JOIN problem_operators
    USING (problem_id)
),
reinterpreted_problems_extended AS (
    SELECT
        default_numbers.col_index,
        COALESCE(reinterpreted_problems.value, default_numbers.value) AS value,
        problem_id,
        problem_col_index
    FROM default_numbers
    LEFT JOIN reinterpreted_problems
    USING (problem_id, problem_col_index)
),
reinterpreted_value_lines AS (
    SELECT
        STRING_AGG(value, ' ' ORDER BY col_index) AS lines
    FROM
    reinterpreted_problems_extended
    GROUP BY problem_col_index
    ORDER BY problem_col_index
),
reinterpreted_input AS (
    SELECT lines FROM reinterpreted_value_lines
    UNION ALL
    SELECT lines
    FROM lines_with_rows
    WHERE row_index = (SELECT MAX(row_index) FROM lines_with_rows)
),
-- reinterpreted_input can now be fed into SELECT from day06A.sql
-- SQL below is copied over and slightly modified
formatted_input AS (
    SELECT ROW_NUMBER() OVER () AS row_index,
    REGEXP_REPLACE(TRIM(lines), ' +', ',', 'g') AS lines
    FROM reinterpreted_input
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
