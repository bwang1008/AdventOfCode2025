CREATE TEMPORARY TABLE input (
    lines TEXT
);

\copy input FROM 'day10/input.txt' WITH (FORMAT text);


WITH RECURSIVE numbered_input AS (
    SELECT
        ROW_NUMBER() OVER () AS row_index,
        SPLIT_PART(lines, ' ', 1) AS lights,
        TRIM(SPLIT_PART(SPLIT_PART(lines, '{', 1), ']', 2)) AS buttons,
        '{' || SPLIT_PART(lines, '{', 2) AS joltages
    FROM input
),
longest_light AS (
    SELECT
        MAX(LENGTH(lights)) - 2 AS value
    FROM numbered_input
),
light_number_builder AS (
    SELECT
        row_index,
        SUBSTRING(lights, 2, (SELECT LENGTH(lights) - 2)) AS lights,
        0 AS light_index,
        0 AS value
    FROM numbered_input
    UNION ALL
    SELECT
        row_index,
        lights,
        1 + light_index AS light_index,
        CASE
            WHEN SUBSTRING(lights, 1 + light_index, 1) = '#'
            THEN CAST(POWER(2, light_index) AS INTEGER) + value
            ELSE value
        END AS value
    FROM light_number_builder
    WHERE light_index < (SELECT value FROM longest_light)
),
light_number AS (
    SELECT
        row_index,
        lights,
        value
    FROM light_number_builder
    WHERE light_index = (SELECT MAX(light_index) FROM light_number_builder)
    ORDER BY row_index
),
button_array AS (
    SELECT
        row_index,
        UNNEST(STRING_TO_ARRAY(buttons, ' ')) AS button_values
    FROM numbered_input
),
numbered_button_array AS (
    SELECT
        row_index,
        ROW_NUMBER() OVER (PARTITION BY row_index) AS position_index,
        button_values
    FROM button_array
),
button_positions AS (
    SELECT
        row_index,
        position_index,
        UNNEST(
            STRING_TO_ARRAY(
                SUBSTRING(button_values, 2, (SELECT LENGTH(button_values) - 2)),
                ','
            )
        ) AS position
    FROM numbered_button_array
),
button_values AS (
    SELECT DISTINCT
        row_index,
        position_index,
        SUM(
            CAST(POWER(2, CAST(position AS INTEGER)) AS INTEGER)
        )
        OVER (PARTITION BY row_index, position_index)
        AS position
    FROM button_positions
    ORDER BY row_index, position_index
),
button_values_agg AS (
    SELECT DISTINCT
        row_index,
        ARRAY_AGG(position) OVER (PARTITION BY row_index) AS value
    FROM button_values
    ORDER BY row_index
),
most_number_of_buttons AS (
    SELECT
        MAX(ARRAY_LENGTH(value, 1)) AS value
    FROM button_values_agg
),
button_pushes_builder AS (
    SELECT
        row_index,
        value AS button_values,
        0 AS loop_index,
        GENERATE_SERIES(
            0,
            CAST(
                POWER(2, (SELECT value FROM most_number_of_buttons))
                AS INTEGER
            ) - 1
        ) AS subset_index,
        CAST(0 AS BIGINT) AS accumulator,
        0 AS num_button_pushes
    FROM button_values_agg
    UNION ALL
    SELECT
        row_index,
        button_values,
        1 + loop_index AS loop_index,
        subset_index,
        CASE
            WHEN (subset_index >> loop_index) % 2 = 1
            THEN button_values[1 + loop_index]
            ELSE 0
        END # accumulator AS accumulator,
        CASE
            WHEN (subset_index >> loop_index) % 2 = 1
            THEN 1
            ELSE 0
        END + num_button_pushes AS num_button_pushes
    FROM button_pushes_builder
    WHERE loop_index < (SELECT value FROM most_number_of_buttons)
),
button_pushes AS (
    SELECT
        row_index,
        button_values,
        subset_index,
        accumulator,
        num_button_pushes
    FROM button_pushes_builder
    WHERE loop_index = (SELECT MAX(loop_index) FROM button_pushes_builder)
    AND accumulator IS NOT NULL
    ORDER BY row_index, subset_index
),
desired_button_pushes AS (
    SELECT
        button_pushes.row_index,
        button_pushes.accumulator,
        button_pushes.num_button_pushes
    FROM button_pushes
    INNER JOIN light_number
    ON (
        light_number.row_index = button_pushes.row_index
        AND light_number.value = button_pushes.accumulator
    )
    ORDER BY button_pushes.row_index, button_pushes.num_button_pushes
),
min_desired_button_pushes AS (
    SELECT DISTINCT ON (row_index)
        row_index,
        accumulator,
        num_button_pushes
    FROM desired_button_pushes
    ORDER BY row_index, num_button_pushes
)
SELECT
    SUM(num_button_pushes) AS answer
FROM min_desired_button_pushes;

--  answer
-- --------
--     509
-- (1 row)

-- Time: 36670.729 ms (00:36.671)
