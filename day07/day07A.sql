CREATE TEMPORARY TABLE input (
    lines TEXT
);

\copy input FROM 'day07/input.txt' WITH (FORMAT text);

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
beam_positions AS (
    SELECT
        row_index,
        col_index,
        0 AS has_split
    FROM board
    WHERE row_index = 1
    AND value = 'S'
    UNION
    (
        WITH row_comparisons AS (
            SELECT
                board.row_index,
                board.col_index,
                board.value
            FROM board
            JOIN beam_positions
            ON (
                board.row_index = 1 + beam_positions.row_index
                AND board.col_index = beam_positions.col_index
            )
            WHERE board.row_index <= (SELECT MAX(row_index) FROM board)
        ),
        current_row_positions AS (
            SELECT
                row_index,
                col_index,
                0 AS has_split
            FROM row_comparisons
            WHERE value != '^'
            UNION
            SELECT
                row_index,
                col_index - 1 AS col_index,
                1 AS has_split
            FROM row_comparisons
            WHERE value = '^'
            UNION
            SELECT
                row_index,
                col_index + 1 AS col_index,
                0 AS has_split
            FROM row_comparisons
            WHERE value = '^'
        )
        SELECT DISTINCT ON (row_index, col_index)
            row_index,
            col_index,
            has_split
        FROM current_row_positions
        ORDER BY row_index, col_index, has_split DESC
    )
)
SELECT
    SUM(has_split) AS answer
FROM beam_positions;
