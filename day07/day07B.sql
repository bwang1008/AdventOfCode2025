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
        CAST(1 AS BIGINT) AS num_timelines
    FROM board
    WHERE row_index = 1
    AND value = 'S'
    UNION
    (
        WITH row_comparisons AS (
            SELECT
                board.row_index,
                board.col_index,
                board.value,
                beam_positions.num_timelines AS num_parent_timelines
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
                num_parent_timelines AS num_timelines
            FROM row_comparisons
            WHERE value != '^'
            UNION ALL
            SELECT
                row_index,
                col_index - 1 AS col_index,
                num_parent_timelines AS num_timelines
            FROM row_comparisons
            WHERE value = '^'
            UNION ALL
            SELECT
                row_index,
                col_index + 1 AS col_index,
                num_parent_timelines AS num_timelines
            FROM row_comparisons
            WHERE value = '^'
        )
        SELECT
            row_index,
            col_index,
            CAST(SUM(num_timelines) AS BIGINT) AS num_timelines
        FROM current_row_positions
        GROUP BY row_index, col_index
        ORDER BY row_index, col_index
    )
)
SELECT
    SUM(num_timelines) AS answer
FROM beam_positions
WHERE row_index = (SELECT MAX(row_index) FROM beam_positions);
