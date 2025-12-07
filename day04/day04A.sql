CREATE TEMPORARY TABLE input (
    lines TEXT
);

\copy input FROM 'day04/input.txt' WITH (FORMAT text);

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
neighbor_directions (drow, dcol) AS (
    VALUES
    (-1, -1),
    (-1, 0),
    (-1, 1),
    (0, -1),
    (0, 1),
    (1, -1),
    (1, 0),
    (1, 1)
),
board_with_potential_neighbors AS (
    SELECT
        board.row_index,
        board.col_index,
        board.value,
        board.row_index + neighbor_directions.drow AS row_index2,
        board.col_index + neighbor_directions.dcol AS col_index2
    FROM board
    CROSS JOIN neighbor_directions
),
board_with_neighbors AS (
    SELECT
        board_with_potential_neighbors.row_index,
        board_with_potential_neighbors.col_index,
        board_with_potential_neighbors.value,
        board_with_potential_neighbors.row_index2,
        board_with_potential_neighbors.col_index2,
        board.value AS value2
    FROM board_with_potential_neighbors
    INNER JOIN board
      ON (
        board.row_index = board_with_potential_neighbors.row_index2
        AND board.col_index = board_with_potential_neighbors.col_index2
      )
),
board_with_positive_paper_roll_neighbor_counts AS (
    SELECT
        row_index,
        col_index,
        value,
        COUNT(1) AS num_paper_roll_neighbors
    FROM
    board_with_neighbors
    WHERE value2 = '@'
    GROUP BY row_index, col_index, value
),
board_with_paper_roll_neighbor_counts AS (
    SELECT
        board.row_index,
        board.col_index,
        board.value,
        COALESCE(num_paper_roll_neighbors, 0) AS num_paper_roll_neighbors
    FROM board
    LEFT JOIN board_with_positive_paper_roll_neighbor_counts
      ON (
        board_with_positive_paper_roll_neighbor_counts.row_index = board.row_index
        AND board_with_positive_paper_roll_neighbor_counts.col_index = board.col_index
      )
)
SELECT
    COUNT(*) AS answer
FROM board_with_paper_roll_neighbor_counts
WHERE value = '@'
  AND num_paper_roll_neighbors < 4;
