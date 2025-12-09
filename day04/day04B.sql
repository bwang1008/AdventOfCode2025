CREATE TEMPORARY TABLE input (
    lines TEXT
);

\copy input FROM 'day04/input.txt' WITH (FORMAT text);

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
reachable_paper_rolls AS (
    SELECT
        row_index,
        col_index,
        value,
        1 AS depth,
        FALSE AS is_reachable,
        CAST(0 AS BIGINT) AS num_is_reachable
    FROM board
    WHERE value = '@'
    UNION
    (
        WITH board_with_potential_neighbors_augmented AS (
            SELECT
                board.row_index,
                board.col_index,
                board.value,
                board.row_index + neighbor_directions.drow AS row_index2,
                board.col_index + neighbor_directions.dcol AS col_index2
            FROM board
            CROSS JOIN neighbor_directions
        ),
        board_with_neighbors_augmented AS (
            SELECT
                board_with_potential_neighbors_augmented.row_index,
                board_with_potential_neighbors_augmented.col_index,
                board_with_potential_neighbors_augmented.value,
                board_with_potential_neighbors_augmented.row_index2,
                board_with_potential_neighbors_augmented.col_index2,
                reachable_paper_rolls.value AS value2,
                reachable_paper_rolls.depth AS depth,
                reachable_paper_rolls.num_is_reachable AS prev_num_is_reachable
            FROM board_with_potential_neighbors_augmented
            INNER JOIN reachable_paper_rolls
                ON (
                    reachable_paper_rolls.row_index = board_with_potential_neighbors_augmented.row_index2
                    AND reachable_paper_rolls.col_index = board_with_potential_neighbors_augmented.col_index2
                )
            WHERE NOT reachable_paper_rolls.is_reachable
        ),
        board_with_positive_paper_roll_neighbor_counts AS (
            SELECT
                row_index,
                col_index,
                value,
                COUNT(1) AS num_paper_roll_neighbors,
                depth,
                prev_num_is_reachable
            FROM board_with_neighbors_augmented
            WHERE value2 = '@'
            GROUP BY row_index, col_index, value, depth, prev_num_is_reachable
        ),
        board_with_paper_roll_neighbor_counts AS (
            SELECT
                board.row_index,
                board.col_index,
                board.value,
                COALESCE(num_paper_roll_neighbors, 0) AS num_paper_roll_neighbors,
                -- can't use just "depth", as LEFT JOIN means some of this depth is NULL.
                -- this select max makes this column non-null
                (SELECT MAX(depth) FROM board_with_positive_paper_roll_neighbor_counts) AS depth,
                prev_num_is_reachable
            FROM board
            LEFT JOIN board_with_positive_paper_roll_neighbor_counts
            ON (
                board_with_positive_paper_roll_neighbor_counts.row_index = board.row_index
                AND board_with_positive_paper_roll_neighbor_counts.col_index = board.col_index
            )
        ),
        result AS (
            SELECT
                row_index,
                col_index,
                value,
                1 + depth AS depth,
                num_paper_roll_neighbors < 4 AS is_reachable,
                prev_num_is_reachable
            FROM board_with_paper_roll_neighbor_counts
            WHERE value = '@'
        )
        -- don't think the result set returned is completely right... but num_is_reachable is correct
        SELECT
            row_index,
            col_index,
            value,
            depth,
            is_reachable,
            (SELECT COUNT(1) FROM result WHERE is_reachable) AS num_is_reachable
        FROM result
        WHERE (SELECT COUNT(1) FROM result WHERE is_reachable) != prev_num_is_reachable
    )
)
SELECT
    DISTINCT(num_is_reachable) AS answer
FROM reachable_paper_rolls WHERE depth = (SELECT MAX(depth) FROM reachable_paper_rolls) AND is_reachable;
