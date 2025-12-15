DROP TABLE IF EXISTS input;

CREATE TEMPORARY TABLE input (
    lines TEXT
);

\copy input FROM 'day08/input.txt' WITH (FORMAT text);


WITH RECURSIVE initial_values AS (
    SELECT
        STRING_TO_ARRAY(lines, ',') AS distances
    FROM input
),
coordinates AS (
    SELECT
        ROW_NUMBER() OVER () AS row_index,
        CAST(distances[1] AS BIGINT) AS x,
        CAST(distances[2] AS BIGINT) AS y,
        CAST(distances[3] AS BIGINT) AS z
    FROM initial_values
),
point_pairs AS (
    SELECT
        c1.row_index AS row_index1,
        c1.x AS x1,
        c1.y AS y1,
        c1.z AS z1,
        c2.row_index AS row_index2,
        c2.x AS x2,
        c2.y AS y2,
        c2.z AS z2
    FROM coordinates c1
    CROSS JOIN coordinates c2
    WHERE c1.row_index < c2.row_index
),
pairwise_distances AS (
    SELECT
        row_index1,
        row_index2,
        (x2 - x1) * (x2 - x1) + (y2 - y1) * (y2 - y1) + (z2 - z1) * (z2 - z1) AS distance
    FROM point_pairs
),
relevant_pairwise_distances AS (
    SELECT
        row_index1,
        row_index2,
        distance
    FROM pairwise_distances
    ORDER BY distance
    LIMIT 1000
),
numbered_pairwise_distances AS (
    SELECT
        ROW_NUMBER() OVER () AS step,
        row_index1,
        row_index2,
        distance
    FROM relevant_pairwise_distances
),
leader AS (
    SELECT
        row_index AS node,
        row_index AS parent,
        0 AS step
    FROM coordinates
    UNION
    (
        WITH prev AS (
            SELECT * FROM leader WHERE step < (SELECT MAX(step) FROM numbered_pairwise_distances)
        ),
        current_pair AS (
            SELECT
                row_index1,
                row_index2
            FROM numbered_pairwise_distances
            WHERE numbered_pairwise_distances.step = 1 + (SELECT MAX(step) FROM prev)
        ),
        related AS (
            SELECT
                node,
                parent,
                1 + step AS step
            FROM prev
            WHERE node = (SELECT MAX(row_index1) FROM current_pair)
            OR node = (SELECT MAX(row_index2) FROM current_pair)
        ),
        changed AS (
            SELECT
                node,
                (SELECT MIN(parent) FROM related) AS parent
            FROM prev
            WHERE parent = (SELECT MAX(parent) FROM related)
        ),
        result AS (
            SELECT
                node,
                parent
            FROM changed
            UNION ALL
            SELECT
                node,
                parent
            FROM prev
        )
        SELECT DISTINCT ON (node)
            node,
            parent,
            1 + (SELECT MAX(step) FROM prev) AS step
        FROM result
        ORDER BY node, parent
    )
),
circuit_sizes AS (
    SELECT
        parent,
        COUNT(1) AS circuit_size
    FROM leader
    WHERE step = (SELECT MAX(step) FROM numbered_pairwise_distances)
    GROUP BY 1
    ORDER BY 2 DESC
    LIMIT 3
),
top3_sizes AS (
    SELECT
        ARRAY_AGG(circuit_size) AS sizes
    FROM circuit_sizes
)
SELECT
    sizes[1] * sizes[2] * sizes[3] AS answer
FROM top3_sizes;
