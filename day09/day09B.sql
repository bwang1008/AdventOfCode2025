CREATE TEMPORARY TABLE input (
    lines TEXT
);

\copy input FROM 'day09/input.txt' WITH (FORMAT text);

WITH input_corners AS (
    SELECT
        ROW_NUMBER() OVER () AS row_index,
        CAST(SPLIT_PART(lines, ',', 1) AS BIGINT) AS x,
        CAST(SPLIT_PART(lines, ',', 2) AS BIGINT) AS y
    FROM input
),
edges AS (
    SELECT
        c1.row_index,
        c1.x AS x1,
        c1.y AS y1,
        c2.x AS x2,
        c2.y AS y2,
        CASE
            WHEN c1.x = c2.x AND c1.y < c2.y
            THEN 'DOWN'
            WHEN c1.x = c2.x AND c1.y > c2.y
            THEN 'UP'
            WHEN c1.x < c2.x AND c1.y = c2.y
            THEN 'RIGHT'
            WHEN c1.x > c2.x AND c1.y = c2.y
            THEN 'LEFT'
            ELSE NULL
        END AS direction
    FROM input_corners c1
    INNER JOIN input_corners c2
    ON (
        (c2.row_index = c1.row_index + 1)
        OR (c2.row_index = ((c1.row_index + 1) % (SELECT COUNT(1) FROM input_corners)))
    )
),
corners AS (
    SELECT
        e1.row_index,
        e1.x1,
        e1.y1,
        e1.x2,
        e1.y2,
        e2.x2 AS x3,
        e2.y2 AS y3,
        e1.direction AS direction1,
        e2.direction AS direction2
    FROM edges e1
    INNER JOIN edges e2
    ON (
        (e2.row_index = e1.row_index + 1)
        OR (e2.row_index = (e1.row_index + 1) % (SELECT COUNT(1) FROM edges))
    )
),
corner_offsets (direction1, direction2, x_offset, y_offset) AS (
    VALUES
    ('UP', 'RIGHT', -1, -1),
    ('RIGHT', 'DOWN', 0, -1),
    ('DOWN', 'LEFT', 0, 0),
    ('LEFT', 'UP', -1, 0),
    ('LEFT', 'DOWN', 0, 0),
    ('UP', 'LEFT', -1, 0),
    ('RIGHT', 'UP', -1, -1),
    ('DOWN', 'RIGHT', 0, -1)
),
-- map input corners that occupy entire squares, to the corners on the boundaries
point_corners AS (
    SELECT
        corners.row_index,
        corners.x2 AS orig_x2,
        corners.y2 AS orig_y2,
        corners.direction1,
        corners.direction2,
        corners.x2 + corner_offsets.x_offset AS x,
        corners.y2 + corner_offsets.y_offset AS y
    FROM corners
    INNER JOIN corner_offsets
    ON (
        corner_offsets.direction1 = corners.direction1
        AND corner_offsets.direction2 = corners.direction2
    )
    ORDER BY corners.row_index
),
distinct_x AS (
    SELECT DISTINCT x
    FROM point_corners
    ORDER BY x
),
distinct_y AS (
    SELECT DISTINCT y
    FROM point_corners
    ORDER BY y
),
distinct_numbered_x AS (
    SELECT
        ROW_NUMBER() OVER () AS row_index,
        x
    FROM distinct_x
),
distinct_numbered_y AS (
    SELECT
        ROW_NUMBER() OVER () AS row_index,
        y
    FROM distinct_y
),
boundary_edges AS (
    SELECT
        pc1.row_index,
        pc1.x AS x1,
        pc1.y AS y1,
        pc2.x AS x2,
        pc2.y AS y2,
        CASE
            WHEN pc1.x = pc2.x AND pc1.y < pc2.y
            THEN 'DOWN'
            WHEN pc1.x = pc2.x AND pc1.y > pc2.y
            THEN 'UP'
            WHEN pc1.x < pc2.x AND pc1.y = pc2.y
            THEN 'RIGHT'
            WHEN pc1.x > pc2.x AND pc1.y = pc2.y
            THEN 'LEFT'
            ELSE NULL
        END AS direction
    FROM point_corners pc1
    INNER JOIN point_corners pc2
    ON (
        (pc2.row_index = pc1.row_index + 1)
        OR (pc2.row_index = ((pc1.row_index + 1) % (SELECT COUNT(1) FROM point_corners)))
    )
    ORDER BY pc1.row_index
),
horizontal_boundary_edges AS (
    SELECT * FROM boundary_edges WHERE y1 = y2
),
vertical_boundary_edges AS (
    SELECT * FROM boundary_edges WHERE x1 = x2
),
-- split edge into multiple points, with positions on all distinct_x/distinct_y
horizontal_boundary_points_helper AS (
    SELECT
        horizontal_boundary_edges.row_index AS edge_index,
        GENERATE_SERIES(
            dnx1.row_index,
            dnx2.row_index,
            CASE
                WHEN dnx1.row_index > dnx2.row_index
                THEN -1
                ELSE 1
            END
        ) AS squashed_x,
        dny.row_index AS squashed_y
    FROM horizontal_boundary_edges
    INNER JOIN distinct_numbered_x dnx1
    ON (dnx1.x = horizontal_boundary_edges.x1)
    INNER JOIN distinct_numbered_x dnx2
    ON (dnx2.x = horizontal_boundary_edges.x2)
    INNER JOIN distinct_numbered_y dny
    ON (dny.y = horizontal_boundary_edges.y1)
),
horizontal_boundary_points_helper_2 AS (
    SELECT
        ROW_NUMBER() OVER () AS row_index,
        edge_index,
        squashed_x,
        squashed_y
    FROM horizontal_boundary_points_helper
),
horizontal_boundary_points_helper_3 AS (
    SELECT
        edge_index,
        ROW_NUMBER() OVER (PARTITION BY edge_index ORDER BY row_index) AS segment_index,
        squashed_x,
        squashed_y
    FROM horizontal_boundary_points_helper_2
),
horizontal_boundary_points AS (
    SELECT
        edge_index,
        segment_index,
        squashed_x,
        squashed_y
    FROM horizontal_boundary_points_helper_3
    ORDER BY edge_index, segment_index
),
vertical_boundary_points_helper AS (
    SELECT
        vertical_boundary_edges.row_index AS edge_index,
        dnx.row_index AS squashed_x,
        GENERATE_SERIES(
            dny1.row_index,
            dny2.row_index,
            CASE
                WHEN dny1.row_index > dny2.row_index
                THEN -1
                ELSE 1
            END
        ) AS squashed_y
    FROM vertical_boundary_edges
    INNER JOIN distinct_numbered_y dny1
    ON (dny1.y = vertical_boundary_edges.y1)
    INNER JOIN distinct_numbered_y dny2
    ON (dny2.y = vertical_boundary_edges.y2)
    INNER JOIN distinct_numbered_x dnx
    ON (dnx.x = vertical_boundary_edges.x1)
),
vertical_boundary_points_helper_2 AS (
    SELECT
        ROW_NUMBER() OVER () AS row_index,
        edge_index,
        squashed_x,
        squashed_y
    FROM vertical_boundary_points_helper
),
vertical_boundary_points_helper_3 AS (
    SELECT
        edge_index,
        ROW_NUMBER() OVER (PARTITION BY edge_index ORDER BY row_index) AS segment_index,
        squashed_x,
        squashed_y
    FROM vertical_boundary_points_helper_2
),
vertical_boundary_points AS (
    SELECT
        edge_index,
        segment_index,
        squashed_x,
        squashed_y
    FROM vertical_boundary_points_helper_3
    ORDER BY edge_index, segment_index
),
horizontal_boundary_segments_helper AS (
    SELECT
        edge_index,
        segment_index,
        squashed_x,
        squashed_y,
        LEAD(squashed_x) OVER (PARTITION BY edge_index ORDER BY segment_index) AS squashed_x2
    FROM horizontal_boundary_points
),
-- upper left corner of line segments whose endpoints are in (distinct_x, distinct_y)
horizontal_boundary_segments AS (
    SELECT
        LEAST(squashed_x, squashed_x2) AS squashed_x,
        squashed_y
    FROM horizontal_boundary_segments_helper
    WHERE squashed_x2 IS NOT NULL
),
vertical_boundary_segments_helper AS (
    SELECT
        edge_index,
        segment_index,
        squashed_x,
        squashed_y,
        LEAD(squashed_y) OVER (PARTITION BY edge_index ORDER BY segment_index) AS squashed_y2
    FROM vertical_boundary_points
),
vertical_boundary_segments AS (
    SELECT
        squashed_x,
        LEAST(squashed_y, squashed_y2) AS squashed_y
    FROM vertical_boundary_segments_helper
    WHERE squashed_y2 IS NOT NULl
),
squashed_xs AS (
    SELECT
        GENERATE_SERIES(1, (SELECT COUNT(1) - 1 FROM distinct_x)) AS squashed_x
),
squashed_grid_indices AS (
    SELECT
        squashed_x,
        GENERATE_SERIES(1, (SELECT COUNT(1) - 1 FROM distinct_y)) AS squashed_y
    FROM squashed_xs
),
horizontal_walls AS (
    SELECT
        squashed_x,
        squashed_y,
        boundary
    FROM (
        SELECT DISTINCT ON (squashed_x, squashed_y)
            squashed_x,
            squashed_y,
            boundary
        FROM (
            SELECT
                squashed_x,
                squashed_y,
                1 AS boundary
            FROM horizontal_boundary_segments
            UNION ALL
            SELECT
                squashed_x,
                squashed_y,
                0 AS boundary
            FROM squashed_grid_indices
        ) AS subquery2
        ORDER BY squashed_x, squashed_y, boundary DESC
    ) AS subquery1
    ORDER BY squashed_x, squashed_y
),
vertical_walls AS (
    SELECT
        squashed_x,
        squashed_y,
        boundary
    FROM (
        SELECT DISTINCT ON (squashed_x, squashed_y)
            squashed_x,
            squashed_y,
            boundary
        FROM (
            SELECT
                squashed_x,
                squashed_y,
                1 AS boundary
            FROM vertical_boundary_segments
            UNION ALL
            SELECT
                squashed_x,
                squashed_y,
                0 AS boundary
            FROM squashed_grid_indices
        ) AS subquery2
        ORDER BY squashed_x, squashed_y, boundary DESC
    ) AS subquery1
    ORDER BY squashed_x, squashed_y
),
horizontal_interior AS (
    SELECT
        squashed_x,
        squashed_y,
        SUM(boundary) OVER (PARTITION BY squashed_x ORDER BY squashed_y) % 2 = 1 AS is_interior
    FROM horizontal_walls
),
vertical_interior AS (
    SELECT
        squashed_x,
        squashed_y,
        SUM(boundary) OVER (PARTITION BY squashed_y ORDER BY squashed_x) % 2 = 1 AS is_interior
    FROM vertical_walls
),
squashed_grid AS (
    SELECT
        squashed_grid_indices.squashed_x,
        squashed_grid_indices.squashed_y,
        CAST(horizontal_interior.is_interior AND vertical_interior.is_interior AS INTEGER) AS is_interior
    FROM squashed_grid_indices
    INNER JOIN horizontal_interior
    ON (
        horizontal_interior.squashed_x = squashed_grid_indices.squashed_x
        AND horizontal_interior.squashed_y = squashed_grid_indices.squashed_y
    )
    INNER JOIN vertical_interior
    ON (
        vertical_interior.squashed_x = squashed_grid_indices.squashed_x
        AND vertical_interior.squashed_y = squashed_grid_indices.squashed_y
    )
)
SELECT * FROM squashed_grid;




squashed_grid AS (
    SELECT
        squashed_x,
        squashed_y,
        CAST(
            (
                SELECT
                    COUNT(1) % 2 = 1
                FROM horizontal_boundary_segments
                WHERE horizontal_boundary_segments.squashed_x = squashed_grid_indices.squashed_x
                AND horizontal_boundary_segments.squashed_y <= squashed_grid_indices.squashed_y
            )
            AND
            (
                SELECT
                    COUNT(1) % 2 = 1
                FROM vertical_boundary_segments
                WHERE vertical_boundary_segments.squashed_x <= squashed_grid_indices.squashed_x
                AND vertical_boundary_segments.squashed_y = squashed_grid_indices.squashed_y
            )
        AS INTEGER) AS is_interior
    FROM squashed_grid_indices
),
squashed_point_corners AS (
    SELECT
        point_corners.row_index,
        distinct_numbered_x.row_index AS squashed_x,
        distinct_numbered_y.row_index AS squashed_y
    FROM point_corners
    INNER JOIN distinct_numbered_x
    ON (point_corners.x = distinct_numbered_x.x)
    INNER JOIN distinct_numbered_y
    ON (point_corners.y = distinct_numbered_y.y)
),
squashed_point_corner_pairs AS (
    SELECT
        spc1.row_index AS row_index1,
        spc1.squashed_x AS squashed_x1,
        spc1.squashed_y AS squashed_y1,
        spc2.row_index AS row_index2,
        spc2.squashed_x AS squashed_x2,
        spc2.squashed_y AS squashed_y2
    FROM squashed_point_corners spc1
    CROSS JOIN squashed_point_corners spc2
    WHERE spc1.row_index < spc2.row_index
),
corner_pairs_region_count AS (
    SELECT
        squashed_x1,
        squashed_y1,
        squashed_x2,
        squashed_y2,
        (
            SELECT
                SUM(squashed_grid.is_interior)
            FROM squashed_grid
            WHERE LEAST(squashed_x1, squashed_x2) <= squashed_grid.squashed_x
            AND squashed_grid.squashed_x < GREATEST(squashed_x1, squashed_x2)
            AND LEAST(squashed_y1, squashed_y2) <= squashed_grid.squashed_y
            AND squashed_grid.squashed_y < GREATEST(squashed_y1, squashed_y2)
        ) AS num_regions,
        ABS(squashed_x2 - squashed_x1) * ABS(squashed_y2 - squashed_y1) AS expected_num_regions
    FROM squashed_point_corner_pairs
),
interior_corner_pairs AS (
    SELECT
        squashed_x1,
        squashed_y1,
        squashed_x2,
        squashed_y2,
        num_regions
    FROM corner_pairs_region_count
    WHERE num_regions = expected_num_regions
),
interior_corner_pair_expanded AS (
    SELECT
        dnx1.x AS x1,
        dny1.y AS y1,
        dnx2.x AS x2,
        dny2.y AS y2
    FROM interior_corner_pairs
    INNER JOIN distinct_numbered_x dnx1
    ON (dnx1.row_index = squashed_x1)
    INNER JOIN distinct_numbered_y dny1
    ON (dny1.row_index = squashed_y1)
    INNER JOIN distinct_numbered_x dnx2
    ON (dnx2.row_index = squashed_x2)
    INNER JOIN distinct_numbered_y dny2
    ON (dny2.row_index = squashed_y2)
),
interior_corner_pair_original AS (
    SELECT
        pc1.orig_x2 AS x1,
        pc1.orig_y2 AS y1,
        pc2.orig_x2 AS x2,
        pc2.orig_y2 AS y2,
        (ABS(pc2.orig_x2 - pc1.orig_x2) + 1) * (ABS(pc2.orig_y2 - pc1.orig_y2) + 1) AS area
    FROM interior_corner_pair_expanded
    INNER JOIN point_corners pc1
    ON (pc1.x = interior_corner_pair_expanded.x1 AND pc1.y = interior_corner_pair_expanded.y1)
    INNER JOIN point_corners pc2
    ON (pc2.x = interior_corner_pair_expanded.x2 AND pc2.y = interior_corner_pair_expanded.y2)
)
SELECT
    MAX(area) AS answer
FROM interior_corner_pair_original;

--    answer
-- ------------
--  1543501936
-- (1 row)

-- Time: 1867519.421 ms (31:07.519)
