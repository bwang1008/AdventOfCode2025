CREATE TEMPORARY TABLE input (
    lines TEXT
);

\copy input FROM 'day09/input.txt' WITH (FORMAT text);


WITH corners AS (
    SELECT
        CAST(SPLIT_PART(lines, ',', 1) AS BIGINT) AS x,
        CAST(SPLIT_PART(lines, ',', 2) AS BIGINT) AS y
    FROM input
),
corner_pairs AS (
    SELECT
        c1.x AS x1,
        c1.y AS y1,
        c2.x AS x2,
        c2.y AS y2
    FROM corners c1
    CROSS JOIN corners c2
)
SELECT
    MAX(ABS(y2 - y1 + 1) * ABS(x2 - x1 + 1)) AS answer
FROM corner_pairs;
