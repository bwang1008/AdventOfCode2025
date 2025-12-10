CREATE TEMPORARY TABLE input (
    lines TEXT
);

\copy input FROM 'day05/input.txt' WITH (FORMAT text);

WITH lines_with_row AS (
    SELECT ROW_NUMBER() OVER () AS row_index,
    lines
    FROM input
),
blank_line_index AS (
    SELECT
        row_index
    FROM lines_with_row
    WHERE LENGTH(lines) = 0
),
fresh_ranges_input AS (
    SELECT
        lines
    FROM lines_with_row
    WHERE row_index < (SELECT row_index FROM blank_line_index)
),
fresh_ranges AS (
    SELECT
        CAST(SPLIT_PART(lines, '-', 1) AS BIGINT) AS lower_range,
        CAST(SPLIT_PART(lines, '-', 2) AS BIGINT) AS upper_range
    FROM fresh_ranges_input
),
available_ingredients AS (
    SELECT
        CAST(lines AS BIGINT) AS ingredient_id
    FROM lines_with_row
    WHERE row_index > (SELECT row_index FROM blank_line_index)
),
ingredients_in_freshness_range AS (
    SELECT
        DISTINCT available_ingredients.ingredient_id
    FROM available_ingredients
    CROSS JOIN fresh_ranges
    WHERE fresh_ranges.lower_range <= available_ingredients.ingredient_id
        AND available_ingredients.ingredient_id <= fresh_ranges.upper_range
    ORDER BY available_ingredients.ingredient_id
)
SELECT
    COUNT(*) AS answer
FROM ingredients_in_freshness_range;
