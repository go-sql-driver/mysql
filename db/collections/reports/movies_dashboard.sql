SELECT
    COUNT(*) AS total_movies,

    SUM(
        CASE
            WHEN featured = TRUE THEN 1
            ELSE 0
        END
    ) AS featured_total,

    SUM(
        CASE
            WHEN featured = FALSE THEN 1
            ELSE 0
        END
    ) AS pending_total,

    ROUND(
        (
            SUM(
                CASE
                    WHEN featured = TRUE THEN 1
                    ELSE 0
                END
            ) / COUNT(*)
        ) * 100,
        2
    ) AS completion_percent,

    (
        SELECT title
        FROM movies
        WHERE featured = TRUE
        ORDER BY updated_at DESC
        LIMIT 1
    ) AS last_featured

FROM movies;