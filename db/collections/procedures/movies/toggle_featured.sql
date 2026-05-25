DROP PROCEDURE IF EXISTS ToggleFeatured;

DELIMITER //

CREATE PROCEDURE ToggleFeatured(
    IN p_id INT,
    IN p_featured BOOLEAN
)
BEGIN
    UPDATE movies
    SET featured = p_featured
    WHERE id = p_id
      AND featured <> p_featured;

    SELECT ROW_COUNT() AS updated;
END //

DELIMITER ;