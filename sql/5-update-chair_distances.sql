WITH latest_locations AS (
    SELECT chair_id, id AS latest_id
    FROM (
        SELECT chair_id, id,
               ROW_NUMBER() OVER (PARTITION BY chair_id ORDER BY created_at DESC) AS row_num
        FROM chair_locations
    ) sub
    WHERE row_num = 1
)
UPDATE chair_distances
SET current_chair_location_id = (
    SELECT latest_id
    FROM latest_locations
    WHERE latest_locations.chair_id = chair_distances.chair_id
)
WHERE EXISTS (
    SELECT 1
    FROM latest_locations
    WHERE latest_locations.chair_id = chair_distances.chair_id
);