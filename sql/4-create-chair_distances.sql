INSERT INTO chair_distances (chair_id, total_distance, total_distance_updated_at) 
SELECT 
    id,
    IFNULL(total_distance, 0) AS total_distance,
    IFNULL(total_distance_updated_at, CURRENT_TIMESTAMP(6)) AS total_distance_updated_at
FROM chairs
LEFT JOIN (SELECT chair_id,
            SUM(IFNULL(distance, 0)) AS total_distance,
            MAX(created_at)          AS total_distance_updated_at
    FROM (SELECT chair_id,
                created_at,
                ABS(latitude - LAG(latitude) OVER (PARTITION BY chair_id ORDER BY created_at)) +
                ABS(longitude - LAG(longitude) OVER (PARTITION BY chair_id ORDER BY created_at)) AS distance
            FROM chair_locations) tmp
    GROUP BY chair_id
) distance_table ON distance_table.chair_id = chairs.id