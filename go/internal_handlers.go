package main

import (
	"database/sql"
	"errors"
	"log/slog"
	"net/http"
	"sort"
)

// このAPIをインスタンス内から一定間隔で叩かせることで、椅子とライドをマッチングさせる
// ユーザーから近い椅子をマッチ
// /  ridesのpickup_latitude / pickup_longitude
// /  chair_locationsのlatitude / longitude
// 移動距離が遠い場合には早い椅子をマッチ
// ISUCON_MATCHING_INTERVALを小さくして、早くマッチさせる
func internalGetMatching(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	// MEMO: 一旦最も待たせているリクエストに適当な空いている椅子マッチさせる実装とする。おそらくもっといい方法があるはず…
	rides := []Ride{}
	if err := db.SelectContext(ctx, &rides, `SELECT * FROM rides WHERE chair_id IS NULL order by created_at`); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		writeError(w, http.StatusInternalServerError, err)
		return
	}

	chairs := []ChairLocation{}
	if err := db.SelectContext(ctx, &chairs, `WITH uncompleted_chairs AS (
    select
        r.id ride_id,
        r.chair_id chair_id
    from
        chairs c
        inner join rides r on c.id = r.chair_id
        inner join ride_statuses rs on rs.ride_id = r.id
    where
        c.is_active = true
    group by
        r.id, r.chair_id
    having count(1) < 6
),
active_available_chairs AS (
    select
        c.*
    from
        chairs c
    where
        c.id not in (select chair_id from uncompleted_chairs)
        and c.is_active = true
)
select
    cd.*
from
    active_available_chairs c 
inner join chair_distances
on chair_distances.chair_id = c.id
inner join chair_locations cd
on chair_distances.current_chair_location_id = cd.id
where
	cd.created_at < NOW() - INTERVAL 3 SECOND;
`); err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}

	if len(chairs) == 0 {
		w.WriteHeader(http.StatusNoContent)
		return
	}

	slog.Info("Remaing chairs count", "value", len(chairs))
	slog.Info("Waiting rides count", "value", len(rides))
	after_assined_count := 0
	if len(rides) > len(chairs) {
		after_assined_count = len(rides) - len(chairs)
	}
	slog.Info("After Assinged rides count", "value", after_assined_count)
	
	filtered_chair_ids := []string{}
	for  _, ride := range rides {

		coordinate := Coordinate{Latitude: ride.PickupLatitude, Longitude: ride.PickupLongitude}

		nearbyChairs := []appGetNearbyChairsResponseChairDistance{}

		// フィルタリングして新しいスライスを作成
		filtered := []ChairLocation{}
		for _, chair := range chairs {
			if !contains(filtered_chair_ids, chair.ChairID) {
				filtered = append(filtered, chair)
			}
		}

		for _, chairLocation := range filtered {
			nearbyChairs = append(nearbyChairs, appGetNearbyChairsResponseChairDistance{
				ID:    chairLocation.ChairID,
				CurrentCoordinate: Coordinate{
					Latitude:  chairLocation.Latitude,
					Longitude: chairLocation.Longitude,
				},
				Distance: calculateDistance(coordinate.Latitude, coordinate.Longitude, chairLocation.Latitude, chairLocation.Longitude),
			})
		}

		sort.Slice(nearbyChairs, func(i, j int) bool {
			return nearbyChairs[i].Distance < nearbyChairs[j].Distance
		})

		if _, err := db.ExecContext(ctx, "UPDATE rides SET chair_id = ? WHERE id = ?", nearbyChairs[0].ID, ride.ID); err != nil {
			slog.Error("ExecContext")
			writeError(w, http.StatusInternalServerError, err)
			return
		}

		// 次回の処理では当該Chairは使用しない
		filtered_chair_ids = append(filtered_chair_ids, nearbyChairs[0].ID)
	}

	w.WriteHeader(http.StatusNoContent)
}

func contains(slice []string, target string) bool {
    for _, s := range slice {
        if s == target {
            return true
        }
    }
    return false
}