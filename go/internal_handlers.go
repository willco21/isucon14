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
	ride := &Ride{}
	if err := db.GetContext(ctx, ride, `SELECT * FROM rides WHERE chair_id IS NULL ORDER BY created_at LIMIT 1`); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		writeError(w, http.StatusInternalServerError, err)
		return
	}

	chairs := []Chair{}
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
    *
from
    active_available_chairs
`); err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}

	if len(chairs) == 0 {
		w.WriteHeader(http.StatusNoContent)
		return
	}

	coordinate := Coordinate{Latitude: ride.PickupLatitude, Longitude: ride.PickupLongitude}

	nearbyChairs := []appGetNearbyChairsResponseChairDistance{}
	for _, chair := range chairs {
		if !chair.IsActive {
			continue
		}

		// 最新の位置情報を取得
		chairLocation := &ChairLocation{}
		if err := db.GetContext(ctx, chairLocation,
			`SELECT * FROM chair_locations WHERE chair_id = ? ORDER BY created_at DESC LIMIT 1`,
			chair.ID,
		); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				continue
			}
			slog.Error("chairLocation")
			writeError(w, http.StatusInternalServerError, err)
			return
		}

		/* distance := 50
		if calculateDistance(coordinate.Latitude, coordinate.Longitude, chairLocation.Latitude, chairLocation.Longitude) <= distance {
			nearbyChairs = append(nearbyChairs, appGetNearbyChairsResponseChair{
				ID:    chair.ID,
				Name:  chair.Name,
				Model: chair.Model,
				CurrentCoordinate: Coordinate{
					Latitude:  chairLocation.Latitude,
					Longitude: chairLocation.Longitude,
				},
			})
		} */
		nearbyChairs = append(nearbyChairs, appGetNearbyChairsResponseChairDistance{
			ID:    chair.ID,
			Name:  chair.Name,
			Model: chair.Model,
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

	w.WriteHeader(http.StatusNoContent)
}
