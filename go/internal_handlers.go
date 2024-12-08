package main

import (
	"database/sql"
	"errors"
	"net/http"
	"log/slog"
)

// このAPIをインスタンス内から一定間隔で叩かせることで、椅子とライドをマッチングさせる
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
	slog.Info("rides fetched..")

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

	chair_count := 0
	// slog.Info("len(chairs)", len(chairs))
	// slog.Info("len(rides)", len(rides))
	for _, chair := range chairs {
		if len(rides) >= chair_count {
			if _, err := db.ExecContext(ctx, "UPDATE rides SET chair_id = ? WHERE id = ?", chair.ID, rides[chair_count].ID); err != nil {
				writeError(w, http.StatusInternalServerError, err)
				return
			}
			chair_count = chair_count + 1
		}
	}

	w.WriteHeader(http.StatusNoContent)
}
