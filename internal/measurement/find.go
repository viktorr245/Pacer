package measurement

import (
	"context"
	"encoding/json"
	"time"
)

// FindRecords reads the current records of one provider that started before
// `before`, newest first, for a caller-facing view. `hint` narrows the scan to
// stored bodies that contain it, so it must be specific (a session ID); `keep`
// decides each candidate. It returns at most `limit` records, and whether
// older ones that `keep` accepts remain.
func (s *Store) FindRecords(ctx context.Context, provider, hint string, before time.Time, limit int, keep func(Record) bool) ([]Record, bool, error) {
	if provider == "" || hint == "" || limit < 1 || !validTime(before) || keep == nil {
		return nil, false, ErrInvalid
	}
	rows, err := s.db.QueryContext(ctx, `SELECT body FROM measurement_records WHERE provider=? AND started_at<? AND instr(CAST(body AS TEXT),?)>0 ORDER BY started_at DESC, event_id DESC`, provider, before.UnixNano(), hint)
	if err != nil {
		return nil, false, err
	}
	defer rows.Close()
	found := []Record{}
	for rows.Next() {
		var body []byte
		if err = rows.Scan(&body); err != nil {
			return nil, false, err
		}
		var r Record
		if err = json.Unmarshal(body, &r); err != nil {
			return nil, false, err
		}
		if !keep(r) {
			continue
		}
		if len(found) == limit {
			return found, true, nil
		}
		found = append(found, r)
	}
	return found, false, rows.Err()
}
