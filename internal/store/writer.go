package store

import (
	"context"
	"time"

	"github.com/Darlene-13/mqtt-ingester/internal/model"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

var cols = []string{
	"time", "device_id", "temperature", "humidity",
	"acc_x", "acc_y", "acc_z", "gyro_x", "gyro_y", "gyro_z",
	"lat", "lon", "altitude", "speed",
}

type Writer struct {
	Pool *pgxpool.Pool
}

type rowSource struct {
	rows []model.Reading
	i    int
	vals []any
}

func (s *rowSource) Next() bool {
	s.i++
	return s.i <= len(s.rows)
}
func (s *rowSource) Err() error {
	return nil
}
func (s *rowSource) Values() ([]any, error) {
	r := &s.rows[s.i-1]
	v := s.vals
	v[0] = time.UnixMicro(r.TsMicro).UTC()
	v[1] = int32(r.DeviceID)
	v[2], v[3] = r.Temp, r.Hum
	v[4], v[5], v[6] = r.AccX, r.AccY, r.AccZ
	v[7], v[8], v[9] = r.GyroX, r.GyroY, r.GyroZ
	v[10], v[11] = r.Lat, r.Lon
	v[12], v[13] = r.Alt, r.Speed
	return v, nil
}

func (w *Writer) Write(ctx context.Context, rows []model.Reading) (int64, error) {
	src := &rowSource{rows: rows, vals: make([]any, len(cols))}
	return w.Pool.CopyFrom(ctx, pgx.Identifier{"telemetry"}, cols, src)

}
