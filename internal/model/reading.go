package model

import (
	"encoding/binary"
	"errors"
	"math"
)

// WireSize Network wire mqtt payload
const WireSize = 68

var ErrBadSize = errors.New("wire size is too small")

type Reading struct {
	TsMicro  int64
	Lat      float64
	Lon      float64
	DeviceID uint32
	Temp     float32
	Hum      float32
	AccX     float32
	AccY     float32
	AccZ     float32
	GyroX    float32
	GyroY    float32
	GyroZ    float32
	Alt      float32
	Speed    float32
}

// Dictates byte ordering (endianness)
var le = binary.LittleEndian

func f32(b []byte) float32 {
	return math.Float32frombits(le.Uint32(b))
}
func f64(b []byte) float64 {
	return math.Float64frombits(le.Uint64(b))
}

func Decode(b []byte, r *Reading) error {
	if len(b) != WireSize {
		return ErrBadSize
	}

	r.DeviceID = le.Uint32(b[0:])
	r.TsMicro = int64(le.Uint32(b[4:]))
	r.Temp, r.Hum = f32(b[12:]), f32(b[16:])
	r.AccX, r.AccY, r.AccZ = f32(b[20:]), f32(b[24:]), f32(b[28:])
	r.GyroX, r.GyroY, r.GyroZ = f32(b[32:]), f32(b[36:]), f32(b[40:])
	r.Lat, r.Lon = f64(b[44:]), f64(b[52:])
	r.Alt, r.Speed = f32(b[60:]), f32(b[64:])

	return nil
}

func Encode(r *Reading, b []byte) {
	_ = b[WireSize-1] // bounds check hint
	le.PutUint32(b[0:], r.DeviceID)
	le.PutUint64(b[4:], uint64(r.TsMicro))
	le.PutUint32(b[12:], math.Float32bits(r.Temp))
	le.PutUint32(b[16:], math.Float32bits(r.Hum))
	le.PutUint32(b[20:], math.Float32bits(r.AccX))
	le.PutUint32(b[24:], math.Float32bits(r.AccY))
	le.PutUint32(b[28:], math.Float32bits(r.AccZ))
	le.PutUint32(b[32:], math.Float32bits(r.GyroX))
	le.PutUint32(b[36:], math.Float32bits(r.GyroY))
	le.PutUint32(b[40:], math.Float32bits(r.GyroZ))
	le.PutUint64(b[44:], math.Float64bits(r.Lat))
	le.PutUint64(b[52:], math.Float64bits(r.Lon))
	le.PutUint32(b[60:], math.Float32bits(r.Alt))
	le.PutUint32(b[64:], math.Float32bits(r.Speed))
}
