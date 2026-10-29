package model

import "testing"

func sample() Reading {
	return Reading{
		DeviceID: 7, TsMicro: 1700000000000000,
		Temp: 24.5, Hum: 55, AccX: 0.1, AccY: -0.2, AccZ: 9.81,
		GyroX: 1, GyroY: 2, GyroZ: 3,
		Lat: -1.2921, Lon: 36.8219, Alt: 1795, Speed: 12.5,
	}
}

func TestRoundTrip(t *testing.T) {
	in := sample()
	buf := make([]byte, WireSize)

	Encode(&in, buf)

	var out Reading
	if err := Decode(buf, &out); err != nil {
		t.Fatal(err)
	}

	if in != out {
		t.Fatalf("mismatch: \n in=%+v\nout=%+v", in, out)
	}
}

func TestDecodeBadSize(t *testing.T) {
	var r Reading
	if err := Decode(make([]byte, WireSize-1), &r); err != ErrBadSize {
		t.Fatalf("want ErrBadSize, got %v", err)
	}
}

func BenchmarkDecode(b *testing.B) {
	in := sample()
	buf := make([]byte, WireSize)
	Encode(&in, buf)
	var r Reading
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := Decode(buf, &r); err != nil {
			b.Fatal(err)
		}
	}
}
