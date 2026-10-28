package main

import (
	"context"
	"flag"
	"fmt"
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Darlene-13/mqtt-ingester/internal/model"
	mqtt "github.com/eclipse/paho.mqtt.golang"
)

func main() {
	broker := flag.String("broker", "tcp://localhost:1883", "broker URL")
	devices := flag.Int("devices", 100, "simulated devices")
	hz := flag.Float64("hz", 1.0, "messages/sec per device")
	dur := flag.Duration("duration", 60*time.Second, "test duration")
	qos := flag.Int("qos", 1, "MQTT QoS (0/1)")
	flag.Parse()

	ctx, cancel := context.WithTimeout(context.Background(), *dur)
	defer cancel()

	//atomic to allow safe concurrent writes
	var sent, failed atomic.Uint64
	lats := make([][]time.Duration, *devices)
	interval := time.Duration(float64(time.Second) / *hz)

	var wg sync.WaitGroup
	start := time.Now()
	for i := 0; i < *devices; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			opts := mqtt.NewClientOptions().AddBroker(*broker).
				SetClientID(fmt.Sprintf("sim-%d", id))
			c := mqtt.NewClient(opts)
			if t := c.Connect(); t.Wait() && t.Error() != nil {
				failed.Add(1)
				return
			}
			defer c.Disconnect(250)

			rng := rand.New(rand.NewSource(int64(id)))
			// Stagger starts so devices don't fire in lockstep
			select {
			case <-time.After(time.Duration(rng.Int63n(int64(interval)))):
			case <-ctx.Done():
				return
			}

			r := model.Reading{
				DeviceID: uint32(id),
				Lat:      -1.2921 + rng.Float64()*0.01,
				Lon:      36.8219 + rng.Float64()*0.01,
				Alt:      1795,
			}

			topic := fmt.Sprintf("telemetry/%d/data", id)
			local := make([]time.Duration, 0, int(dur.Seconds()**hz)+16)
			tk := time.NewTicker(interval)
			defer tk.Stop()

			for {
				select {
				case <-ctx.Done():
					lats[id] = local
					return
				case <-tk.C:
					// random-walk the sensors
					r.TsMicro = time.Now().UnixMicro()
					r.Temp = 24 + float32(rng.NormFloat64())
					r.Hum = 55 + float32(rng.NormFloat64()*2)
					r.AccX, r.AccY, r.AccZ = float32(rng.NormFloat64()*0.1), float32(rng.NormFloat64()*0.1), 9.81+float32(rng.NormFloat64()*0.05)
					r.GyroX, r.GyroY, r.GyroZ = float32(rng.NormFloat64()), float32(rng.NormFloat64()), float32(rng.NormFloat64())
					r.Lat += rng.NormFloat64() * 1e-5
					r.Lon += rng.NormFloat64() * 1e-5
					r.Speed = float32(rng.Float64() * 20)

					// Fresh buffer each time: paho holds QoS1 payloads until PUBACK.
					buf := make([]byte, model.WireSize)
					model.Encode(&r, buf)

					t0 := time.Now()
					tok := c.Publish(topic, byte(*qos), false, buf)
					if !tok.WaitTimeout(5*time.Second) || tok.Error() != nil {
						failed.Add(1)
						continue
					}
					local = append(local, time.Since(t0))
					sent.Add(1)
				}
			}
		}(i)
	}
	wg.Wait()
	elapsed := time.Since(start)

	var all []time.Duration
	for _, l := range lats {
		all = append(all, l...)
	}
	sort.Slice(all, func(i, j int) bool { return all[i] < all[j] })
	pct := func(p float64) time.Duration {
		if len(all) == 0 {
			return 0
		}
		return all[int(float64(len(all)-1)*p)]
	}
	fmt.Printf("sent=%d failed=%d elapsed=%s rate=%.1f msg/s\n",
		sent.Load(), failed.Load(), elapsed.Round(time.Millisecond),
		float64(sent.Load())/elapsed.Seconds())
	fmt.Printf("ack latency p50=%s p95=%s p99=%s max=%s\n",
		pct(.50), pct(.95), pct(.99), pct(1))
}
