package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"

	"github.com/Darlene-13/mqtt-ingester/internal/ingest"
	"github.com/Darlene-13/mqtt-ingester/internal/store"
	"github.com/jackc/pgx/v5/pgxpool"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	cfg := ingest.Config{
		Workers: 4, QueueSize: 4096, BatchSize: 500,
		FlushEvery: 250 * time.Millisecond, EnqueueWait: 5 * time.Millisecond,
	}

	pcfg, err := pgxpool.ParseConfig(os.Getenv("DATABASE_URL"))
	if err != nil {
		log.Fatal(err)
	}

	pcfg.MaxConns = int32(cfg.Workers + 2) // one connection per worker + some headroom
	pool, err := pgxpool.NewWithConfig(ctx, pcfg)
	if err != nil {
		log.Fatal(err)
	}
	defer pool.Close()
	p := ingest.New(cfg, &store.Writer{Pool: pool})
	p.Start()

	topic := os.Getenv("MQTT_TOPIC")
	if topic == "" {
		topic = "telemetry/+/data"
	}
	opts := mqtt.NewClientOptions().
		AddBroker(os.Getenv("MQTT_BROKER")).
		SetClientID(os.Getenv("MQTT_CLIENT_ID")).
		SetCleanSession(false).
		SetAutoReconnect(true).
		SetMaxReconnectInterval(10 * time.Second).
		SetKeepAlive(30 * time.Second).
		SetOnConnectHandler(func(c mqtt.Client) {
			c.Subscribe(topic, 1, func(_ mqtt.Client, m mqtt.Message) {
				p.Submit(m.Payload()) //0(1): enqueue and return
			})
		})
	client := mqtt.NewClient(opts)
	if t := client.Connect(); t.Wait() && t.Error() != nil {
		log.Fatal(t.Error())
	}

	go func() {
		tk := time.NewTicker(time.Second)
		defer tk.Stop()
		var last uint64
		for {
			select {
			case <-ctx.Done():
				return
			case <-tk.C:
				w := p.Stats.Written.Load()
				log.Printf("written/s=%d total=%d recv=%d dropped=%d failed=%d decodeErr=%d queue=%d",
					w-last, w, p.Stats.Received.Load(), p.Stats.Dropped.Load(),
					p.Stats.Failed.Load(), p.Stats.DecodeErr.Load(), p.QueueDepth())

				last = w
			}
		}
	}()

	<-ctx.Done()
	client.Disconnect(500)
	p.Stop()
	log.Println("clean shutdown yaay!!")
}
