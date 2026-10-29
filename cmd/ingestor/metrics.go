package main

import (
	"github.com/prometheus/client_golang/prometheus"

	"github.com/Darlene-13/mqtt-ingester/internal/ingest"
)

func registerMetrics(p *ingest.Pipeline, queueCap int) {
	counter := func(name, help string, f func() float64) {
		prometheus.MustRegister(prometheus.NewCounterFunc(
			prometheus.CounterOpts{Name: name, Help: help}, f))
	}
	gauge := func(name, help string, f func() float64) {
		prometheus.MustRegister(prometheus.NewGaugeFunc(
			prometheus.GaugeOpts{Name: name, Help: help}, f))
	}

	counter("ingest_received_total", "Messages handed to the pipeline",
		func() float64 { return float64(p.Stats.Received.Load()) })
	counter("ingest_written_total", "Rows committed to the database",
		func() float64 { return float64(p.Stats.Written.Load()) })
	counter("ingest_dropped_total", "Messages shed because the queue was full",
		func() float64 { return float64(p.Stats.Dropped.Load()) })
	counter("ingest_failed_total", "Rows lost after retries were exhausted",
		func() float64 { return float64(p.Stats.Failed.Load()) })
	counter("ingest_decode_errors_total", "Malformed frames skipped",
		func() float64 { return float64(p.Stats.DecodeErr.Load()) })

	gauge("ingest_queue_depth", "Current channel depth",
		func() float64 { return float64(p.QueueDepth()) })
	gauge("ingest_queue_capacity", "Channel capacity",
		func() float64 { return float64(queueCap) })
}
