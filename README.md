# MQTT INGESTER
This is a high throughput MQTT telemetry ingester written in Go. It subscribes to sensor topics, decodes a fixed binary frame, batches rows and writes them to Timescale DB with COPY.

The goal is to: Sustain atleast 100writes/s with large headroom (design goal 10x to 50x), never block MQTT network loop and fail in ways that are visible and recoverable.


## Contents



## Architecture
