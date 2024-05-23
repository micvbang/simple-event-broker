# simple-event-broker
![master](https://github.com/micvbang/simple-event-broker/actions/workflows/test.yml/badge.svg?branch=master)

simple-event-broker, or seb, is an opinionated event broker with the stated goal of being simple and cheap to both maintain and operate.

Seb is inspired by [Kafka](https://kafka.apache.org/), [Warp Stream](https://www.warpstream.com/), and [turbopuffer](https://turbopuffer.com/). It explicitly trades latency for simpler code and low operational costs by utilizing cloud object storage; local disk is only used for caching.
