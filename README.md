# simple-message-broker (smb)
![master](https://github.com/micvbang/simple-message-broker/actions/workflows/test.yml/badge.svg?branch=master)

smb (es-em-bee) is an opinionated message broker with the stated goal of being simple and cheap to both maintain and operate.

smb is inspired by [Kafka](https://kafka.apache.org/) and [Warp Stream](https://www.warpstream.com/). It explicitly trades latency for simpler code and low operational costs by utilizing cloud object storage; local disk is only used for caching.
