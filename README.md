# simple-event-broker

![master](https://github.com/micvbang/simple-event-broker/actions/workflows/test.yml/badge.svg?branch=master)

Seb is an event broker that was inspired by [Kafka](https://kafka.apache.org/), [Warp Stream](https://www.warpstream.com/), and [turbopuffer](https://turbopuffer.com/). It explicitly trades latency for simpler code and low operational costs by utilizing cloud object storage; local disk is only used for caching.

There's an ongoing blog series on the development of Seb:

- [Hello World, Simple Event Broker!](https://blog.vbang.dk/2024/05/26/seb/)
- [Simple event broker tries Tiger Style](https://blog.vbang.dk/2024/07/10/seb-tiger-style/)
- [Simple event broker: data serialization is expensive](https://blog.vbang.dk/2024/09/10/seb-tiger-style-read-path/)

## Goals

The design goals of Seb are, in order:

1) cheap to run
2) easy to manage
3) easy to use

The goal “don’t lose my data” is actually the very first goal on that list, but I wanted a list of three, and I thought not losing data reasonably could be assumed to be table stakes. Let’s call that item 0.

Seb explicitly does not attempt to reach sub-millisecond latencies nor scale to fantastic workloads. If you need this, there are systems infinitely more capable, designed for exactly these workloads. See Kafka, Red Panda, RabbitMQ et al.

In order to reach the goals of being both cheap to run and easy to manage, Seb embraces the fact that writing data to disk and _ensuring that data is actually written and stays written_ is rather difficult. It utilizes the hundreds of thousands of engineering hours that were poured into object stores and pays the price of latency at the gates of the cloud vendors. For the use cases it was designed for, this trade-off is perfect; it gives reasonable throughput at a (very) low price, and makes the service super easy to manage and use.

Because Seb uses an object store as source of truth, it provides high guarantees of durability and is super easy to manage; if your server crashes and its disk goes up in flames, you can just spin up Seb on another machine and continue whatever you were doing. Seb only uses the local disk for caching the immutable batches of data that it creates, so starting up a new instance requires nothing more than access to the S3 bucket that the previous instance used.