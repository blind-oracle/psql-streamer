[![Go Report Card](https://goreportcard.com/badge/github.com/blind-oracle/psql-streamer)](https://goreportcard.com/report/github.com/blind-oracle/psql-streamer)
[![Coverage Status](https://coveralls.io/repos/github/blind-oracle/psql-streamer/badge.svg?branch=master)](https://coveralls.io/github/blind-oracle/psql-streamer?branch=master)
[![Build Status](https://travis-ci.org/blind-oracle/psql-streamer.svg?branch=master)](https://travis-ci.org/blind-oracle/psql-streamer)

# PSQL-Streamer
This service receives the database events from PostgreSQL using logical replication protocol and feeds them to sinks based on the configuration file settings.
Also can receive the events from Kafka generated by e.g. another *psql-streamer* instance.

## Features
* You can configure a sink to receive events from several different sources (e.g. several Kafka clusters, PostgreSQL databases)
* Basic HTTP API
* Custom sources/sinks can be easily added - they only need to implement a simple interface
* **Requires PostgreSQL 10+**

## Sources
### PostgreSQL
Receive database events from PostgreSQL using logical replication protocol and transform them into a common event format.

#### Features
* **WAL log position persistence**: the service persists each log position update it receives in a BoltDB database.
* **Configurable WAL logs retention** on the PostgreSQL side. To allow us to rewind back (in case we need to replay some events) the service implements delayed confirmation of applied logs. This makes PostgreSQL retain the logs for the specified replication slot for some time. See *walRetain* parameter in the configuration file.

#### PostgreSQL configuration
* In `postgresql.conf` you need to set `wal_level = logical` to make logical replication possible. You also may need to adjust `max_wal_senders` and `max_replication_slots` to match your requirements.
* Create a publication in PostgreSQL like this: `CREATE PUBLICATION pub1 FOR ALL TABLES`. This will include in the publication all existing tables and also the ones that will be created in future. If you want only a subset of tables to be replicated - list them specifically. See [PostgreSQL documentation](https://www.postgresql.org/docs/10/static/sql-createpublication.html) for details.
* Specify the publication name in the *psql-streamer.conf*

### Kafka
Receive events which were generated by e.g. another **psql-streamer** instance from one or more Kafka topics. Expects messages in a JSON format conforming to a predefined structure (see *Event structure* below).

#### Features
Kafka source works in batching mode with confirmation: if all events in a batch from Kafka are ACKed by sinks then we commit the whole batch or don't commit anything. The batch elements are sent concurrently using goroutines, so it should not be made very large. If several topics are specified then they're worked on in separate goroutines which should provide more parallelism.

## Sinks
### Kafka
Send events into a Kafka topic based on several configurable rules like:
* Table -> Topic mapping
* Fallback topic messages with no mapping defined

#### Kafka sink handlers
Incoming events are processed by one or more handlers that generate Kafka messages. They can be used to enrich Events with additional data or encode them in different format.

Currently there's only one handler:
* **passthrough**: simply marshal an event into JSON and send it to Kafka

### Stub
Stub sink is a discard/noop sink that can be used for testing.

## HTTP API
All requests are GET.

* **/health** - no-op just returns 200
* **/status** - checks all sinks/sources statuses and outputs them. If something is not OK then it returns 500 instead of 200. Useful for monitoring.
* **/stats** - returns statistics from all sinks/sources
* **/debug/[on|off]** - toggles debugging mode for all sinks/sources
* **/metrics** - Prometheus metrics

## Configuration
See *psql-streamer.toml* for detailed instructions.

## Event structure
```json
{
    "Host": "db1",
    "Database": "test",
    "Table": "test",
    "Action": "insert",
    "WALPosition": 418152976,
    "Timestamp": "2018-07-03T16:04:27.263625156+02:00",
    "Columns": {
        "a": 369223,
        "b": "a"
    }
}
```
