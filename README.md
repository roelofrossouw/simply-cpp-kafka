# simply-cpp-kafka

C++20 consumer wrapper for Apache Kafka, built on
[modern-cpp-kafka](https://github.com/morganstanley/modern-cpp-kafka).

**Documentation:** the [simply-cpp wiki](https://github.com/roelofrossouw/simply-cpp/wiki) has a [Kafka guide](https://github.com/roelofrossouw/simply-cpp/wiki/Kafka) and the [sc-kafka reference](https://github.com/roelofrossouw/simply-cpp/wiki/Reference-sc-kafka), plus [getting started](https://github.com/roelofrossouw/simply-cpp/wiki/Getting-Started) for the whole suite.

## Install

### apt (Ubuntu)

```bash
curl -fsSL https://apt.roelof.co.za/setup.sh | bash
sudo apt install simply-cpp-kafka-dev
```

### CMake

```cmake
find_package(sc-kafka CONFIG REQUIRED)

add_executable(consumer main.cpp)
target_link_libraries(consumer PRIVATE sc::sc-kafka-shared)
```

## Usage

```cpp
#include <sc/kafka/kafka.h>

sc::kafka::consumer consumer{"broker.example:9092"};
consumer.client_id("vehicle-consumer");
consumer.group_id("vehicle-consumers");
consumer.from_beginning(false);
consumer.poll(std::chrono::milliseconds{200});
consumer.poll_at_end(std::chrono::milliseconds{1000});
consumer.add_topic("vehicle-events");

consumer.consume([](const sc::kafka::message& message) {
    std::cout << message.topic << ": " << message.data << '\n';
});
```

The constructor takes an `sc::ip_endpoints`: one broker or several separated by
`;` (`"kafka1;kafka2:9093"`), a `std::vector<sc::ip_endpoint>` or a braced list.
Brokers without a port use 9092. It throws `std::invalid_argument` for an empty
list or a `,` in a host, since librdkafka's comma-separated form isn't used here.

`AddTopic()` also takes a `std::vector<std::string>` or a braced list, adding
every item, e.g. `consumer.add_topic(sc::explode("topic1;topic2"))` or
`consumer.add_topic({"topic1", "topic2"})`. Adding a topic twice has no
effect. `consume()` returns without creating a Kafka connection when no topics have
been added. Set `MaxRecords()` to a positive number for bounded consumers and
tests; the default (`0`) consumes until interrupted with `SIGINT` or `SIGTERM`.
Set `ClientId()` and `GroupId()` for each deployed consumer; both default to
`simply-cpp-kafka`.

## Demo

`sc-kafka-demo` consumes a few messages and prints a line for each (time, topic,
key, size), then a summary. It is installed with the runtime package
(`simply-cpp-kafka`), so it also checks a machine can consume from Kafka without
the `-dev` package. It is a demonstration, not a test, so CTest doesn't run it:

```bash
SC_KAFKA_DEMO_TOPICS="topic1;topic2" SC_KAFKA_DEMO_GROUP_ID=my-demo-group sc-kafka-demo
SC_KAFKA_DEMO_BROKERS="kafka1.example.com;kafka2.example.com:9093" SC_KAFKA_DEMO_TOPICS=topic1 sc-kafka-demo
```

It reads up to `SC_KAFKA_DEMO_MAX_RECORDS` (default 10) new messages and stops
early when the topics go quiet. Its settings come from the environment:

| Variable | Default |
|---|---|
| `SC_KAFKA_DEMO_BROKERS` | `127.0.0.1:9092`; one or more brokers separated by `;` (an invalid value is an error) |
| `SC_KAFKA_DEMO_TOPICS` | required; one or more topics separated by `;` |
| `SC_KAFKA_DEMO_CLIENT_ID` | `sc-kafka-demo` |
| `SC_KAFKA_DEMO_GROUP_ID` | `sc-kafka-demo` |
| `SC_KAFKA_DEMO_MAX_RECORDS` | `10` |

`consume()` commits after each batch it handled. `before_commit(f)` (since 4.1.0) calls `f`
first, on the consuming thread: a handler that only queues its work can wait there until it's
done, so a committed offset means the work is done. A commit that fails (the group rebalancing as
a consumer joins or leaves) is reported and counted (`consumer::failed_commits()`), not thrown:
those messages come again, so a handler must take a message twice. Each partition assignment and
revocation is written to `std::cout` and counted (`consumer::rebalances()`).

Quote values containing `;` in a shell. The consumer commits offsets for its
group, so don't point `SC_KAFKA_DEMO_GROUP_ID` at a group a real consumer uses:
the demo would take its partitions and the messages it reads. Build servers have these
variables in `/etc/simply-cpp/test.env`, for running it there by hand.

Its source is `examples/sc-kafka-demo.cpp`; the code below is copied from it at
configure time, so it always matches code that compiles:

<!-- sc-example: examples/sc-kafka-demo.cpp -->
```cpp
sc::kafka::consumer consumer{brokers};
consumer.client_id(client_id);
consumer.group_id(group_id);
consumer.max_records(max_records);
consumer.add_topic(topics);

sc::console::heading("Messages, from where this group left off");
int received = 0;
consumer.consume([&](const sc::kafka::message &message) {
    if (++received > max_records) return; // the rest of the last batch, counted below
    const auto created = sc::datetime::from_unix(message.created / 1000);
    sc::console::note(created.format("%H:%M:%S") + "  " + message.topic + "  key " + message.key + "  (" +
                      std::to_string(message.data.size()) + " bytes)");
});
```
<!-- /sc-example -->

## Producing

`sc::kafka::producer` sends messages: `send(topic, key, value)` queues one and
returns at once; it goes out in the background, in small batches. A delivery
that fails later (no broker, or not delivered within 30 seconds) is counted in
`failed()` and reported to std::cerr; `delivered()` counts the rest.
`flush(timeout)` waits for everything sent so far. The destructor waits up to
10 seconds for what is still queued, then drops it. One producer can be shared
by threads.

```cpp
sc::kafka::producer producer{"kafka1:9092;kafka2:9092", "twc-poller"};
producer.send("ta_twc_topic", imei, record.dump());
```

`send()` throws std::runtime_error when Kafka refuses a message at once: a full
queue, or a topic the broker doesn't have and won't create.
