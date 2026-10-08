# simply-cpp-kafka

C++20 consumer wrapper for Apache Kafka, built on
[modern-cpp-kafka](https://github.com/morganstanley/modern-cpp-kafka).

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
#include <kafka.h>

sc::kafka consumer{"broker.example:9092"};
consumer.ClientId("vehicle-consumer");
consumer.GroupId("vehicle-consumers");
consumer.FromBeginning(false);
consumer.Poll(200);
consumer.PollAtEnd(1000);
consumer.AddTopic("vehicle-events");

consumer.consume([](const sc::kafka_message& message) {
    std::cout << message.topic << ": " << message.data << '\n';
});
```

`AddTopic()` also takes a `std::vector<std::string>` or a braced list, adding
every item, e.g. `consumer.AddTopic(sc::explode("topic1;topic2"))` or
`consumer.AddTopic({"topic1", "topic2"})`. Adding a topic twice has no
effect. `consume()` returns without creating a Kafka connection when no topics have
been added. Set `MaxRecords()` to a positive number for bounded consumers and
tests; the default (`0`) consumes until interrupted with `SIGINT` or `SIGTERM`.
Set `ClientId()` and `GroupId()` for each deployed consumer; both default to
`simply-cpp-kafka`.

## Demo

`sc-kafka-demo` is installed with the runtime package (`simply-cpp-kafka`), so
you can check a machine can consume from Kafka without the `-dev` package:

```bash
SC_KAFKA_DEMO_TOPICS="topic1;topic2" SC_KAFKA_DEMO_GROUP_ID=my-demo-group sc-kafka-demo
SC_KAFKA_DEMO_BROKERS="kafka1.example.com;kafka2.example.com:9093" SC_KAFKA_DEMO_TOPICS=topic1 sc-kafka-demo
```

It reads up to `SC_KAFKA_DEMO_MAX_RECORDS` (default 10) new messages and stops
early when the topics go quiet. Its settings come from the environment:

| Variable | Default |
|---|---|
| `SC_KAFKA_DEMO_BROKERS` | `127.0.0.1:9092`; one or more brokers separated by `;` (also used when the value is invalid) |
| `SC_KAFKA_DEMO_TOPICS` | required; one or more topics separated by `;` |
| `SC_KAFKA_DEMO_CLIENT_ID` | `sc-kafka-demo` |
| `SC_KAFKA_DEMO_GROUP_ID` | `sc-kafka-demo` |
| `SC_KAFKA_DEMO_MAX_RECORDS` | `10` |

Quote values containing `;` in a shell. The consumer commits offsets for its
group, so don't point `SC_KAFKA_DEMO_GROUP_ID` at a group a real consumer uses:
the demo would take its partitions and the messages it reads. The
`example-sc-kafka-demo` CTest uses the same variables, which build servers get
from `/etc/simply-cpp/test.env`.

Its source is `examples/sc-kafka-demo.cpp`; the code below is copied from it at
configure time, so it always matches code that compiles:

<!-- sc-example: examples/sc-kafka-demo.cpp -->
```cpp
sc::timer sw;
sc::kafka consumer{brokers};
consumer.ClientId(client_id);
consumer.GroupId(group_id);
consumer.MaxRecords(max_records);
consumer.AddTopic(topics);

int received = 0;
consumer.consume([&](const sc::kafka_message &message) {
    ++received;
    std::cout << message.topic << " key=" << message.key << " created=" << message.created
            << " (" << message.data.size() << " bytes)\n";
});
std::cout << "Received " << received << " messages after " << sw << '\n';
```
<!-- /sc-example -->
