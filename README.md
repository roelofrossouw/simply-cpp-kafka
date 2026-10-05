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

`consume()` returns without creating a Kafka connection when no topics have
been added. Set `MaxRecords()` to a positive number for bounded consumers and
tests; the default (`0`) consumes until interrupted with `SIGINT` or `SIGTERM`.
Set `ClientId()` and `GroupId()` for each deployed consumer; both default to
`simply-cpp-kafka`.
