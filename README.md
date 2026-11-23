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
done, so a committed offset means the work is done. A commit sent while the group rebalances (a
consumer joining or leaving) is refused: the broker answers it only once this member has rejoined,
with the generation that just ended. Since 5.0.1 its offsets are kept and go out with the next
commit, or, for partitions this consumer gives up, from the revoke callback before their new owner
starts (`commit put off ... trying again`, then `went through` or `committed N partition(s) ...
before giving them up`). Offsets that still can't be committed are reported and counted
(`consumer::failed_commits()`), not thrown: those messages come again, so a handler must take a
message twice. Each partition assignment and
revocation is written to `std::cout` and counted (`consumer::rebalances()`).

Since 5.0.0 (a new soname: `consumer` and `message` changed layout):

- `message` also has its `partition` and `offset`.
- `assignment("cooperative-sticky")` sets librdkafka's `partition.assignment.strategy`: only the
  partitions that change owner are given up at a rebalance, so a member busy with a long batch
  doesn't hold up one joining. Empty (the default) keeps librdkafka's eager default. Every member
  of a group must use the same kind.
- `max_batch(d)` ends a batch once it has taken `d`: the handled messages are committed by
  explicit offsets and the rest read again (a seek back), so a slow handler still polls often.
  Not with `auto_commit()`. Counted in `consumer::cut_batches()`.
- `subscribe_wait(d)` (default 5 s): with no assignment by then (the group rebalancing), the
  consumer carries on waiting in `poll()` instead of throwing as modern-cpp-kafka's `subscribe()`
  does. Counted in `consumer::subscribe_waits()` and `consumer::assignment_wait_seconds()`.
- `stop()` ends each consumer after the message it's handling (the rest of the batch comes
  again), and `consumer::stopping()` tells a long handler to give up: throwing `sc::kafka::abandon`
  leaves its message unhandled and uncommitted, for the next owner. Each consumer then closes,
  leaving the group at once.

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

## Signing in (SASL, TLS)

Both the consumer and the producer take `sc::kafka::security` (since 5.1.0): plaintext by
default; `ssl`, `sasl_plaintext` or `sasl_ssl`, with SCRAM-SHA-256, SCRAM-SHA-512 (the default)
or PLAIN, a username and password, and for TLS an optional CA file (else the system's CAs).

```cpp
sc::kafka::security security;
security.protocol = sc::kafka::security_protocol_from(conf_protocol); // "sasl_plaintext"
security.mechanism = sc::kafka::sasl_mechanism_from(conf_mechanism);  // "SCRAM-SHA-512"
security.username = "streamvms";
security.password = conf_password;

consumer.security(security);
sc::kafka::producer producer{"kafka1;kafka2;kafka3", "twc-poller", security};
```

`security::from(protocol, mechanism, username, password, ca)` builds one from a configuration's
text in one go (empty protocol: plaintext). `security_protocol_from()` and `sasl_mechanism_from()` read config text (any case, `-` or `_`)
and throw `std::invalid_argument` for anything else, as does SASL without a username or
password. `describe()` gives the settings for a log line or status page without the password;
modern-cpp-kafka masks `*.password` when it logs its properties. Ubuntu's librdkafka (noble 2.3,
resolute 2.13) is built with `ssl`, `sasl_plain` and `sasl_scram`.

Since 5.2.0 the password can come from a file instead (the apps' `sasl_password_file` key):
`security::from(protocol, mechanism, username, password, ca, password_file)` reads it when
`password_file` is set, and `security::password_from_file(path)` reads one on its own. The file
holds just the password (one line; trailing whitespace and newlines are dropped), so give each
client its own root-only file rather than a shared "user password" list. Setting both a password
and a file, or a file that's missing, unreadable or empty, throws `std::invalid_argument`, which
names the file but never its content.

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
