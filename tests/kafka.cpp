#include <kafka.h>
#include <sc_test.h>

#include <atomic>
#include <chrono>
#include <csignal>
#include <cstdlib>
#include <iostream>
#include <stdexcept>
#include <thread>
#include <vector>

int main() {
    sc::kafka consumer{"localhost:9092"};

    SECTION("Brokers");
    CHECK_EQ(consumer.Brokers().to_string(), "localhost:9092");
    const sc::kafka several{"kafka1;kafka2:9093;[::1]"};
    CHECK_EQ(several.Brokers().to_string(","), "kafka1:9092,kafka2:9093,[::1]:9092");
    const sc::kafka listed{{{"kafka1", 9094}, {"kafka2", 0}}};
    CHECK_EQ(listed.Brokers().to_string(), "kafka1:9094;kafka2:9092");
    CHECK_THROWS_AS(sc::kafka{""}, std::invalid_argument);
    CHECK_THROWS_AS(sc::kafka{"kafka1,kafka2"}, std::invalid_argument);
    CHECK_THROWS_AS(sc::kafka{"kafka1:nope"}, std::invalid_argument);

    SECTION("Default consumer configuration");
    CHECK_EQ(consumer.MaxRecords(), 0);
    CHECK_EQ(consumer.FromBeginning(), false);
    CHECK_EQ(consumer.Poll(), 200);
    CHECK_EQ(consumer.PollAtEnd(), 5000);
    CHECK_EQ(consumer.ClientId(), "simply-cpp-kafka");
    CHECK_EQ(consumer.GroupId(), "simply-cpp-kafka");
    CHECK_EQ(consumer.AutoCommit(), false);
    CHECK_EQ(consumer.LogLevel(), 3);

    SECTION("Consumer configuration");
    consumer.MaxRecords(25);
    consumer.FromBeginning(true);
    consumer.Poll(100);
    consumer.PollAtEnd(750);
    consumer.ClientId("streamvms-client");
    consumer.GroupId("streamvms-consumers");
    consumer.AutoCommit(true);
    consumer.LogLevel(6);

    CHECK_EQ(consumer.MaxRecords(), 25);
    CHECK_EQ(consumer.FromBeginning(), true);
    CHECK_EQ(consumer.Poll(), 100);
    CHECK_EQ(consumer.PollAtEnd(), 750);
    CHECK_EQ(consumer.ClientId(), "streamvms-client");
    CHECK_EQ(consumer.GroupId(), "streamvms-consumers");
    CHECK_EQ(consumer.AutoCommit(), true);
    CHECK_EQ(consumer.LogLevel(), 6);

    SECTION("No-topic consumption does not contact Kafka");
    CHECK_NOTHROW(consumer.consume([](const sc::kafka_message &) {}));

    SECTION("Adding an empty topic list adds no topics");
    consumer.AddTopic(std::vector<std::string>{});
    CHECK_NOTHROW(consumer.consume([](const sc::kafka_message &) {}));

    SECTION("Topics can be added one at a time, as a vector or as a braced list");
    // The topic set is private, so this checks that every call form compiles and resolves.
    sc::kafka topics{"localhost:9092"};
    topics.AddTopic("one");
    topics.AddTopic(std::vector<std::string>{"two", "three"});
    topics.AddTopic({"four"});
    topics.AddTopic({"five", "six"});
    CHECK_NOTHROW(topics.AddTopic("one"));

    SECTION("Consumers on their own threads share the signal handling");
    {
        // No broker answers on port 1, so each consume() ends when subscribe() times out (about
        // 30 seconds). While they wait, SIGTERM (systemctl stop) must be caught - asking them all
        // to stop - not the default kill; once the last has finished, the default is back.
        std::vector<std::thread> threads;
        std::atomic<int> finished{0};
        for (const auto *topic: {"first", "second"}) {
            threads.emplace_back([topic, &finished] {
                sc::kafka quiet{"127.0.0.1:1"};
                quiet.AddTopic(topic);
                try {
                    quiet.consume([](const sc::kafka_message &) {});
                } catch (const std::exception &) {
                    // the subscription timing out
                }
                ++finished;
            });
        }
        std::this_thread::sleep_for(std::chrono::seconds{2});
        std::raise(SIGTERM); // still running afterwards: the consumers' handler took it
        sc::kafka::stop();
        for (auto &thread: threads) thread.join();
        CHECK_EQ(finished.load(), 2);
        const auto handler = std::signal(SIGTERM, SIG_DFL);
        CHECK(handler == SIG_DFL);
    }

    SECTION("A producer checks its brokers as a consumer does");
    {
        CHECK_THROWS_AS(sc::kafka_producer{""}, std::invalid_argument);
        CHECK_THROWS_AS(sc::kafka_producer{"kafka1,kafka2"}, std::invalid_argument);
        const sc::kafka_producer producer{"kafka1;kafka2:9093"};
        CHECK_EQ(producer.Brokers().to_string(), "kafka1:9092;kafka2:9093");
    }

    SECTION("A producer with no broker: send() queues, flush() times out, nothing is delivered");
    {
        sc::kafka_producer producer{"127.0.0.1:1", "sc-kafka-test"};
        CHECK_NOTHROW(producer.send("nowhere", "key", "value"));
        CHECK(!producer.flush(std::chrono::milliseconds{500}));
        CHECK_EQ(producer.delivered(), std::uint64_t{0});
    }

    // Against the real broker, only with a topic to write to (SC_KAFKA_TEST_TOPIC): what is sent
    // comes back to a consumer, key and value as they were.
    if (const char *topic = std::getenv("SC_KAFKA_TEST_TOPIC"); topic && *topic) {
        SECTION("Produced, then consumed");
        const char *brokers = std::getenv("SC_KAFKA_DEMO_BROKERS");
        const std::string key = "sc-kafka-test-" + std::to_string(std::chrono::system_clock::now().time_since_epoch().count());
        {
            sc::kafka_producer producer{brokers ? brokers : "localhost:9092", "sc-kafka-test"};
            producer.send(topic, key, R"({"hello":"kafka"})");
            CHECK(producer.flush(std::chrono::seconds{15}));
            CHECK_EQ(producer.delivered(), std::uint64_t{1});
            CHECK_EQ(producer.failed(), std::uint64_t{0});
        }
        sc::kafka consumer{brokers ? brokers : "localhost:9092"};
        consumer.GroupId("sc-kafka-test-" + key);
        consumer.FromBeginning(true);
        consumer.AddTopic(topic);
        // consume() keeps polling (a new group's first polls are empty until its partitions are
        // assigned): stop it once the message is back, or after a minute.
        std::atomic<bool> found{false};
        std::thread deadline{[&found] {
            for (int i = 0; i < 600 && !found; ++i) std::this_thread::sleep_for(std::chrono::milliseconds{100});
            sc::kafka::stop();
        }};
        consumer.consume([&](const sc::kafka_message &message) {
            if (message.key == key && message.data == R"({"hello":"kafka"})") {
                found = true;
                sc::kafka::stop();
            }
        });
        deadline.join();
        CHECK(found.load());
    } else {
        std::cout << "   (SC_KAFKA_TEST_TOPIC isn't set: no round trip through the broker)" << std::endl;
    }

    TEST_SUMMARY();
}
