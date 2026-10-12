#include <kafka.h>
#include <sc_test.h>

#include <atomic>
#include <chrono>
#include <csignal>
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

    TEST_SUMMARY();
}
