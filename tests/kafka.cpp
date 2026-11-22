#include <sc/kafka/kafka.h>
#include "../src/pending_offsets.h"
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
    sc::kafka::consumer consumer{"localhost:9092"};

    SECTION("Brokers");
    CHECK_EQ(consumer.brokers().to_string(), "localhost:9092");
    const sc::kafka::consumer several{"kafka1;kafka2:9093;[::1]"};
    CHECK_EQ(several.brokers().to_string(","), "kafka1:9092,kafka2:9093,[::1]:9092");
    const sc::kafka::consumer listed{{{"kafka1", 9094}, {"kafka2", 0}}};
    CHECK_EQ(listed.brokers().to_string(), "kafka1:9094;kafka2:9092");
    CHECK_THROWS_AS(sc::kafka::consumer{""}, std::invalid_argument);
    CHECK_THROWS_AS(sc::kafka::consumer{"kafka1,kafka2"}, std::invalid_argument);
    CHECK_THROWS_AS(sc::kafka::consumer{"kafka1:nope"}, std::invalid_argument);

    SECTION("Default consumer configuration");
    CHECK_EQ(consumer.max_records(), 0);
    CHECK_EQ(consumer.from_beginning(), false);
    CHECK_EQ(consumer.poll().count(), 200);
    CHECK_EQ(consumer.poll_at_end().count(), 5000);
    CHECK_EQ(consumer.client_id(), "simply-cpp-kafka");
    CHECK_EQ(consumer.group_id(), "simply-cpp-kafka");
    CHECK_EQ(consumer.auto_commit(), false);
    CHECK(consumer.log_level() == sc::log_level::error);
    CHECK(!consumer.before_commit()); // nothing to wait for
    CHECK_EQ(sc::kafka::consumer::rebalances(), 0u);
    CHECK_EQ(sc::kafka::consumer::failed_commits(), 0u);
    CHECK_EQ(sc::kafka::consumer::subscribe_waits(), 0u);
    CHECK_EQ(sc::kafka::consumer::cut_batches(), 0u);
    CHECK_EQ(consumer.assignment(), ""); // librdkafka's default (eager)
    CHECK_EQ(consumer.max_batch().count(), 0);
    CHECK_EQ(consumer.subscribe_wait().count(), 5000);
    {
        int waited = 0;
        consumer.before_commit([&waited] { ++waited; });
        consumer.before_commit()();
        CHECK_EQ(waited, 1);
        consumer.before_commit({});
        CHECK(!consumer.before_commit());
    }

    SECTION("Consumer configuration");
    consumer.max_records(25);
    consumer.from_beginning(true);
    consumer.poll(std::chrono::milliseconds{100});
    consumer.poll_at_end(std::chrono::milliseconds{750});
    consumer.client_id("streamvms-client");
    consumer.group_id("streamvms-consumers");
    consumer.auto_commit(true);
    consumer.log_level(sc::log_level::info);
    consumer.assignment("cooperative-sticky");
    consumer.max_batch(std::chrono::seconds{10});
    consumer.subscribe_wait(std::chrono::seconds{2});

    CHECK_EQ(consumer.assignment(), "cooperative-sticky");
    CHECK_EQ(consumer.max_batch().count(), 10000);
    CHECK_EQ(consumer.subscribe_wait().count(), 2000);
    CHECK_EQ(consumer.max_records(), 25);
    CHECK_EQ(consumer.from_beginning(), true);
    CHECK_EQ(consumer.poll().count(), 100);
    CHECK_EQ(consumer.poll_at_end().count(), 750);
    CHECK_EQ(consumer.client_id(), "streamvms-client");
    CHECK_EQ(consumer.group_id(), "streamvms-consumers");
    CHECK_EQ(consumer.auto_commit(), true);
    CHECK(consumer.log_level() == sc::log_level::info);

    SECTION("No-topic consumption does not contact Kafka");
    CHECK_NOTHROW(consumer.consume([](const sc::kafka::message &) {}));

    SECTION("Adding an empty topic list adds no topics");
    consumer.add_topic(std::vector<std::string>{});
    CHECK_NOTHROW(consumer.consume([](const sc::kafka::message &) {}));

    SECTION("Topics can be added one at a time, as a vector or as a braced list");
    // The topic set is private, so this checks that every call form compiles and resolves.
    sc::kafka::consumer topics{"localhost:9092"};
    topics.add_topic("one");
    topics.add_topic(std::vector<std::string>{"two", "three"});
    topics.add_topic({"four"});
    topics.add_topic({"five", "six"});
    CHECK_NOTHROW(topics.add_topic("one"));

    SECTION("A cut batch commits what was handled and reads the rest again");
    {
        using key = std::pair<std::string, int>;
        const std::vector<sc::kafka::batch_record> records{
            {"video", 0, 10}, {"video", 1, 7}, {"video", 0, 11}, {"video", 1, 8}, {"video", 0, 12}, {"photo", 3, 40}};
        const auto all = sc::kafka::cut_batch(records, records.size());
        CHECK(all.seek.empty());
        CHECK_EQ(all.commit.size(), 3u);
        CHECK_EQ(all.commit.at(key{"video", 0}), 13);
        CHECK_EQ(all.commit.at(key{"video", 1}), 9);
        CHECK_EQ(all.commit.at(key{"photo", 3}), 41);

        const auto three = sc::kafka::cut_batch(records, 3); // video-0 10 and 11, video-1 7
        CHECK_EQ(three.commit.size(), 2u);
        CHECK_EQ(three.commit.at(key{"video", 0}), 12);
        CHECK_EQ(three.commit.at(key{"video", 1}), 8);
        CHECK_EQ(three.seek.size(), 3u);
        CHECK_EQ(three.seek.at(key{"video", 0}), 12);
        CHECK_EQ(three.seek.at(key{"video", 1}), 8);
        CHECK_EQ(three.seek.at(key{"photo", 3}), 40); // nothing of it handled: not committed, read again

        const auto none = sc::kafka::cut_batch(records, 0);
        CHECK(none.commit.empty());
        CHECK_EQ(none.seek.size(), 3u);
        CHECK_EQ(none.seek.at(key{"video", 0}), 10);

        CHECK(sc::kafka::cut_batch({}, 0).commit.empty());
    }

    SECTION("Offsets a rebalance put off go out later, never going back");
    {
        using key = std::pair<std::string, int>;
        sc::kafka::detail::offsets pending{{key{"loc", 0}, 50}, {key{"loc", 1}, 70}};
        // The next batch: loc-0 moved on, loc-2 is new; loc-1 keeps its pending offset.
        sc::kafka::detail::offsets next = pending;
        sc::kafka::detail::merge(next, {{key{"loc", 0}, 55}, {key{"loc", 2}, 9}});
        CHECK_EQ(next.size(), 3u);
        CHECK_EQ(next.at(key{"loc", 0}), 55);
        CHECK_EQ(next.at(key{"loc", 1}), 70);
        CHECK_EQ(next.at(key{"loc", 2}), 9);
        // An older offset never replaces a newer one.
        sc::kafka::detail::merge(next, {{key{"loc", 0}, 40}});
        CHECK_EQ(next.at(key{"loc", 0}), 55);

        // A revoke takes its partitions out, to commit before giving them up.
        const auto revoked = sc::kafka::detail::take(next, {key{"loc", 1}, key{"loc", 7}});
        CHECK_EQ(revoked.size(), 1u);
        CHECK_EQ(revoked.at(key{"loc", 1}), 70);
        CHECK_EQ(next.size(), 2u);

        // Partitions no longer assigned (lost) are dropped from what is retried.
        const auto gone = sc::kafka::detail::take_unassigned(next, {key{"loc", 2}});
        CHECK_EQ(gone.size(), 1u);
        CHECK(gone.contains(key{"loc", 0}));
        CHECK_EQ(next.size(), 1u);
        CHECK_EQ(next.at(key{"loc", 2}), 9);
    }

    SECTION("Consumers on their own threads share the signal handling");
    {
        // No broker answers on port 1, so each consume() waits for partitions that never come
        // (subscribe_wait(), then in poll()) until stop(). While they wait, SIGTERM (systemctl
        // stop) must be caught - asking them all to stop - not the default kill; once the last
        // has finished, the default is back. Waiting past subscribe_wait() is counted, not thrown.
        std::vector<std::thread> threads;
        std::atomic<int> finished{0};
        for (const auto *topic: {"first", "second"}) {
            threads.emplace_back([topic, &finished] {
                sc::kafka::consumer quiet{"127.0.0.1:1"};
                quiet.subscribe_wait(std::chrono::seconds{1});
                quiet.add_topic(topic);
                try {
                    quiet.consume([](const sc::kafka::message &) {});
                } catch (const std::exception &) {
                    // the subscription timing out
                }
                ++finished;
            });
        }
        std::this_thread::sleep_for(std::chrono::seconds{2});
        std::raise(SIGTERM); // still running afterwards: the consumers' handler took it
        sc::kafka::consumer::stop();
        for (auto &thread: threads) thread.join();
        CHECK_EQ(finished.load(), 2);
        CHECK_EQ(sc::kafka::consumer::subscribe_waits(), 2u); // waited on in poll(), not given up
        const auto handler = std::signal(SIGTERM, SIG_DFL);
        CHECK(handler == SIG_DFL);
    }

    SECTION("A producer checks its brokers as a consumer does");
    {
        CHECK_THROWS_AS(sc::kafka::producer{""}, std::invalid_argument);
        CHECK_THROWS_AS(sc::kafka::producer{"kafka1,kafka2"}, std::invalid_argument);
        const sc::kafka::producer producer{"kafka1;kafka2:9093"};
        CHECK_EQ(producer.brokers().to_string(), "kafka1:9092;kafka2:9093");
    }

    SECTION("A producer with no broker: send() queues, flush() times out, nothing is delivered");
    {
        sc::kafka::producer producer{"127.0.0.1:1", "sc-kafka-test"};
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
            sc::kafka::producer producer{brokers ? brokers : "localhost:9092", "sc-kafka-test"};
            producer.send(topic, key, R"({"hello":"kafka"})");
            CHECK(producer.flush(std::chrono::seconds{15}));
            CHECK_EQ(producer.delivered(), std::uint64_t{1});
            CHECK_EQ(producer.failed(), std::uint64_t{0});
        }
        sc::kafka::consumer consumer{brokers ? brokers : "localhost:9092"};
        consumer.group_id("sc-kafka-test-" + key);
        consumer.from_beginning(true);
        consumer.add_topic(topic);
        // consume() keeps polling (a new group's first polls are empty until its partitions are
        // assigned): stop it once the message is back, or after a minute.
        std::atomic<bool> found{false};
        std::thread deadline{[&found] {
            for (int i = 0; i < 600 && !found; ++i) std::this_thread::sleep_for(std::chrono::milliseconds{100});
            sc::kafka::consumer::stop();
        }};
        consumer.consume([&](const sc::kafka::message &message) {
            if (message.key == key && message.data == R"({"hello":"kafka"})") {
                found = true;
                sc::kafka::consumer::stop();
            }
        });
        deadline.join();
        CHECK(found.load());
    } else {
        std::cout << "   (SC_KAFKA_TEST_TOPIC isn't set: no round trip through the broker)" << std::endl;
    }

    TEST_SUMMARY();
}
