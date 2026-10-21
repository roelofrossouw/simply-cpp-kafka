#ifndef SC_KAFKA_H
#define SC_KAFKA_H
#include <ip_endpoints.h>

#include <atomic>
#include <chrono>
#include <cstdint>
#include <functional>
#include <memory>
#include <initializer_list>
#include <set>
#include <string>
#include <utility>
#include <vector>

namespace sc {
    struct kafka_message {
        std::string topic;
        std::string key;
        std::int64_t created;
        std::string data;
    };

    class kafka {
    public:
        // One or more brokers: "kafka1;kafka2:9093", a std::vector<ip_endpoint> or a braced list.
        // Entries without a port use 9092. Throws std::invalid_argument for an empty list or a
        // ',' in a host (separate brokers with ';', not librdkafka's ',').
        explicit kafka(ip_endpoints brokers);

        [[nodiscard]] const ip_endpoints &Brokers() const { return brokers_; }


        // Polls the topics and calls handler for each message, committing after each batch, until
        // MaxRecords() messages (when not 0), the topics stay quiet for PollAtEnd() (when it is 0
        // or less), SIGINT/SIGTERM, or stop(). Several kafka objects may consume at once, each on
        // its own thread; a signal or stop() ends them all.
        void consume(const std::function<void(const kafka_message &)> &handler);

        // Asks every consume() running in the process to finish after its current batch.
        static void stop();

        [[nodiscard]] int MaxRecords() const { return max_records; }
        void MaxRecords(const int value) { max_records = value; }

        [[nodiscard]] bool FromBeginning() const { return from_beginning; }
        void FromBeginning(const bool value) { from_beginning = value; }

        [[nodiscard]] int Poll() const { return poll; }
        void Poll(const int value) { poll = value; }

        [[nodiscard]] int PollAtEnd() const { return poll_at_end; }
        void PollAtEnd(const int value) { poll_at_end = value; }

        [[nodiscard]] const std::string &ClientId() const { return client_id; }
        void ClientId(std::string value) { client_id = std::move(value); }

        [[nodiscard]] const std::string &GroupId() const { return group_id; }
        void GroupId(std::string value) { group_id = std::move(value); }

        [[nodiscard]] bool AutoCommit() const { return auto_commit; }
        void AutoCommit(const bool value) { auto_commit = value; }

        [[nodiscard]] int LogLevel() const { return log_level; }
        void LogLevel(const int value) { log_level = value; }

        void AddTopic(const std::string &topic_name) { topics.emplace(topic_name); }
        void AddTopic(const std::vector<std::string> &topic_names) { topics.insert(topic_names.begin(), topic_names.end()); }
        // Keeps AddTopic({"a"}) unambiguous now that a vector is accepted too.
        void AddTopic(const std::initializer_list<std::string> topic_names) { topics.insert(topic_names); }

    private:
        ip_endpoints brokers_;
        int max_records = 0;
        bool from_beginning = false;
        int poll = 200;
        int poll_at_end = 5000;
        std::string client_id;
        std::string group_id;
        bool auto_commit = false;
        int log_level = 3;
        std::set<std::string> topics{};
    };

    // Sends messages to Kafka. send() queues a message and returns at once; it goes out in the
    // background, in batches. A delivery that fails later (no broker, or not delivered within 30
    // seconds) is counted in failed() and reported to std::cerr. The destructor waits up to 10
    // seconds for what is still queued, then drops it. Safe to share between threads.
    class kafka_producer {
    public:
        // Brokers as for sc::kafka (9092 without a port; separate them with ';').
        explicit kafka_producer(ip_endpoints brokers, std::string client_id = "simply-cpp-kafka");
        ~kafka_producer();

        kafka_producer(const kafka_producer &) = delete;
        kafka_producer &operator=(const kafka_producer &) = delete;

        // Throws std::runtime_error when Kafka refuses it at once: a full queue, or a topic the
        // broker doesn't have and won't create.
        void send(const std::string &topic, const std::string &key, const std::string &value);

        // Waits until every message sent so far is delivered or has failed; false on timeout.
        bool flush(std::chrono::milliseconds timeout = std::chrono::seconds{10});

        [[nodiscard]] std::uint64_t delivered() const { return delivered_->load(); }
        [[nodiscard]] std::uint64_t failed() const { return failed_->load(); }
        [[nodiscard]] const ip_endpoints &Brokers() const { return brokers_; }

    private:
        class implementation;
        ip_endpoints brokers_;
        std::shared_ptr<std::atomic<std::uint64_t>> delivered_ = std::make_shared<std::atomic<std::uint64_t>>(0);
        std::shared_ptr<std::atomic<std::uint64_t>> failed_ = std::make_shared<std::atomic<std::uint64_t>>(0);
        std::unique_ptr<implementation> implementation_;
    };
} // sc

#endif //SC_KAFKA_H
