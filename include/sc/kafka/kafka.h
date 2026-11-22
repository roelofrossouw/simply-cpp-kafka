#ifndef SC_KAFKA_H
#define SC_KAFKA_H
#include <sc/core/ip_endpoints.h>
#include <sc/core/log_level.h>

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

namespace sc::kafka {
    struct message {
        std::string topic;
        std::string key;
        std::int64_t created;
        std::string data;
    };

    // Reads topics (sc::kafka until 4.0.0).
    class consumer {
    public:
        // One or more brokers: "kafka1;kafka2:9093", a std::vector<ip_endpoint> or a braced list.
        // Entries without a port use 9092. Throws std::invalid_argument for an empty list or a
        // ',' in a host (separate brokers with ';', not librdkafka's ',').
        explicit consumer(ip_endpoints brokers);

        [[nodiscard]] const ip_endpoints &brokers() const { return brokers_; }


        // Polls the topics and calls handler for each message, committing after each batch, until
        // max_records() messages (when not 0), the topics stay quiet for poll_at_end() (when it is
        // 0 or less), SIGINT/SIGTERM, or stop(). Several consumers may consume at once, each on
        // its own thread; a signal or stop() ends them all.
        //
        // A commit that fails (the group rebalancing as a consumer joins or leaves, say) is reported
        // and counted (failed_commits()), not thrown: those messages come again to whichever
        // consumer gets their partition, so a handler must take a message twice. Rebalances are
        // reported on std::cout, with the partitions, and counted (rebalances()).
        void consume(const std::function<void(const message &)> &handler);

        // Asks every consume() running in the process to finish after its current batch.
        static void stop();

        // Called before each commit, on the consuming thread: for a handler that only queues its
        // work, to wait until it's done, so a committed offset means the batch's work is done.
        [[nodiscard]] const std::function<void()> &before_commit() const { return before_commit_; }
        void before_commit(std::function<void()> wait) { before_commit_ = std::move(wait); }

        // Since the process started, over every consumer in it.
        [[nodiscard]] static std::uint64_t rebalances();
        [[nodiscard]] static std::uint64_t failed_commits();

        [[nodiscard]] int max_records() const { return max_records_; }
        void max_records(const int value) { max_records_ = value; }

        [[nodiscard]] bool from_beginning() const { return from_beginning_; }
        void from_beginning(const bool value) { from_beginning_ = value; }

        [[nodiscard]] std::chrono::milliseconds poll() const { return poll_; }
        void poll(const std::chrono::milliseconds value) { poll_ = value; }

        [[nodiscard]] std::chrono::milliseconds poll_at_end() const { return poll_at_end_; }
        void poll_at_end(const std::chrono::milliseconds value) { poll_at_end_ = value; }

        [[nodiscard]] const std::string &client_id() const { return client_id_; }
        void client_id(std::string value) { client_id_ = std::move(value); }

        [[nodiscard]] const std::string &group_id() const { return group_id_; }
        void group_id(std::string value) { group_id_ = std::move(value); }

        [[nodiscard]] bool auto_commit() const { return auto_commit_; }
        void auto_commit(const bool value) { auto_commit_ = value; }

        // librdkafka's own messages, as syslog levels (sc::log_level::error by default).
        [[nodiscard]] sc::log_level log_level() const { return log_level_; }
        void log_level(const sc::log_level value) { log_level_ = value; }

        void add_topic(const std::string &topic_name) { topics_.emplace(topic_name); }
        void add_topic(const std::vector<std::string> &topic_names) { topics_.insert(topic_names.begin(), topic_names.end()); }
        // Keeps add_topic({"a"}) unambiguous now that a vector is accepted too.
        void add_topic(const std::initializer_list<std::string> topic_names) { topics_.insert(topic_names); }

    private:
        ip_endpoints brokers_;
        int max_records_ = 0;
        bool from_beginning_ = false;
        std::chrono::milliseconds poll_{200};
        std::chrono::milliseconds poll_at_end_{5000};
        std::string client_id_;
        std::string group_id_;
        bool auto_commit_ = false;
        sc::log_level log_level_ = sc::log_level::error;
        std::set<std::string> topics_{};
        std::function<void()> before_commit_;
    };

    // Sends messages to Kafka. send() queues a message and returns at once; it goes out in the
    // background, in batches. A delivery that fails later (no broker, or not delivered within 30
    // seconds) is counted in failed() and reported to std::cerr. The destructor waits up to 10
    // seconds for what is still queued, then drops it. Safe to share between threads.
    class producer {
    public:
        // Brokers as for sc::kafka::consumer (9092 without a port; separate them with ';').
        explicit producer(ip_endpoints brokers, std::string client_id = "simply-cpp-kafka");
        ~producer();

        producer(const producer &) = delete;
        producer &operator=(const producer &) = delete;

        // Throws std::runtime_error when Kafka refuses it at once: a full queue, or a topic the
        // broker doesn't have and won't create.
        void send(const std::string &topic, const std::string &key, const std::string &value);

        // Waits until every message sent so far is delivered or has failed; false on timeout.
        bool flush(std::chrono::milliseconds timeout = std::chrono::seconds{10});

        [[nodiscard]] std::uint64_t delivered() const { return delivered_->load(); }
        [[nodiscard]] std::uint64_t failed() const { return failed_->load(); }
        [[nodiscard]] const ip_endpoints &brokers() const { return brokers_; }

    private:
        class implementation;
        ip_endpoints brokers_;
        std::shared_ptr<std::atomic<std::uint64_t>> delivered_ = std::make_shared<std::atomic<std::uint64_t>>(0);
        std::shared_ptr<std::atomic<std::uint64_t>> failed_ = std::make_shared<std::atomic<std::uint64_t>>(0);
        std::unique_ptr<implementation> implementation_;
    };
} // sc::kafka

#endif //SC_KAFKA_H
