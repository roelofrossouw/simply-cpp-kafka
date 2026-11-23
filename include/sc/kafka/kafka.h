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
#include <map>
#include <set>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace sc::kafka {
    struct message {
        std::string topic;
        std::string key;
        std::int64_t created;
        std::string data;
        int partition = -1;       // since 5.0.0
        std::int64_t offset = -1; // since 5.0.0
    };

    // Thrown by a consume() handler to leave its message unhandled (a service stopping mid-way
    // through a long job, say): the batch ends there, what was handled before it is committed, and
    // this message and the rest come again. Since 5.0.0.
    struct abandon : std::runtime_error {
        abandon() : std::runtime_error{"message abandoned"} {}
        using std::runtime_error::runtime_error;
    };

    // How a client reaches its brokers (librdkafka's security.protocol). Since 5.1.0.
    enum class security_protocol { plaintext, ssl, sasl_plaintext, sasl_ssl };
    // How a SASL client signs in (librdkafka's sasl.mechanism). Since 5.1.0.
    enum class sasl_mechanism { scram_sha_256, scram_sha_512, plain };

    // Their names as librdkafka and config files spell them ("sasl_ssl", "SCRAM-SHA-512"); the
    // parsers ignore case and '-' versus '_', and throw std::invalid_argument for anything else.
    [[nodiscard]] std::string to_string(security_protocol value);
    [[nodiscard]] std::string to_string(sasl_mechanism value);
    [[nodiscard]] security_protocol security_protocol_from(std::string_view name);
    [[nodiscard]] sasl_mechanism sasl_mechanism_from(std::string_view name);

    // Signing in to the brokers, for a consumer or a producer: plaintext (the default, no
    // sign-in), TLS, SASL (SCRAM or PLAIN) or both. Since 5.1.0.
    struct security {
        security_protocol protocol = security_protocol::plaintext;
        sasl_mechanism mechanism = sasl_mechanism::scram_sha_512; // with sasl_plaintext / sasl_ssl
        std::string username;                                     // with sasl_plaintext / sasl_ssl
        std::string password;                                     // never logged; see describe()
        std::string ca_location; // a CA file (PEM) for ssl / sasl_ssl; empty: the system's CAs

        [[nodiscard]] bool uses_sasl() const {
            return protocol == security_protocol::sasl_plaintext || protocol == security_protocol::sasl_ssl;
        }
        [[nodiscard]] bool uses_tls() const {
            return protocol == security_protocol::ssl || protocol == security_protocol::sasl_ssl;
        }

        // The librdkafka properties for these settings (none for plaintext). Throws
        // std::invalid_argument when SASL has no username or password.
        [[nodiscard]] std::map<std::string, std::string> properties() const;
        // For logs and status pages: "sasl_ssl SCRAM-SHA-512 as streamvms, CA /etc/x.pem";
        // never the password.
        [[nodiscard]] std::string describe() const;

        // From a configuration's text: protocol and mechanism as security_protocol_from() and
        // sasl_mechanism_from() read them (empty: plaintext, SCRAM-SHA-512). Throws
        // std::invalid_argument as they and properties() do.
        [[nodiscard]] static security from(std::string_view protocol, std::string_view mechanism, std::string username,
                                           std::string password, std::string ca_location = {});
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
        // A commit refused while the group rebalances (a consumer joining or leaving) is tried
        // again: with the next commit once the rebalance is over, and for partitions this consumer
        // gives up, from the revoke callback before their new owner starts (since 5.0.1). Offsets
        // that still can't be committed are reported and counted (failed_commits()), not thrown:
        // those messages come again to whichever consumer gets their partition, so a handler must
        // take a message twice. Rebalances are reported on std::cout, with the partitions, and
        // counted (rebalances()).
        void consume(const std::function<void(const message &)> &handler);

        // Asks every consume() running in the process to finish: after the message being
        // handled (the rest of the batch comes again; see abandon), then it leaves the group.
        static void stop();
        // Whether stop() (or SIGINT/SIGTERM) asked them to: for a long handler to give up early.
        [[nodiscard]] static bool stopping();

        // Called before each commit, on the consuming thread: for a handler that only queues its
        // work, to wait until it's done, so a committed offset means the batch's work is done.
        [[nodiscard]] const std::function<void()> &before_commit() const { return before_commit_; }
        void before_commit(std::function<void()> wait) { before_commit_ = std::move(wait); }

        // Since the process started, over every consumer in it.
        [[nodiscard]] static std::uint64_t rebalances();
        // Commits given up on (since 5.0.1 not the ones a rebalance only put off): their
        // messages may be handled again.
        [[nodiscard]] static std::uint64_t failed_commits();
        // Consumers that had no partitions yet when subscribing took longer than subscribe_wait()
        // (the group rebalancing, waiting for its other members), and kept waiting in poll()
        // rather than giving up; and the seconds they waited, from subscribing to their first
        // assignment.
        [[nodiscard]] static std::uint64_t subscribe_waits();
        [[nodiscard]] static double assignment_wait_seconds();
        // Batches ended early by max_batch() (or stop()), the rest read again by the next poll.
        [[nodiscard]] static std::uint64_t cut_batches();

        // librdkafka's partition.assignment.strategy; empty for its default (range,roundrobin:
        // eager, every member gives up all its partitions at each rebalance and waits for the
        // slowest to finish its batch). "cooperative-sticky" moves only the partitions that change
        // owner, so a member busy with a long batch holds up only those. Every member of a group
        // must use the same kind (eager or cooperative): switch them all at once.
        [[nodiscard]] const std::string &assignment() const { return assignment_; }
        void assignment(std::string value) { assignment_ = std::move(value); }

        // Ends a batch early once it has taken this long (0, the default: never): what was handled
        // is committed and the rest is read again by the next poll, so a slow handler (seconds a
        // message) still polls often enough that a rebalance doesn't wait long for it. Ignored
        // with auto_commit(), which would commit the rest unhandled.
        [[nodiscard]] std::chrono::milliseconds max_batch() const { return max_batch_; }
        void max_batch(const std::chrono::milliseconds value) { max_batch_ = value; }

        // How long subscribing waits for the first assignment before carrying on in poll(),
        // where the assignment arrives once the group has rebalanced.
        [[nodiscard]] std::chrono::milliseconds subscribe_wait() const { return subscribe_wait_; }
        void subscribe_wait(const std::chrono::milliseconds value) { subscribe_wait_ = value; }

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

        // Signing in to the brokers (plaintext by default). Since 5.1.0.
        [[nodiscard]] const kafka::security &security() const { return security_; }
        void security(kafka::security value) { security_ = std::move(value); }

        [[nodiscard]] const std::set<std::string> &topics() const { return topics_; }

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
        std::string assignment_;
        std::chrono::milliseconds max_batch_{0};
        std::chrono::milliseconds subscribe_wait_{5000};
        kafka::security security_;
    };

    // Where a batch cut after its first `handled` records leaves each partition: the offsets to
    // commit (one past the last handled record of a partition) and the offsets to read again from
    // (the first record not handled). For consumer::max_batch(); public for its tests.
    struct batch_record {
        std::string topic;
        int partition;
        std::int64_t offset;
    };
    struct batch_cut {
        std::map<std::pair<std::string, int>, std::int64_t> commit;
        std::map<std::pair<std::string, int>, std::int64_t> seek;
    };
    batch_cut cut_batch(const std::vector<batch_record> &records, std::size_t handled);

    // Sends messages to Kafka. send() queues a message and returns at once; it goes out in the
    // background, in batches. A delivery that fails later (no broker, or not delivered within 30
    // seconds) is counted in failed() and reported to std::cerr. The destructor waits up to 10
    // seconds for what is still queued, then drops it. Safe to share between threads.
    class producer {
    public:
        // Brokers as for sc::kafka::consumer (9092 without a port; separate them with ';').
        explicit producer(ip_endpoints brokers, std::string client_id = "simply-cpp-kafka");
        // Signing in as security says (since 5.1.0).
        producer(ip_endpoints brokers, std::string client_id, const kafka::security &security);
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
