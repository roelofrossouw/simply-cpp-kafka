#include <sc/kafka/kafka.h>

#include <kafka/KafkaConsumer.h>
#include <kafka/KafkaProducer.h>

#include <atomic>
#include <algorithm>
#include <chrono>
#include <csignal>
#include <exception>
#include <iostream>
#include <mutex>
#include <stdexcept>

using namespace kafka;
using namespace kafka::clients::consumer;
using kafka::clients::producer::KafkaProducer;
using kafka::clients::producer::ProducerRecord;

using ms = std::chrono::milliseconds;

namespace {
    // Shared by every consume() in the process, so several consumers can run on their own threads:
    // SIGINT/SIGTERM (or consumer::stop()) ends them all. The handlers are installed while any
    // consumer runs, by the first to start, and restored by the last to finish.
    std::atomic<bool> stop_loop{false};
    static_assert(std::atomic<bool>::is_always_lock_free, "set from a signal handler");

    void interrupt_handler(int) {
        stop_loop.store(true, std::memory_order_relaxed);
    }

    std::atomic<std::uint64_t> rebalance_count{0}, failed_commit_count{0}, subscribe_wait_count{0}, cut_batch_count{0};
    std::atomic<std::uint64_t> assignment_wait_ms{0};

    std::mutex consumers_mutex;
    int running_consumers = 0;
    void (*previous_sigint)(int) = SIG_DFL;
    void (*previous_sigterm)(int) = SIG_DFL;

    struct consuming {
        consuming() {
            const std::lock_guard lock{consumers_mutex};
            if (running_consumers++ == 0) {
                stop_loop.store(false); // a fresh start once every earlier consumer has finished
                previous_sigint = signal(SIGINT, interrupt_handler);
                previous_sigterm = signal(SIGTERM, interrupt_handler);
            }
        }

        ~consuming() {
            const std::lock_guard lock{consumers_mutex};
            if (--running_consumers == 0) {
                signal(SIGINT, previous_sigint);
                signal(SIGTERM, previous_sigterm);
            }
        }

        consuming(const consuming &) = delete;
        consuming &operator=(const consuming &) = delete;
    };
}

namespace sc::kafka {
    namespace {
        std::string text(const ConstBuffer data) { return {static_cast<const char *>(data.data()), data.size()}; }
    }

    namespace {
        ip_endpoints checked_brokers(ip_endpoints brokers) {
            if (brokers.empty()) throw std::invalid_argument{"kafka needs at least one broker"};
            for (auto &broker: brokers) {
                if (broker.host.find(',') != std::string::npos) {
                    throw std::invalid_argument{"kafka broker '" + broker.host + "' contains ','; separate brokers with ';'"};
                }
                if (broker.port == 0) broker.port = 9092;
            }
            return brokers;
        }
    }

    consumer::consumer(ip_endpoints brokers) : brokers_(checked_brokers(std::move(brokers))),
                                         client_id_("simply-cpp-kafka"),
                                         group_id_("simply-cpp-kafka") {
    }

    batch_cut cut_batch(const std::vector<batch_record> &records, const std::size_t handled) {
        batch_cut cut;
        for (std::size_t i = 0; i < records.size(); ++i) {
            const auto &record = records[i];
            const std::pair key{record.topic, record.partition};
            if (i < handled) {
                auto &next = cut.commit[key];
                next = std::max(next, record.offset + 1);
            } else if (!cut.seek.contains(key)) {
                cut.seek[key] = record.offset; // the first one not handled: a partition's records come in order
            }
        }
        return cut;
    }

    void consumer::consume(const std::function<void(const message &)> &handler) {
        if (topics_.empty()) {
            std::cerr << "No topics to consume..." << std::endl;
            return;
        }

        Properties props;
        props.put("bootstrap.servers", brokers_.to_string(","));
        props.put("auto.offset.reset", from_beginning_ ? "earliest" : "latest");
        props.put("client.id", client_id_);
        props.put("group.id", group_id_);
        props.put("enable.auto.commit", auto_commit_ ? "true" : "false");
        props.put("log_level", std::to_string(static_cast<int>(log_level_)));
        if (!assignment_.empty()) props.put("partition.assignment.strategy", assignment_);

        const consuming running;
        KafkaConsumer consumer(props);

        // The first assignment can take long: a rebalance waits for every member of the group to
        // rejoin, and an eager member only does once its current batch is done. librdkafka's
        // subscription stands either way; the assignment then arrives in a later poll().
        const auto subscribed_at = std::chrono::steady_clock::now();
        bool assigned = false, waiting = false;
        const auto note_assigned = [&] {
            if (assigned) return;
            assigned = true;
            const auto waited = std::chrono::duration_cast<ms>(std::chrono::steady_clock::now() - subscribed_at);
            assignment_wait_ms.fetch_add(static_cast<std::uint64_t>(waited.count()), std::memory_order_relaxed);
            if (waiting) {
                std::cout << "Kafka " << client_id_ << ": partitions assigned after " << waited.count() / 1000.0
                          << " s" << std::endl;
            }
        };
        try {
            consumer.subscribe(topics_, [this, &note_assigned](const RebalanceEventType type, const TopicPartitions &partitions) {
                rebalance_count.fetch_add(1, std::memory_order_relaxed);
                if (type == RebalanceEventType::PartitionsAssigned) note_assigned();
                std::string listed;
                for (const auto &[topic, partition]: partitions) listed += (listed.empty() ? "" : ", ") + topic + "-" + std::to_string(partition);
                std::cout << "Kafka " << client_id_ << ": partitions "
                          << (type == RebalanceEventType::PartitionsAssigned ? "assigned: " : "revoked: ")
                          << (listed.empty() ? "none" : listed) << std::endl;
            }, subscribe_wait_);
        } catch (const KafkaException &error) {
            if (error.error().value() != RD_KAFKA_RESP_ERR__TIMED_OUT) throw;
            waiting = true;
            subscribe_wait_count.fetch_add(1, std::memory_order_relaxed);
            std::cerr << "Kafka " << client_id_ << ": no partitions yet after " << subscribe_wait_.count() / 1000.0
                      << " s (the group is rebalancing, waiting for its other members); still waiting" << std::endl;
        }
        bool seek_pending = from_beginning_;
        if (seek_pending && !consumer.assignment().empty()) {
            consumer.seekToBeginning();
            seek_pending = false;
        }

        // Commits a batch's handled records (once before_commit() says their work is done), by
        // explicit offsets: what poll() stored would also cover records a cut batch didn't handle.
        // A failed commit isn't fatal: those messages come again after the rebalance.
        const auto commit = [this, &consumer](const batch_cut &cut) {
            if (cut.commit.empty()) return;
            if (before_commit_) before_commit_();
            TopicPartitionOffsets offsets;
            for (const auto &[partition, offset]: cut.commit) offsets[TopicPartition{partition.first, partition.second}] = offset;
            try {
                consumer.commitSync(offsets);
            } catch (const std::exception &error) {
                failed_commit_count.fetch_add(1, std::memory_order_relaxed);
                std::cerr << "Kafka " << client_id_ << ": commit failed, the batch may come again: " << error.what() << std::endl;
            }
        };
        const bool may_cut = !auto_commit_;

        unsigned int counter = 0;
        while (!stop_loop.load(std::memory_order_relaxed) && (!max_records_ || counter < static_cast<unsigned>(max_records_))) {
            auto records = consumer.poll(poll_);
            if (records.empty()) {
                if (poll_at_end_.count() <= 0) break;
                records = consumer.poll(poll_at_end_);
            }
            if (seek_pending && !consumer.assignment().empty()) {
                consumer.seekToBeginning(); // what this poll read comes again from the beginning
                seek_pending = false;
                continue;
            }

            const auto started = std::chrono::steady_clock::now();
            std::size_t handled = 0;
            for (const auto &record: records) {
                if (record.error()) {
                    std::cerr << "Message error: " << record.topic() << std::endl;
                } else {
#ifndef NDEBUG
                    for (const auto &header: record.headers()) std::cout << header.key << ": " << header.value.toString() << '\n';
#endif
                    try {
                        handler({record.topic(), text(record.key()), record.timestamp().msSinceEpoch, text(record.value()), record.partition(), record.offset()});
                    } catch (const abandon &) {
                        if (may_cut) break; // this one and the rest come again
                    } catch (const std::exception &error) {
                        std::cerr << "Handler error: " << error.what() << std::endl;
                    }
                }
                ++handled;
                if (may_cut && handled < records.size() &&
                    (stop_loop.load(std::memory_order_relaxed) ||
                     (max_batch_.count() > 0 && std::chrono::steady_clock::now() - started >= max_batch_))) {
                    break;
                }
            }

            if (!records.empty()) {
                std::vector<batch_record> positions;
                positions.reserve(records.size());
                for (const auto &record: records) positions.push_back({record.topic(), record.partition(), record.offset()});
                const auto cut = cut_batch(positions, handled);
                commit(cut);
                if (!cut.seek.empty()) {
                    cut_batch_count.fetch_add(1, std::memory_order_relaxed);
                    if (!stop_loop.load(std::memory_order_relaxed)) {
                        for (const auto &[partition, offset]: cut.seek) {
                            try {
                                consumer.seek(TopicPartition{partition.first, partition.second}, offset);
                            } catch (const std::exception &error) {
                                // Revoked meanwhile, say: its new owner starts at the committed offset.
                                std::cerr << "Kafka " << client_id_ << ": can't go back to " << partition.first << "-"
                                          << partition.second << " offset " << offset << ": " << error.what() << std::endl;
                            }
                        }
                    }
                }
            }
            counter += handled;
        }

        consumer.close();
    }

    void consumer::stop() {
        stop_loop.store(true, std::memory_order_relaxed);
    }

    bool consumer::stopping() {
        return stop_loop.load(std::memory_order_relaxed);
    }

    std::uint64_t consumer::rebalances() {
        return rebalance_count.load(std::memory_order_relaxed);
    }

    std::uint64_t consumer::failed_commits() {
        return failed_commit_count.load(std::memory_order_relaxed);
    }

    std::uint64_t consumer::subscribe_waits() {
        return subscribe_wait_count.load(std::memory_order_relaxed);
    }

    double consumer::assignment_wait_seconds() {
        return static_cast<double>(assignment_wait_ms.load(std::memory_order_relaxed)) / 1000.0;
    }

    std::uint64_t consumer::cut_batches() {
        return cut_batch_count.load(std::memory_order_relaxed);
    }

    class producer::implementation {
    public:
        implementation(const ip_endpoints &brokers, const std::string &client_id)
            : producer_(properties(brokers, client_id)) {
        }

        KafkaProducer producer_;

    private:
        static Properties properties(const ip_endpoints &brokers, const std::string &client_id) {
            Properties props;
            props.put("bootstrap.servers", brokers.to_string(","));
            props.put("client.id", client_id);
            props.put("linger.ms", "5");             // small batches: positions and alarms should go out at once
            props.put("enable.idempotence", "true"); // no duplicates from the producer's own retries
            props.put("message.timeout.ms", "30000"); // not librdkafka's 5 minutes: late data is stale data
            return props;
        }
    };

    producer::producer(ip_endpoints brokers, std::string client_id)
        : brokers_(checked_brokers(std::move(brokers))),
          implementation_(std::make_unique<implementation>(brokers_, client_id)) {
    }

    producer::~producer() {
        if (!flush(std::chrono::seconds{10})) std::cerr << "Kafka: messages still queued at shutdown were dropped" << std::endl;
        implementation_->producer_.close(std::chrono::milliseconds{0}); // purges what's left, rather than waiting for it
    }

    void producer::send(const std::string &topic, const std::string &key, const std::string &value) {
        const ProducerRecord record{topic, Key{key.data(), key.size()}, Value{value.data(), value.size()}};
        try {
            implementation_->producer_.send(
                record,
                [delivered = delivered_, failed = failed_, topic](const ::kafka::clients::producer::RecordMetadata &,
                                                                  const Error &error) {
                    if (error) {
                        ++*failed;
                        std::cerr << "Kafka: delivery to " << topic << " failed: " << error.message() << std::endl;
                    } else {
                        ++*delivered;
                    }
                },
                KafkaProducer::SendOption::ToCopyRecordValue, KafkaProducer::ActionWhileQueueIsFull::NoBlock);
        } catch (const KafkaException &error) {
            throw std::runtime_error("Kafka: can't send to " + topic + ": " + error.what());
        }
    }

    bool producer::flush(const std::chrono::milliseconds timeout) {
        return !implementation_->producer_.flush(timeout);
    }
} // namespace sc::kafka
