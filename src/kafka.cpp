#include "kafka.h"

#include <kafka/KafkaConsumer.h>
#include <kafka/KafkaProducer.h>

#include <atomic>
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
    // SIGINT/SIGTERM (or kafka::stop()) ends them all. The handlers are installed while any
    // consumer runs, by the first to start, and restored by the last to finish.
    std::atomic<bool> stop_loop{false};
    static_assert(std::atomic<bool>::is_always_lock_free, "set from a signal handler");

    void interrupt_handler(int) {
        stop_loop.store(true, std::memory_order_relaxed);
    }

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

namespace sc {
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

    kafka::kafka(ip_endpoints brokers) : brokers_(checked_brokers(std::move(brokers))),
                                         client_id("simply-cpp-kafka"),
                                         group_id("simply-cpp-kafka") {
    }

    void kafka::consume(const std::function<void(const kafka_message &)> &handler) {
        if (topics.empty()) {
            std::cerr << "No topics to consume..." << std::endl;
            return;
        }

        Properties props;
        props.put("bootstrap.servers", brokers_.to_string(","));
        props.put("auto.offset.reset", from_beginning ? "earliest" : "latest");
        props.put("client.id", client_id);
        props.put("group.id", group_id);
        props.put("enable.auto.commit", auto_commit ? "true" : "false");
        props.put("log_level", std::to_string(log_level));

        const consuming running;
        KafkaConsumer consumer(props);
        consumer.subscribe(topics);
        if (from_beginning) consumer.seekToBeginning();

        unsigned int counter = 0;
        while (!stop_loop.load(std::memory_order_relaxed) && (!max_records || counter < max_records)) {
            auto records = consumer.poll(ms(poll));
            if (records.empty()) {
                if (poll_at_end <= 0) break;
                records = consumer.poll(ms(poll_at_end));
            }

            for (const auto &record: records) {
                if (record.error()) {
                    std::cerr << "Message error: " << record.topic() << std::endl;
                } else {
#ifndef NDEBUG
                    for (const auto &header: record.headers()) std::cout << header.key << ": " << header.value.toString() << '\n';
#endif
                    try {
                        handler({record.topic(), text(record.key()), record.timestamp().msSinceEpoch, text(record.value())});
                    } catch (const std::exception &error) {
                        std::cerr << "Handler error: " << error.what() << std::endl;
                    }
                }
            }

            consumer.commitSync();
            counter += records.size();
        }

        consumer.close();
    }

    void kafka::stop() {
        stop_loop.store(true, std::memory_order_relaxed);
    }

    class kafka_producer::implementation {
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

    kafka_producer::kafka_producer(ip_endpoints brokers, std::string client_id)
        : brokers_(checked_brokers(std::move(brokers))),
          implementation_(std::make_unique<implementation>(brokers_, client_id)) {
    }

    kafka_producer::~kafka_producer() {
        if (!flush(std::chrono::seconds{10})) std::cerr << "Kafka: messages still queued at shutdown were dropped" << std::endl;
        implementation_->producer_.close(std::chrono::milliseconds{0}); // purges what's left, rather than waiting for it
    }

    void kafka_producer::send(const std::string &topic, const std::string &key, const std::string &value) {
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

    bool kafka_producer::flush(const std::chrono::milliseconds timeout) {
        return !implementation_->producer_.flush(timeout);
    }
} // namespace sc
