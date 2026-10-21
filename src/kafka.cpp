#include "kafka.h"

#include <kafka/KafkaConsumer.h>

#include <atomic>
#include <chrono>
#include <csignal>
#include <exception>
#include <iostream>
#include <mutex>
#include <stdexcept>

using namespace kafka;
using namespace kafka::clients::consumer;

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

    kafka::kafka(ip_endpoints brokers) : brokers_(std::move(brokers)),
                                         client_id("simply-cpp-kafka"),
                                         group_id("simply-cpp-kafka") {
        if (brokers_.empty()) throw std::invalid_argument{"kafka needs at least one broker"};
        for (auto &broker: brokers_) {
            if (broker.host.find(',') != std::string::npos) {
                throw std::invalid_argument{"kafka broker '" + broker.host + "' contains ','; separate brokers with ';'"};
            }
            if (broker.port == 0) broker.port = 9092;
        }
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
} // namespace sc
