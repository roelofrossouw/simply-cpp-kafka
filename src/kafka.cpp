#include "kafka.h"

#include <kafka/AdminClient.h>
#include <kafka/KafkaConsumer.h>

#include <chrono>
#include <csignal>
#include <exception>
#include <iostream>
#include <stdexcept>

using namespace kafka;
using namespace kafka::clients::consumer;

using ms = std::chrono::milliseconds;

namespace {
    volatile sig_atomic_t stop_loop = 0;

    void interrupt_handler(int) {
        stop_loop = 1;
    }
}

namespace sc {
    namespace {
        struct kafka_buffer {
            std::string str;

            explicit kafka_buffer(const ConstBuffer data) : str(static_cast<const char *>(data.data()), data.size()) {
            }

            operator std::string() const {
                return str;
            }
        };
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

        stop_loop = 0;

        Properties props;
        props.put("bootstrap.servers", brokers_.to_string(","));
        props.put("auto.offset.reset", from_beginning ? "earliest" : "latest");
        props.put("client.id", client_id);
        props.put("group.id", group_id);
        props.put("enable.auto.commit", auto_commit ? "true" : "false");
        props.put("log_level", std::to_string(log_level));

        KafkaConsumer consumer(props);
        consumer.subscribe(topics);
        if (from_beginning) consumer.seekToBeginning();

        const auto old_handler1 = signal(SIGINT, interrupt_handler);
        const auto old_handler2 = signal(SIGTERM, interrupt_handler);

        unsigned int counter = 0;
        while (!stop_loop && (!max_records || counter < max_records)) {
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
                    if (const auto header_size = record.headers().size()) {
                        for (int i = 0; i < header_size; ++i) {
                            std::cout << record.headers()[i].key << ": " << record.headers()[i].value.toString();
                        }
                    }
#endif
                    const kafka_buffer key{record.key()};
                    const kafka_buffer value{record.value()};
                    try {
                        handler({record.topic(), key, record.timestamp().msSinceEpoch, value});
                    } catch (const std::exception &error) {
                        std::cerr << "Handler error: " << error.what() << std::endl;
                    }
                }
            }

            consumer.commitSync();
            counter += records.size();
        }

        signal(SIGINT, old_handler1);
        signal(SIGTERM, old_handler2);
        consumer.close();
    }
} // namespace sc
