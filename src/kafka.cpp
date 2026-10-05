#include "kafka.h"
#include <kafka/KafkaConsumer.h>
#include <kafka/AdminClient.h>
#include <csignal>

using namespace kafka;
using namespace kafka::clients::consumer;

using clocktime = std::chrono::time_point<std::chrono::system_clock>;
using ms = std::chrono::milliseconds;

namespace {
    volatile sig_atomic_t stopLoop;

    void InterruptHandler(int signum) {
        stopLoop = 1;
    }
}

namespace sc {
    namespace {
        struct kafka_buffer {
            std::string str;

            kafka_buffer(ConstBuffer data) : str(static_cast<const char *>(data.data()), data.size()) {
            }

            operator std::string() const {
                return {str};
            }
        };
    }

    kafka::kafka(const std::string &url) : url_(url) {
#ifdef NDEBUG
        client_id = "oneweb-cconsumer-client";
        group_id = "oneweb-cconsumer-group";
#else
        client_id = "oneweb-consumer-dev-client";
        group_id = "oneweb-consumer-dev-group";
#endif
    }

    void kafka::consume(const std::function<void(const kafka_message &)> &handler) {
        if (topics.empty()) {
            std::cerr << "No topics to consume..." << std::endl;
            return;
        }
        Properties props;
        props.put("bootstrap.servers", url_);
        props.put("auto.offset.reset", from_beginning ? "earliest" : "latest");
        props.put("client.id", client_id);
        props.put("group.id", group_id);
        props.put("enable.auto.commit", auto_commit ? "true" : "false");
        props.put("log_level", std::to_string(log_level));

        KafkaConsumer consumer(props);
        consumer.subscribe(topics);
        if (from_beginning) consumer.seekToBeginning();

        const auto oldHandler1 = signal(SIGINT, InterruptHandler);
        const auto oldHandler2 = signal(SIGTERM, InterruptHandler);

        unsigned int counter = 0;
        while (!stopLoop && (!max_records || counter < max_records)) {
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
                    // auto record_time = clocktime{std::chrono::milliseconds(record.timestamp().msSinceEpoch)};
                    // if (record.timestamp().type == Timestamp::Type::CreateTime) {
                    //     if (std::chrono::system_clock::now() - record_time > std::chrono::hours(1)) {
                    //         // std::cout << "Skipping old record " << record_time << "\n";
                    //         counter--;
                    //         continue;
                    //     }
                    // }

                    if (auto headersize = record.headers().size()) {
                        for (int i = 0; i < headersize; i++) {
                            std::cout << record.headers()[i].key << ": " << record.headers()[i].value.toString();
                        }
                    }

#endif
                    kafka_buffer key{record.key()};
                    kafka_buffer value{record.value()};
                    try {
                        handler({record.topic(), key, record.timestamp().msSinceEpoch, value});
                    } catch (std::exception &e) {
                        std::cerr << "Handler error: " << e.what() << std::endl;
                    }
                }
            }
            consumer.commitSync();
            counter += records.size();
        }
        signal(SIGINT, oldHandler1);
        signal(SIGTERM, oldHandler2);
        consumer.close();
    }
} // sc
