// Consumes a few messages from Kafka and prints a line for each, then a summary.
// Settings come from the environment:
//   SC_KAFKA_DEMO_BROKERS    one broker, or several separated by ';' (default 127.0.0.1:9092)
//   SC_KAFKA_DEMO_TOPICS     the topics, separated by ';' (required)
//   SC_KAFKA_DEMO_CLIENT_ID  default sc-kafka-demo
//   SC_KAFKA_DEMO_GROUP_ID   default sc-kafka-demo; the consumer commits offsets for its group,
//                            so give the demo a group of its own, not one a real consumer uses
//   SC_KAFKA_DEMO_MAX_RECORDS  default 10

#include <core.h>
#include <datetime.h>
#include <ip_endpoints.h>
#include <kafka.h>
#include <timer.h>

#include <iostream>
#include <string>
#include <string_view>

namespace {
    void heading(const std::string_view title) { std::cout << '\n' << title << '\n'; }
}

int main() {
    try {
        const sc::ip_endpoints brokers{sc::getenv("SC_KAFKA_DEMO_BROKERS", "127.0.0.1"), 9092};
        auto topics = sc::explode(sc::getenv("SC_KAFKA_DEMO_TOPICS"));
        std::erase(topics, ""); // tolerate "a;;b" and a trailing ';'
        if (topics.empty()) {
            std::cerr << "sc-kafka-demo: set SC_KAFKA_DEMO_TOPICS to one or more topics, separated by ';'\n";
            return 1;
        }
        const auto client_id = sc::getenv("SC_KAFKA_DEMO_CLIENT_ID", "sc-kafka-demo");
        const auto group_id = sc::getenv("SC_KAFKA_DEMO_GROUP_ID", "sc-kafka-demo");
        const auto max_records = std::stoi(sc::getenv("SC_KAFKA_DEMO_MAX_RECORDS", "10"));

        std::cout << "simply-cpp kafka: consuming a few messages\n"
                  << "  brokers  " << brokers << "  (SC_KAFKA_DEMO_BROKERS)\n"
                  << "  topics   " << sc::getenv("SC_KAFKA_DEMO_TOPICS") << "  (SC_KAFKA_DEMO_TOPICS)\n"
                  << "  group    " << group_id << ", client " << client_id << '\n'
                  << "  stopping after " << max_records << " messages, or when the topics go quiet\n";
        sc::timer sw;

        // [readme]
        sc::kafka consumer{brokers};
        consumer.ClientId(client_id);
        consumer.GroupId(group_id);
        consumer.MaxRecords(max_records);
        consumer.AddTopic(topics);

        heading("Messages, from where this group left off");
        int received = 0;
        consumer.consume([&](const sc::kafka_message &message) {
            if (++received > max_records) return; // the rest of the last batch, counted below
            const auto created = sc::datetime::from_unix(message.created / 1000);
            std::cout << "  " << created.format("%H:%M:%S") << "  " << message.topic << "  key " << message.key
                      << "  (" << message.data.size() << " bytes)\n";
        });
        // [/readme]

        heading("Summary");
        std::cout << "  " << received << (received == 1 ? " message" : " messages") << " in " << sw << '\n';
        if (received > max_records) {
            // MaxRecords is checked between batches, and the whole batch is committed.
            std::cout << "  (the last batch brought " << received - max_records << " more than the "
                      << max_records << " shown)\n";
        }
    } catch (const std::exception &error) {
        std::cerr << "sc-kafka-demo: " << error.what() << '\n';
        return 1;
    }
    return 0;
}
