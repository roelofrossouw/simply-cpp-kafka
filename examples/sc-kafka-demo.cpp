// Consumes a few messages from Kafka and prints a line per message.
// Brokers come from SC_KAFKA_DEMO_BROKERS: one broker, or several separated by ';'
// ("kafka1:9092;kafka2:9092"). Unset or empty falls back to 127.0.0.1:9092;
// an invalid value is an error.
// Topics come from SC_KAFKA_DEMO_TOPICS, also ';'-separated, and are required.
// SC_KAFKA_DEMO_CLIENT_ID, SC_KAFKA_DEMO_GROUP_ID and SC_KAFKA_DEMO_MAX_RECORDS default to
// sc-kafka-demo, sc-kafka-demo and 10. The consumer commits offsets for its group, so give
// the demo a group of its own rather than one a real consumer uses.

#include <core.h>
#include <ip_endpoints.h>
#include <kafka.h>
#include <timer.h>

#include <iostream>
#include <string>

int main() {
    try {
        const sc::ip_endpoints brokers{sc::getenv("SC_KAFKA_DEMO_BROKERS", "127.0.0.1"), 9092};
        auto topics = sc::explode(sc::getenv("SC_KAFKA_DEMO_TOPICS"));
        std::erase(topics, "");  // tolerate "a;;b" and a trailing ';'
        if (topics.empty()) {
            std::cerr << "sc-kafka-demo: set SC_KAFKA_DEMO_TOPICS to one or more topics, separated by ';'\n";
            return 1;
        }
        const auto client_id = sc::getenv("SC_KAFKA_DEMO_CLIENT_ID", "sc-kafka-demo");
        const auto group_id = sc::getenv("SC_KAFKA_DEMO_GROUP_ID", "sc-kafka-demo");
        const auto max_records = std::stoi(sc::getenv("SC_KAFKA_DEMO_MAX_RECORDS", "10"));
        std::cout << "Kafka brokers: " << brokers << ", group " << group_id << '\n';

        // [readme]
        sc::timer sw;
        sc::kafka consumer{brokers};
        consumer.ClientId(client_id);
        consumer.GroupId(group_id);
        consumer.MaxRecords(max_records);
        consumer.AddTopic(topics);

        int received = 0;
        consumer.consume([&](const sc::kafka_message &message) {
            ++received;
            std::cout << message.topic << " key=" << message.key << " created=" << message.created
                    << " (" << message.data.size() << " bytes)\n";
        });
        std::cout << "Received " << received << " messages after " << sw << '\n';
        // [/readme]
    } catch (const std::exception &error) {
        std::cerr << "sc-kafka-demo: " << error.what() << '\n';
        return 1;
    }
    return 0;
}
