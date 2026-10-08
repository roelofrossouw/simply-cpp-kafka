// Consumes a few messages from Kafka and prints a line per message.
// Brokers come from SC_KAFKA_DEMO_BROKERS: one broker, or several separated by ';'
// ("kafka1:9092;kafka2:9092"). Unset or invalid falls back to 127.0.0.1:9092.
// Topics come from SC_KAFKA_DEMO_TOPICS, also ';'-separated, and are required.
// SC_KAFKA_DEMO_CLIENT_ID, SC_KAFKA_DEMO_GROUP_ID and SC_KAFKA_DEMO_MAX_RECORDS default to
// sc-kafka-demo, sc-kafka-demo and 10. The consumer commits offsets for its group, so give
// the demo a group of its own rather than one a real consumer uses.

#include <demo_servers.h>
#include <kafka.h>
#include <timer.h>

#include <cstdlib>
#include <iostream>
#include <string>
#include <string_view>
#include <vector>

namespace {
    std::string setting(const char *variable, const char *fallback) {
        const char *value = std::getenv(variable);
        return value && *value ? value : fallback;
    }

    // ';'-separated, with whitespace and empty entries ignored, like the brokers.
    std::vector<std::string> list_setting(const char *variable) {
        std::vector<std::string> items;
        const auto value = setting(variable, "");
        std::string_view rest{value};
        while (!rest.empty()) {
            const auto separator = rest.find(';');
            auto item = rest.substr(0, separator);
            rest = separator == std::string_view::npos ? std::string_view{} : rest.substr(separator + 1);
            const auto first = item.find_first_not_of(" \t");
            if (first == std::string_view::npos) continue;
            items.emplace_back(item.substr(first, item.find_last_not_of(" \t") - first + 1));
        }
        return items;
    }
}

int main() {
    try {
        std::string brokers;
        for (const auto &broker : sc::demo_servers("SC_KAFKA_DEMO_BROKERS", 9092)) {
            if (!brokers.empty()) brokers += ',';
            brokers += broker.to_string();
        }
        const auto topics = list_setting("SC_KAFKA_DEMO_TOPICS");
        if (topics.empty()) {
            std::cerr << "sc-kafka-demo: set SC_KAFKA_DEMO_TOPICS to one or more topics, separated by ';'\n";
            return 1;
        }
        const auto client_id = setting("SC_KAFKA_DEMO_CLIENT_ID", "sc-kafka-demo");
        const auto group_id = setting("SC_KAFKA_DEMO_GROUP_ID", "sc-kafka-demo");
        const auto max_records = std::stoi(setting("SC_KAFKA_DEMO_MAX_RECORDS", "10"));
        std::cout << "Kafka brokers: " << brokers << ", group " << group_id << '\n';

        // [readme]
        sc::timer sw;
        sc::kafka consumer{brokers};
        consumer.ClientId(client_id);
        consumer.GroupId(group_id);
        consumer.MaxRecords(max_records);
        for (const auto &topic : topics) consumer.AddTopic(topic);

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
