#ifndef SC_KAFKA_H
#define SC_KAFKA_H
#include <string>
#include <functional>
#include <set>
#include <cstdint>

namespace sc {
    struct kafka_message {
        std::string topic;
        std::string key;
        std::int64_t created;
        std::string data;
    };

    class kafka {
    public:
        explicit kafka(const std::string &url);


        void consume(const std::function<void(const kafka_message &)> &handler);

        [[nodiscard]] int MaxRecords() const { return max_records; }
        void MaxRecords(const int value) { max_records = value; }

        [[nodiscard]] bool FromBeginning() const { return from_beginning; }
        void FromBeginning(const bool value) { from_beginning = value; }

        [[nodiscard]] int Poll() const { return poll; }
        void Poll(const int value) { poll = value; }

        [[nodiscard]] int PollAtEnd() const { return poll_at_end; }
        void PollAtEnd(const int value) { poll_at_end = value; }

        void AddTopic(const std::string &topic_name) { topics.emplace(topic_name); }

    private:
        std::string url_;
        int max_records = 0;
        bool from_beginning = false;
        int poll = 200;
        int poll_at_end = 5000;
        std::string client_id;
        std::string group_id;
        int auto_commit = false;
        int log_level = 3;
        std::set<std::string> topics{};
    };
} // sc

#endif //SC_KAFKA_H
