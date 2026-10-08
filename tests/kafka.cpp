#include <kafka.h>
#include <sc_test.h>

int main() {
    sc::kafka consumer{"localhost:9092"};

    SECTION("Default consumer configuration");
    CHECK_EQ(consumer.MaxRecords(), 0);
    CHECK_EQ(consumer.FromBeginning(), false);
    CHECK_EQ(consumer.Poll(), 200);
    CHECK_EQ(consumer.PollAtEnd(), 5000);
    CHECK_EQ(consumer.ClientId(), "simply-cpp-kafka");
    CHECK_EQ(consumer.GroupId(), "simply-cpp-kafka");
    CHECK_EQ(consumer.AutoCommit(), false);
    CHECK_EQ(consumer.LogLevel(), 3);

    SECTION("Consumer configuration");
    consumer.MaxRecords(25);
    consumer.FromBeginning(true);
    consumer.Poll(100);
    consumer.PollAtEnd(750);
    consumer.ClientId("streamvms-client");
    consumer.GroupId("streamvms-consumers");
    consumer.AutoCommit(true);
    consumer.LogLevel(6);

    CHECK_EQ(consumer.MaxRecords(), 25);
    CHECK_EQ(consumer.FromBeginning(), true);
    CHECK_EQ(consumer.Poll(), 100);
    CHECK_EQ(consumer.PollAtEnd(), 750);
    CHECK_EQ(consumer.ClientId(), "streamvms-client");
    CHECK_EQ(consumer.GroupId(), "streamvms-consumers");
    CHECK_EQ(consumer.AutoCommit(), true);
    CHECK_EQ(consumer.LogLevel(), 6);

    SECTION("No-topic consumption does not contact Kafka");
    CHECK_NOTHROW(consumer.consume([](const sc::kafka_message &) {}));

    SECTION("Adding an empty topic list adds no topics");
    consumer.AddTopic(std::vector<std::string>{});
    CHECK_NOTHROW(consumer.consume([](const sc::kafka_message &) {}));

    SECTION("Topics can be added one at a time, as a vector or as a braced list");
    // The topic set is private, so this checks that every call form compiles and resolves.
    sc::kafka topics{"localhost:9092"};
    topics.AddTopic("one");
    topics.AddTopic(std::vector<std::string>{"two", "three"});
    topics.AddTopic({"four"});
    topics.AddTopic({"five", "six"});
    CHECK_NOTHROW(topics.AddTopic("one"));

    TEST_SUMMARY();
}
