#ifndef SC_KAFKA_PENDING_OFFSETS_H
#define SC_KAFKA_PENDING_OFFSETS_H
// Offsets handled but not committed yet: a commit sent while the group was rebalancing fails
// (the broker answers it with the generation that just ended) and is kept here, to go out with the
// next commit or, for a partition being given up, from the revoke callback. Internal to sc-kafka;
// tests/kafka.cpp includes it.

#include <algorithm>
#include <cstdint>
#include <map>
#include <set>
#include <string>
#include <utility>

namespace sc::kafka::detail {
    using partition_key = std::pair<std::string, int>;
    using offsets = std::map<partition_key, std::int64_t>;

    // Adds next to into, keeping the higher offset of a partition: a commit never goes back.
    inline void merge(offsets &into, const offsets &next) {
        for (const auto &[partition, offset]: next) {
            auto [it, added] = into.try_emplace(partition, offset);
            if (!added) it->second = std::max(it->second, offset);
        }
    }

    // Takes the given partitions out of pending, returning their offsets.
    inline offsets take(offsets &pending, const std::set<partition_key> &partitions) {
        offsets taken;
        for (auto it = pending.begin(); it != pending.end();) {
            if (partitions.contains(it->first)) {
                taken.insert(*it);
                it = pending.erase(it);
            } else {
                ++it;
            }
        }
        return taken;
    }

    // Takes out of pending the partitions this consumer no longer has, returning them.
    inline offsets take_unassigned(offsets &pending, const std::set<partition_key> &assigned) {
        offsets taken;
        for (auto it = pending.begin(); it != pending.end();) {
            if (!assigned.contains(it->first)) {
                taken.insert(*it);
                it = pending.erase(it);
            } else {
                ++it;
            }
        }
        return taken;
    }
} // sc::kafka::detail

#endif //SC_KAFKA_PENDING_OFFSETS_H
