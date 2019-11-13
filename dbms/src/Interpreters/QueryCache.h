#pragma once

#include <Core/Block.h>
#include <Core/QueryProcessingStage.h>
#include <Common/LRUCache.h>

#include <set>
#include <mutex>
#include <vector>
#include <condition_variable>
#include <iterator>

namespace DB
{

class Set;
using SetPtr = std::shared_ptr<Set>;

struct QueryResult
{
    BlocksPtr blocks;
    SetPtr set;

    QueryResult();
    QueryResult(BlocksPtr blocks_) : blocks(blocks_) {}
    QueryResult(SetPtr set_) : blocks(std::make_shared<Blocks>()), set(set_) {}

    void add(const std::shared_ptr<QueryResult> & res);

    size_t operator()(const QueryResult & x) const;

    size_t size() const;
};

using QueryResultPtr = std::shared_ptr<QueryResult>;

struct QueryResultWeightFunction
{
    size_t operator()(const QueryResult & result) const
    {
        return result.size();
    }
};

class QueryCache : public LRUCache<String, QueryResult, std::hash<String>, QueryResultWeightFunction>
{
private:
    using Base = LRUCache<String, QueryResult, std::hash<String>, QueryResultWeightFunction>;
    using Reserved = std::set<Key>;

public:
    QueryCache(size_t max_size_in_bytes, const Delay & expiration_delay_ = Delay::zero())
        : Base(max_size_in_bytes, expiration_delay_) {}

    bool reserveOrGet(const Key & key, MappedPtr & mapped)
    {
        std::lock_guard lock(mutex);

        if (auto v = getImpl(key))
        {
            mapped = v;
            return false;
        }

        if (resv.find(key) == resv.end())
        {
            LOG_INFO(&Logger::get("QueryCache"), "CACHE reserved " << key);
            resv.insert(key);
            return true;
        }

        return false;
    }

    MappedPtr waitAndGet(const Key & key)
    {
        std::unique_lock<std::mutex> lock(mutex);
        MappedPtr ret = nullptr;

        cv.wait(lock,
                [&] {
                    auto v = getImpl(key);
                    if (v)
                    {
                        ret = v;
                        return true;
                    }
                    else
                    {
                        return false;
                    }
                }
            );

        LOG_INFO(&Logger::get("QueryCache"), "CACHE waitAndGet " << key);

        return ret;
    }

    void set(const Key & key, const MappedPtr & mapped) override
    {
        std::lock_guard lock(mutex);

        if (auto v = getImpl(key))
            v->add(mapped);
        else
            setImpl(key, mapped, lock);

        clearReservation(key);
    }

    static String makeKey(const String query, const UInt32 shard_num = 0, const QueryProcessingStage::Enum processed_stage = QueryProcessingStage::FetchColumns)
    {
        return query + "_" + std::to_string(shard_num) + "_" + QueryProcessingStage::toString(processed_stage);
    }

private:
    void clearReservation(const Key & key)
    {
        if (resv.find(key) != resv.end())
        {
            resv.erase(key);
        }
        cv.notify_all();
    }

    Reserved resv;
    std::condition_variable cv;
};

using QueryCachePtr = std::shared_ptr<QueryCache>;

extern QueryCache g_query_cache;

}
