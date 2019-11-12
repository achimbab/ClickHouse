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

// TODO: Refactor struct QueryCacheItem
struct QueryCacheItem
{
    QueryCacheItem() : shard_num(0), query(""), processed_stage(QueryProcessingStage::FetchColumns)
    {
    }

    QueryCacheItem(UInt32 shard_num_, String query_, QueryProcessingStage::Enum processed_stage_) :
        shard_num(shard_num_), query(query_), processed_stage(processed_stage_)
    {
    }

    QueryCacheItem(UInt32 shard_num_, String query_, QueryProcessingStage::Enum processed_stage_, Block block_) :
        shard_num(shard_num_), query(query_), processed_stage(processed_stage_)
    {
        blocks.push_back(block_);
    }

    QueryCacheItem(UInt32 shard_num_, String query_, QueryProcessingStage::Enum processed_stage_, SetPtr set_) :
        shard_num(shard_num_), query(query_), processed_stage(processed_stage_), set(set_)
    {
    }

    bool operator < (const QueryCacheItem & rhs) const
    {
        if (query == rhs.query)
            if (processed_stage == rhs.processed_stage)
                return shard_num < rhs.shard_num;
            else
                return processed_stage < rhs.processed_stage;
        else 
            return query < rhs.query;
    }

    std::vector<Block> getBlocks() const { return blocks; }
    SetPtr getSet() const { return set; }

    UInt32 shard_num;
    String query;
    QueryProcessingStage::Enum processed_stage;

    mutable std::vector<Block> blocks;
    mutable SetPtr set;
};

bool getQueryCache(UInt32 shard_num, String query, QueryProcessingStage::Enum processed_stage, QueryCacheItem & cache);
bool reserveQueryCache(UInt32 shard_num, String query, QueryProcessingStage::Enum processed_stage);
void waitAndGetQueryCache(UInt32 shard_num, String query, QueryProcessingStage::Enum processed_stage, QueryCacheItem & cache);
void addQueryCache(UInt32 shard_num, String query, QueryProcessingStage::Enum processed_stage, Block block);
void addQueryCache(UInt32 shard_num, String query, QueryProcessingStage::Enum processed_stage, SetPtr set);

extern std::set<QueryCacheItem> g_cache;
extern std::set<QueryCacheItem> g_resv;
extern std::mutex g_cache_lock;
extern std::condition_variable g_cache_cv;

struct QueryResult
{
    BlocksPtr blocks;
    SetPtr set;

    QueryResult();

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

    void set(const Key & key, const MappedPtr & mapped)
    {
        std::lock_guard lock(mutex);

        setImpl(key, mapped, lock);
        
        clearReservation(key);
    }

    void add(const Key & key, const MappedPtr & mapped)
    {
        std::lock_guard lock(mutex);
        
        if (auto v = getImpl(key))
            v->add(mapped);
        else
            setImpl(key, mapped, lock);

        clearReservation(key);
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

}
