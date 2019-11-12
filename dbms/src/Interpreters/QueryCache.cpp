#include <Interpreters/QueryCache.h>
#include <Interpreters/Set.h>
#include <Parsers/queryToString.h>

#include <iostream>

namespace DB
{

// TODO: Move temp global vars to global_context
std::set<QueryCacheItem> g_cache;
std::set<QueryCacheItem> g_resv;
std::mutex g_cache_lock;
std::condition_variable g_cache_cv;

bool getQueryCache(UInt32 shard_num, String query, QueryProcessingStage::Enum processed_stage, QueryCacheItem & cache)
{
    std::lock_guard<std::mutex> guard(g_cache_lock);
    auto c = g_cache.find(QueryCacheItem(shard_num, query, processed_stage));
    if (c != g_cache.end())
    {
        std::cout << "    CACHE hit for shard " << shard_num << " stage: " << QueryProcessingStage::toString(processed_stage) << " \"" << query << "\"\n";
        cache = *c;
        return true;
    }
    else
    {
        std::cout << "    CACHE miss for shard " << shard_num << " stage: " << QueryProcessingStage::toString(processed_stage) << " \"" << query << "\"\n";
        return false;
    }
}

bool reserveQueryCache(UInt32 shard_num, String query, QueryProcessingStage::Enum processed_stage)
{
    std::lock_guard<std::mutex> guard(g_cache_lock);
    auto c = g_resv.find(QueryCacheItem(shard_num, query, processed_stage));
    if (c == g_resv.end())
    {
        std::cout << "    CACHE reserve for shard " << shard_num << " stage: " << QueryProcessingStage::toString(processed_stage) << " \"" << query << "\"\n";
        g_resv.insert(QueryCacheItem(shard_num, query, processed_stage));
        return true;
    }
    else
    {
        std::cout << "    CACHE reserved for shard " << shard_num << " stage: " << QueryProcessingStage::toString(processed_stage) << " \"" << query << "\"\n";
        return false;
    }
}

void waitAndGetQueryCache(UInt32 shard_num, String query, QueryProcessingStage::Enum processed_stage, QueryCacheItem & cache)
{
    std::unique_lock<std::mutex> ul(g_cache_lock);
    g_cache_cv.wait(ul, 
        [&] {
            auto c = g_cache.find(QueryCacheItem(shard_num, query, processed_stage));
            if (c != g_cache.end())
            {
                cache = *c;
                return true;
            }
            else
            {
                return false;
            }
        }
    );
    std::cout << "    CACHE waitAndGet for shard " << shard_num << " stage: " << QueryProcessingStage::toString(processed_stage) << " \"" << query << "\"\n";
}

void addQueryCache(UInt32 shard_num, String query, QueryProcessingStage::Enum processed_stage, Block block)
{
    std::lock_guard<std::mutex> guard(g_cache_lock);
    std::cout << "    CACHE block added for shard " << shard_num << " stage: " << QueryProcessingStage::toString(processed_stage) << " \"" << query << "\"\n";
    std::set<QueryCacheItem>::iterator it = g_cache.find(QueryCacheItem(shard_num, query, processed_stage));
    if (it == g_cache.end())
    {
        g_cache.insert(QueryCacheItem(shard_num, query, processed_stage, block));
    }
    else
    {
        (*it).blocks.push_back(block);
    }
}

void addQueryCache(UInt32 shard_num, String query, QueryProcessingStage::Enum processed_stage, SetPtr set)
{
    std::lock_guard<std::mutex> guard(g_cache_lock);
    std::cout << "    CACHE set added for shard " << shard_num << " stage: " << QueryProcessingStage::toString(processed_stage) << " \"" << query << "\"\n";
    g_cache.insert(QueryCacheItem(shard_num, query, processed_stage, set));
}

QueryResult::QueryResult() :
    blocks(std::make_shared<Blocks>())
{
}

void QueryResult::add(const std::shared_ptr<QueryResult> & res)
{
    assert(set == nullptr);
    blocks->insert(blocks->end(), 
        res->blocks->begin(), 
        res->blocks->end());
}

size_t QueryResult::operator()(const QueryResult & x) const
{
    return x.blocks->size() || x.set;
}

size_t QueryResult::size() const
{
    size_t bytes = 0;

    for (auto & block : *blocks)
        bytes += block.bytes();

    if (set)
        bytes += set->getTotalByteCount();

    return bytes;
}

}
