#pragma once

#include <common/logger_useful.h>

#include <Core/Block.h>
#include <Core/QueryProcessingStage.h>
#include <Common/LRUCache.h>
#include <Parsers/IAST.h>

#include <set>
#include <mutex>
#include <vector>

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

public:
    QueryCache(size_t max_size_in_bytes, const Delay & expiration_delay_ = Delay::zero())
        : Base(max_size_in_bytes, expiration_delay_) {}

    static String makeKey(const IAST & ast, const UInt32 shard_num = 0, const QueryProcessingStage::Enum processed_stage = QueryProcessingStage::FetchColumns)
    {
        std::ostringstream out;
        IAST::FormatSettings settings(out, true);
        settings.with_alias = false;
        ast.format(settings);
        out << "_" << shard_num << "_" << QueryProcessingStage::toString(processed_stage);
        auto key = out.str();
        LOG_DEBUG(&Logger::get("QueryCache"), "key: " << key);
        return key;
    }
};

using QueryCachePtr = std::shared_ptr<QueryCache>;

extern QueryCache g_query_cache;

}
