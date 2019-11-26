#pragma once

#include <common/logger_useful.h>

#include <Core/Block.h>
#include <Core/QueryProcessingStage.h>
#include <Common/LRUCache.h>
#include <Parsers/IAST.h>
#include <Interpreters/DatabaseAndTableWithAlias.h>

#include <set>
#include <mutex>
#include <vector>

namespace DB
{

class Set;
using SetPtr = std::shared_ptr<Set>;

struct TableInfo
{
    String database;
    String table;
    String alias;
    UInt64 version;
};

struct QueryResult
{
    std::vector<TableInfo> tables;
    BlocksPtr blocks;
    SetPtr set;

    QueryResult();
    QueryResult(std::vector<TableInfo> tables_, BlocksPtr blocks_) : 
        tables(tables_), blocks(blocks_) {}
    QueryResult(std::vector<TableInfo> tables_, SetPtr set_) : 
        tables(tables_), blocks(std::make_shared<Blocks>()), set(set_) {}

    void add(const std::shared_ptr<QueryResult> & res);

    size_t operator()(const QueryResult & x) const;

    size_t size() const;
};

using QueryResultPtr = std::shared_ptr<QueryResult>;

struct QueryInfo
{
    String key;
    std::vector<TableInfo> tables;

    operator bool() const
    {
        return key.size() > 0 || tables.size() > 0;
    }
};

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

    std::shared_ptr<QueryResult> getCache(const Key & key, const Context & context);

    static QueryInfo getQueryInfo(const IAST & ast, const Context & ctx, const UInt32 shard_num = 0, const QueryProcessingStage::Enum processed_stage = QueryProcessingStage::FetchColumns);
};

using QueryCachePtr = std::shared_ptr<QueryCache>;

void getTables(const IAST & ast, std::vector<DatabaseAndTableWithAlias> & databasesAndTables, const String & current_database);

}
