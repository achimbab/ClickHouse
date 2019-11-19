#include <Interpreters/QueryCache.h>
#include <Interpreters/Set.h>

#include <iostream>

namespace DB
{

QueryResult::QueryResult() :
    blocks(std::make_shared<Blocks>())
{
}

void QueryResult::add(const std::shared_ptr<QueryResult> & res)
{
    assert(set == nullptr);
    blocks->insert(blocks->end(), res->blocks->begin(), res->blocks->end());
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
