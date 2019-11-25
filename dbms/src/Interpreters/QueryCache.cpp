#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTSubquery.h>
#include <Parsers/ASTTablesInSelectQuery.h>

#include <Interpreters/DatabaseAndTableWithAlias.h>
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

QueryInfo QueryCache::getQueryInfo(const IAST & ast, const Context & ctx, const UInt32 shard_num, const QueryProcessingStage::Enum processed_stage)
{
    std::ostringstream out_key;
    IAST::FormatSettings settings(out_key, true);
    settings.with_alias = false;
    ast.format(settings);
    out_key << "_" << shard_num << "_" << QueryProcessingStage::toString(processed_stage);
    auto key = out_key.str();

    std::ostringstream out_table;
    std::vector<DatabaseAndTableWithAlias> tables;
    getTables(ast, tables, ctx.getCurrentDatabase());
    for (auto it = tables.begin(); it != tables.end(); ++it)
    {
        out_table << it->database << "." << it->table << "(" << it->alias << ") ";
    }

    LOG_DEBUG(&Logger::get("QueryCache"), "key: " << key << ", refs: " << out_table.str());
    return QueryInfo{key, tables};
}

void getTables(const IAST & ast, std::vector<DatabaseAndTableWithAlias> & databasesAndTables, const String & current_database)
{
    if (auto * expr = ast.as<ASTTableExpression>())
    {
        ASTPtr table;
        if (expr->subquery)
            table = expr->subquery;
        else if (expr->table_function)
            table = expr->table_function;
        else if (expr->database_and_table_name)
            table = expr->database_and_table_name;

        return getTables(*table, databasesAndTables, current_database);
    }

    if (const auto * table = ast.as<ASTIdentifier>())
    {
        databasesAndTables.push_back(DatabaseAndTableWithAlias(*table, current_database));
    }
    else if (auto * subquery = ast.as<ASTSubquery>())
    {
        const auto * select_union = subquery->children[0]->as<ASTSelectWithUnionQuery>();
        const size_t num_selects = select_union->list_of_selects->children.size();
        for (size_t query_num = 0; query_num < num_selects; ++query_num)
        {
            const auto & select_query = select_union->list_of_selects->children[query_num]->as<ASTSelectQuery &>();

            const auto * tables = select_query.tables()->as<ASTTablesInSelectQuery>();
            if (!tables)
                continue;

            for (auto & child : tables->children)
            {
                auto element = child->as<ASTTablesInSelectQueryElement>();
                if (element && element->table_expression)
                {
                    const auto & expr = element->table_expression->as<ASTTableExpression &>();
                    databasesAndTables.push_back(DatabaseAndTableWithAlias(expr, current_database));
                }
            }
        }
    }
}

}
