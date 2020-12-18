#pragma once

#include <Core/ColumnNumbers.h>
#include <Core/Names.h>
#include <AggregateFunctions/IAggregateFunction.h>


namespace DB
{

struct AggregateDescription
{
    AggregateFunctionPtr function;
    Array parameters;        /// Parameters of the (parametric) aggregate function.
    ColumnNumbers arguments;
    Names argument_names;    /// used if no `arguments` are specified.
    String column_name;      /// What name to use for a column with aggregate function values

    bool is_sorting_key;        /// It is true when this column is used by ORDER-BY statement.
    String sorting_column_name; /// Alias for column_name used internally for limit_pushdown.

    void explain(WriteBuffer & out, size_t indent) const; /// Get description for EXPLAIN query.
};

using AggregateDescriptions = std::vector<AggregateDescription>;

}
