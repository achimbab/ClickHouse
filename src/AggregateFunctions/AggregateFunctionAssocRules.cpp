#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/AggregateFunctionAssocRules.h>
#include <AggregateFunctions/Helpers.h>
#include <AggregateFunctions/FactoryHelpers.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <ext/range.h>


namespace DB
{

auto createAggregateFunctionAssocRules(const std::string & name, const DataTypes & arg_types, const Array & params)
{
    if (arg_types.size() < 2)
        throw Exception("Aggregate function " + name + " requires at least two arguments.",
                        ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    UInt64 minimum_support = 0;
    if (params.size() == 1)
    {
        minimum_support = params.front().safeGet<UInt64>();
    }

    const auto * transaction_col = arg_types[0].get();
    if (WhichDataType(transaction_col).idx != TypeIndex::UInt64)
        throw Exception{"Illegal type " + arg_types[0].get()->getName()
                + " of first argument of aggregate function " + name + ", must be UInt64",
            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};

    const auto * item_col = arg_types[1].get();
    if (WhichDataType(item_col).idx != TypeIndex::String)
        throw Exception{"Illegal type " + arg_types[1].get()->getName()
                + " of second argument of aggregate function " + name + ", must be String",
            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};

    return std::make_shared<AssocRulesImpl>(std::make_shared<DataTypeString>(), arg_types, minimum_support);
}

void registerAggregateFunctionAssocRules(AggregateFunctionFactory & factory)
{
    factory.registerFunction("assocRules", { createAggregateFunctionAssocRules });
}

}
