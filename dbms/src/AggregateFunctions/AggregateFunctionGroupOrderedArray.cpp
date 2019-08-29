#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/AggregateFunctionGroupOrderedArray.h>
#include <AggregateFunctions/Helpers.h>
#include <AggregateFunctions/FactoryHelpers.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>


namespace DB
{

namespace ErrorCodes
{
}

namespace
{

static AggregateFunctionPtr createAggregateFunctionGroupOrderedArray(const std::string & name, const DataTypes & arguments, const Array & params)
{
    assertBinary(name, arguments);

    return std::make_shared<GroupOrderedArray>(arguments, params);
}

}


void registerAggregateFunctionGroupOrderedArray(AggregateFunctionFactory & factory)
{
    factory.registerFunction("groupOrderedArray", createAggregateFunctionGroupOrderedArray);
}

}
