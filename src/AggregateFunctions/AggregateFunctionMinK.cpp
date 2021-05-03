#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/AggregateFunctionMinK.h>
#include <AggregateFunctions/Helpers.h>
#include <AggregateFunctions/FactoryHelpers.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>
#include "registerAggregateFunctions.h"


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int BAD_ARGUMENTS;
}

namespace
{

/*
/// Substitute return type for Date and DateTime
template <UInt64 Limit>
class AggregateFunctionMinKDate : public AggregateFunctionMinK<DataTypeDate::FieldType, Limit>
{
public:
    explicit AggregateFunctionMinKDate(const DataTypePtr & argument_type)
        : AggregateFunctionMinK<DataTypeDate::FieldType, Limit>(argument_type) {}
    DataTypePtr getReturnType() const override { return std::make_shared<DataTypeArray>(std::make_shared<DataTypeDate>()); }
};

template <UInt64 Limit>
class AggregateFunctionMinKDateTime : public AggregateFunctionMinK<DataTypeDateTime::FieldType, Limit>
{
public:
    explicit AggregateFunctionMinKDateTime(const DataTypePtr & argument_type)
        : AggregateFunctionMinK<DataTypeDateTime::FieldType, Limit>(argument_type) {}
    DataTypePtr getReturnType() const override { return std::make_shared<DataTypeArray>(std::make_shared<DataTypeDateTime>()); }
};

template <UInt64 Limit>
static IAggregateFunction * createWithExtraTypes(const DataTypePtr & argument_type)
{
    WhichDataType which(argument_type);
    if (which.idx == TypeIndex::Date) return new AggregateFunctionMinKDate<Limit>(argument_type);
    else if (which.idx == TypeIndex::DateTime) return new AggregateFunctionMinKDateTime<Limit>(argument_type);
    else
    {
        throw Exception("The first argument for aggregate function 'minK' should be number-type", ErrorCodes::BAD_ARGUMENTS);
    }
}

template <UInt64 Limit>
inline AggregateFunctionPtr createAggregateFunctionMinKImpl(const DataTypePtr & argument_type)
{
    WhichDataType timestamp_type(argument_type[0]);
    if (timestamp_type.idx == TypeIndex::UInt8)
        return AggregateFunctionPtr(new AggregateFunctionMinK<UInt8, Limit>(argument_type));
    else if (timestamp_type.idx == TypeIndex::UInt16)
        return AggregateFunctionPtr(new AggregateFunctionMinK<UInt16, Limit>(argument_type));
    else if (timestamp_type.idx == TypeIndex::UInt32)
        return AggregateFunctionPtr(new AggregateFunctionMinK<UInt32, Limit>(argument_type));
    else if (timestamp_type.idx == TypeIndex::UInt64)
        return AggregateFunctionPtr(new AggregateFunctionMinK<UInt64, Limit>(argument_type));
    else
        return AggregateFunctionPtr(createWithExtraTypes<Limit>(argument_type));
}

AggregateFunctionPtr createAggregateFunctionMinK(const std::string & name, const DataTypes & argument_types, const Array & parameters)
{
    assertUnary(name, argument_types);

    UInt64 max_elems = std::numeric_limits<UInt64>::max();

    if (parameters.size() != 1)
        throw Exception("Incorrect number of parameters for aggregate function " + name + ", should be 1",
                        ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    auto type = parameters[0].getType();
    if (type != Field::Types::Int64 && type != Field::Types::UInt64)
        throw Exception("Parameter for aggregate function " + name + " should be positive number", ErrorCodes::BAD_ARGUMENTS);

    if ((type == Field::Types::Int64 && parameters[0].get<Int64>() < 0) ||
        (type == Field::Types::UInt64 && parameters[0].get<UInt64>() == 0))
        throw Exception("Parameter for aggregate function " + name + " should be positive number", ErrorCodes::BAD_ARGUMENTS);

    max_elems = parameters[0].get<UInt64>();

    switch (max_elems)
    {
        case 0:
            return createAggregateFunctionMinKImpl<0>(argument_types[0]);
        case 1:
            return createAggregateFunctionMinKImpl<1>(argument_types[0]);
        case 2:
            return createAggregateFunctionMinKImpl<2>(argument_types[0]);
        case 3:
            return createAggregateFunctionMinKImpl<3>(argument_types[0]);
        case 4:
            return createAggregateFunctionMinKImpl<4>(argument_types[0]);
        default:
            return createAggregateFunctionMinKImpl<5>(argument_types[0]);
    }
}
*/

AggregateFunctionPtr createAggregateFunctionMinK(const std::string &, const DataTypes & argument_types, const Array &)
{
    return std::make_shared<AggregateFunctionMinK<UInt64, 5>>(argument_types[0]);
}

}

void registerAggregateFunctionMinK(AggregateFunctionFactory & factory)
{
    AggregateFunctionProperties properties = { .returns_default_when_only_null = false, .is_order_dependent = true };

    factory.registerFunction("minK", { createAggregateFunctionMinK, properties });
}

}

