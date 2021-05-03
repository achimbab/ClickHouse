#pragma once

#include <cassert>

#include <IO/WriteHelpers.h>
#include <IO/ReadHelpers.h>

#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeString.h>

#include <Columns/ColumnArray.h>

#include <Common/HashTable/HashSet.h>
#include <Common/HashTable/HashTableKeyHolder.h>
#include <Common/assert_cast.h>

#include <AggregateFunctions/IAggregateFunction.h>
#include <AggregateFunctions/KeyHolderHelpers.h>


namespace DB
{


template <typename T, UInt64 Tlimit_num_elem>
struct AggregateFunctionMinKData
{
    using List = PODArrayWithStackMemory<T, Tlimit_num_elem>;

    List value;

    void add(T t)
    {
        if (value.size() == 0)
        {
            value.emplace_back(t);
            return;
        }

        if (value.size() == Tlimit_num_elem && value[Tlimit_num_elem - 1] < t)
            return;

        if (value.size() < Tlimit_num_elem)
        {
            value.emplace_back(t);
            std::sort(std::begin(value), std::end(value));
            return;
        }

        for (size_t i = 0; i < Tlimit_num_elem; ++i)
            if (t < value[i] && t != value[i - 1])
            {
                for (size_t k = i + 1; k < std::min(value.size(), Tlimit_num_elem); ++k)
                    value[k] = value[k - 1];
                value[i] = t;
                return;
            }
    }

    void merge(const AggregateFunctionMinKData & other)
    {
        if (other.value.empty())
            return;

        for (size_t i = 0; i < other.value.size(); i++)
            add(other.value[i]);
    }
};


/// Puts all values to the hash set. Returns an array of unique values. Implemented for numeric types.
template <typename T, UInt64 Tlimit_num_elem>
class AggregateFunctionMinK
    : public IAggregateFunctionDataHelper<AggregateFunctionMinKData<T, Tlimit_num_elem>, AggregateFunctionMinK<T, Tlimit_num_elem>>
{
    //static constexpr bool limit_num_elems = Tlimit_num_elem::value;

private:
    using State = AggregateFunctionMinKData<T, Tlimit_num_elem>;

public:
    AggregateFunctionMinK(const DataTypePtr & argument_type)
        : IAggregateFunctionDataHelper<AggregateFunctionMinKData<T, Tlimit_num_elem>,
          AggregateFunctionMinK<T, Tlimit_num_elem>>({argument_type}, {}) {}

    String getName() const override { return "minK"; }

    DataTypePtr getReturnType() const override
    {
        return std::make_shared<DataTypeArray>(this->argument_types[0]);
    }

    bool allocatesMemoryInArena() const override { return false; }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena *) const override
    {
        this->data(place).add(assert_cast<const ColumnVector<T> &>(*columns[0]).getData()[row_num]);
    }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena *) const override
    {
        this->data(place).merge(this->data(rhs));
    }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf) const override
    {
        auto & list = this->data(place).value;
        size_t size = list.size();
        writeVarUInt(size, buf);
        for (const auto & elem : list)
            writeIntBinary(elem, buf);
    }

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, Arena *) const override
    {
        size_t size;
        readVarUInt(size, buf);

        auto & list = this->data(place).value;
        T t;
        for (size_t i = 0; i < size; ++i)
        {
            readIntBinary(t, buf);
            list.emplace_back(t);
        }
    }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena *) const override
    {
        ColumnArray & arr_to = assert_cast<ColumnArray &>(to);
        ColumnArray::Offsets & offsets_to = arr_to.getOffsets();

        const typename State::List & list = this->data(place).value;
        size_t size = list.size();

        offsets_to.push_back(offsets_to.back() + size);

        typename ColumnVector<T>::Container & data_to = assert_cast<ColumnVector<T> &>(arr_to.getData()).getData();
        size_t old_size = data_to.size();
        data_to.resize(old_size + size);

        for (size_t i = 0; i < list.size(); ++i)
            data_to[old_size + i] = list[i];
    }
};

}
