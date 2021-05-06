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

    static const char * name() { return "minK"; }

    void add(T t)
    {
        if (value.size() == 0)
        {
            value.emplace_back(t);
            return;
        }

        if (value.size() == Tlimit_num_elem && value[Tlimit_num_elem - 1] < t)
            return;

        for (size_t i = 0; i < value.size(); ++i)
            if (t == value[i])
                return;

        size_t i = 0;
        for (; i < value.size(); ++i)
        {
            if (t < value[i])
            {
                size_t to = std::min(value.size() + 1, Tlimit_num_elem);
                for (size_t k = to; k > 0; --k)
                    if (value.size() <= k)
                        value.emplace_back(value[k-1]);
                    else
                        value[k] = value[k - 1];
                value[i] = t;
                return;
            }
        }

        if (i < Tlimit_num_elem)
            value.emplace_back(t);
    }

    void merge(const AggregateFunctionMinKData & other)
    {
        if (other.value.empty())
            return;

        for (size_t i = 0; i < other.value.size(); i++)
            add(other.value[i]);
    }

    void insertResult(auto & data_to, ColumnArray::Offsets & offsets_to)
    {
        size_t size = value.size();

        offsets_to.push_back(offsets_to.back() + size);

        size_t old_size = data_to.size();
        data_to.resize(old_size + size);

        for (size_t i = 0; i < value.size(); ++i)
            data_to[old_size + i] = value[i];
    }

    void serialize(WriteBuffer & buf) const
    {
        size_t size = value.size();
        writeVarUInt(size, buf);
        for (const auto & elem : value)
            writeIntBinary(elem, buf);
    }

    void deserialize(ReadBuffer & buf)
    {
        size_t size;
        readVarUInt(size, buf);
        T t;
        for (size_t i = 0; i < size; ++i)
        {
            readIntBinary(t, buf);
            value.emplace_back(t);
        }
    }
};


template <typename T, UInt64 Tlimit_num_elem>
struct AggregateFunctionMinKPKData
{
    using List = PODArrayWithStackMemory<T, Tlimit_num_elem>;

    List value;

    static const char * name() { return "minKPK"; }

    void add(T t)
    {
        std::cout << "minKPK add: " << t << std::endl;
        if (value.size() != 0)
        {
            std::cout << "minKPK add(skip): " << t << std::endl;
            return;
        }

        value.emplace_back(t);
    }

    void merge(const AggregateFunctionMinKPKData & other)
    {
        if (other.value.empty())
            return;

        List tmp;
        tmp.insert(std::begin(value), std::end(value));
        tmp.insert(std::begin(other.value), std::end(other.value));
        std::sort(std::begin(tmp), std::end(tmp));

        value.clear();
        T tmp_v = 0;
        for (size_t i = 0; i < std::min(Tlimit_num_elem, tmp.size()); ++i)
        {
            if (i != 0 && tmp_v == tmp[i])
                continue;

            tmp_v = tmp[i];
            value.emplace_back(tmp_v);
        }
    }

    void insertResult(auto & data_to, ColumnArray::Offsets & offsets_to)
    {
        size_t size = value.size();

        offsets_to.push_back(offsets_to.back() + size);

        size_t old_size = data_to.size();
        data_to.resize(old_size + size);

        for (size_t i = 0; i < value.size(); ++i)
            data_to[old_size + i] = value[i];
    }

    void serialize(WriteBuffer & buf) const
    {
        size_t size = value.size();
        writeVarUInt(size, buf);
        for (const auto & elem : value)
            writeIntBinary(elem, buf);
    }

    void deserialize(ReadBuffer & buf)
    {
        size_t size;
        readVarUInt(size, buf);
        T t;
        for (size_t i = 0; i < size; ++i)
        {
            readIntBinary(t, buf);
            value.emplace_back(t);
        }
    }
};

template <typename T, UInt64 Tlimit_num_elem>
struct AggregateFunctionMinKHData
{
    using Set = std::set<T>;

    Set value;

    static const char * name() { return "minKH"; }

    void add(T t)
    {
        if (value.size() < Tlimit_num_elem)
            value.insert(t);
        else
        {
            if (*value.rbegin() > t && value.find(t) == value.end())
            {
                std::cout << *value.rbegin() << ", " << t << std::endl;
                value.erase(*value.rbegin());
                value.insert(t);
            }
        }
    }

    void merge(const AggregateFunctionMinKHData & other)
    {
        if (other.value.empty())
            return;

        auto o = const_cast<AggregateFunctionMinKHData &>(other);

        for (auto & elem : o.value)
            add(elem);
    }

    void insertResult(auto & data_to, ColumnArray::Offsets & offsets_to)
    {
        size_t size = value.size();

        offsets_to.push_back(offsets_to.back() + size);

        size_t old_size = data_to.size();
        data_to.resize(old_size + size);

        size_t i = 0;
        for (auto & elem : value)
        {
            add(elem);
            data_to[old_size + i] = elem;
            std::cout << elem << std::endl;
            i++;
        }
    }

    void serialize(WriteBuffer & buf) const
    {
        size_t size = value.size();
        writeVarUInt(size, buf);
        for (const auto & elem : value)
            writeIntBinary(elem, buf);
    }

    void deserialize(ReadBuffer & buf)
    {
        size_t size;
        readVarUInt(size, buf);
        T t;
        for (size_t i = 0; i < size; ++i)
        {
            readIntBinary(t, buf);
            value.insert(t);
        }
    }
};


/// Puts all values to the hash set. Returns an array of unique values. Implemented for numeric types.
template <template <typename, UInt64> class Data, typename T, UInt64 Tlimit_num_elem>
class AggregateFunctionMinK
    : public IAggregateFunctionDataHelper<Data<T, Tlimit_num_elem>, AggregateFunctionMinK<Data, T, Tlimit_num_elem>>
{
    //static constexpr bool limit_num_elems = Tlimit_num_elem::value;

private:
    using State = Data<T, Tlimit_num_elem>;

public:
    AggregateFunctionMinK(const DataTypePtr & argument_type)
        : IAggregateFunctionDataHelper<Data<T, Tlimit_num_elem>,
          AggregateFunctionMinK<Data, T, Tlimit_num_elem>>({argument_type}, {}) {}

    String getName() const override { return Data<T, Tlimit_num_elem>::name(); }

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
        this->data(place).serialize(buf);
    }

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, Arena *) const override
    {
        this->data(place).deserialize(buf);

        /*
        size_t size;
        readVarUInt(size, buf);

        auto & list = this->data(place).value;
        T t;
        for (size_t i = 0; i < size; ++i)
        {
            readIntBinary(t, buf);
            list.emplace_back(t);
        }
        */
    }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena *) const override
    {
        ColumnArray & arr_to = assert_cast<ColumnArray &>(to);
        ColumnArray::Offsets & offsets_to = arr_to.getOffsets();

        typename ColumnVector<T>::Container & data_to = assert_cast<ColumnVector<T> &>(arr_to.getData()).getData();

        this->data(place).insertResult(data_to, offsets_to);
    }
};

}
