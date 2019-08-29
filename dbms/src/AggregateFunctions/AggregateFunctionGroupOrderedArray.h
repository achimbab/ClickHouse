#pragma once

#include <IO/WriteHelpers.h>
#include <IO/ReadHelpers.h>

#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeString.h>

#include <Columns/ColumnVector.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnString.h>

#include <Common/ArenaAllocator.h>

#include <AggregateFunctions/IAggregateFunction.h>

#include <common/likely.h>
#include <type_traits>

#define AGGREGATE_FUNCTION_GROUP_ORDERED_ARRAY_MAX_ARRAY_SIZE 0xFFFFFF


namespace DB
{

namespace ErrorCodes
{
    extern const int TOO_LARGE_ARRAY_SIZE;
    extern const int LOGICAL_ERROR;
}

struct ComparePairFirst final
{
    template <typename T1, typename T2>
    bool operator()(const std::pair<T1, T2> & lhs, const std::pair<T1, T2> & rhs) const
    {
        return lhs.first < rhs.first;
    }
};


struct GroupOrderedArrayData
{
    using TimestampElem = std::pair<UInt32, StringRef>;
    using TimestampElems = PODArray<TimestampElem, 64>;
    using Comparator = ComparePairFirst;

    bool sorted = false;
    TimestampElems elem_list;


    size_t size() const
    {
        return elem_list.size();
    }

    void add(const UInt32 timestamp, StringRef elem)
    {
        elem_list.emplace_back(timestamp, elem);
    }

    void merge(const GroupOrderedArrayData & other)
    {
        const auto size = elem_list.size();

        elem_list.insert(std::begin(other.elem_list), std::end(other.elem_list));

        /// either sort whole container or do so partially merging ranges afterwards
        if (!sorted && !other.sorted)
            std::stable_sort(std::begin(elem_list), std::end(elem_list), Comparator{});
        else
        {
            const auto begin = std::begin(elem_list);
            const auto middle = std::next(begin, size);
            const auto end = std::end(elem_list);

            if (!sorted)
                std::stable_sort(begin, middle, Comparator{});

            if (!other.sorted)
                std::stable_sort(middle, end, Comparator{});

            std::inplace_merge(begin, middle, end, Comparator{});
        }
    }

    void sort()
    {
        if (!sorted)
        {
            std::stable_sort(std::begin(elem_list), std::end(elem_list), Comparator{});
            sorted = true;
        }
    }

    void serialize(WriteBuffer & buf) const
    {
        writeBinary(sorted, buf);
        writeBinary(elem_list.size(), buf);

        for (const auto & elem : elem_list)
        {
            writeBinary(elem.first, buf);
            //writeStringBinary(elem.second, buf);
        }
    }

    void deserialize(ReadBuffer & buf)
    {
        readBinary(sorted, buf);

        size_t size;
        readBinary(size, buf);

        elem_list.clear();
        elem_list.reserve(size);

        UInt32 timestamp;
        StringRef str;

        for (size_t i = 0; i < size; ++i)
        {
            readBinary(timestamp, buf);
            //readStringBinary(str, buf);
            //elem_list.emplace_back(timestamp, str);
        }
    }
};


class GroupOrderedArray final
    : public IAggregateFunctionDataHelper<GroupOrderedArrayData, GroupOrderedArray>
{
public:
    GroupOrderedArray(const DataTypes & arguments, const Array & params)
        : IAggregateFunctionDataHelper<GroupOrderedArrayData, GroupOrderedArray>(arguments, params)
    {}

    String getName() const override { return "groupOrderedArray"; }

    DataTypePtr getReturnType() const override { return std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>()); }

    void add(AggregateDataPtr place, const IColumn ** columns, size_t row_num, Arena *) const override
    {
        const auto timestamp = static_cast<const ColumnVector<UInt32> *>(columns[0])->getData()[row_num];
        // reverse iteration and stable sorting are needed for events that are qualified by more than one condition.
        auto val = static_cast<const ColumnString *>(columns[1])->getDataAt(row_num);
        this->data(place).add(timestamp, val);
    }

    void merge(AggregateDataPtr place, ConstAggregateDataPtr rhs, Arena *) const override
    {
      this->data(place).merge(this->data(rhs));
    }

    void serialize(ConstAggregateDataPtr place, WriteBuffer & buf) const override
    {
        this->data(place).serialize(buf);
    }

    void deserialize(AggregateDataPtr place, ReadBuffer & buf, Arena *) const override
    {
        this->data(place).deserialize(buf);
    }

    void insertResultInto(ConstAggregateDataPtr place, IColumn & to) const override
    {
        auto & data = this->data(place);
        const_cast<Data &>(data).sort();

        auto & arr_to = static_cast<ColumnArray &>(to);
        auto & offsets = arr_to.getOffsets();

        IColumn & data_to = arr_to.getData();

        const StringRef *prev = NULL;
        int uniqCnt = 0;
        for (const auto & pair : data.elem_list)
        {
            if (prev == NULL || (prev->size != pair.second.size || strncmp(prev->data, pair.second.data, prev->size) != 0))
            {
                uniqCnt++;
                prev = &pair.second;
                static_cast<ColumnString &>(data_to).insertData(pair.second.data, pair.second.size);
            }
        }

        offsets.push_back(offsets.back() + uniqCnt);
    }

    const char * getHeaderFilePath() const override { return __FILE__; }
};

#undef AGGREGATE_FUNCTION_GROUP_ORDERED_ARRAY_MAX_ARRAY_SIZE

}
