#include <Functions/IFunctionImpl.h>
#include <Functions/FunctionHelpers.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <Common/assert_cast.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/NumberTraits.h>
#include <DataTypes/DataTypeNullable.h>
#include <iostream>
#include <map>


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

class FunctionCountAccumulateImpl : public IFunction
{
private:
    /// It is possible to track value from previous block, to calculate continuously across all blocks. Not implemented.

    template <typename Src, typename Dst>
    static void process(const PaddedPODArray<Src> & src, const DB::ColumnString * groupby, PaddedPODArray<Dst> & dst)
    {
        std::map<String, Src> partitions;
        size_t size = src.size();
        dst.resize(size);

        if (size == 0)
            return;

        for (size_t i = 0; i < size; ++i)
        {
            std::cout << "    GROUP_BY " << i << " : " << groupby->getDataAt(i).data << std::endl;
            auto prev = partitions.find(groupby->getDataAt(i).data);
            if (prev != partitions.end())
            {
                dst[i] = prev->second + src[i];
                prev->second = dst[i];
                std::cout << "    GROUP_BY " << i << " : " << prev->second << ", " << src[i] << ", " << dst[i] << std::endl;
            }
            else
            {
                dst[i] = src[i];
                partitions.emplace(std::make_pair(groupby->getDataAt(i).data, src[i]));
                std::cout << "    GROUP_BY " << i << " : " << dst[i] << std::endl;
            }
        }
    }

    /// Result type is same as result of subtraction of argument types.
    template <typename SrcFieldType>
    using DstFieldType = typename NumberTraits::ResultOfSubtraction<SrcFieldType, SrcFieldType>::Type;

    /// Call polymorphic lambda with tag argument of concrete field type of src_type.
    template <typename F>
    void dispatchForSourceType(const IDataType & src_type, F && f) const
    {
        WhichDataType which(src_type);

        if (which.isUInt8())
            f(UInt8());
        else if (which.isUInt16())
            f(UInt16());
        else if (which.isUInt32())
            f(UInt32());
        else if (which.isUInt64())
            f(UInt64());
        else if (which.isInt8())
            f(Int8());
        else if (which.isInt16())
            f(Int16());
        else if (which.isInt32())
            f(Int32());
        else if (which.isInt64())
            f(Int64());
        else if (which.isFloat32())
            f(Float32());
        else if (which.isFloat64())
            f(Float64());
        else if (which.isDate())
            f(DataTypeDate::FieldType());
        else if (which.isDateTime())
            f(DataTypeDateTime::FieldType());
        else
            throw Exception("Argument for function " + getName() + " must have numeric type.", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
    }

public:
    static constexpr auto name = "countAccumulate";

    static FunctionPtr create(const Context &)
    {
        return std::make_shared<FunctionCountAccumulateImpl>();
    }

    String getName() const override
    {
        return name;
    }

    bool isStateful() const override
    {
        return true;
    }

    size_t getNumberOfArguments() const override
    {
        return 2;
    }

    bool isDeterministic() const override { return false; }
    bool isDeterministicInScopeOfQuery() const override
    {
        return false;
    }

    bool useDefaultImplementationForNulls() const override { return false; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        DataTypePtr res;
        dispatchForSourceType(*removeNullable(arguments[0]), [&](auto field_type_tag)
        {
            res = std::make_shared<DataTypeNumber<DstFieldType<decltype(field_type_tag)>>>();
        });

        if (arguments[0]->isNullable())
            res = makeNullable(res);

        return res;
    }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result, size_t input_rows_count) override
    {
        auto & src = block.getByPosition(arguments.at(0));
        auto & groupby = block.getByPosition(arguments.at(1));
        const auto & res_type = block.getByPosition(result).type;

        /// When column is constant, its difference is zero.
        if (isColumnConst(*src.column))
        {
            block.getByPosition(result).column = res_type->createColumnConstWithDefaultValue(input_rows_count);
            return;
        }

        auto res_column = removeNullable(res_type)->createColumn();
        auto * src_column = src.column.get();
        if (auto * nullable_column = checkAndGetColumn<ColumnNullable>(src_column))
        {
            src_column = &nullable_column->getNestedColumn();
        }

        // TODO : null_map
        const ColumnString * groupby_column = typeid_cast<const ColumnString *>(groupby.column.get());

        dispatchForSourceType(*removeNullable(src.type), [&](auto src_field_type_tag)
        {
            using SrcFieldType = decltype(src_field_type_tag);

            process(assert_cast<const ColumnVector<SrcFieldType> &>(*src_column).getData(),
                groupby_column,
                assert_cast<ColumnVector<DstFieldType<SrcFieldType>> &>(*res_column).getData());
        });

        block.getByPosition(result).column = std::move(res_column);
    }
};

}
