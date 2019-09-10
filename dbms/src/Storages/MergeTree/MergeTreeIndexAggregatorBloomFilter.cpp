#include <Storages/MergeTree/MergeTreeIndexAggregatorBloomFilter.h>

#include <ext/bit_cast.h>
#include <DataTypes/IDataType.h>
#include <Columns/IColumn.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnFixedString.h>
#include <Common/HashTable/Hash.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/BloomFilterHash.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int ILLEGAL_COLUMN;
}

MergeTreeIndexAggregatorBloomFilter::MergeTreeIndexAggregatorBloomFilter(
    size_t bits_per_row_, size_t hash_functions_, const Names & columns_name_)
    : bits_per_row(bits_per_row_), hash_functions(hash_functions_), index_columns_name(columns_name_)
{
}

bool MergeTreeIndexAggregatorBloomFilter::empty() const
{
    return !total_rows;
}

MergeTreeIndexGranulePtr MergeTreeIndexAggregatorBloomFilter::getGranuleAndReset()
{
    const auto granule = std::make_shared<MergeTreeIndexGranuleBloomFilter>(bits_per_row, hash_functions, total_rows, granule_index_blocks);
    total_rows = 0;
    granule_index_blocks.clear();
    return granule;
}

void MergeTreeIndexAggregatorBloomFilter::update(const Block & block, size_t * pos, size_t limit)
{
    if (*pos >= block.rows())
        throw Exception("The provided position is not less than the number of block rows. Position: " + toString(*pos) + ", Block rows: " +
                        toString(block.rows()) + ".", ErrorCodes::LOGICAL_ERROR);

    Block granule_index_block;
    size_t max_read_rows = std::min(block.rows() - *pos, limit);

    for (size_t index = 0; index < index_columns_name.size(); ++index)
    {
        const auto & column_and_type = block.getByName(index_columns_name[index]);

        const ColumnArray * array_col = typeid_cast<const ColumnArray *>(column_and_type.column.get());
        if (array_col)
        {
            // Get nested type
            const IColumn * nested_col = nullptr;
            const IDataType * nested_type = nullptr;
            if (const ColumnNullable *nullable = checkAndGetColumn<ColumnNullable>(array_col->getData()))
            {
                nested_col = nullable->getNestedColumnPtr().get();
                nested_type = static_cast<const DataTypeNullable *>(column_and_type.type.get())->getNestedType().get();
            }
            else
            {
                nested_col = array_col->getDataPtr().get();
                nested_type = static_cast<const DataTypeArray *>(column_and_type.type.get())->getNestedType().get();
            }

            if (!nested_col)
                throw Exception("Not supported type " + array_col->getName(), ErrorCodes::ILLEGAL_COLUMN);

            const ColumnString * nc = checkAndGetColumn<ColumnString>(nested_col);
            if (!nc)
                throw Exception("Not supported type " + array_col->getName(), ErrorCodes::ILLEGAL_COLUMN);

            size_t num_elems = 0;
            const ColumnArray::Offsets & offsets = array_col->getOffsets();
            for (size_t i = 0; i < max_read_rows; ++i)
            {
                num_elems += offsets[i + (*pos)];
            }

            const auto & index_column = BloomFilterHash::hashWithColumn(nested_type, nested_col, *pos, num_elems);
            granule_index_block.insert({std::move(index_column), std::make_shared<DataTypeUInt64>(), column_and_type.name});
            *pos += num_elems;
            total_rows += max_read_rows;
        }
        else
        {
            const auto & index_column = BloomFilterHash::hashWithColumn(column_and_type.type, column_and_type.column, *pos, max_read_rows);
            granule_index_block.insert({std::move(index_column), std::make_shared<DataTypeUInt64>(), column_and_type.name});
            *pos += max_read_rows;
            total_rows += max_read_rows;
        }
    }

    granule_index_blocks.push_back(granule_index_block);
}

}
