#pragma once

#include <vector>
#include <memory>
#include <cstddef>
#include <string>
#include <Core/Field.h>
#include <Core/SettingsEnums.h>

class Collator;

namespace DB
{

struct FillColumnDescription
{
    /// All missed values in range [FROM, TO) will be filled
    /// Range [FROM, TO) respects sorting direction
    Field fill_from;        /// Fill value >= FILL_FROM
    Field fill_to;          /// Fill value + STEP < FILL_TO
    Field fill_step;        /// Default = 1 or -1 according to direction
};

/// Description of the sorting rule by one column.
struct SortColumnDescription
{
    std::string column_name; /// The name of the column.
    size_t column_number;    /// Column number (used if no name is given).
    int direction;           /// 1 - ascending, -1 - descending.
    int nulls_direction;     /// 1 - NULLs and NaNs are greater, -1 - less.
                             /// To achieve NULLS LAST, set it equal to direction, to achieve NULLS FIRST, set it opposite.
    std::shared_ptr<Collator> collator; /// Collator for locale-specific comparison of strings
    bool with_fill;
    FillColumnDescription fill_description;

    std::vector<std::string> function_type_arguments; /// Collection of argument's names for function types.

    SortColumnDescription(
            size_t column_number_, int direction_, int nulls_direction_,
            const std::shared_ptr<Collator> & collator_ = nullptr,
            bool with_fill_ = false, const FillColumnDescription & fill_description_ = {},
            std::vector<std::string> function_type_arguments_ = {})
            : column_number(column_number_), direction(direction_), nulls_direction(nulls_direction_), collator(collator_)
            , with_fill(with_fill_), fill_description(fill_description_), function_type_arguments(function_type_arguments_) {}

    SortColumnDescription(
            const std::string & column_name_, int direction_, int nulls_direction_,
            const std::shared_ptr<Collator> & collator_ = nullptr,
            bool with_fill_ = false, const FillColumnDescription & fill_description_ = {},
            std::vector<std::string> function_type_arguments_ = {})
            : column_name(column_name_), column_number(0), direction(direction_), nulls_direction(nulls_direction_)
            , collator(collator_), with_fill(with_fill_), fill_description(fill_description_), function_type_arguments(function_type_arguments_) {}

    bool operator == (const SortColumnDescription & other) const
    {
        return column_name == other.column_name && column_number == other.column_number
            && direction == other.direction && nulls_direction == other.nulls_direction;
    }

    bool operator != (const SortColumnDescription & other) const
    {
        return !(*this == other);
    }

    std::string dump() const
    {
        return fmt::format("{}:{}:dir {}nulls ", column_name, column_number, direction, nulls_direction);
    }
};

/// Description of the sorting rule for several columns.
using SortDescription = std::vector<SortColumnDescription>;

class Block;

/// Outputs user-readable description into `out`.
void dumpSortDescription(const SortDescription & description, const Block & header, WriteBuffer & out);

std::string dumpSortDescription(const SortDescription & description);

}
