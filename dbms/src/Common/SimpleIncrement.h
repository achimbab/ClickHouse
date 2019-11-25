#pragma once

#include <common/likely.h>
#include <common/Types.h>
#include <atomic>


/** Is used for numbering of files.
  */
struct SimpleIncrement
{
    std::atomic<UInt64> value;

    SimpleIncrement(UInt64 start = 0) : value(start) {}

    void set(UInt64 new_value)
    {
        value = new_value;
    }

    UInt64 get(const bool increment = false)
    {
        if (unlikely(increment))
            return value;
        else
            return ++value;
    }
};
