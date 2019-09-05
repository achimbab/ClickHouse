#include "arrayIndex.h"
#include <Functions/FunctionFactory.h>


namespace DB
{

struct NameIndexOf { static constexpr auto name = "indexOf"; };

/// indexOf(arr, x) - returns the index of the element x (starting with 1), if it exists in the array, or 0 if it is not.
using FunctionIndexOf = FunctionArrayIndex<IndexIdentity, NameIndexOf>;

struct NameBloomFilterExact { static constexpr auto name = "bloomFilterExact"; };

/// indexOf(arr, x) - returns the index of the element x (starting with 1), if it exists in the array, or 0 if it is not.
using FunctionBloomFilterExact = FunctionArrayIndex2<IndexIdentity, NameBloomFilterExact>;

void registerFunctionIndexOf(FunctionFactory & factory)
{
    factory.registerFunction<FunctionIndexOf>();
    // TODO
    factory.registerFunction<FunctionBloomFilterExact>();
}


}
