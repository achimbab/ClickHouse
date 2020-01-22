#include <Functions/countAccumulate.h>
#include <Functions/FunctionFactory.h>


namespace DB
{

void registerFunctionCountAccumulate(FunctionFactory & factory)
{
    factory.registerFunction<FunctionCountAccumulateImpl>();
}

}
