#pragma once

#include <Common/config.h>

#if !defined(ARCADIA_BUILD) && USE_STATS

#    include <common/types.h>
#    include <Common/PODArray.h>

#    include <algorithm>
#    include <iostream>
#    include <vector>


namespace DB
{

struct Variant
{
    Float64 x;
    Float64 y;
    Float64 beats_control;
    Float64 best;
    std::vector<Float64> samples;

    Variant(const Float64 _x, const Float64 _y) : x(_x), y(_y), beats_control(0.0), best(0.0), samples() {}
};

using Variants = PODArray<Variant>;

template <bool higher_is_better>
Variants bayesian_ab_test(String distribution, PODArray<Float64> & xs, PODArray<Float64> & ys);

String convertToJson(const PODArray<String> & variant_names, Variants & variants, const bool include_density);

}

#endif
