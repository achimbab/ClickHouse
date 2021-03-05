#include <AggregateFunctions/AggregateFunctionSequenceNextNode.h>

int main(int, char **)
{
    using namespace DB;

    auto column_string = ColumnString::create();
    const size_t NUM_VALUES = 10;
    for (size_t i = 0; i < NUM_VALUES; ++i)
    {
        String str = toString(i);
        column_string->insertData(str.data(), str.size());
    }

{
    SequenceNextNodeGeneralData<NodeString, false> data;
    Arena arena;

    for (size_t i = 0; i < NUM_VALUES; ++i)
    {
        NodeString * node = NodeString::allocate(*column_string.get(), NUM_VALUES - i - 1, &arena);
        node->event_time = 1;
        node->events_bitset = 0;

        data.value.push_back(node, &arena);
    }
    
    data.sort();
    std::cout << "ascending" << std::endl;
    for (auto node : data.value)
    {
        std::cout << node->data()[0] << std::endl; 
    }
}

{
    SequenceNextNodeGeneralData<NodeString, true> data;
    Arena arena;

    for (size_t i = 0; i < NUM_VALUES; ++i)
    {
        NodeString * node = NodeString::allocate(*column_string.get(), NUM_VALUES - i - 1, &arena);
        node->event_time = 1;
        node->events_bitset = 0;

        data.value.push_back(node, &arena);
    }
    
    data.sort();
    std::cout << "descending" << std::endl;
    for (auto node : data.value)
    {
        std::cout << node->data()[0] << std::endl; 
    }
}

    return 0;
}
