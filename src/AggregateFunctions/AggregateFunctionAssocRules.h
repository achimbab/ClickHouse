#pragma once

#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>
#include <IO/Operators.h>

#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeDateTime.h>

#include <Columns/ColumnString.h>
#include <Columns/ColumnVector.h>
#include <Columns/ColumnNullable.h>

#include <Common/ArenaAllocator.h>
#include <Common/assert_cast.h>

#include <AggregateFunctions/IAggregateFunction.h>
#include <AggregateFunctions/AggregateFunctionNull.h>

#include <type_traits>


namespace DB
{

static constexpr UInt64 MaxCardinality = 64;

using Item = String;
using Transaction = std::vector<Item>;
using Transactions = std::map<UInt64, Transaction>;
using ItemFrequency = std::map<Item, UInt64>;

struct AssocRulesData
{
    ItemFrequency item_freq; // TODO use it for optimization
    Transactions tran; // TODO use compressed form for optimization
    // TID -> List of Items
    // Compressed : List of Items -> List of TIDs
    // Item -> Frequency
};

using TransformedPrefixPath = std::pair<std::vector<Item>, UInt64>;
using Pattern = std::pair<std::set<Item>, UInt64>;
struct PatternComparator
{
    bool operator()(const Pattern & lhs, const Pattern & rhs) const
    {
        return lhs.first < rhs.first;
    }
};
using Patterns = std::set<Pattern, PatternComparator>;

struct FPNode
{
    const Item item;
    UInt64 frequency;
    std::shared_ptr<FPNode> node_link;
    std::weak_ptr<FPNode> parent;
    std::vector<std::shared_ptr<FPNode>> children;

    FPNode(const Item & _item, const std::shared_ptr<FPNode> & _parent) :
        item(_item), 
        frequency(1), 
        node_link(nullptr), 
        parent(_parent), 
        children()
    {
    }
};

struct FPTree {
    std::shared_ptr<FPNode> root;
    std::map<Item, std::shared_ptr<FPNode>> header_table;
    UInt64 minimum_support_threshold;

    FPTree(const std::vector<Transaction>& transactions, 
        UInt64 _minimum_support_threshold) :
        root(std::make_shared<FPNode>(Item{}, nullptr)), 
        header_table(),
        minimum_support_threshold(_minimum_support_threshold)
    {
        // scan the transactions counting the frequency of each item
        std::map<Item, UInt64> frequency_by_item;
        for ( const Transaction& transaction : transactions ) {
            for ( const Item& item : transaction ) {
                ++frequency_by_item[item];
            }
        }

        // keep only items which have a frequency greater or equal than the minimum support threshold
        for ( auto it = frequency_by_item.cbegin(); it != frequency_by_item.cend(); ) {
            const UInt64 item_frequency = (*it).second;
            if ( item_frequency < minimum_support_threshold ) { frequency_by_item.erase( it++ ); }
            else { ++it; }
        }

        // order items by decreasing frequency
        struct frequency_comparator
        {
            bool operator()(const std::pair<Item, UInt64> &lhs, const std::pair<Item, UInt64> &rhs) const
            {
                return std::tie(lhs.second, lhs.first) > std::tie(rhs.second, rhs.first);
            }
        };
        std::set<std::pair<Item, UInt64>, frequency_comparator> items_ordered_by_frequency(frequency_by_item.cbegin(), frequency_by_item.cend());

        // start tree construction

        // scan the transactions again
        for ( const Transaction& transaction : transactions ) {
            auto curr_fpnode = root;

            // select and sort the frequent items in transaction according to the order of items_ordered_by_frequency
            for ( const auto& pair : items_ordered_by_frequency ) {
                const Item& item = pair.first;

                // check if item is contained in the current transaction
                if ( std::find( transaction.cbegin(), transaction.cend(), item ) != transaction.cend() ) {
                    // insert item in the tree

                    // check if curr_fpnode has a child curr_fpnode_child such that curr_fpnode_child.item = item
                    const auto it = std::find_if(
                        curr_fpnode->children.cbegin(), curr_fpnode->children.cend(),  [item](const std::shared_ptr<FPNode>& fpnode) {
                            return fpnode->item == item;
                    } );
                    if ( it == curr_fpnode->children.cend() ) {
                        // the child doesn't exist, create a new node
                        const auto curr_fpnode_new_child = std::make_shared<FPNode>( item, curr_fpnode );

                        // add the new node to the tree
                        curr_fpnode->children.push_back( curr_fpnode_new_child );

                        // update the node-link structure
                        if ( header_table.count( curr_fpnode_new_child->item ) ) {
                            auto prev_fpnode = header_table[curr_fpnode_new_child->item];
                            while ( prev_fpnode->node_link ) { prev_fpnode = prev_fpnode->node_link; }
                            prev_fpnode->node_link = curr_fpnode_new_child;
                        }
                        else {
                            header_table[curr_fpnode_new_child->item] = curr_fpnode_new_child;
                        }

                        // advance to the next node of the current transaction
                        curr_fpnode = curr_fpnode_new_child;
                    }
                    else {
                        // the child exist, increment its frequency
                        auto curr_fpnode_child = *it;
                        ++curr_fpnode_child->frequency;

                        // advance to the next node of the current transaction
                        curr_fpnode = curr_fpnode_child;
                    }
                }
            }
        }
    }

    bool empty() const
    {
        assert( root );
        return root->children.size() == 0;
    }
};

bool contains_single_path(const std::shared_ptr<FPNode>& fpnode)
{
    assert( fpnode );
    if ( fpnode->children.size() == 0 ) { return true; }
    if ( fpnode->children.size() > 1 ) { return false; }
    return contains_single_path( fpnode->children.front() );
}
bool contains_single_path(const FPTree& fptree)
{
    return fptree.empty() || contains_single_path( fptree.root );
}

Patterns fptree_growth(const FPTree& fptree)
{
    if ( fptree.empty() ) { return {}; }

    if ( contains_single_path( fptree ) ) {
        // generate all possible combinations of the items in the tree

        Patterns single_path_patterns;

        // for each node in the tree
        assert( fptree.root->children.size() == 1 );
        auto curr_fpnode = fptree.root->children.front();
        while ( curr_fpnode ) {
            const Item& curr_fpnode_item = curr_fpnode->item;
            const UInt64 curr_fpnode_frequency = curr_fpnode->frequency;

            // add a pattern formed only by the item of the current node
            Pattern new_pattern{ { curr_fpnode_item }, curr_fpnode_frequency };
            single_path_patterns.insert( new_pattern );

            // create a new pattern by adding the item of the current node to each pattern generated until now
            for ( const Pattern& pattern : single_path_patterns ) {
                Pattern new_pattern2{ pattern };
                new_pattern2.first.insert( curr_fpnode_item );
                assert( curr_fpnode_frequency <= pattern.second );
                new_pattern2.second = curr_fpnode_frequency;

                single_path_patterns.insert(new_pattern2);
            }

            // advance to the next node until the end of the tree
            assert( curr_fpnode->children.size() <= 1 );
            if ( curr_fpnode->children.size() == 1 ) { curr_fpnode = curr_fpnode->children.front(); }
            else { curr_fpnode = nullptr; }
        }

        return single_path_patterns;
    }
    else {
        // generate conditional fptrees for each different item in the fptree, then join the results

        Patterns multi_path_patterns;

        // for each item in the fptree
        for ( const auto& pair : fptree.header_table ) {
            const Item& curr_item = pair.first;

            // build the conditional fptree relative to the current item

            // start by generating the conditional pattern base
            std::vector<TransformedPrefixPath> conditional_pattern_base;

            // for each path in the header_table (relative to the current item)
            auto path_starting_fpnode = pair.second;
            while ( path_starting_fpnode ) {
                // construct the transformed prefix path

                // each item in th transformed prefix path has the same frequency (the frequency of path_starting_fpnode)
                const UInt64 path_starting_fpnode_frequency = path_starting_fpnode->frequency;

                auto curr_path_fpnode = path_starting_fpnode->parent.lock();
                // check if curr_path_fpnode is already the root of the fptree
                if ( curr_path_fpnode->parent.lock() ) {
                    // the path has at least one node (excluding the starting node and the root)
                    TransformedPrefixPath transformed_prefix_path{ {}, path_starting_fpnode_frequency };

                    while ( curr_path_fpnode->parent.lock() ) {
                        assert( curr_path_fpnode->frequency >= path_starting_fpnode_frequency );
                        transformed_prefix_path.first.push_back( curr_path_fpnode->item );

                        // advance to the next node in the path
                        curr_path_fpnode = curr_path_fpnode->parent.lock();
                    }

                    conditional_pattern_base.push_back( transformed_prefix_path );
                }

                // advance to the next path
                path_starting_fpnode = path_starting_fpnode->node_link;
            }

            // generate the transactions that represent the conditional pattern base
            std::vector<Transaction> conditional_fptree_transactions;
            for ( const TransformedPrefixPath& transformed_prefix_path : conditional_pattern_base ) {
                const std::vector<Item>& transformed_prefix_path_items = transformed_prefix_path.first;
                const UInt64 transformed_prefix_path_items_frequency = transformed_prefix_path.second;

                Transaction transaction = transformed_prefix_path_items;

                // add the same transaction transformed_prefix_path_items_frequency times
                for ( UInt64 i = 0; i < transformed_prefix_path_items_frequency; ++i ) {
                    conditional_fptree_transactions.push_back( transaction );
                }
            }

            // build the conditional fptree relative to the current item with the transactions just generated
            const FPTree conditional_fptree( conditional_fptree_transactions, fptree.minimum_support_threshold );
            // call recursively fptree_growth on the conditional fptree (empty fptree: no patterns)
            Patterns conditional_patterns = fptree_growth( conditional_fptree );

            // construct patterns relative to the current item using both the current item and the conditional patterns
            Patterns curr_item_patterns;

            // the first pattern is made only by the current item
            // compute the frequency of this pattern by summing the frequency of the nodes which have the same item (follow the node links)
            UInt64 curr_item_frequency = 0;
            auto fpnode = pair.second;
            while ( fpnode ) {
                curr_item_frequency += fpnode->frequency;
                fpnode = fpnode->node_link;
            }
            // add the pattern as a result
            Pattern pattern{ { curr_item }, curr_item_frequency };
            curr_item_patterns.insert( pattern );

            // the next patterns are generated by adding the current item to each conditional pattern
            for ( const Pattern& pattern2 : conditional_patterns ) {
                Pattern new_pattern{pattern2};
                new_pattern.first.insert( curr_item );
                assert( curr_item_frequency >= pattern2.second );
                new_pattern.second = pattern2.second;

                curr_item_patterns.insert( { new_pattern } );
            }

            // join the patterns generated by the current item with all the other items of the fptree
            multi_path_patterns.insert( curr_item_patterns.cbegin(), curr_item_patterns.cend() );
        }

        return multi_path_patterns;
    }
}

struct AssocRuleResult
{
    std::vector<Item> antecedent;
    std::vector<Item> consequent;
    Float64 sAC;
    Float64 sA;
    Float64 sC;
    Float64 support;
    Float64 confidence;
    Float64 lift;
    Float64 conviction;
    Float64 leverage;
};

using AssocRuleResults = std::vector<AssocRuleResult>;

/* input ---> a set of elements
   data ---> Temporary vector to store current combination
   start & end ---> Staring and Ending indexes in input
   index ---> Current index in data
   r ---> Size of a combination to be printed */
void combinations(std::vector<std::vector<Item>> & result, std::vector<Item> & input, std::vector<Item> data,
        int start, int end,
        int index, int r)
{
    // Current combination is ready
    // to be printed, print it
    if (index == r)
    {
        result.push_back(data);
        return;
    }

    // replace index with all possible
    // elements. The condition "end-i+1 >= r-index"
    // makes sure that including one element
    // at index will make a combination with
    // remaining elements at remaining positions
    for (int i = start; i <= end &&
            end - i + 1 >= r - index; i++)
    {
        data[index] = input[i];
        combinations(result, input, data, i+1, end, index+1, r);
    }
}

// Get Result

/*
assoc_rules(  user_tsid -- Transaction ID col
            , common_service_code -- Item col
           )
*/

// Implementation of Association Rules
class AssocRulesImpl final
    : public IAggregateFunctionDataHelper<AssocRulesData, AssocRulesImpl>
{
    using Data = AssocRulesData;
    static Data & data(AggregateDataPtr place) { return *reinterpret_cast<Data *>(place); }
    static const Data & data(ConstAggregateDataPtr place) { return *reinterpret_cast<const Data *>(place); }
    static constexpr size_t TransactionCol = 1;
    static constexpr size_t ItemCol = 2;

    // TODO
    DataTypePtr & data_type;
    Float64 min_support;

public:
    AssocRulesImpl(const DataTypePtr & data_type_, [[maybe_unused]] const DataTypes & arguments)
        : IAggregateFunctionDataHelper<AssocRulesData, AssocRulesImpl>(
            {data_type_}, {})
        , data_type(this->argument_types[0])
    {
    }

    String getName() const override { return "assocRules"; }

    DataTypePtr getReturnType() const override { return data_type; }

    void add([[maybe_unused]] AggregateDataPtr __restrict place, [[maybe_unused]] const IColumn ** columns, [[maybe_unused]] size_t row_num, [[maybe_unused]] Arena *) const override
    {
        const UInt64 tran_id = assert_cast<const ColumnVector<UInt64> *>(columns[0])->getData()[row_num];
        const auto item = assert_cast<const ColumnString *>(columns[1])->getDataAt(row_num);

        auto & d = data(place);

        d.item_freq[item.data]++;

        auto t = d.tran.find(tran_id);
        if (t != d.tran.end())
            t->second.push_back(item.data);
        else
            d.tran[tran_id] = {item.data};
    }

    void merge([[maybe_unused]] AggregateDataPtr __restrict place, [[maybe_unused]] ConstAggregateDataPtr rhs, [[maybe_unused]] Arena *) const override
    {
        auto & d = data(place);
        const auto & other = data(rhs);

        for (auto & kv : other.tran)
        {
            auto t = d.tran.find(kv.first);
            if (t == d.tran.end())
                d.tran[kv.first] = kv.second;
            else
                for (auto & item : kv.second)
                    t->second.push_back(item);
        }
    }

    void serialize([[maybe_unused]] ConstAggregateDataPtr __restrict place, [[maybe_unused]] WriteBuffer & buf) const override
    {
        auto & d = data(place);

        // TODO: item_freq

        writeBinary(d.tran.size(), buf);
        for (auto & kv : d.tran)
        {
            writeBinary(kv.first, buf);

            writeBinary(kv.second.size(), buf);
            for (auto & item : kv.second)
            {
                writeVarUInt(item.size(), buf);
                buf.write(item.c_str(), item.size());
            }
        }
    }

    void deserialize([[maybe_unused]] AggregateDataPtr __restrict place, [[maybe_unused]] ReadBuffer & buf, [[maybe_unused]] Arena * arena) const override
    {
        auto & d = data(place);

        size_t tran_size;
        readBinary(tran_size, buf);

        for (size_t i = 0; i < tran_size; ++i)
        {
            UInt64 tran_id;
            readBinary(tran_id, buf);

            d.tran[tran_id] = {};

            size_t item_size;
            readBinary(item_size, buf);

            for (size_t item_idx = 0; item_idx < item_size; ++item_idx)
            {
                Item item;
                readStringBinary(item, buf);
                d.tran[tran_id].push_back(item);
            }
        }
    }

    void insertResultInto([[maybe_unused]] AggregateDataPtr __restrict place, [[maybe_unused]] IColumn & to, [[maybe_unused]] Arena *) const override
    {
        auto & d = data(place);

        std::vector<Transaction> trans;
        std::transform(d.tran.begin(), d.tran.end(), std::back_inserter(trans), 
            [](const auto & kv){ return kv.second; });

        const UInt64 N = d.tran.size();

        const FPTree fptree{ trans, 0 };

        [[maybe_unused]] const Patterns patterns = fptree_growth(fptree);

        std::stringstream ss;
        for (auto & p : patterns)
        {
            //std::sort(p.first.begin(), p.first.end());

            std::copy(p.first.begin(), p.first.end(), 
                std::ostream_iterator<std::string>(ss, ","));
            ss << ": " << p.second << std::endl; 
        }

        AssocRuleResults rules;

        for (auto & p : patterns)
        {
            std::vector<Item> items;
            for (auto item : p.first)
                items.push_back(item);
            std::sort(items.begin(), items.end());
            for (auto kkk : items)
            {
                std::cout << kkk << std::endl;
            }

            for (int idx = p.first.size() - 1; idx >= 0; --idx)
            {
                std::vector<std::vector<Item>> combs;

                std::vector<Item> tmp;
                tmp.resize(idx);

                combinations(combs, items, tmp, 0, items.size() - 1, 0, idx);

                for (auto & comb : combs)
                {
                    if (comb.size() == 0)
                        continue;

                    std::sort(comb.begin(), comb.end());

                    AssocRuleResult rule;
                    rule.antecedent = comb;
                    std::set_difference(items.begin(), items.end(), comb.begin(), comb.end(),
                            std::inserter(rule.consequent, rule.consequent.begin()));
                    rule.sAC = p.second;

                    {
                    Pattern aset;
                    for (auto v : rule.antecedent)
                        aset.first.insert(v);
                    auto it = patterns.find(aset);
                    rule.sA = static_cast<Float64>(it->second);
                    rule.sA /= N;
                    }

                    {
                    Pattern cset;
                    for (auto v : rule.consequent)
                        cset.first.insert(v);
                    auto it = patterns.find(cset);
                    rule.sC = static_cast<Float64>(it->second);
                    rule.sC /= N;
                    }

                    rule.support = static_cast<Float64>(p.second);
                    rule.support /= N;

                    rule.confidence = rule.support / rule.sA;
                    rule.lift = std::max(rule.confidence / rule.sC, .0);
                    rule.leverage = rule.support - (rule.sA * rule.sC);
                    rule.conviction = INFINITY;
                    if (rule.confidence < 1.0)
                        rule.conviction = std::max((1 - rule.sC) / (1 - rule.confidence), .0);
                    if (rule.leverage < -1.0)
                        rule.leverage = -1.0;
                    else if (rule.leverage > 1.0)
                        rule.leverage = 1.0;

                    rules.push_back(rule);
                }
            }
        }

        ss << "antecedents consequents  antecedent support  consequent support  support  confidence  lift  leverage  conviction\n";
        for (auto & rule : rules)
        {
            ss << "(";
            std::copy(rule.antecedent.begin(), rule.antecedent.end(), 
                std::ostream_iterator<std::string>(ss, ","));
            ss.seekp(-1, std::ios_base::end);
            ss << ")";

            ss << " (";
            std::copy(rule.consequent.begin(), rule.consequent.end(), 
                std::ostream_iterator<std::string>(ss, ","));
            ss.seekp(-1, std::ios_base::end);
            ss << ")";

            ss << " " << rule.sA;
            ss << " " << rule.sC;
            ss << " " << rule.support;
            ss << " " << rule.confidence;
            ss << " " << rule.lift;
            ss << " " << rule.leverage;
            ss << " " << rule.conviction << std::endl;
        }

        std::string result_str = ss.str();
        assert_cast<ColumnString &>(to).insertData(result_str.c_str(), result_str.length());
    }
};

}
