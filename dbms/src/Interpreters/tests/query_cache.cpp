#include <iostream>
#include <atomic>

#include <Columns/ColumnsNumber.h>
#include <Common/ThreadPool.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/QueryCache.h>

using namespace DB;
using namespace std;

#ifdef ASSERT
#undef ASSERT
#endif
#define ASSERT(cond) \
do \
{ \
    if (!(cond)) \
    { \
        cout << __FILE__ << ":" << __LINE__ << ":" \
            << "Assertion " << #cond << " failed.\n"; \
        exit(1); \
    } \
} \
while (0)

using Ptr = QueryCache::MappedPtr;

Ptr toPtr(const QueryResult & res)
{
    return Ptr(make_shared<QueryResult>(res));
}

void test_basic_operations()
{
    const char * KEY = "query";

    QueryCache cache(1024*1024);

    ASSERT(!cache.get(KEY));
    cache.set(KEY, toPtr(QueryResult{}));
    ASSERT(cache.get(KEY));
}

void test_concurrency()
{
    const char MAX_LOOP = 10;
    const size_t NUM_RESERVER = 1;
    const size_t NUM_WAITER = 100;
    const size_t NUM_GETTER = 100;

    atomic<int> cnt_reserver(0);
    atomic<int> cnt_waiter(0);
    atomic<int> cnt_getter(0);
    QueryCache cache(1024*1024);

    ThreadPool pool((NUM_RESERVER + NUM_WAITER + NUM_GETTER) * MAX_LOOP);

    for (char j = 0; j < MAX_LOOP; ++j)
    {
        const int str_len = 8;
        char key[str_len] = "query- ";
        key[str_len - 2] = 'a' + j;

        for (size_t i = 0; i < NUM_RESERVER + NUM_WAITER ; ++i)
            pool.scheduleOrThrowOnError([&, key]() { 
                Ptr vPtr;
                if (cache.reserveOrGet(key, vPtr))
                    ++cnt_reserver;
                else
                    ++cnt_waiter;
            }
        );

        for (size_t i = 0; i < NUM_GETTER; ++i)
            pool.scheduleOrThrowOnError([&, key]() { 
                Ptr vPtr = cache.waitAndGet(key);
                if (vPtr)
                    ++cnt_getter;
            }
        );

        cache.add(key, toPtr({}));
    }

    pool.wait();

    ASSERT(cnt_reserver==(NUM_RESERVER*MAX_LOOP));
    ASSERT(cnt_waiter==(NUM_WAITER*MAX_LOOP));
    ASSERT(cnt_getter==(NUM_GETTER*MAX_LOOP));
}

void test_expiration()
{
    QueryCache cache(1024*1024, chrono::seconds(1));

    auto now = chrono::steady_clock::now();

    cache.set("query-1", toPtr(QueryResult{}));
    cache.add("query-2", toPtr(QueryResult{}));
    cache.add("query-2", toPtr(QueryResult{}));

    cache.evict(now - chrono::seconds(10));
    ASSERT(cache.get("query-1"));
    ASSERT(cache.get("query-2"));

    cache.evict(now + chrono::seconds(10));
    ASSERT(!cache.get("query-1"));
    ASSERT(!cache.get("query-2"));

    cache.set("query-1", toPtr(QueryResult{}));
    cache.add("query-2", toPtr(QueryResult{}));
    ASSERT(cache.get("query-1"));
    ASSERT(cache.get("query-2"));
}

void test_max_size()
{
	const size_t ROWS = 1000;
    auto now = chrono::steady_clock::now();
    Block sample;

    {
        ColumnWithTypeAndName cn;

        cn.type = make_shared<DataTypeUInt64>();
		auto c = cn.type->createColumn();
		ColumnUInt64::Container & vec = typeid_cast<ColumnUInt64 &>(*c).getData();

		vec.resize(ROWS);
		for (size_t i = 0; i < ROWS; ++i)
			vec[i] = i;
		cn.column = move(c);

        sample.insert(move(cn));
    }

    {
        QueryCache cache(1000, chrono::seconds(1));
        cache.set("query-small", toPtr(QueryResult{}));
        cache.evict(now - chrono::seconds(10));
        ASSERT(cache.get("query-small"));

        auto result = toPtr(QueryResult{});
        result->blocks->push_back(sample);
        cache.set("query-large", result);
        cache.evict(now - chrono::seconds(10));
        ASSERT(!cache.get("query-small"));
        ASSERT(!cache.get("query-large"));
    }

    {
        QueryCache cache(10000, chrono::seconds(1));
        cache.set("query-small", toPtr(QueryResult{}));
        cache.evict(now - chrono::seconds(10));
        ASSERT(cache.get("query-small"));

        auto result = toPtr(QueryResult{});
        result->blocks->push_back(sample);
        cache.set("query-large", result);
        cache.evict(now - chrono::seconds(10));
        ASSERT(cache.get("query-small"));
        ASSERT(cache.get("query-large"));
    }
}

int main(int, char **)
{
    test_basic_operations();
    test_concurrency();
    test_expiration();
    test_max_size();

    return 0;
}
