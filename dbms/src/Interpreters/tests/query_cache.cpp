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
            pool.scheduleOrThrowOnError([&, key]()
            {
                Ptr vPtr;
                if (cache.reserveOrGet(key, vPtr))
                    ++cnt_reserver;
                else
                    ++cnt_waiter;
            }
        );

        for (size_t i = 0; i < NUM_GETTER; ++i)
            pool.scheduleOrThrowOnError([&, key]()
            {
                Ptr vPtr = cache.waitAndGet(key);
                if (vPtr)
                    ++cnt_getter;
            }
        );

        cache.set(key, toPtr({}));
    }

    pool.wait();

    ASSERT(cnt_reserver==(NUM_RESERVER*MAX_LOOP));
    ASSERT(cnt_waiter==(NUM_WAITER*MAX_LOOP));
    ASSERT(cnt_getter==(NUM_GETTER*MAX_LOOP));
}


int main(int, char **)
{
    test_basic_operations();
    test_concurrency();

    return 0;
}
