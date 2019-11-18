#include <Columns/ColumnsNumber.h>
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

int main(int, char **)
{
    test_basic_operations();

    return 0;
}
