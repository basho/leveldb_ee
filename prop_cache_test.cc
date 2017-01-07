// -------------------------------------------------------------------
//
// prop_cache_test.cc
//
// Copyright (c) 2017 Basho Technologies, Inc. All Rights Reserved.
//
// This file is provided to you under the Apache License,
// Version 2.0 (the "License"); you may not use this file
// except in compliance with the License.  You may obtain
// a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
//
// -------------------------------------------------------------------

#include <string>

#include "util/testharness.h"
#include "util/testutil.h"

#include "leveldb_ee/prop_cache.h"


/**
 * Execution routine
 */
int main(int argc, char** argv)
{
    int ret_val(0);;

    ret_val=leveldb::test::RunAllTests();

    return(ret_val);
}   // main


namespace leveldb {


// prebuilt constant Slices for tests
static const char one_str[]="one";
static const char two_str[]="two";
static const char three_str[]="three";
static const char four_str[]="four";

static Slice one_slice(one_str, sizeof(one_str));
static Slice two_slice(two_str, sizeof(two_str));
static Slice three_slice(three_str, sizeof(three_str));
static Slice four_slice(four_str, sizeof(four_str));

/**
 * Wrapper class for tests.  Holds working variables
 * and helper functions.
 */
class PropCacheNoRouter : public PropertyCache
{
public:

    PropCacheNoRouter()
        : PropertyCache(NULL)
    {
    };

    virtual ~PropCacheNoRouter()
    {
    };

    // use small cache size to validate LRU
    virtual int GetCacheLimit() const {return(3);};


    
};  // class PropCacheNoRouter


// test global's basic methods    
TEST(PropCacheNoRouter, CreateNull)
{
    Cache::Handle * handle_one, * handle_two, * handle_three,
        * handle_four;
    
    InitPropertyCache(NULL);

    ASSERT_TRUE(NULL==Lookup(one_slice));
    ASSERT_TRUE(NULL==Lookup(two_slice));
    ASSERT_TRUE(NULL==Lookup(three_slice));
    ASSERT_TRUE(NULL==Lookup(four_slice));

    // default size is 1000, all four should insert and retrieve
    ASSERT_TRUE(Insert(one_slice, NULL, &handle_one));
    GetCache().Release(handle_one);
    ASSERT_TRUE(Insert(two_slice, NULL, &handle_two));
    GetCache().Release(handle_two);
    ASSERT_TRUE(Insert(three_slice, NULL, &handle_three));
    GetCache().Release(handle_three);
    ASSERT_TRUE(Insert(four_slice, NULL, &handle_four));
    GetCache().Release(handle_four);
    
    handle_one=Lookup(one_slice);
    ASSERT_TRUE(NULL!=handle_one);
    GetCache().Release(handle_one);
    
    handle_two=Lookup(two_slice);
    ASSERT_TRUE(NULL!=handle_two);
    GetCache().Release(handle_two);

    handle_three=Lookup(three_slice);
    ASSERT_TRUE(NULL!=handle_three);
    GetCache().Release(handle_three);
    
    handle_four=Lookup(four_slice);
    ASSERT_TRUE(NULL!=handle_four);
    GetCache().Release(handle_four);
    
    ShutdownPropertyCache();

    // try main interfaces again, see if segfault
    ASSERT_TRUE(NULL==GetPropertyCachePtr());
    handle_one=Lookup(one_slice);
    ASSERT_TRUE(NULL==handle_one);

    ASSERT_FALSE(Insert(one_slice, NULL, &handle_one));
    
}   // PropCacheNoRouter::CreateNull

    
/**
 * Wrapper class for tests.  Holds working variables
 * and helper functions.
 */
class CachePtrTester
{
public:

    CachePtrTester()
    {
    };

    virtual ~CachePtrTester()
    {
    };

};  // class CachePtrTester



/**
- do propcache first: no router, then local router
- need way to know limit of cache
- insert limit + 1, verify first gone ... test that delete called?
- with Router=NULL, verify Lookup fails
- verify shutdown clears, m_Cache
- thread / sequence test of LookupWait & InsertInternal
- thread / sequence test if 2nd InsertInternal has desired key (20ms pause between Insert calls)
- ?? does each test function create new PropCacheTester?



 */







/**
 * Verify ...
 *  as flag to start a backup.
 */
TEST(PropCacheTester, KeyDecodeTest)
{

}   // KeyDecodeTester


}   // namespace leveldb

