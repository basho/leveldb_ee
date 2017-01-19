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

#include "leveldb/options.h"
#include "util/prop_cache.h"
#include "port/port.h"
#include "util/mutexlock.h"

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
class PropCacheWithRouter : public PropertyCache
{
public:

    PropCacheWithRouter()
        : PropertyCache(&PropTestRouter),
          m_TestCond(&m_TestMutex)
    {
        int ret_val;

        m_PropTestRouterState=0;

        ret_val=pthread_create(&m_ThreadId, NULL, &PropTestThread, this);
        ASSERT_TRUE(0==ret_val);

        m_TestObj=this;
    };

    virtual ~PropCacheWithRouter()
    {
        pthread_cancel(m_ThreadId);  // may fail, ignore
        pthread_join(m_ThreadId, NULL);
    };

    // use small cache size to validate LRU
    virtual int GetCacheLimit() const {return(3);};

    // a pretend version of eleveldb's router, used for threading
    //  checks
    static bool PropTestRouter(EleveldbRouterActions_t Action, int ParamCount, const void ** Params);
    static volatile int m_PropTestRouterState;
    static PropCacheWithRouter * m_TestObj;

protected:
    port::Mutex m_TestMutex;
    port::CondVar m_TestCond;
    pthread_t m_ThreadId;

    void AwaitState(int TargetState)
    {
        MutexLock lock(&m_TestMutex);

        while(m_PropTestRouterState<TargetState)
            m_TestCond.Wait();

        ASSERT_TRUE(m_PropTestRouterState==TargetState);

    }   // AwaitState


    void SetState(int TargetState)
    {
        MutexLock lock(&m_TestMutex);

        m_PropTestRouterState=TargetState;
        m_TestCond.SignalAll();

    }   // SetState


    static void * PropTestThread(void *);
    void RouterThread();

};  // class PropCacheWithRouter

// statics
volatile int PropCacheWithRouter::m_PropTestRouterState(0);
PropCacheWithRouter * PropCacheWithRouter::m_TestObj(NULL);

/**
 * static pthread_create entry point.  arg is the PropCacheWithRouter object
 */
void *
PropCacheWithRouter::PropTestThread(
    void * Arg)
{
    PropCacheWithRouter * with_router;

    with_router=(PropCacheWithRouter *)Arg;

    with_router->RouterThread();

    return(NULL);

}   // PropCacheWithRouter::PropTestThread


/**
 * thread with object access
 */
void
PropCacheWithRouter::RouterThread()
{
    Cache::Handle * handle;
    struct timespec ts;

    // test one:  entry exists upon lookup call
    handle=InsertInternal(one_slice, NULL);
    ASSERT_TRUE(NULL!=handle);
    GetCachePtr()->Release(handle);
    SetState(1);

    // test three:  quarter second delay in delivering properties
    AwaitState(2);
    ts.tv_sec=0;
    ts.tv_nsec=250000000;
    nanosleep(&ts, &ts);
    handle=InsertInternal(three_slice, NULL);
    ASSERT_TRUE(NULL!=handle);
    GetCachePtr()->Release(handle);

}   // PropCacheWithRouter::RouterThread


/**
 * Pretend eleveldb router.  Called by LookupInternal()
 */
bool
PropCacheWithRouter::PropTestRouter(
    EleveldbRouterActions_t Action,
    int ParamCount,
    const void ** Params)
{
    const Slice * composite;
    Cache::Handle * handle;

    composite=(const Slice *)Params[2];

    // second test ... insert key before returning
    if (*composite==two_slice)
    {
        handle=m_TestObj->InsertInternal(two_slice, NULL);
        m_TestObj->GetCachePtr()->Release(handle);
        m_TestObj->SetState(4);
    }   // if

    return(true);

}   // PropCacheWithRouter::PropTestRouter


TEST(PropCacheWithRouter, LookupTest)
{
    Cache::Handle * handle;

    // test if an "existing" cache entry found
    AwaitState(1);
    handle=LookupInternal(one_slice);
    ASSERT_TRUE(NULL!=handle);
    GetCachePtr()->Release(handle);

    // setup for race test
    handle=LookupInternal(two_slice);
    ASSERT_TRUE(NULL!=handle);
    GetCachePtr()->Release(handle);

    // delayed arrival of prop data
    SetState(2);
    handle=LookupInternal(three_slice);
    ASSERT_TRUE(NULL!=handle);
    GetCachePtr()->Release(handle);

}   // PropCacheWithRouter::LookupTest


/**
 * Wrapper class for tests.  Holds working variables
 * and helper functions.
 */
class CachePtrTester : public PropertyCache
{
public:

    CachePtrTester()
        : PropertyCache(NULL)
    {
        ShutdownPropertyCache();
        SetGlobalPropertyCache(this);
    };

    virtual ~CachePtrTester()
    {
        SetGlobalPropertyCache(NULL);
    };

};  // class CachePtrTester


TEST(CachePtrTester, SmartPointerTests)
{
    ExpiryPropPtr_t ptr1, ptr2, ptr3;

    // ExpiryPropPtr_t utilizes the global cache.  Start it.

    // cache size is two, fill it up.
    ASSERT_TRUE(ptr1.Insert(one_slice, NULL));
    ASSERT_TRUE(ptr2.Insert(two_slice, NULL));
    ASSERT_TRUE(ptr1.Lookup(one_slice));
    ASSERT_TRUE(ptr2.Lookup(two_slice));

    ptr1.Erase(one_slice);
    ASSERT_TRUE(ptr1.Insert(three_slice, NULL));
    ASSERT_FALSE(ptr1.Lookup(one_slice));

}   // CachePtrTester::SmartPointerTests

}   // namespace leveldb

