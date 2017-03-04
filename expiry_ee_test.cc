// -------------------------------------------------------------------
//
// expiry_ee_tests.cc
//
// Copyright (c) 2016-2017 Basho Technologies, Inc. All Rights Reserved.
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

#include <limits.h>
#include <algorithm>
#include <memory>
#include <string>

#include "util/testharness.h"
#include "util/testutil.h"

#include "leveldb/comparator.h"
#include "leveldb/env.h"
#include "leveldb/options.h"
#include "leveldb/slice.h"
#include "leveldb/write_batch.h"
#include "leveldb_ee/expiry_ee.h"
#include "leveldb_ee/riak_object.h"

#include "db/db_impl.h"
#include "db/dbformat.h"
#include "db/filename.h"
#include "db/version_set.h"
#include "port/port.h"
#include "util/mutexlock.h"
#include "util/throttle.h"
#include "util/prop_cache.h"
/**
 * Execution routine
 */
int main(int argc, char** argv)
{
  return leveldb::test::RunAllTests();
}

namespace leveldb {

// helper function to clean up heap objects
static void ClearMetaArray(Version::FileMetaDataVector_t & ClearMe);

static bool TestRouter(EleveldbRouterActions_t Action, int ParamCount, const void ** Params);
static volatile int gRouterCalls(0), gRouterFails(0);


/**
 * This this router contains static replies for select buckets
 */
bool
TestRouter(
    EleveldbRouterActions_t Action,
    int ParamCount,
    const void ** Params)
{
    bool ret_flag(false), use_flag(false);
    const char ** params;

    params=(const char **)Params;

    if (3==ParamCount)
    {
        ExpiryPropPtr_t cache;
        ExpiryModuleEE * ee;

        ee=(leveldb::ExpiryModuleEE *)ExpiryModule::CreateExpiryModule(NULL);

        if ('\0'==*params[0] && 0==strcmp(params[1],"hello"))
        {
            ee->SetExpiryEnabled(true);
            ee->SetExpiryUnlimited(true);
            ee->SetWholeFileExpiryEnabled(true);
            use_flag=true;
        }   // if
        else if(0==strcmp(params[0],"type_one") && 0==strcmp(params[1],"wild"))
        {
            ee->SetExpiryEnabled(false);
            ee->SetExpiryMinutes(0);
            ee->SetWholeFileExpiryEnabled(false);
            use_flag=true;
        }   // else if
        else if(0==strcmp(params[0],"type_one") && 0==strcmp(params[1],"free"))
        {
            ee->SetExpiryEnabled(true);
            ee->SetExpiryMinutes(5);
            ee->SetWholeFileExpiryEnabled(false);
            use_flag=true;
        }   // else if
        else if ('\0'==*params[0] && 0==strcmp(params[1],"dolly"))
        {
            ee->SetExpiryEnabled(true);
            ee->SetExpiryMinutes(0);
            ee->SetWholeFileExpiryEnabled(false);
            use_flag=true;
        }   // else if
        else if(0==strcmp(params[0],"type_two") && 0==strcmp(params[1],"dos_equis"))
        {
            ee->SetExpiryEnabled(true);
            ee->SetExpiryMinutes(15);
            ee->SetWholeFileExpiryEnabled(true);
            use_flag=true;
        }   // else if

        if (use_flag)
        {
            ret_flag=cache.Insert(*(Slice *)Params[2], (ExpiryModuleOS*)ee);
            if (ret_flag)
                ++gRouterCalls;
            else
                ++gRouterFails;
        }   // if
        else
        {
            delete ee;
        }   // else
    }   // if

    return(ret_flag);

}   // TestRouter


// helper function to clean up heap objects
static void ClearMetaArray(Version::FileMetaDataVector_t & ClearMe);


/**
 * Wrapper class for tests.  Holds working variables
 * and helper functions.
 */
class ExpiryEETester
{
public:
    ExpiryEETester()
    {
        ExpiryModuleEE * ee;

        // establish default settings
        //  (its delete managed by smart pointer)
        ee=(leveldb::ExpiryModuleEE *)ExpiryModule::CreateExpiryModule(&TestRouter);
        ee->SetExpiryEnabled(false);
        ee->SetExpiryMinutes(424242);  // just a recognizable number
        ee->SetWholeFileExpiryEnabled(false);
        ee->NoteUserExpirySettings();

        // initialize clock that Throttle typically starts
        SetTimeMinutes(port::TimeMicros());

    };

    ~ExpiryEETester()
    {
    };
};  // class ExpiryEETester


struct InserterSampleSet
{
    const char * m_BucketType;
    const char * m_Bucket;
    bool m_Test0, m_Test1, m_Test2, m_Test3, m_Test4, m_Test5;
} Set1[]=
{
    // last two cases used to simulate bucket properties that
    //  that Riak fails to populate ... for any reason
    {"","hello",            true, true, true, true,false,false},
    {"","dolly",            true,false,false, true,false,false},
    {"type_one","wild",     true,false,false,false,false,false},
    {"type_one","free",     true, true, true, true, true, true},
    {"type_two","dos_equis",true, true, true, true,false, true},
    {"type_two","odouls",  false,false, true,false,false,false},
    {"","default",         false,false, true,false,false,false}
};


/**
 * Validate MemTableInserterCallback
 */
TEST(ExpiryEETester, MemTableInserterCallback)
{
    bool flag;
    uint64_t before, after;
    ExpiryModuleEE module;
    ValueType type;
    ExpiryTimeMicros expiry;
    Slice key, value;
    int set_size, loop, router_count, router_fail;
    std::string key_string;
    ExpiryModuleEE * ee;

    set_size=sizeof(Set1)/sizeof(Set1[0]);

    // this is the "base" module, must be enabled
    module.SetExpiryEnabled(true);
    module.SetExpiryMinutes(0);
    module.SetWholeFileExpiryEnabled(false);
    ASSERT_EQ(module.ExpiryActivated(), true);

    // deletion, do nothing (test 0)
    for (loop=0; loop<set_size; ++loop)
    {
        flag=BuildRiakKey(Set1[loop].m_BucketType,Set1[loop].m_Bucket,"Text",key_string);
        ASSERT_TRUE(flag);

        Slice loop_slice(key_string);
        type=kTypeDeletion;
        expiry=0;
        router_count=gRouterCalls;
        router_fail=gRouterFails;
        flag=module.MemTableInserterCallback(loop_slice, value, type, expiry);
        ASSERT_EQ(flag, true);
        ASSERT_EQ(type, kTypeDeletion);
        ASSERT_EQ(expiry, 0);
        if (Set1[loop].m_Test0)
            ASSERT_EQ(router_count+1, gRouterCalls);
        else
            ASSERT_EQ(router_count, gRouterCalls);
        ASSERT_EQ(router_fail, gRouterFails);
    }   // for


    // default is expiry_minutes=0 (test 1)
    module.SetExpiryMinutes(0);

    for (loop=0; loop<set_size; ++loop)
    {
        flag=BuildRiakKey(Set1[loop].m_BucketType,Set1[loop].m_Bucket,"Text",key_string);
        ASSERT_TRUE(flag);

        Slice loop_slice(key_string);

        // plain value, needs expiry
        type=kTypeValue;
        expiry=0;
        router_count=gRouterCalls;
        router_fail=gRouterFails;
        before=port::TimeMicros();
        SetTimeMinutes(before);
        flag=module.MemTableInserterCallback(loop_slice, value, type, expiry);
        after=port::TimeMicros();
        ASSERT_EQ(flag, true);
        if (Set1[loop].m_Test1)
        {
            ASSERT_EQ(type, kTypeValueWriteTime);
            ASSERT_TRUE(before <= expiry && expiry <=after && 0!=expiry);
        }
        else
        {
            ASSERT_EQ(type, kTypeValue);
            ASSERT_EQ(expiry, 0);
        }   // else
        ASSERT_EQ(router_count, gRouterCalls);
        ASSERT_EQ(router_fail, gRouterFails);
    }   // for

    // default is expiry_minutes=30 (test 2)
    module.SetExpiryMinutes(30);

    for (loop=0; loop<set_size; ++loop)
    {
        flag=BuildRiakKey(Set1[loop].m_BucketType,Set1[loop].m_Bucket,"Text",key_string);
        ASSERT_TRUE(flag);

        Slice loop_slice(key_string);

        // plain value, needs expiry
        type=kTypeValue;
        expiry=0;
        router_count=gRouterCalls;
        router_fail=gRouterFails;
        before=port::TimeMicros();
        SetTimeMinutes(before);
        module.SetExpiryMinutes(30);
        flag=module.MemTableInserterCallback(loop_slice, value, type, expiry);
        after=port::TimeMicros();
        ASSERT_EQ(flag, true);
        if (Set1[loop].m_Test2)
        {
            ASSERT_EQ(type, kTypeValueWriteTime);
            ASSERT_TRUE(before <= expiry && expiry <=after && 0!=expiry);
        }
        else
        {
            ASSERT_EQ(type, kTypeValue);
            ASSERT_EQ(expiry, 0);
        }   // else
        ASSERT_EQ(router_count, gRouterCalls);
        ASSERT_EQ(router_fail, gRouterFails);
    }   // for

    // default is expiry_minutes=ExpiryModule::kExpiryUnlimited
    //   (should have same results as test 2)
    module.SetExpiryUnlimited(true);

    for (loop=0; loop<set_size; ++loop)
    {
        flag=BuildRiakKey(Set1[loop].m_BucketType,Set1[loop].m_Bucket,"Text",key_string);
        ASSERT_TRUE(flag);

        Slice loop_slice(key_string);

        // plain value, needs expiry
        type=kTypeValue;
        expiry=0;
        router_count=gRouterCalls;
        router_fail=gRouterFails;
        before=port::TimeMicros();
        SetTimeMinutes(before);
        module.SetExpiryMinutes(30);
        flag=module.MemTableInserterCallback(loop_slice, value, type, expiry);
        after=port::TimeMicros();
        ASSERT_EQ(flag, true);
        if (Set1[loop].m_Test2)
        {
            ASSERT_EQ(type, kTypeValueWriteTime);
            ASSERT_TRUE(before <= expiry && expiry <=after && 0!=expiry);
        }
        else
        {
            ASSERT_EQ(type, kTypeValue);
            ASSERT_EQ(expiry, 0);
        }   // else
        ASSERT_EQ(router_count, gRouterCalls);
        ASSERT_EQ(router_fail, gRouterFails);
    }   // for

    // plain value, expiry disabled
    //   (should have same results as test 0)
    module.SetExpiryEnabled(false);

    for (loop=0; loop<set_size; ++loop)
    {
        flag=BuildRiakKey(Set1[loop].m_BucketType,Set1[loop].m_Bucket,"Text",key_string);
        ASSERT_TRUE(flag);

        Slice loop_slice(key_string);

        // plain value, needs expiry
        type=kTypeValue;
        expiry=0;
        router_count=gRouterCalls;
        router_fail=gRouterFails;
        before=port::TimeMicros();
        SetTimeMinutes(before);
        module.SetExpiryMinutes(30);
        flag=module.MemTableInserterCallback(loop_slice, value, type, expiry);
        after=port::TimeMicros();
        ASSERT_EQ(flag, true);
        ASSERT_EQ(type, kTypeValue);
        ASSERT_EQ(expiry, 0);
        ASSERT_EQ(router_count, gRouterCalls);
        ASSERT_EQ(router_fail, gRouterFails);
    }   // for

    // Explicit kTypeValueWriteTime, but no explicit time ()
    module.SetExpiryEnabled(true);
    module.SetExpiryMinutes(30);

    for (loop=0; loop<set_size; ++loop)
    {
        flag=BuildRiakKey(Set1[loop].m_BucketType,Set1[loop].m_Bucket,"Text",key_string);
        ASSERT_TRUE(flag);

        Slice loop_slice(key_string);

        type=kTypeValueWriteTime;
        expiry=0;
        router_count=gRouterCalls;
        router_fail=gRouterFails;
        before=port::TimeMicros();
        SetTimeMinutes(before);
        module.SetExpiryMinutes(30);
        flag=module.MemTableInserterCallback(loop_slice, value, type, expiry);
        after=port::TimeMicros();
        ASSERT_EQ(flag, true);
        ASSERT_EQ(type, kTypeValueWriteTime);
        ASSERT_TRUE(before <= expiry && expiry <=after && 0!=expiry);
        ASSERT_EQ(router_count, gRouterCalls);
        ASSERT_EQ(router_fail, gRouterFails);
    }   // for

    // write time value, expiry supplied (as if copied from another db)
    for (loop=0; loop<set_size; ++loop)
    {
        flag=BuildRiakKey(Set1[loop].m_BucketType,Set1[loop].m_Bucket,"Text",key_string);
        ASSERT_TRUE(flag);

        Slice loop_slice(key_string);

        type=kTypeValueWriteTime;
        router_count=gRouterCalls;
        router_fail=gRouterFails;
        before=port::TimeMicros();
        expiry=before - 1000;
        SetTimeMinutes(before);
        flag=module.MemTableInserterCallback(loop_slice, value, type, expiry);
        after=port::TimeMicros();
        ASSERT_EQ(flag, true);
        ASSERT_EQ(type, kTypeValueWriteTime);
        ASSERT_TRUE((before - 1000) == expiry && expiry <=after && 0!=expiry);
        ASSERT_EQ(router_count, gRouterCalls);
        ASSERT_EQ(router_fail, gRouterFails);
    }   // for

    // explicit expiry, not changed
    for (loop=0; loop<set_size; ++loop)
    {
        flag=BuildRiakKey(Set1[loop].m_BucketType,Set1[loop].m_Bucket,"Text",key_string);
        ASSERT_TRUE(flag);

        Slice loop_slice(key_string);

        type=kTypeValueExplicitExpiry;
        router_count=gRouterCalls;
        router_fail=gRouterFails;
        expiry=97531;
        flag=module.MemTableInserterCallback(loop_slice, value, type, expiry);
        after=port::TimeMicros();
        ASSERT_EQ(flag, true);
        ASSERT_EQ(type, kTypeValueExplicitExpiry);
        ASSERT_EQ(expiry, 97531);
        ASSERT_EQ(router_count, gRouterCalls);
        ASSERT_EQ(router_fail, gRouterFails);
    }   // for

}   // test MemTableInserterCallback


/**
 * Validate MemTableCallback which piggy backs on KeyRetirementCallback
 */
TEST(ExpiryEETester, MemTableCallback)
{
    bool flag;
    uint64_t before, after;
    ExpiryModuleEE module;
    Slice key, value;
    int set_size, loop, router_count, router_fail;
    std::string key_string;
    ExpiryModuleEE * ee;

    set_size=sizeof(Set1)/sizeof(Set1[0]);

    // this is the "base" module, must be enabled
    ASSERT_EQ(module.ExpiryActivated(), false);
    module.SetExpiryEnabled(true);
    module.SetWholeFileExpiryEnabled(true);
    module.SetExpiryMinutes(5);
    ASSERT_EQ(module.ExpiryActivated(), true);

    before=port::TimeMicros();
    SetTimeMinutes(before);

    // deletion, do nothing
    for (loop=0; loop<set_size; ++loop)
    {
        flag=BuildRiakKey(Set1[loop].m_BucketType,Set1[loop].m_Bucket,"DeleteMeKey",key_string);
        ASSERT_TRUE(flag);

        Slice loop_slice(key_string);
        InternalKey key1(loop_slice, 0, 0, kTypeDeletion);
        router_count=gRouterCalls;
        router_fail=gRouterFails;
        flag=module.MemTableCallback(key1.internal_key());
        ASSERT_EQ(flag, false);
        ASSERT_EQ(router_count, gRouterCalls);
        ASSERT_EQ(router_fail, gRouterFails);
    }   // for

    // plain value, no expiry
    //  (expiry is not "added" to plain keys as of Jan 2017)
    for (loop=0; loop<set_size; ++loop)
    {
        flag=BuildRiakKey(Set1[loop].m_BucketType,Set1[loop].m_Bucket,"PlainKey",key_string);
        ASSERT_TRUE(flag);

        Slice loop_slice(key_string);
        InternalKey key1(loop_slice, 0, 0, kTypeValue);
        router_count=gRouterCalls;
        router_fail=gRouterFails;
        flag=module.MemTableCallback(key1.internal_key());
        ASSERT_EQ(flag, false);
        ASSERT_EQ(router_count, gRouterCalls);
        ASSERT_EQ(router_fail, gRouterFails);
    }   // for

    // explicit expiry, but time in the future (test 3)
    //  (key test is that buckets that do not retrieve properties
    //   should return false)
    before=GetTimeMinutes();
    after=GetTimeMinutes() + 60*port::UINT64_ONE_SECOND_MICROS;
    for (loop=0; loop<set_size; ++loop)
    {
        flag=BuildRiakKey(Set1[loop].m_BucketType,Set1[loop].m_Bucket,"ExplicitExpiryKey",key_string);
        ASSERT_TRUE(flag);

        Slice loop_slice(key_string);
        InternalKey key1(loop_slice, after, 0, kTypeValueExplicitExpiry);
        router_count=gRouterCalls;
        router_fail=gRouterFails;
        flag=module.MemTableCallback(key1.internal_key());
        ASSERT_EQ(flag, false);
        // advance the clock
        SetTimeMinutes(after + 60*port::UINT64_ONE_SECOND_MICROS);
        flag=module.MemTableCallback(key1.internal_key());
        ASSERT_EQ(flag, Set1[loop].m_Test3);
        // disable expiry
        module.SetExpiryEnabled(false);
        ASSERT_EQ(module.ExpiryActivated(), false);
        flag=module.MemTableCallback(key1.internal_key());
        ASSERT_EQ(flag, false);
        ASSERT_EQ(router_count, gRouterCalls);
        ASSERT_EQ(router_fail, gRouterFails);

        // reset expiry state each loop
        SetTimeMinutes(before);
        module.SetExpiryEnabled(true);
    }   // for

    // age expiry
    //  (key test is that buckets that do not retrieve properties
    //   should return false)
    module.SetExpiryEnabled(true);
    ASSERT_EQ(module.ExpiryActivated(), true);
    SetTimeMinutes(port::TimeMicros());
    before=GetTimeMinutes();
    for (loop=0; loop<set_size; ++loop)
    {
        module.SetExpiryMinutes(2);
        after=GetTimeMinutes();
        flag=BuildRiakKey(Set1[loop].m_BucketType,Set1[loop].m_Bucket,"AgeKey",key_string);
        ASSERT_TRUE(flag);

        Slice loop_slice(key_string);
        InternalKey key1(loop_slice, after, 0, kTypeValueWriteTime);
        router_count=gRouterCalls;
        router_fail=gRouterFails;
        flag=module.MemTableCallback(key1.internal_key());
        ASSERT_EQ(flag, false);
        // advance the clock one minute
        SetTimeMinutes(after + 60*port::UINT64_ONE_SECOND_MICROS);
        flag=module.MemTableCallback(key1.internal_key());
        ASSERT_EQ(flag, false);
        // advance the clock three minute
        SetTimeMinutes(after + 180*port::UINT64_ONE_SECOND_MICROS);
        flag=module.MemTableCallback(key1.internal_key());
        ASSERT_EQ(flag, false);
        // advance the clock 10 minute
        SetTimeMinutes(after + 600*port::UINT64_ONE_SECOND_MICROS);
        flag=module.MemTableCallback(key1.internal_key());
        ASSERT_EQ(flag, Set1[loop].m_Test4);
        // advance the clock twenty minute
        SetTimeMinutes(after + 1200*port::UINT64_ONE_SECOND_MICROS);
        flag=module.MemTableCallback(key1.internal_key());
        ASSERT_EQ(flag, Set1[loop].m_Test5);
        // disable expiry
        module.SetExpiryEnabled(false);
        ASSERT_EQ(module.ExpiryActivated(), false);
        flag=module.MemTableCallback(key1.internal_key());
        ASSERT_EQ(flag, false);
        if (Set1[loop].m_Test0)
            ASSERT_EQ(router_count+2, gRouterCalls);
        else
            ASSERT_EQ(router_count, gRouterCalls);
        ASSERT_EQ(router_fail, gRouterFails);

        // reset expiry state each loop
        SetTimeMinutes(before);
        module.SetExpiryEnabled(true);
    }   // for

}   // test MemTableCallback


/**
 * Wrapper class to Version that allows manipulation
 *  of internal objects for testing purposes
 */
class VersionTester : public Version
{
public:
    VersionTester() : Version(&m_Vset), m_Icmp(m_Options.comparator),
                      m_Vset("", &m_Options, NULL, &m_Icmp)  {};

    void SetFileList(int Level, FileMetaDataVector_t & Files)
        {files_[Level]=Files;};

    Options m_Options;
    InternalKeyComparator m_Icmp;
    VersionSet m_Vset;
};  // class VersionTester


/**
 * Validate CompactionFinalizeCallback's
 *  identification of expired files
 */

TEST(ExpiryEETester, CompactionFinalizeCallback1)
{
    bool flag;
    uint64_t now, aged, temp_time;
    std::vector<FileMetaData*> files;
    FileMetaData * file_ptr;
    ExpiryModuleEE module;
    VersionTester ver;
    int level;
    std::string user_key;

    ASSERT_EQ(ver.m_Options.ExpiryActivated(), false);

    module.SetExpiryEnabled(true);
    module.SetWholeFileExpiryEnabled(true);
    module.SetExpiryMinutes(5);
    level=config::kNumOverlapLevels;

    now=port::TimeMicros();
    SetTimeMinutes(now);

    // put two files into the level, no expiry
    file_ptr=new FileMetaData;
    flag=BuildRiakKey("type_one","free","AA1",user_key);
    ASSERT_TRUE(flag);
    file_ptr->smallest.SetFrom(ParsedInternalKey(user_key, 0, 1, kTypeValue));
    flag=BuildRiakKey("type_one","free","CC1",user_key);
    ASSERT_TRUE(flag);
    file_ptr->largest.SetFrom(ParsedInternalKey(user_key, 0, 2, kTypeValue));
    files.push_back(file_ptr);

    file_ptr=new FileMetaData;
    flag=BuildRiakKey("type_one","free","DD1",user_key);
    ASSERT_TRUE(flag);
    file_ptr->smallest.SetFrom(ParsedInternalKey(user_key, 0, 3, kTypeValue));
    flag=BuildRiakKey("type_one","free","FF1",user_key);
    ASSERT_TRUE(flag);
    file_ptr->largest.SetFrom(ParsedInternalKey(user_key, 0, 4, kTypeValue));
    files.push_back(file_ptr);

    // disable
    module.SetExpiryEnabled(false);
    module.SetWholeFileExpiryEnabled(false);
    module.SetExpiryMinutes(0);
    ver.SetFileList(level, files);
    flag=module.CompactionFinalizeCallback(true, ver, level, NULL);
    ASSERT_EQ(flag, false);
    flag=module.CompactionFinalizeCallback(false, ver, level, NULL);
    ASSERT_EQ(flag, false);

    // enable and move clock
    //  (bucket type_one/free has 5 minute expiry)
    module.SetExpiryEnabled(true);
    module.SetWholeFileExpiryEnabled(true);
    module.SetExpiryMinutes(5);
    SetTimeMinutes(now + 360*port::UINT64_ONE_SECOND_MICROS);
    ver.SetFileList(level, files);
    flag=module.CompactionFinalizeCallback(true, ver, level, NULL);
    ASSERT_EQ(flag, false);
    flag=module.CompactionFinalizeCallback(false, ver, level, NULL);
    ASSERT_EQ(flag, false);

    // add file only containing explicit
    //  (explicit only shown in counts, not keys)
    file_ptr=new FileMetaData;
    flag=BuildRiakKey("type_one","free","GG1",user_key);
    ASSERT_TRUE(flag);
    file_ptr->smallest.SetFrom(ParsedInternalKey(user_key, 0, 5, kTypeValue));
    flag=BuildRiakKey("type_one","free","HH1",user_key);
    ASSERT_TRUE(flag);
    file_ptr->largest.SetFrom(ParsedInternalKey(user_key, 0, 6, kTypeValue));
    file_ptr->exp_write_low=ULLONG_MAX;  // sign of no aged expiry, or plain keys
    file_ptr->exp_explicit_high=now + 60*port::UINT64_ONE_SECOND_MICROS;
    files.push_back(file_ptr);

    // disable
    module.SetExpiryEnabled(false);
    module.SetWholeFileExpiryEnabled(false);
    module.SetExpiryMinutes(0);
    ver.SetFileList(level, files);
    flag=module.CompactionFinalizeCallback(true, ver, level, NULL);
    ASSERT_EQ(flag, false);
    flag=module.CompactionFinalizeCallback(false, ver, level, NULL);
    ASSERT_EQ(flag, false);

    // enable compaction expiry only
    module.SetExpiryEnabled(true);
    module.SetWholeFileExpiryEnabled(false);
    module.SetExpiryMinutes(5);
    ver.SetFileList(level, files);
    flag=module.CompactionFinalizeCallback(true, ver, level, NULL);
    ASSERT_EQ(flag, false);
    flag=module.CompactionFinalizeCallback(false, ver, level, NULL);
    ASSERT_EQ(flag, false);

    // switch to bucket that has file expiry enabled
    // add file only containing explicit
    //  (explicit only shown in counts, not keys)
    file_ptr=new FileMetaData;
    flag=BuildRiakKey("type_two","dos_equis","II1",user_key);
    ASSERT_TRUE(flag);
    file_ptr->smallest.SetFrom(ParsedInternalKey(user_key, 0, 7, kTypeValue));
    flag=BuildRiakKey("type_two","dos_equis","JJ1",user_key);
    ASSERT_TRUE(flag);
    file_ptr->largest.SetFrom(ParsedInternalKey(user_key, 0, 8, kTypeValue));
    file_ptr->exp_write_low=ULLONG_MAX;  // sign of no aged expiry, or plain keys
    file_ptr->exp_explicit_high=now + 60*port::UINT64_ONE_SECOND_MICROS;
    files.push_back(file_ptr);
    ver.SetFileList(level, files);

    flag=module.CompactionFinalizeCallback(true, ver, level, NULL);
    ASSERT_EQ(flag, true);
    flag=module.CompactionFinalizeCallback(false, ver, level, NULL);
    ASSERT_EQ(flag, true);

    // remove explicit
    files.pop_back();
    delete file_ptr;

    // Use bucket where file enabled, minutes as unlimited
    //   ... but file without aged expiries or plain keys
    file_ptr=new FileMetaData;
    flag=BuildRiakKey("","hello","KK1",user_key);
    ASSERT_TRUE(flag);
    file_ptr->smallest.SetFrom(ParsedInternalKey(user_key, 0, 9, kTypeValue));
    flag=BuildRiakKey("","hello","LL1",user_key);
    ASSERT_TRUE(flag);
    file_ptr->largest.SetFrom(ParsedInternalKey(user_key, 0, 10, kTypeValue));
    files.push_back(file_ptr);
    ver.SetFileList(level, files);

    flag=module.CompactionFinalizeCallback(true, ver, level, NULL);
    ASSERT_EQ(flag, false);
    flag=module.CompactionFinalizeCallback(false, ver, level, NULL);
    ASSERT_EQ(flag, false);

    // add file only containing aged
    //   ... but bucket has kExpiryUnlimited ... so "disabled"
    file_ptr=new FileMetaData;
    flag=BuildRiakKey("","hello","MM1",user_key);
    ASSERT_TRUE(flag);
    file_ptr->smallest.SetFrom(ParsedInternalKey(user_key, 0, 11, kTypeValue));
    flag=BuildRiakKey("","hello","NN1",user_key);
    ASSERT_TRUE(flag);
    file_ptr->largest.SetFrom(ParsedInternalKey(user_key, 0, 12, kTypeValue));
    file_ptr->exp_write_low=now - 60*port::UINT64_ONE_SECOND_MICROS;
    file_ptr->exp_write_high=now + 60*port::UINT64_ONE_SECOND_MICROS;
    files.push_back(file_ptr);
    ver.SetFileList(level, files);

    flag=module.CompactionFinalizeCallback(true, ver, level, NULL);
    ASSERT_EQ(flag, false);
    flag=module.CompactionFinalizeCallback(false, ver, level, NULL);
    ASSERT_EQ(flag, false);

    // add file only containing aged ... in active bucket (15 min expiry)
    file_ptr=new FileMetaData;
    flag=BuildRiakKey("type_two","dos_equis","OO1",user_key);
    ASSERT_TRUE(flag);
    file_ptr->smallest.SetFrom(ParsedInternalKey(user_key, 0, 13, kTypeValue));
    flag=BuildRiakKey("type_two","dos_equis","PP1",user_key);
    ASSERT_TRUE(flag);
    file_ptr->largest.SetFrom(ParsedInternalKey(user_key, 0, 12, kTypeValue));
    file_ptr->exp_write_low=now - 16*60*port::UINT64_ONE_SECOND_MICROS;
    file_ptr->exp_write_high=now - 15*60*port::UINT64_ONE_SECOND_MICROS;
    files.push_back(file_ptr);
    ver.SetFileList(level, files);

    flag=module.CompactionFinalizeCallback(true, ver, level, NULL);
    ASSERT_EQ(flag, true);
    flag=module.CompactionFinalizeCallback(false, ver, level, NULL);
    ASSERT_EQ(flag, true);

    // clean up phony files or Version destructor will crash
    ClearMetaArray(files);
    ver.SetFileList(level,files);

}   // test CompactionFinalizeCallback


/**
 * Note:  constructor and destructor NOT called, this is
 *        an interface class only
 */

class ExpDB : public DBImpl
{
public:
    ExpDB(const Options& options, const std::string& dbname)
        : DBImpl(options, dbname) {}



    virtual ~ExpDB() {};

    VersionSet * GetVersionSet() {return(versions_);};
    const Options * GetOptions() {return(&options_);};

    void OneCompaction()
    {
        MutexLock l(&mutex_);
        MaybeScheduleCompaction();
        while (IsCompactionScheduled())
            bg_cv_.Wait();
    };  // OneCompaction

    void SetClock(uint64_t Time)
        {SetTimeMinutes(Time);};

    void ShiftClockMinutes(int Min)
    {
        uint64_t shift;

        shift=Min * 60 * port::UINT64_ONE_SECOND_MICROS;
        SetTimeMinutes(GetTimeMinutes() + shift);
    };
};  // class ExpDB


class ExpiryDBTester
{
public:
    ExpiryDBTester()
        : m_Good(false), m_DB(NULL),
          m_BaseTime(port::TimeMicros())
    {
        m_DBName = test::TmpDir() + "/expiry";

        // clean up previous execution
        leveldb::DestroyDB(m_DBName, m_Options);

        m_Options.create_if_missing=true;
        m_Options.error_if_exists=false;

        // Note: m_Options.expiry_module is a smart pointer.  It
        //  owns the m_Expiry object and will automatically delete the
        //  allocation.
        m_Expiry=new leveldb::ExpiryModuleOS;
        m_Options.expiry_module=m_Expiry;

        OpenTestDB();
    };

    ~ExpiryDBTester()
    {
        // clean up
        delete m_DB;
        leveldb::DestroyDB(m_DBName, m_Options);
    };

    void OpenTestDB()
    {
        leveldb::Status status;

        status=leveldb::DB::Open(m_Options, m_DBName, (DB**)&m_DB);

        m_Good=status.ok();
        ASSERT_OK(status);
        m_DB->SetClock(m_BaseTime);
    }   // OpenTestDB

protected:
    bool m_Good;
    std::string m_DBName;
    Options m_Options;
    leveldb::ExpiryModuleOS * m_Expiry;
    ExpDB * m_DB;
    uint64_t m_BaseTime;

};  // ExpiryDBTester



/**
 * Test "default options" ... first expiry's options
 *  paste to subsequent.
 */
TEST(ExpiryDBTester, DefaultOptionsAssignment)
{
    ExpiryModuleEE * exp_ee, *exp2_ee;

    exp_ee=(ExpiryModuleEE *)ExpiryModule::CreateExpiryModule(NULL);

    // verify default state
    ASSERT_EQ(exp_ee->IsExpiryEnabled(), false);
    ASSERT_EQ(exp_ee->GetExpiryMinutes(), 424242);
    ASSERT_EQ(exp_ee->IsWholeFileExpiryEnabled(), false);
    ASSERT_EQ(exp_ee->ExpiryActivated(), false);

    // manually change
    exp_ee->SetExpiryEnabled(true);
    exp_ee->SetExpiryMinutes(4242);
    exp_ee->SetWholeFileExpiryEnabled(true);

    exp_ee->NoteUserExpirySettings();

    // see what gets created now
    exp2_ee=(ExpiryModuleEE *)ExpiryModule::CreateExpiryModule(NULL);

    // verify default state
    ASSERT_EQ(exp2_ee->IsExpiryEnabled(), true);
    ASSERT_EQ(exp2_ee->GetExpiryMinutes(), 4242);
    ASSERT_EQ(exp2_ee->IsWholeFileExpiryEnabled(), true);
    ASSERT_EQ(exp2_ee->ExpiryActivated(), true);

    // now verify assignment, field by field
    exp_ee->SetExpiryEnabled(false);
    *exp2_ee=*exp_ee;
    ASSERT_EQ(exp2_ee->IsExpiryEnabled(), false);
    ASSERT_EQ(exp2_ee->GetExpiryMinutes(), 4242);
    ASSERT_EQ(exp2_ee->IsWholeFileExpiryEnabled(), true);
    ASSERT_EQ(exp2_ee->ExpiryActivated(), false);

    exp_ee->SetExpiryMinutes(110516);
    *exp2_ee=*exp_ee;
    ASSERT_EQ(exp2_ee->IsExpiryEnabled(), false);
    ASSERT_EQ(exp2_ee->GetExpiryMinutes(), 110516);
    ASSERT_EQ(exp2_ee->IsWholeFileExpiryEnabled(), true);
    ASSERT_EQ(exp2_ee->ExpiryActivated(), false);

    exp_ee->SetWholeFileExpiryEnabled(false);
    *exp2_ee=*exp_ee;
    ASSERT_EQ(exp2_ee->IsExpiryEnabled(), false);
    ASSERT_EQ(exp2_ee->GetExpiryMinutes(), 110516);
    ASSERT_EQ(exp2_ee->IsWholeFileExpiryEnabled(), false);
    ASSERT_EQ(exp2_ee->ExpiryActivated(), false);

    // update default state
    exp_ee->NoteUserExpirySettings();
    delete exp2_ee;
    exp2_ee=(ExpiryModuleEE *)ExpiryModule::CreateExpiryModule(NULL);

    ASSERT_EQ(exp2_ee->IsExpiryEnabled(), false);
    ASSERT_EQ(exp2_ee->GetExpiryMinutes(), 110516);
    ASSERT_EQ(exp2_ee->IsWholeFileExpiryEnabled(), false);
    ASSERT_EQ(exp2_ee->ExpiryActivated(), false);

    // clean up (leave exp_ee alone, smart pointer in expiry_ee.cc will delete)
    delete exp2_ee;

}   // ExpiryEETester::DefaultOptions

static void
ClearMetaArray(
    Version::FileMetaDataVector_t & ClearMe)
{
    // clean up phony files or Version destructor will crash
    std::vector<FileMetaData*>::iterator it;
    for (it=ClearMe.begin(); ClearMe.end()!=it; ++it)
        delete (*it);
    ClearMe.clear();

}   // ClearMetaArray



}  // namespace leveldb

