// -------------------------------------------------------------------
//
// expiry_ee_tests.cc
//
// Copyright (c) 2016 Basho Technologies, Inc. All Rights Reserved.
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

#include "util/testharness.h"
#include "util/testutil.h"

#include "leveldb/env.h"
#include "leveldb/slice.h"
#include "leveldb_ee/expiry_ee.h"

#include "db/dbformat.h"
#include "util/throttle.h"

/**
 * Execution routine
 */
int main(int argc, char** argv)
{
  return leveldb::test::RunAllTests();
}


namespace leveldb {




/**
 * Wrapper class for tests.  Holds working variables
 * and helper functions.
 */
class ExpiryTester
{
public:
    ExpiryTester()
    {
    };

    ~ExpiryTester()
    {
    };
};  // class ExpiryTester


/**
 * Validate option defaults
 */
TEST(ExpiryTester, Defaults)
{
    ExpiryModuleEE expiry;

    ASSERT_EQ(expiry.m_WholeFiles, true);
    ASSERT_EQ(expiry.m_ExpiryMinutes, 0);

}   // test Defaults


/**
 * Validate MemTableInserterCallback
 */
TEST(ExpiryTester, MemTableInserterCallback)
{
    bool flag;
    uint64_t before, after;
    ExpiryModuleEE module;
    ValueType type;
    ExpiryTime expiry;
    Slice key, value;

    module.m_WholeFiles=true;

    // deletion, do nothing
    type=kTypeDeletion;
    expiry=0;
    flag=module.MemTableInserterCallback(key, value, type, expiry);
    ASSERT_EQ(flag, true);
    ASSERT_EQ(type, kTypeDeletion);
    ASSERT_EQ(expiry, 0);

    // plain value, needs expiry
    type=kTypeValue;
    expiry=0;
    module.m_ExpiryMinutes=30;
    before=port::TimeUint64();
    SetTimeMinutes(before);
    flag=module.MemTableInserterCallback(key, value, type, expiry);
    after=port::TimeUint64();
    ASSERT_EQ(flag, true);
    ASSERT_EQ(type, kTypeValueWriteTime);
    ASSERT_TRUE(before <= expiry && expiry <=after && 0!=expiry);

    // plain value, expiry disabled
    type=kTypeValue;
    expiry=0;
    module.m_ExpiryMinutes=0;
    before=port::TimeUint64();
    SetTimeMinutes(before);
    flag=module.MemTableInserterCallback(key, value, type, expiry);
    after=port::TimeUint64();
    ASSERT_EQ(flag, true);
    ASSERT_EQ(type, kTypeValue);
    ASSERT_EQ(expiry, 0);

    // write time value, needs expiry
    type=kTypeValueWriteTime;
    expiry=0;
    module.m_ExpiryMinutes=30;
    before=port::TimeUint64();
    SetTimeMinutes(before);
    flag=module.MemTableInserterCallback(key, value, type, expiry);
    after=port::TimeUint64();
    ASSERT_EQ(flag, true);
    ASSERT_EQ(type, kTypeValueWriteTime);
    ASSERT_TRUE(before <= expiry && expiry <=after && 0!=expiry);

    // explicit expiry, not changed
    type=kTypeValueExplicitExpiry;
    expiry=97531;
    module.m_ExpiryMinutes=30;
    flag=module.MemTableInserterCallback(key, value, type, expiry);
    ASSERT_EQ(flag, true);
    ASSERT_EQ(type, kTypeValueExplicitExpiry);
    ASSERT_EQ(expiry, 97531);

}   // test MemTableInserterCallback


/**
 * Validate MemTableCallback
 *   (supports KeyRetirementCallback in generic case)
 */
TEST(ExpiryTester, MemTableCallback)
{
    bool flag;
    uint64_t before, after;
    ExpiryModuleEE module;
    ValueType type;
    ExpiryTime expiry;
    Slice key, value;

    module.m_WholeFiles=true;
    module.m_ExpiryMinutes=5;

    before=port::TimeUint64();
    SetTimeMinutes(before);

    // deletion, do nothing
    InternalKey key1("DeleteMeKey", 0, 0, kTypeDeletion);
    flag=module.MemTableCallback(key1.internal_key());
    ASSERT_EQ(flag, false);

    // plain value, no expiry
    InternalKey key2("PlainKey", 0, 0, kTypeValue);
    flag=module.MemTableCallback(key2.internal_key());
    ASSERT_EQ(flag, false);

    // explicit, but time in the future
    after=GetTimeMinutes() + 60*1000000;
    InternalKey key3("ExplicitKey", after, 0, kTypeValueExplicitExpiry);
    flag=module.MemTableCallback(key3.internal_key());
    ASSERT_EQ(flag, false);
    // advance the clock
    SetTimeMinutes(after + 60*1000000);
    flag=module.MemTableCallback(key3.internal_key());
    ASSERT_EQ(flag, true);
    // disable expiry
    module.m_ExpiryMinutes=0;
    flag=module.MemTableCallback(key3.internal_key());
    ASSERT_EQ(flag, false);

    // age expiry
    module.m_ExpiryMinutes=2;
    after=GetTimeMinutes();
    InternalKey key4("AgeKey", after, 0, kTypeValueWriteTime);
    flag=module.MemTableCallback(key4.internal_key());
    ASSERT_EQ(flag, false);
    // advance the clock
    SetTimeMinutes(after + 60*1000000);
    flag=module.MemTableCallback(key4.internal_key());
    ASSERT_EQ(flag, false);
    SetTimeMinutes(after + 120*1000000);
    flag=module.MemTableCallback(key4.internal_key());
    ASSERT_EQ(flag, true);
    // disable expiry
    module.m_ExpiryMinutes=0;
    flag=module.MemTableCallback(key4.internal_key());
    ASSERT_EQ(flag, false);

}   // test MemTableCallback


/**
 * Validate CompactionFinalizeCallback
 */
TEST(ExpiryTester, CompactionFinalizeCallback)
{
    bool flag;
    uint64_t now, aged, temp_time;
    std::vector<FileMetaData*> files;
    FileMetaData * file_ptr;
    ExpiryModuleEE module;

    module.m_WholeFiles=true;
    module.m_ExpiryMinutes=5;

    now=port::TimeUint64();
    SetTimeMinutes(now);

    // put two files into the level, no expiry
    file_ptr=new FileMetaData;
    file_ptr->smallest.SetFrom(ParsedInternalKey("AA1", 0, 1, kTypeValue));
    file_ptr->largest.SetFrom(ParsedInternalKey("CC1", 0, 2, kTypeValue));
    files.push_back(file_ptr);

    file_ptr=new FileMetaData;
    file_ptr->smallest.SetFrom(ParsedInternalKey("DD1", 0, 3, kTypeValue));
    file_ptr->largest.SetFrom(ParsedInternalKey("FF1", 0, 4, kTypeValue));
    files.push_back(file_ptr);

    // disable
    module.m_WholeFiles=false;
    module.m_ExpiryMinutes=0;
    flag=module.CompactionFinalizeCallback(files);
    ASSERT_EQ(flag, false);

    // enable and move clock
    module.m_WholeFiles=true;
    module.m_ExpiryMinutes=1;
    SetTimeMinutes(now + 120*1000000);
    flag=module.CompactionFinalizeCallback(files);
    ASSERT_EQ(flag, false);

    // add file only containing explicit
    //  (explicit only shown in counts, not keys)
    file_ptr=new FileMetaData;
    file_ptr->smallest.SetFrom(ParsedInternalKey("GG1", 0, 5, kTypeValue));
    file_ptr->largest.SetFrom(ParsedInternalKey("HH1", 0, 6, kTypeValue));
    file_ptr->expiry3=now + 60*1000000;
    files.push_back(file_ptr);

    // disable
    module.m_WholeFiles=false;
    module.m_ExpiryMinutes=0;
    flag=module.CompactionFinalizeCallback(files);
    ASSERT_EQ(flag, false);

    // enable aged only
    module.m_WholeFiles=false;
    module.m_ExpiryMinutes=1;
    flag=module.CompactionFinalizeCallback(files);
    ASSERT_EQ(flag, false);

    // enable file too
    module.m_WholeFiles=true;
    module.m_ExpiryMinutes=1;
    flag=module.CompactionFinalizeCallback(files);
    ASSERT_EQ(flag, true);

    // enable file, but not expiry minutes (disable)
    module.m_WholeFiles=true;
    module.m_ExpiryMinutes=0;
    flag=module.CompactionFinalizeCallback(files);
    ASSERT_EQ(flag, false);

    // remove explicit
    files.pop_back();
    delete file_ptr;

    // add file only containing aged
    //  (explicit only shown in counts, not keys)
    file_ptr=new FileMetaData;
    file_ptr->smallest.SetFrom(ParsedInternalKey("II1", 0, 7, kTypeValue));
    file_ptr->largest.SetFrom(ParsedInternalKey("JJ1", 0, 8, kTypeValue));
    file_ptr->expiry1=now - 60*1000000;
    file_ptr->expiry2=now + 60*1000000;
    files.push_back(file_ptr);

    // disable
    module.m_WholeFiles=false;
    module.m_ExpiryMinutes=0;
    flag=module.CompactionFinalizeCallback(files);
    ASSERT_EQ(flag, false);

    // enable aged only
    module.m_WholeFiles=false;
    module.m_ExpiryMinutes=1;
    flag=module.CompactionFinalizeCallback(files);
    ASSERT_EQ(flag, false);

    // enable file too
    module.m_WholeFiles=true;
    module.m_ExpiryMinutes=1;
    flag=module.CompactionFinalizeCallback(files);
    ASSERT_EQ(flag, true);

    // enable file, but not expiry minutes (disable)
    module.m_WholeFiles=true;
    module.m_ExpiryMinutes=0;
    flag=module.CompactionFinalizeCallback(files);
    ASSERT_EQ(flag, false);

    // extend aging
    module.m_WholeFiles=true;
    module.m_ExpiryMinutes=5;
    flag=module.CompactionFinalizeCallback(files);
    ASSERT_EQ(flag, false);

    // push clock back
    module.m_WholeFiles=true;
    module.m_ExpiryMinutes=1;
    SetTimeMinutes(now + 30*1000000);
    flag=module.CompactionFinalizeCallback(files);
    ASSERT_EQ(flag, false);

    // recreate fail case
    module.m_WholeFiles=true;
    module.m_ExpiryMinutes=1;
    SetTimeMinutes(now + 90*1000000);
    flag=module.CompactionFinalizeCallback(files);
    ASSERT_EQ(flag, false);

    // recreate fail case
    module.m_WholeFiles=true;
    module.m_ExpiryMinutes=1;
    SetTimeMinutes(now + 120*1000000);
    flag=module.CompactionFinalizeCallback(files);
    ASSERT_EQ(flag, true);


}   // test CompactionFinalizeCallback

}  // namespace leveldb

