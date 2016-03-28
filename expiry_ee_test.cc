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

#include <limits.h>

#include "util/testharness.h"
#include "util/testutil.h"

#include "leveldb/comparator.h"
#include "leveldb/env.h"
#include "leveldb/slice.h"
#include "leveldb_ee/expiry_ee.h"

#include "db/dbformat.h"
#include "db/version_set.h"
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

    ASSERT_EQ(expiry.m_ExpiryEnabled, false);
    ASSERT_EQ(expiry.m_ExpiryMinutes, 0);
    ASSERT_EQ(expiry.m_WholeFileExpiry, false);

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

    module.m_ExpiryEnabled=true;
    module.m_WholeFileExpiry=true;

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

    module.m_ExpiryEnabled=true;
    module.m_WholeFileExpiry=true;
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
    module.m_ExpiryEnabled=false;
    flag=module.MemTableCallback(key3.internal_key());
    ASSERT_EQ(flag, false);

    // age expiry
    module.m_ExpiryEnabled=true;
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
    module.m_ExpiryEnabled=false;
    flag=module.MemTableCallback(key4.internal_key());
    ASSERT_EQ(flag, false);

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

TEST(ExpiryTester, CompactionFinalizeCallback1)
{
    bool flag;
    uint64_t now, aged, temp_time;
    std::vector<FileMetaData*> files;
    FileMetaData * file_ptr;
    ExpiryModuleEE module;
    VersionTester ver;
    int level;

    module.m_ExpiryEnabled=true;
    module.m_WholeFileExpiry=true;
    module.m_ExpiryMinutes=5;
    level=config::kNumOverlapLevels;

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
    module.m_ExpiryEnabled=false;
    module.m_WholeFileExpiry=false;
    module.m_ExpiryMinutes=0;
    ver.SetFileList(level, files);
    flag=module.CompactionFinalizeCallback(true, ver, level, NULL);
    ASSERT_EQ(flag, false);

    // enable and move clock
    module.m_ExpiryEnabled=true;
    module.m_WholeFileExpiry=true;
    module.m_ExpiryMinutes=1;
    SetTimeMinutes(now + 120*1000000);
    ver.SetFileList(level, files);
    flag=module.CompactionFinalizeCallback(true, ver, level, NULL);
    ASSERT_EQ(flag, false);

    // add file only containing explicit
    //  (explicit only shown in counts, not keys)
    file_ptr=new FileMetaData;
    file_ptr->smallest.SetFrom(ParsedInternalKey("GG1", 0, 5, kTypeValue));
    file_ptr->largest.SetFrom(ParsedInternalKey("HH1", 0, 6, kTypeValue));
    file_ptr->expiry1=ULONG_MAX;  // sign of no aged expiry, or plain keys
    file_ptr->expiry3=now + 60*1000000;
    files.push_back(file_ptr);

    // disable
    module.m_ExpiryEnabled=false;
    module.m_WholeFileExpiry=false;
    module.m_ExpiryMinutes=0;
    ver.SetFileList(level, files);
    flag=module.CompactionFinalizeCallback(true, ver, level, NULL);
    ASSERT_EQ(flag, false);

    // enable compaction expiry only
    module.m_ExpiryEnabled=true;
    module.m_WholeFileExpiry=false;
    module.m_ExpiryMinutes=1;
    ver.SetFileList(level, files);
    flag=module.CompactionFinalizeCallback(true, ver, level, NULL);
    ASSERT_EQ(flag, false);

    // enable file expiry too
    module.m_WholeFileExpiry=true;
    module.m_ExpiryMinutes=1;
    flag=module.CompactionFinalizeCallback(true, ver, level, NULL);
    ASSERT_EQ(flag, true);

    // enable file, but not expiry minutes (disable)
    //   ... but file without aged expiries or plain keys
    module.m_WholeFileExpiry=true;
    module.m_ExpiryMinutes=0;
    ver.SetFileList(level, files);
    flag=module.CompactionFinalizeCallback(true, ver, level, NULL);
    ASSERT_EQ(flag, true);

    // remove explicit
    files.pop_back();
    delete file_ptr;

    // add file only containing aged
    //  (aging only shown in counts, not keys)
    file_ptr=new FileMetaData;
    file_ptr->smallest.SetFrom(ParsedInternalKey("II1", 0, 7, kTypeValue));
    file_ptr->largest.SetFrom(ParsedInternalKey("JJ1", 0, 8, kTypeValue));
    file_ptr->expiry1=now - 60*1000000;
    file_ptr->expiry2=now + 60*1000000;
    files.push_back(file_ptr);

    // disable
    module.m_WholeFileExpiry=false;
    module.m_ExpiryMinutes=0;
    ver.SetFileList(level, files);
    flag=module.CompactionFinalizeCallback(true, ver, level, NULL);
    ASSERT_EQ(flag, false);

    // enable compaction only
    module.m_WholeFileExpiry=false;
    module.m_ExpiryMinutes=1;
    flag=module.CompactionFinalizeCallback(true, ver, level, NULL);
    ASSERT_EQ(flag, false);

    // enable file too
    module.m_WholeFileExpiry=true;
    module.m_ExpiryMinutes=1;
    flag=module.CompactionFinalizeCallback(true, ver, level, NULL);
    ASSERT_EQ(flag, true);

    // enable file, but not expiry minutes (disable)
    module.m_WholeFileExpiry=true;
    module.m_ExpiryMinutes=0;
    flag=module.CompactionFinalizeCallback(true, ver, level, NULL);
    ASSERT_EQ(flag, false);

    // file_ptr at 1min, setting at 5 min
    module.m_WholeFileExpiry=true;
    module.m_ExpiryMinutes=5;
    flag=module.CompactionFinalizeCallback(true, ver, level, NULL);
    ASSERT_EQ(flag, false);

    // file_ptr at 1min, setting at 1m, clock at 30 seconds
    module.m_WholeFileExpiry=true;
    module.m_ExpiryMinutes=1;
    SetTimeMinutes(now + 30*1000000);
    flag=module.CompactionFinalizeCallback(true, ver, level, NULL);
    ASSERT_EQ(flag, false);

    // file_ptr at 1min, setting at 1m, clock at 1.5minutes
    module.m_WholeFileExpiry=true;
    module.m_ExpiryMinutes=1;
    SetTimeMinutes(now + 90*1000000);
    flag=module.CompactionFinalizeCallback(true, ver, level, NULL);
    ASSERT_EQ(flag, false);

    // file_ptr at 1min, setting at 1m, clock at 2minutes
    module.m_WholeFileExpiry=true;
    module.m_ExpiryMinutes=1;
    SetTimeMinutes(now + 120*1000000);
    flag=module.CompactionFinalizeCallback(true, ver, level, NULL);
    ASSERT_EQ(flag, true);

    // same settings, but show an explicit expiry too that has not
    //  expired
    file_ptr->expiry3=now +240*1000000;
    flag=module.CompactionFinalizeCallback(true, ver, level, NULL);
    ASSERT_EQ(flag, false);

    // same settings, but show an explicit expiry has expired
    //  expired
    file_ptr->expiry3=now +90*1000000;
    flag=module.CompactionFinalizeCallback(true, ver, level, NULL);
    ASSERT_EQ(flag, true);

    // same settings, explicit has expired, but not the aged
    //  expired
    file_ptr->expiry2=now +240*1000000;
    flag=module.CompactionFinalizeCallback(true, ver, level, NULL);
    ASSERT_EQ(flag, false);

    // clean up phony files or Version destructor will crash
    std::vector<FileMetaData*>::iterator it;
    for (it=files.begin(); files.end()!=it; ++it)
        delete (*it);
    files.clear();
    ver.SetFileList(level,files);

}   // test CompactionFinalizeCallback


/**
 * Building static sets of file levels to increase visibility
 */

#define TEST_NOW 1457326800000000ull

struct TestFileMetaData
{
    uint64_t m_Number;          // file number
    const char * m_Smallest;
    const char * m_Largest;
    ExpiryTime m_Expiry1;              // minutes
    ExpiryTime m_Expiry2;
    ExpiryTime m_Expiry3;
};


static void
CreateMetaArray(
    Version::FileMetaDataVector_t & Output,
    TestFileMetaData * Data,
    size_t Count)
{
    size_t loop;
    TestFileMetaData * cursor;
    FileMetaData * file_ptr;
    ExpiryTime now;

    Output.clear();
    now=GetTimeMinutes();

    for (loop=0, cursor=Data; loop<Count; ++loop, ++cursor)
    {
        file_ptr=new FileMetaData;
        file_ptr->number=cursor->m_Number;
        file_ptr->smallest.SetFrom(ParsedInternalKey(cursor->m_Smallest, 0, cursor->m_Number, kTypeValue));
        file_ptr->largest.SetFrom(ParsedInternalKey(cursor->m_Largest, 0, cursor->m_Number, kTypeValue));
        if (0!=cursor->m_Expiry1)
        {
            if (ULONG_MAX!=cursor->m_Expiry1)
                file_ptr->expiry1=now + cursor->m_Expiry1*60000000;
            else
                file_ptr->expiry1=cursor->m_Expiry1;
        }   // if

        if (0!=cursor->m_Expiry2)
            file_ptr->expiry2=now + cursor->m_Expiry2*60000000;

        if (0!=cursor->m_Expiry3)
            file_ptr->expiry3=now + cursor->m_Expiry3*60000000;

        Output.push_back(file_ptr);
    }   // for

}   // CreateMetaArray


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


/** case: two levels, no overlap, no expiry **/
TestFileMetaData levelA[]=
{
    {100, "AA", "BA", 0, 0, 0},
    {101, "LA", "NA", 0, 0, 0}
};  // levelA

TestFileMetaData levelB[]=
{
    {200, "CA", "DA", 0, 0, 0},
    {201, "SA", "TA", 0, 0, 0}
};  // levelB


/** case: two levels, 100% overlap, both levels expired **/
TestFileMetaData levelC[]=
{
    {200, "CA", "DA", 1, 3, 0},
    {201, "SA", "TA", ULONG_MAX, 0, 4}
};  // levelC

TestFileMetaData levelD[]=
{
    {200, "CA", "DA", 1, 2, 0},
    {201, "SA", "TA", ULONG_MAX, 0, 2}
};  // levelD


TEST(ExpiryTester, OverlapTests)
{
    bool flag;
    Version::FileMetaDataVector_t level1, level2, level_clear, expired_files;
    uint64_t now;
    ExpiryModuleEE module;
    VersionTester ver;
    const int overlap0(0), overlap1(1), sorted0(3), sorted1(4);
    VersionEdit edit;

    module.m_ExpiryEnabled=true;
    module.m_WholeFileExpiry=true;
    module.m_ExpiryMinutes=2;

    now=port::TimeUint64();
    SetTimeMinutes(now);


    /** case: two levels, no overlap, no expiry **/
    CreateMetaArray(level1, levelA, 2);
    CreateMetaArray(level2, levelB, 2);
    ver.SetFileList(sorted0, level1);
    ver.SetFileList(sorted1, level2);
    flag=module.CompactionFinalizeCallback(true, ver, sorted0, &edit);
    ASSERT_EQ(flag, false);
    ASSERT_EQ(edit.DeletedFileCount(), 0);
    ver.SetFileList(sorted0, level_clear);
    ver.SetFileList(sorted1, level_clear);

    ver.SetFileList(overlap0, level1);
    ver.SetFileList(overlap1, level2);
    flag=module.CompactionFinalizeCallback(true, ver, overlap0, &edit);
    ASSERT_EQ(flag, false);
    ASSERT_EQ(edit.DeletedFileCount(), 0);
    ver.SetFileList(overlap0, level_clear);
    ver.SetFileList(overlap1, level_clear);

    ver.SetFileList(overlap0, level1);
    ver.SetFileList(sorted1, level2);
    flag=module.CompactionFinalizeCallback(true, ver, overlap0, &edit);
    ASSERT_EQ(flag, false);
    ASSERT_EQ(edit.DeletedFileCount(), 0);
    ver.SetFileList(overlap0, level_clear);
    ver.SetFileList(sorted1, level_clear);

    /** case: two levels, 100% overlap, both levels expired **/
    SetTimeMinutes(now);
    CreateMetaArray(level1, levelC, 2);
    CreateMetaArray(level2, levelD, 2);
    SetTimeMinutes(now + 5*60000000);
    ver.SetFileList(sorted0, level1);
    ver.SetFileList(sorted1, level2);
    flag=module.CompactionFinalizeCallback(true, ver, sorted0, &edit);
    ASSERT_EQ(flag, false);
    ASSERT_EQ(edit.DeletedFileCount(), 0);
    flag=module.CompactionFinalizeCallback(true, ver, sorted1, &edit);
    ASSERT_EQ(flag, true);
    ASSERT_EQ(edit.DeletedFileCount(), 2);
    ver.SetFileList(sorted0, level_clear);
    ver.SetFileList(sorted1, level_clear);




}   // OverlapTests

}  // namespace leveldb

