// -------------------------------------------------------------------
//
// cache_warm_test.cc
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

#include <vector>

#include "util/testharness.h"
#include "util/testutil.h"

#include "leveldb/table.h"
#include "leveldb_ee/cache_warm.h"
#include "leveldb/options.h"
#include "util/cache2.h"

/**
 * Execution routine
 */
int main(int argc, char** argv)
{
  return leveldb::test::RunAllTests();
}


namespace leveldb {

struct TestFileInfo
{
    uint64_t m_FileNum;
    uint64_t m_FileSize;
    int m_Level;
};  // struct TestFileInfo

typedef std::vector<struct TestFileInfo> TestFileInfoMap_t;


/**
 * a version of TableCache with FindTable() override for testing
 */
class TestingTableCache : public TableCache
{
public:
    TestingTableCache(const std::string& dbname, const Options* options, Cache * file_cache,
                      DoubleCache & doublecache, TestFileInfoMap_t & Map)
        : TableCache(dbname, options, file_cache, doublecache), m_Map(Map)  {};

    virtual Status FindTable(uint64_t file_number, uint64_t file_size, int level, Cache::Handle**, bool is_compaction=false)
    {
        TestFileInfo info;
        info.m_FileNum=file_number;
        info.m_FileSize=file_size;
        info.m_Level=level;

        m_Map.push_back(info);
        return(Status::OK());
    };

    TestFileInfoMap_t & m_Map;

};  // class TestingTableCache


/**
 * a version of WritableFile that simply accumulates
 * output in a std::string.
 */
class StringWritableFile : public WritableFile
{
public:
    StringWritableFile() {};
    ~StringWritableFile() {};

    virtual Status Append(const Slice& Data)
    {
        m_String.append(Data.data(), Data.size());
        return(Status::OK());
    };
    virtual Status Close() {return(Status::OK());};
    virtual Status Flush() {return(Status::OK());};
    virtual Status Sync() {return(Status::OK());};

    std::string & GetString() {return(m_String);};

protected:
    std::string m_String;

};  // class StringWritableFile


/**
 * SST table stub that allows file size manipulation
 */
class TableStub : public Table
{
public:
    TableStub() : Table((struct Table::Rep *)NULL), m_FileSize(0) {};

    virtual uint64_t GetFileSize() {return(m_FileSize);};

    void SetFileSize(uint64_t Size) {m_FileSize=Size;};

protected:
    uint64_t m_FileSize;

};  // TableStub


/**
 * Wrapper class for tests.  Holds working variables
 * and helper functions.
 */
class CacheWarm
{
public:
    std::string m_DbName;
    Status m_Status;

    CacheWarm()
    {
        std::string cow_name;

        m_DbName=test::TmpDir() + "/cache_warm";
        cow_name=CowFileName(m_DbName);
        Env::Default()->DeleteFile(cow_name);
        Env::Default()->DeleteDir(m_DbName);
        m_Status=Env::Default()->CreateDir(m_DbName);
    };

    ~CacheWarm()
    {
        Env::Default()->DeleteDir(m_DbName);
    };

    static void DeleteStubEntry(const Slice& Key, void* Value)
    {
        TableAndFile * tf;

        tf=(TableAndFile *)Value;
        delete tf->table;
        delete tf;
    }

    static TableAndFile * BuildStubEntry(
        int Level, uint64_t FileNumber, uint64_t FileSize)
    {
        TableAndFile * table;
        TableStub * stub;

        table=new TableAndFile;
        stub=new TableStub;

        table->level=Level;
        table->file_number=FileNumber;
        table->table=stub;
        stub->SetFileSize(FileSize);

        return(table);
    }   // BuildStubEntry

    static void BuildCacheEntry(
        Cache * FileCache,
        int Level, uint64_t FileNumber, uint64_t FileSize)
    {
        Slice key;
        TableAndFile * table;
        Cache::Handle * handle;

        table=BuildStubEntry(Level, FileNumber, FileSize);

        // real key is big endian
        key=Slice((char *)&table->file_number, sizeof(&table->file_number));
        handle=FileCache->Insert(key, table, 55, &DeleteStubEntry);
        FileCache->Release(handle);
    }   // BuildCacheEntry

    struct LogReporter : public log::Reader::Reporter
    {
        Env* env;
        Logger* info_log;
        const char* fname;
        Status* status;
        virtual void Corruption(size_t bytes, const Status& s) {
            Log(info_log, "%s%s: dropping %d bytes; %s",
                (this->status == NULL ? "(ignoring error) " : ""),
                fname, static_cast<int>(bytes), s.ToString().c_str());
            if (this->status != NULL && this->status->ok()) *this->status = s;
        }
    };

};  // class CacheWarm


/**
 * Does filename of warming file form correctly
 */
TEST(CacheWarm, Filename)
{
    std::string cow_name;
    Status s;
    WritableFile * cow_file;

    // verify environment
    ASSERT_OK(m_Status);

    // test name construction
    cow_name=CowFileName("/tmp/dbtest");
    ASSERT_EQ(cow_name, "/tmp/dbtest/COW");

    // test file construction
    cow_name=CowFileName(m_DbName);
    s = Env::Default()->NewWritableFile(cow_name, &cow_file, 4*1024L);
    ASSERT_OK(s);
    delete cow_file;
    Env::Default()->DeleteFile(cow_name);

}   // test Filename


/**
 * Are log records created correctly
 */
TEST(CacheWarm, LogRecords)
{
    StringWritableFile * string_file = new StringWritableFile;
    log::Writer * log_file=new log::Writer(string_file);
    WarmingAccumulator acc(log_file);
    TableStub stub;
    TableAndFile table;

    table.table=&stub;

    // verify environment
    ASSERT_OK(m_Status);

    table.level=1;
    table.file_number=2;
    stub.SetFileSize(3);
    acc((void *)&table);
    ASSERT_EQ(acc.GetRecord().size(), 4);
    ASSERT_EQ(memcmp(acc.GetRecord().data(), "\012\001\002\003",4),0);
    ASSERT_EQ(acc.GetCount(), 1);

    table.level=4;
    table.file_number=5;
    stub.SetFileSize(6);
    acc((void *)&table);
    ASSERT_EQ(acc.GetRecord().size(), 8);
    ASSERT_EQ(memcmp(acc.GetRecord().data(), "\012\001\002\003\012\004\005\006",8),0);
    ASSERT_EQ(acc.GetCount(), 2);

    // reset record via "write"
    acc.WriteRecord();
    ASSERT_EQ(acc.GetRecord().size(), 0);  // record purged ...
    ASSERT_EQ(acc.GetCount(), 2);          // ... but count still good
    ASSERT_EQ(string_file->GetString().size(), 8 + 7); // +7 is log::Writer header/prefix
    ASSERT_EQ(memcmp(string_file->GetString().data()+7, "\012\001\002\003\012\004\005\006",8),0);

}   // LogRecords

/**
 * Simulate table cache, does it produce expected records
 */
TEST(CacheWarm, WalkCache)
{
    StringWritableFile * string_file = new StringWritableFile;
    log::Writer * log_file=new log::Writer(string_file);
    WarmingAccumulator acc(log_file);
    Options options;
    DoubleCache double_cache(options);
    Cache * file_cache;

    file_cache=double_cache.GetFileCache();

    // verify environment
    ASSERT_OK(m_Status);

    //
    // NOTE:  output order from file cache will not necessarily
    //        be same as input order.

    // BuildCacheEntry(cache, level, file_no, file_size)
    BuildCacheEntry(file_cache, 0, 2, 3);
    BuildCacheEntry(file_cache, 0, 4, 5);
    BuildCacheEntry(file_cache, 0, 6, 7);
    BuildCacheEntry(file_cache, 0, 8, 9);
    BuildCacheEntry(file_cache, 1, 12, 13);
    BuildCacheEntry(file_cache, 1, 14, 15);
    BuildCacheEntry(file_cache, 1, 16, 17);
    BuildCacheEntry(file_cache, 2, 22, 23);
    BuildCacheEntry(file_cache, 2, 24, 25);

    file_cache->WalkCache(acc);
    // 4 bytes * 9 entries
    ASSERT_EQ(acc.GetRecord().size(), 4*9);
    ASSERT_EQ(acc.GetCount(), 9);

    acc.WriteRecord();
    ASSERT_EQ(acc.GetRecord().size(), 0);  // record purged ...
    ASSERT_EQ(acc.GetCount(), 9);          // ... but count still good
    ASSERT_EQ(string_file->GetString().size(), (4*9) + 7); // +7 is log::Writer header/prefix
    ASSERT_EQ(memcmp(string_file->GetString().data()+7,
                     "\012\000\004\005" "\012\002\026\027" "\012\000\006\007" "\012\001\020\021"
                     "\012\002\030\031" "\012\000\002\003" "\012\000\010\011" "\012\001\014\015"
                     "\012\001\016\017",
                     36),0);

}   // WalkCache


/**
 * Build same cache as previous step, but actually write to file
 *  then read back.
 */
TEST(CacheWarm, SimpleWriteRead)
{
    Options options;
    DoubleCache double_cache(options);
    Cache * file_cache(double_cache.GetFileCache());
    TestFileInfoMap_t map;
    TestingTableCache table_cache(m_DbName, &options, file_cache, double_cache, map);
    Status s;
    TestFileInfo info;

    // verify environment
    ASSERT_OK(m_Status);

    //
    // NOTE:  output order from file cache will not necessarily
    //        be same as input order.

    // BuildCacheEntry(cache, level, file_no, file_size)
    BuildCacheEntry(file_cache, 0, 2, 3);
    BuildCacheEntry(file_cache, 0, 4, 5);
    BuildCacheEntry(file_cache, 0, 6, 7);
    BuildCacheEntry(file_cache, 0, 8, 9);
    BuildCacheEntry(file_cache, 1, 12, 13);
    BuildCacheEntry(file_cache, 1, 14, 15);
    BuildCacheEntry(file_cache, 1, 16, 17);
    BuildCacheEntry(file_cache, 2, 22, 23);
    BuildCacheEntry(file_cache, 2, 24, 25);

    s=table_cache.SaveOpenFileList();
    ASSERT_OK(s);

    s=table_cache.PreloadTableCache();
    ASSERT_OK(s);

    // validate output map
    ASSERT_EQ(map.size(), 9);

    ASSERT_EQ(map[0].m_FileNum, 4);
    ASSERT_EQ(map[1].m_FileNum, 22);
    ASSERT_EQ(map[2].m_FileNum, 6);
    ASSERT_EQ(map[3].m_FileNum, 16);
    ASSERT_EQ(map[4].m_FileNum, 24);
    ASSERT_EQ(map[5].m_FileNum, 2);
    ASSERT_EQ(map[6].m_FileNum, 8);
    ASSERT_EQ(map[7].m_FileNum, 12);
    ASSERT_EQ(map[8].m_FileNum, 14);

}   // SimpleWriteRead


/**
 * Write 4,521 cache objects.  See if 4,521 come back.
 *  Content not validate in current test.  Validating record management
 */
TEST(CacheWarm, LargeWriteRead)
{
    Options options;
    DoubleCache double_cache(options);
    Cache * file_cache(double_cache.GetFileCache());
    TestFileInfoMap_t map;
    TestingTableCache table_cache(m_DbName, &options, file_cache, double_cache, map);
    Status s;
    TestFileInfo info;
    int loop;

    // verify environment
    ASSERT_OK(m_Status);

    //
    // NOTE:  output order from file cache will not necessarily
    //        be same as input order.

    // BuildCacheEntry(cache, level, file_no, file_size)
    for (loop=0; loop<4521; ++loop)
    {
        BuildCacheEntry(file_cache, 0, loop, 3);
    }   // for

    s=table_cache.SaveOpenFileList();
    ASSERT_OK(s);

    s=table_cache.PreloadTableCache();
    ASSERT_OK(s);

    // validate output map
    ASSERT_EQ(map.size(), 4521);

}   // LargeWriteRead


}  // namespace leveldb

