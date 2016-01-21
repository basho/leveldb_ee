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

#include "util/testharness.h"
#include "util/testutil.h"

#include "leveldb/table.h"
#include "leveldb_ee/cache_warm.h"

/**
 * Execution routine
 */
int main(int argc, char** argv)
{
  return leveldb::test::RunAllTests();
}


namespace leveldb {
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
        m_DbName=test::TmpDir() + "/cache_warm";
        Env::Default()->DeleteDir(m_DbName);
        m_Status=Env::Default()->CreateDir(m_DbName);
    };

    ~CacheWarm()
    {
        Env::Default()->DeleteDir(m_DbName);
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
    log::Writer log_file(string_file);
    WarmingAccumulator acc(log_file);
    TableStub stub;
    TableAndFile table;

    table.table=&stub;

    // verify environment
    ASSERT_OK(m_Status);



}   // LogRecords

#if 0
TEST(DBTest, Empty) {
  do {
    ASSERT_TRUE(db_ != NULL);
    ASSERT_EQ("NOT_FOUND", Get("foo"));
  } while (ChangeOptions());
}

TEST(DBTest, DoubleOpen)
{
    ASSERT_NOTOK(DoubleOpen());
}

TEST(DBTest, ReadWrite) {
  do {
    ASSERT_OK(Put("foo", "v1"));
    ASSERT_EQ("v1", Get("foo"));
    ASSERT_OK(Put("bar", "v2"));
    ASSERT_OK(Put("foo", "v3"));
    ASSERT_EQ("v3", Get("foo"));
    ASSERT_EQ("v2", Get("bar"));
  } while (ChangeOptions());
}

TEST(DBTest, PutDeleteGet) {
  do {
    ASSERT_OK(db_->Put(WriteOptions(), "foo", "v1"));
    ASSERT_EQ("v1", Get("foo"));
    ASSERT_OK(db_->Put(WriteOptions(), "foo", "v2"));
    ASSERT_EQ("v2", Get("foo"));
    ASSERT_OK(db_->Delete(WriteOptions(), "foo"));
    ASSERT_EQ("NOT_FOUND", Get("foo"));
  } while (ChangeOptions());
}

TEST(DBTest, GetFromImmutableLayer) {
  do {
    Options options = CurrentOptions();
    options.env = env_;
    options.write_buffer_size = 100000;  // Small write buffer
    Reopen(&options);

    ASSERT_OK(Put("foo", "v1"));
    ASSERT_EQ("v1", Get("foo"));

    env_->delay_sstable_sync_.Release_Store(env_);   // Block sync calls
    Put("k1", std::string(100000, 'x'));             // Fill memtable
    Put("k2", std::string(100000, 'y'));             // Trigger compaction
    ASSERT_EQ("v1", Get("foo"));
    env_->delay_sstable_sync_.Release_Store(NULL);   // Release sync calls
  } while (ChangeOptions());
}

TEST(DBTest, GetFromVersions) {
  do {
    ASSERT_OK(Put("foo", "v1"));
    dbfull()->TEST_CompactMemTable();
    ASSERT_EQ("v1", Get("foo"));
  } while (ChangeOptions());
}

TEST(DBTest, GetSnapshot) {
  do {
    // Try with both a short key and a long key
    for (int i = 0; i < 2; i++) {
      std::string key = (i == 0) ? std::string("foo") : std::string(200, 'x');
      ASSERT_OK(Put(key, "v1"));
      const Snapshot* s1 = db_->GetSnapshot();
      ASSERT_OK(Put(key, "v2"));
      ASSERT_EQ("v2", Get(key));
      ASSERT_EQ("v1", Get(key, s1));
      dbfull()->TEST_CompactMemTable();
      ASSERT_EQ("v2", Get(key));
      ASSERT_EQ("v1", Get(key, s1));
      db_->ReleaseSnapshot(s1);
    }
  } while (ChangeOptions());
}


TEST(DBTest, ApproximateSizes_MixOfSmallAndLarge) {
  do {
    Options options = CurrentOptions();
    options.compression = kNoCompression;
    Reopen();

    Random rnd(301);
    std::string big1 = RandomString(&rnd, 100000);
    ASSERT_OK(Put(Key(0), RandomString(&rnd, 10000)));
    ASSERT_OK(Put(Key(1), RandomString(&rnd, 10000)));
    ASSERT_OK(Put(Key(2), big1));
    ASSERT_OK(Put(Key(3), RandomString(&rnd, 10000)));
    ASSERT_OK(Put(Key(4), big1));
    ASSERT_OK(Put(Key(5), RandomString(&rnd, 10000)));
    ASSERT_OK(Put(Key(6), RandomString(&rnd, 300000)));
    ASSERT_OK(Put(Key(7), RandomString(&rnd, 10000)));

    // Check sizes across recovery by reopening a few times
    for (int run = 0; run < 3; run++) {
      Reopen(&options);

      ASSERT_TRUE(Between(Size("", Key(0)), 0, 0));
      ASSERT_TRUE(Between(Size("", Key(1)), 10000, 11000));
      ASSERT_TRUE(Between(Size("", Key(2)), 20000, 21000));
      ASSERT_TRUE(Between(Size("", Key(3)), 120000, 121000));
      ASSERT_TRUE(Between(Size("", Key(4)), 130000, 131000));
      ASSERT_TRUE(Between(Size("", Key(5)), 230000, 231000));
      ASSERT_TRUE(Between(Size("", Key(6)), 240000, 241000));
      ASSERT_TRUE(Between(Size("", Key(7)), 540000, 541000));
      ASSERT_TRUE(Between(Size("", Key(8)), 550000, 560000));

      ASSERT_TRUE(Between(Size(Key(3), Key(5)), 110000, 111000));

      dbfull()->TEST_CompactRange(0, NULL, NULL);
    }
  } while (ChangeOptions());
}
#endif

}  // namespace leveldb

