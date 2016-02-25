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
    before=port::NowUint64();
    flag=module.MemTableInserterCallback(key, value, type, expiry);
    after=port::NowUint64();
    ASSERT_EQ(flag, true);
    ASSERT_EQ(type, kTypeValueWriteTime);
    ASSERT_TRUE(before <= expiry && expiry <=after && 0!=expiry);

    // plain value, expiry disabled
    type=kTypeValue;
    expiry=0;
    module.m_ExpiryMinutes=0;
    before=port::NowUint64();
    flag=module.MemTableInserterCallback(key, value, type, expiry);
    after=port::NowUint64();
    ASSERT_EQ(flag, true);
    ASSERT_EQ(type, kTypeValue);
    ASSERT_EQ(expiry, 0);

    // write time value, needs expiry
    type=kTypeValueWriteTime;
    expiry=0;
    module.m_ExpiryMinutes=30;
    before=port::NowUint64();
    flag=module.MemTableInserterCallback(key, value, type, expiry);
    after=port::NowUint64();
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

}  // namespace leveldb

