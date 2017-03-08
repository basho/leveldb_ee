// -------------------------------------------------------------------
//
// cuttlefish_test.cc
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

#include "util/testharness.h"
#include "util/testutil.h"

#include "util/expiry_os.h"

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
class CuttleTest
{
public:
    CuttleTest()
    {
    };

    ~CuttleTest()
    {
    };

};  // class CuttleTest


/**
 * Does the math look good?
 */
TEST(CuttleTest, GoodStuff)
{
    ASSERT_EQ(0, CuttlefishDurationMinutes("59s"));
    ASSERT_EQ(1, CuttlefishDurationMinutes("60s"));
    ASSERT_EQ(1, CuttlefishDurationMinutes("61s"));
    ASSERT_EQ(10, CuttlefishDurationMinutes("600s"));

    ASSERT_EQ(1, CuttlefishDurationMinutes("1m"));
    ASSERT_EQ(2, CuttlefishDurationMinutes("2m"));
    ASSERT_EQ(12, CuttlefishDurationMinutes("2m600s"));

    ASSERT_EQ(60, CuttlefishDurationMinutes("1h"));
    ASSERT_EQ(180, CuttlefishDurationMinutes("3h"));
    ASSERT_EQ(1440, CuttlefishDurationMinutes("24h"));

    ASSERT_EQ(1440, CuttlefishDurationMinutes("1d"));
    ASSERT_EQ(10080, CuttlefishDurationMinutes("7d"));
    ASSERT_EQ(14400, CuttlefishDurationMinutes("10d"));
    ASSERT_EQ(20160, CuttlefishDurationMinutes("14d"));

    ASSERT_EQ(10080, CuttlefishDurationMinutes("1w"));
    ASSERT_EQ(20160, CuttlefishDurationMinutes("2w"));

    // no clue why fortnight is supported
    ASSERT_EQ(20160, CuttlefishDurationMinutes("1f"));

    // misc. (note 'ms' ignored),
    ASSERT_EQ(2, CuttlefishDurationMinutes("1m1s1ms1m"));
    ASSERT_EQ(31741, CuttlefishDurationMinutes("1f1w1d1h1m1s1ms"));

}   // GoodStuff


/**
 *
 */
TEST(CuttleTest, BadStuff)
{

}   // BadStuff


}  // namespace leveldb

