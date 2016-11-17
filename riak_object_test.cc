// -------------------------------------------------------------------
//
// riak_object_test.cc
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

#include <string>

#include "util/testharness.h"
#include "util/testutil.h"

#include "leveldb_ee/riak_object.h"


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

/**
 * Wrapper class for tests.  Holds working variables
 * and helper functions.
 */
class RiakObjectTester
{
public:

    RiakObjectTester()
    {
    };

    virtual ~RiakObjectTester()
    {
    };

};  // class RiakObjectTester


/**
 * Verify key decode into bucket type and bucket.
 *  as flag to start a backup.
 */
TEST(RiakObjectTester, KeyDecodeTest)
{
    bool ret_flag;
    std::string bucket_type, bucket;

    // metadata key:  {md,fixed_indexes}
    const char md_key[]={0x10, 0x00, 0x00, 0x00, 0x02, 0x0c, 0xb6, 0xd9, 0x00, 0x08,
                         0x0c, 0xb3, 0x5a, 0x6f, 0x16, 0x5b, 0x25, 0x7e, 0xd3, 0x6e,
                         0xb2, 0x59, 0x6f, 0x16, 0x5b, 0x98, 0x08};
    Slice md_slice(md_key, sizeof(md_key));

    ret_flag=KeyGetBucket(md_slice, bucket_type, bucket);
    ASSERT_FALSE(ret_flag);
    ASSERT_TRUE(0==bucket_type.length());
    ASSERT_TRUE(0==bucket.length());

    // bucket only: {o,<<buck0>>,<<yesterday>>}
    const char bo_key[]={0x10, 0x00, 0x00, 0x00, 0x03, 0x0c, 0xb7, 0x80, 0x08, 0x12,
                         0xb1, 0x5d, 0x6c, 0x76, 0xb9, 0x80, 0x08, 0x12, 0xbc, 0xd9,
                         0x6e, 0x77, 0x4b, 0x2d, 0xca, 0xc9, 0x61, 0xbc, 0x80, 0x08};
    Slice bo_slice(bo_key, sizeof(bo_key));

    ret_flag=KeyGetBucket(bo_slice, bucket_type, bucket);
    ASSERT_TRUE(ret_flag);
    ASSERT_TRUE(0==bucket_type.length());
    ASSERT_TRUE(0==bucket.compare("buck0"));

    // bucket_type and bucket: {o,{<<bob3>>,<<buck3>>},<<key3>>}
    const char bt_key[]={0x10, 0x00, 0x00, 0x00, 0x03, 0x0c, 0xb7, 0x80, 0x08, 0x10,
                         0x00, 0x00, 0x00, 0x02, 0x12, 0xb1, 0x5b, 0xec, 0x53, 0x30,
                         0x08, 0x12, 0xb1, 0x5d, 0x6c, 0x76, 0xb9, 0x98, 0x08, 0x12,
                         0xb5, 0xd9, 0x6f, 0x33, 0x30, 0x08};
    Slice bt_slice(bt_key, sizeof(bt_key));

    ret_flag=KeyGetBucket(bt_slice, bucket_type, bucket);
    ASSERT_TRUE(ret_flag);
    ASSERT_TRUE(0==bucket_type.compare("bob3"));
    ASSERT_TRUE(0==bucket.compare("buck3"));

    // {o,{<<really_long_bucket_type_name>>,<<buck0>>},<<key0>>}
    const char lbt_key[]={0x10, 0x00, 0x00, 0x00, 0x03, 0x0c, 0xb7, 0x80, 0x08, 0x10,
                          0x00, 0x00, 0x00, 0x02, 0x12, 0xb9, 0x59, 0x6c, 0x36, 0xcb,
                          0x65, 0xe6, 0xbf, 0x6c, 0xb7, 0xdb, 0xac, 0xf5, 0xfb, 0x15,
                          0xd6, 0xc7, 0x6b, 0xb2, 0xdd, 0x2b, 0xf7, 0x4b, 0xcd, 0xc2,
                          0xcb, 0x5f, 0xb7, 0x58, 0x6d, 0xb6, 0x50, 0x08, 0x12, 0xb1,
                          0x5d, 0x6c, 0x76, 0xb9, 0x80, 0x08, 0x12, 0xb5, 0xd9, 0x6f,
                          0x33, 0x00, 0x08};
    Slice lbt_slice(lbt_key, sizeof(lbt_key));

    ret_flag=KeyGetBucket(lbt_slice, bucket_type, bucket);
    ASSERT_TRUE(ret_flag);
    ASSERT_TRUE(0==bucket_type.compare("really_long_bucket_type_name"));
    ASSERT_TRUE(0==bucket.compare("buck0"));

    // {o,{<<really_long_bucket_type_name>>,<<even_longer_than_really_long_bucket_name>>},<<key0>>}
    const char rlb_key[]={0x10, 0x00, 0x00, 0x00, 0x03, 0x0c, 0xb7, 0x80, 0x08, 0x10,
                          0x00, 0x00, 0x00, 0x02, 0x12, 0xb9, 0x59, 0x6c, 0x36, 0xcb,
                          0x65, 0xe6, 0xbf, 0x6c, 0xb7, 0xdb, 0xac, 0xf5, 0xfb, 0x15,
                          0xd6, 0xc7, 0x6b, 0xb2, 0xdd, 0x2b, 0xf7, 0x4b, 0xcd, 0xc2,
                          0xcb, 0x5f, 0xb7, 0x58, 0x6d, 0xb6, 0x50, 0x08, 0x12, 0xb2,
                          0xdd, 0xac, 0xb6, 0xea, 0xfd, 0xb2, 0xdf, 0x6e, 0xb3, 0xd9,
                          0x6e, 0x55, 0xfb, 0xa5, 0xa2, 0xc3, 0x6e, 0xaf, 0xdc, 0xac,
                          0xb6, 0x1b, 0x65, 0xb2, 0xf3, 0x5f, 0xb6, 0x5b, 0xed, 0xd6,
                          0x7a, 0xfd, 0x8a, 0xeb, 0x63, 0xb5, 0xd9, 0x6e, 0x95, 0xfb,
                          0x75, 0x86, 0xdb, 0x65, 0x00, 0x08, 0x12, 0xb5, 0xd9, 0x6f,
                          0x33, 0x00, 0x08};
    Slice rlb_slice(rlb_key, sizeof(rlb_key));

    ret_flag=KeyGetBucket(rlb_slice, bucket_type, bucket);
    ASSERT_TRUE(ret_flag);
    ASSERT_TRUE(0==bucket_type.compare("really_long_bucket_type_name"));
    ASSERT_TRUE(0==bucket.compare("even_longer_than_really_long_bucket_name"));


    //
    // verify the binary decode works for all sizes from 1 to 10 (algorithm repeats after 8)
    //

    //  {o,<<b>>,<<size1>>}
    const char b1_key[]={0x10, 0x00, 0x00, 0x00, 0x03, 0x0c, 0xb7, 0x80, 0x08, 0x12,
                         0xb1, 0x00, 0x08, 0x12, 0xb9, 0xda, 0x6f, 0x56, 0x59, 0x88,
                         0x08};
    Slice b1_slice(b1_key, sizeof(b1_key));

    ret_flag=KeyGetBucket(b1_slice, bucket_type, bucket);
    ASSERT_TRUE(ret_flag);
    ASSERT_TRUE(0==bucket_type.length());
    ASSERT_TRUE(0==bucket.compare("b"));

    // {o,<<b2>>,<<size2>>}
    const char b2_key[]={0x10, 0x00, 0x00, 0x00, 0x03, 0x0c, 0xb7, 0x80, 0x08, 0x12,
                         0xb1, 0x4c, 0x80, 0x08, 0x12, 0xb9, 0xda, 0x6f, 0x56, 0x59,
                         0x90, 0x08};
    Slice b2_slice(b2_key, sizeof(b2_key));

    ret_flag=KeyGetBucket(b2_slice, bucket_type, bucket);
    ASSERT_TRUE(ret_flag);
    ASSERT_TRUE(0==bucket_type.length());
    ASSERT_TRUE(0==bucket.compare("b2"));

    //  {o,<<b23>>,<<size3>>}
    const char b3_key[]={0x10, 0x00, 0x00, 0x00, 0x03, 0x0c, 0xb7, 0x80, 0x08, 0x12,
                         0xb1, 0x4c, 0xa6, 0x60, 0x08, 0x12, 0xb9, 0xda, 0x6f, 0x56,
                         0x59, 0x98, 0x08};
    Slice b3_slice(b3_key, sizeof(b3_key));

    ret_flag=KeyGetBucket(b3_slice, bucket_type, bucket);
    ASSERT_TRUE(ret_flag);
    ASSERT_TRUE(0==bucket_type.length());
    ASSERT_TRUE(0==bucket.compare("b23"));

    //  {o,<<b234>>,<<size4>>}
    const char b4_key[]={0x10, 0x00, 0x00, 0x00, 0x03, 0x0c, 0xb7, 0x80, 0x08, 0x12,
                         0xb1, 0x4c, 0xa6, 0x73, 0x40, 0x08, 0x12, 0xb9, 0xda, 0x6f,
                         0x56, 0x59, 0xa0, 0x08};
    Slice b4_slice(b4_key, sizeof(b4_key));

    ret_flag=KeyGetBucket(b4_slice, bucket_type, bucket);
    ASSERT_TRUE(ret_flag);
    ASSERT_TRUE(0==bucket_type.length());
    ASSERT_TRUE(0==bucket.compare("b234"));

    //  {o,<<b2345>>,<<size5>>}
    const char b5_key[]={0x10, 0x00, 0x00, 0x00, 0x03, 0x0c, 0xb7, 0x80, 0x08, 0x12,
                         0xb1, 0x4c, 0xa6, 0x73, 0x49, 0xa8, 0x08, 0x12, 0xb9, 0xda,
                         0x6f, 0x56, 0x59, 0xa8, 0x08};
    Slice b5_slice(b5_key, sizeof(b5_key));

    ret_flag=KeyGetBucket(b5_slice, bucket_type, bucket);
    ASSERT_TRUE(ret_flag);
    ASSERT_TRUE(0==bucket_type.length());
    ASSERT_TRUE(0==bucket.compare("b2345"));

    //  {o,<<b23456>>,<<size6>>}
    const char b6_key[]={0x10, 0x00, 0x00, 0x00, 0x03, 0x0c, 0xb7, 0x80, 0x08, 0x12,
                         0xb1, 0x4c, 0xa6, 0x73, 0x49, 0xac, 0xd8, 0x08, 0x12, 0xb9,
                         0xda, 0x6f, 0x56, 0x59, 0xb0, 0x08};
    Slice b6_slice(b6_key, sizeof(b6_key));

    ret_flag=KeyGetBucket(b6_slice, bucket_type, bucket);
    ASSERT_TRUE(ret_flag);
    ASSERT_TRUE(0==bucket_type.length());
    ASSERT_TRUE(0==bucket.compare("b23456"));

    //  {o,<<b234567>>,<<size7>>}
    const char b7_key[]={0x10, 0x00, 0x00, 0x00, 0x03, 0x0c, 0xb7, 0x80, 0x08, 0x12,
                         0xb1, 0x4c, 0xa6, 0x73, 0x49, 0xac, 0xda, 0x6e, 0x08, 0x12,
                         0xb9, 0xda, 0x6f, 0x56, 0x59, 0xb8, 0x08};
    Slice b7_slice(b7_key, sizeof(b7_key));

    ret_flag=KeyGetBucket(b7_slice, bucket_type, bucket);
    ASSERT_TRUE(ret_flag);
    ASSERT_TRUE(0==bucket_type.length());
    ASSERT_TRUE(0==bucket.compare("b234567"));


    //  {o,<<b2345678>>,<<size8>>}
    const char b8_key[]={0x10, 0x00, 0x00, 0x00, 0x03, 0x0c, 0xb7, 0x80, 0x08, 0x12,
                         0xb1, 0x4c, 0xa6, 0x73, 0x49, 0xac, 0xda, 0x6f, 0x38, 0x00,
                         0x08, 0x12, 0xb9, 0xda, 0x6f, 0x56, 0x59, 0xc0, 0x08};
    Slice b8_slice(b8_key, sizeof(b8_key));

    ret_flag=KeyGetBucket(b8_slice, bucket_type, bucket);
    ASSERT_TRUE(ret_flag);
    ASSERT_TRUE(0==bucket_type.length());
    ASSERT_TRUE(0==bucket.compare("b2345678"));

    //  {o,<<b23456789>>,<<size9>>}
    const char b9_key[]={0x10, 0x00, 0x00, 0x00, 0x03, 0x0c, 0xb7, 0x80, 0x08, 0x12,
                         0xb1, 0x4c, 0xa6, 0x73, 0x49, 0xac, 0xda, 0x6f, 0x38, 0x9c,
                         0x80, 0x08, 0x12, 0xb9, 0xda, 0x6f, 0x56, 0x59, 0xc8, 0x08};
    Slice b9_slice(b9_key, sizeof(b9_key));

    ret_flag=KeyGetBucket(b9_slice, bucket_type, bucket);
    ASSERT_TRUE(ret_flag);
    ASSERT_TRUE(0==bucket_type.length());
    ASSERT_TRUE(0==bucket.compare("b23456789"));

    //  {o,<<b234567890>>,<<size10>>}
    const char b10_key[]={0x10, 0x00, 0x00, 0x00, 0x03, 0x0c, 0xb7, 0x80, 0x08, 0x12,
                          0xb1, 0x4c, 0xa6, 0x73, 0x49, 0xac, 0xda, 0x6f, 0x38, 0x9c,
                          0xcc, 0x00, 0x08, 0x12, 0xb9, 0xda, 0x6f, 0x56, 0x59, 0x8c,
                          0xc0, 0x08};
    Slice b10_slice(b10_key, sizeof(b10_key));

    ret_flag=KeyGetBucket(b10_slice, bucket_type, bucket);
    ASSERT_TRUE(ret_flag);
    ASSERT_TRUE(0==bucket_type.length());
    ASSERT_TRUE(0==bucket.compare("b234567890"));

    //
    // Riak TS key
    //

    //  {o,{<<GeoCheckin>>,<<GeoCheckin>>},{<<basho-c3s1>>,<<130>>,}}
    const char ts_key[]={0x10, 0x00, 0x00, 0x00, 0x03, 0x0c, 0xb7, 0x80, 0x08, 0x10,
                         0x00, 0x00, 0x00, 0x02, 0x12, 0xa3, 0xd9, 0x6d, 0xf4, 0x3b,
                         0x45, 0x96, 0xc7, 0x6b, 0xb4, 0xdb, 0x80, 0x08, 0x12, 0xa3,
                         0xd9, 0x6d, 0xf4, 0x3b, 0x45, 0x96, 0xc7, 0x6b, 0xb4, 0xdb,
                         0x80, 0x08, 0x10, 0x00, 0x00, 0x00, 0x03, 0x12, 0xb1, 0x58,
                         0x6e, 0x76, 0x8b, 0x7c, 0xb6, 0xc7, 0x33, 0xb9, 0xcc, 0x40,
                         0x08, 0x12, 0x98, 0xcc, 0xe6, 0x00, 0x08, 0x0b, 0xff, 0xc1,
                         0xa0, 0x35, 0x8b, 0x6f, 0x93, 0x95, 0x9f, 0x00, 0x08, 0x00};
    Slice ts_slice(ts_key, sizeof(ts_key));

    ret_flag=KeyGetBucket(ts_slice, bucket_type, bucket);
    ASSERT_TRUE(ret_flag);
    ASSERT_TRUE(0==bucket_type.compare("GeoCheckin"));
    ASSERT_TRUE(0==bucket.compare("GeoCheckin"));

}   // KeyDecodeTester


/**
 * Test decode of various last write time values
 *  (look for quiet failures)
 */
TEST(RiakObjectTester, LastModTest)
{
    bool ret_flag;
    uint64_t ret_time;

    // X-Riak-Meta-Expiry-Base-Sec: 1245494500 (date -d "2009-06-09 07:00:00" +%s)
    const char patriot_val[]={0x35, 0x01, 0x00, 0x00, 0x00, 0x22, 0x83, 0x6c, 0x00, 0x00,
                              0x00, 0x01, 0x68, 0x02, 0x6d, 0x00, 0x00, 0x00, 0x08, 0x23,
                              0x09, 0xfe, 0xf9, 0xef, 0x4a, 0xf0, 0x11, 0x68, 0x02, 0x61,
                              0x01, 0x6e, 0x05, 0x00, 0xc9, 0x69, 0xa2, 0xd1, 0x0e, 0x6a,
                              0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x0e, 0x01, 0x2d,
                              0x2d, 0x2c, 0x62, 0x75, 0x63, 0x6b, 0x30, 0x2c, 0x6b, 0x65,
                              0x79, 0x30, 0x00, 0x00, 0x00, 0xca, 0x00, 0x00, 0x05, 0xc7,
                              0x00, 0x06, 0x2e, 0x09, 0x00, 0x05, 0x66, 0x1c, 0x16, 0x31,
                              0x37, 0x77, 0x33, 0x70, 0x69, 0x49, 0x42, 0x55, 0x47, 0x4d,
                              0x59, 0x4b, 0x32, 0x48, 0x42, 0x7a, 0x32, 0x51, 0x43, 0x32,
                              0x77, 0x00, 0x00, 0x00, 0x00, 0x0c, 0x01, 0x58, 0x2d, 0x52,
                              0x69, 0x61, 0x6b, 0x2d, 0x4d, 0x65, 0x74, 0x61, 0x00, 0x00,
                              0x00, 0x35, 0x00, 0x83, 0x6c, 0x00, 0x00, 0x00, 0x01, 0x68,
                              0x02, 0x6b, 0x00, 0x1b, 0x58, 0x2d, 0x52, 0x69, 0x61, 0x6b,
                              0x2d, 0x4d, 0x65, 0x74, 0x61, 0x2d, 0x45, 0x78, 0x70, 0x69,
                              0x72, 0x79, 0x2d, 0x42, 0x61, 0x73, 0x65, 0x2d, 0x53, 0x65,
                              0x63, 0x6b, 0x00, 0x0a, 0x31, 0x32, 0x34, 0x35, 0x34, 0x39,
                              0x34, 0x35, 0x30, 0x30, 0x6a, 0x00, 0x00, 0x00, 0x06, 0x01,
                              0x69, 0x6e, 0x64, 0x65, 0x78, 0x00, 0x00, 0x00, 0x03, 0x00,
                              0x83, 0x6a, 0x00, 0x00, 0x00, 0x0d, 0x01, 0x63, 0x6f, 0x6e,
                              0x74, 0x65, 0x6e, 0x74, 0x2d, 0x74, 0x79, 0x70, 0x65, 0x00,
                              0x00, 0x00, 0x26, 0x00, 0x83, 0x6b, 0x00, 0x21, 0x61, 0x70,
                              0x70, 0x6c, 0x69, 0x63, 0x61, 0x74, 0x69, 0x6f, 0x6e, 0x2f,
                              0x78, 0x2d, 0x77, 0x77, 0x77, 0x2d, 0x66, 0x6f, 0x72, 0x6d,
                              0x2d, 0x75, 0x72, 0x6c, 0x65, 0x6e, 0x63, 0x6f, 0x64, 0x65,
                              0x64, 0x00, 0x00, 0x00, 0x06, 0x01, 0x4c, 0x69, 0x6e, 0x6b,
                              0x73, 0x00, 0x00, 0x00, 0x03, 0x00, 0x83, 0x6a};
    Slice patriot_slice(patriot_val, sizeof(patriot_val));

    ret_flag=ValueGetLastModTime(patriot_slice, ret_time);
    ASSERT_TRUE(ret_flag);
    ASSERT_TRUE(1245494500000000==ret_time);


    // given date too old: 315501010 (date -d "1979-12-31 10:10:10" +%s)
    // using LastModDate
    const char too_old_val[]={0x35, 0x01, 0x00, 0x00, 0x00, 0x22, 0x83, 0x6c, 0x00, 0x00,
                              0x00, 0x01, 0x68, 0x02, 0x6d, 0x00, 0x00, 0x00, 0x08, 0x23,
                              0x09, 0xfe, 0xf9, 0xef, 0x4a, 0xf0, 0x6a, 0x68, 0x02, 0x61,
                              0x01, 0x6e, 0x05, 0x00, 0x68, 0x6a, 0xa2, 0xd1, 0x0e, 0x6a,
                              0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x0e, 0x01, 0x2d,
                              0x2d, 0x2c, 0x62, 0x75, 0x63, 0x6b, 0x30, 0x2c, 0x6b, 0x65,
                              0x79, 0x30, 0x00, 0x00, 0x00, 0xc9, 0x00, 0x00, 0x05, 0xc7,
                              0x00, 0x06, 0x2e, 0xa8, 0x00, 0x01, 0x58, 0xc7, 0x16, 0x36,
                              0x58, 0x48, 0x71, 0x6a, 0x46, 0x45, 0x53, 0x6d, 0x73, 0x46,
                              0x37, 0x5a, 0x46, 0x38, 0x4f, 0x59, 0x4e, 0x56, 0x55, 0x71,
                              0x73, 0x00, 0x00, 0x00, 0x00, 0x0c, 0x01, 0x58, 0x2d, 0x52,
                              0x69, 0x61, 0x6b, 0x2d, 0x4d, 0x65, 0x74, 0x61, 0x00, 0x00,
                              0x00, 0x34, 0x00, 0x83, 0x6c, 0x00, 0x00, 0x00, 0x01, 0x68,
                              0x02, 0x6b, 0x00, 0x1b, 0x58, 0x2d, 0x52, 0x69, 0x61, 0x6b,
                              0x2d, 0x4d, 0x65, 0x74, 0x61, 0x2d, 0x45, 0x78, 0x70, 0x69,
                              0x72, 0x79, 0x2d, 0x42, 0x61, 0x73, 0x65, 0x2d, 0x53, 0x65,
                              0x63, 0x6b, 0x00, 0x09, 0x33, 0x31, 0x35, 0x35, 0x30, 0x31,
                              0x30, 0x31, 0x30, 0x6a, 0x00, 0x00, 0x00, 0x06, 0x01, 0x69,
                              0x6e, 0x64, 0x65, 0x78, 0x00, 0x00, 0x00, 0x03, 0x00, 0x83,
                              0x6a, 0x00, 0x00, 0x00, 0x0d, 0x01, 0x63, 0x6f, 0x6e, 0x74,
                              0x65, 0x6e, 0x74, 0x2d, 0x74, 0x79, 0x70, 0x65, 0x00, 0x00,
                              0x00, 0x26, 0x00, 0x83, 0x6b, 0x00, 0x21, 0x61, 0x70, 0x70,
                              0x6c, 0x69, 0x63, 0x61, 0x74, 0x69, 0x6f, 0x6e, 0x2f, 0x78,
                              0x2d, 0x77, 0x77, 0x77, 0x2d, 0x66, 0x6f, 0x72, 0x6d, 0x2d,
                              0x75, 0x72, 0x6c, 0x65, 0x6e, 0x63, 0x6f, 0x64, 0x65, 0x64,
                              0x00, 0x00, 0x00, 0x06, 0x01, 0x4c, 0x69, 0x6e, 0x6b, 0x73,
                              0x00, 0x00, 0x00, 0x03, 0x00, 0x83, 0x6a};
    Slice too_old_slice(too_old_val, sizeof(too_old_val));

    ret_flag=ValueGetLastModTime(too_old_slice, ret_time);
    ASSERT_TRUE(ret_flag);
    ASSERT_TRUE(1479405160088263==ret_time);

#if 0
    // given date too futuristic: 3471411600 (date -d "2080-01-02 04:00:00" +%s)
    // using LastModDate
    const char futuristic_val[]={};
#endif
    // date not in seconds: 2012-07-14 07:22:11
    // using LastModDate
    const char bad_num_val[]={0x35, 0x01, 0x00, 0x00, 0x00, 0x22, 0x83, 0x6c, 0x00, 0x00,
                              0x00, 0x01, 0x68, 0x02, 0x6d, 0x00, 0x00, 0x00, 0x08, 0x23,
                              0x09, 0xfe, 0xf9, 0xef, 0x4a, 0xf0, 0x6a, 0x68, 0x02, 0x61,
                              0x01, 0x6e, 0x05, 0x00, 0x71, 0x6b, 0xa2, 0xd1, 0x0e, 0x6a,
                              0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x0e, 0x01, 0x2d,
                              0x2d, 0x2c, 0x62, 0x75, 0x63, 0x6b, 0x30, 0x2c, 0x6b, 0x65,
                              0x79, 0x30, 0x00, 0x00, 0x00, 0xd3, 0x00, 0x00, 0x05, 0xc7,
                              0x00, 0x06, 0x2f, 0xb1, 0x00, 0x0a, 0x2a, 0x45, 0x16, 0x35,
                              0x53, 0x5a, 0x4e, 0x4f, 0x78, 0x6e, 0x65, 0x4b, 0x42, 0x30,
                              0x69, 0x4f, 0x6f, 0x42, 0x73, 0x6b, 0x4b, 0x58, 0x55, 0x76,
                              0x63, 0x00, 0x00, 0x00, 0x00, 0x0c, 0x01, 0x58, 0x2d, 0x52,
                              0x69, 0x61, 0x6b, 0x2d, 0x4d, 0x65, 0x74, 0x61, 0x00, 0x00,
                              0x00, 0x3e, 0x00, 0x83, 0x6c, 0x00, 0x00, 0x00, 0x01, 0x68,
                              0x02, 0x6b, 0x00, 0x1b, 0x58, 0x2d, 0x52, 0x69, 0x61, 0x6b,
                              0x2d, 0x4d, 0x65, 0x74, 0x61, 0x2d, 0x45, 0x78, 0x70, 0x69,
                              0x72, 0x79, 0x2d, 0x42, 0x61, 0x73, 0x65, 0x2d, 0x53, 0x65,
                              0x63, 0x6b, 0x00, 0x13, 0x32, 0x30, 0x31, 0x32, 0x2d, 0x30,
                              0x37, 0x2d, 0x31, 0x34, 0x20, 0x30, 0x37, 0x3a, 0x32, 0x32,
                              0x3a, 0x31, 0x31, 0x6a, 0x00, 0x00, 0x00, 0x06, 0x01, 0x69,
                              0x6e, 0x64, 0x65, 0x78, 0x00, 0x00, 0x00, 0x03, 0x00, 0x83,
                              0x6a, 0x00, 0x00, 0x00, 0x0d, 0x01, 0x63, 0x6f, 0x6e, 0x74,
                              0x65, 0x6e, 0x74, 0x2d, 0x74, 0x79, 0x70, 0x65, 0x00, 0x00,
                              0x00, 0x26, 0x00, 0x83, 0x6b, 0x00, 0x21, 0x61, 0x70, 0x70,
                              0x6c, 0x69, 0x63, 0x61, 0x74, 0x69, 0x6f, 0x6e, 0x2f, 0x78,
                              0x2d, 0x77, 0x77, 0x77, 0x2d, 0x66, 0x6f, 0x72, 0x6d, 0x2d,
                              0x75, 0x72, 0x6c, 0x65, 0x6e, 0x63, 0x6f, 0x64, 0x65, 0x64,
                              0x00, 0x00, 0x00, 0x06, 0x01, 0x4c, 0x69, 0x6e, 0x6b, 0x73,
                              0x00, 0x00, 0x00, 0x03, 0x00, 0x83, 0x6a};
    Slice bad_num_slice(bad_num_val, sizeof(bad_num_val));

    ret_flag=ValueGetLastModTime(bad_num_slice, ret_time);
    ASSERT_TRUE(ret_flag);
    ASSERT_TRUE(1479405425666181==ret_time);


    // text date, not in seconds:  Jan 7, 2009 12:34:00
    // using LastModDate
    const char text_date_val[]={0x35, 0x01, 0x00, 0x00, 0x00, 0x22, 0x83, 0x6c, 0x00, 0x00,
                                0x00, 0x01, 0x68, 0x02, 0x6d, 0x00, 0x00, 0x00, 0x08, 0x23,
                                0x09, 0xfe, 0xf9, 0xef, 0x4a, 0xef, 0x45, 0x68, 0x02, 0x61,
                                0x01, 0x6e, 0x05, 0x00, 0x38, 0x6b, 0xa2, 0xd1, 0x0e, 0x6a,
                                0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x0e, 0x01, 0x2d,
                                0x2d, 0x2c, 0x62, 0x75, 0x63, 0x6b, 0x30, 0x2c, 0x6b, 0x65,
                                0x79, 0x30, 0x00, 0x00, 0x00, 0xd4, 0x00, 0x00, 0x05, 0xc7,
                                0x00, 0x06, 0x2f, 0x78, 0x00, 0x0e, 0xe6, 0x83, 0x16, 0x33,
                                0x38, 0x59, 0x75, 0x6d, 0x75, 0x4e, 0x53, 0x52, 0x78, 0x36,
                                0x72, 0x4a, 0x42, 0x77, 0x48, 0x53, 0x79, 0x64, 0x42, 0x42,
                                0x49, 0x00, 0x00, 0x00, 0x00, 0x0c, 0x01, 0x58, 0x2d, 0x52,
                                0x69, 0x61, 0x6b, 0x2d, 0x4d, 0x65, 0x74, 0x61, 0x00, 0x00,
                                0x00, 0x3f, 0x00, 0x83, 0x6c, 0x00, 0x00, 0x00, 0x01, 0x68,
                                0x02, 0x6b, 0x00, 0x1b, 0x58, 0x2d, 0x52, 0x69, 0x61, 0x6b,
                                0x2d, 0x4d, 0x65, 0x74, 0x61, 0x2d, 0x45, 0x78, 0x70, 0x69,
                                0x72, 0x79, 0x2d, 0x42, 0x61, 0x73, 0x65, 0x2d, 0x53, 0x65,
                                0x63, 0x6b, 0x00, 0x14, 0x4a, 0x61, 0x6e, 0x20, 0x37, 0x2c,
                                0x20, 0x32, 0x30, 0x30, 0x39, 0x20, 0x31, 0x32, 0x3a, 0x33,
                                0x34, 0x3a, 0x30, 0x30, 0x6a, 0x00, 0x00, 0x00, 0x06, 0x01,
                                0x69, 0x6e, 0x64, 0x65, 0x78, 0x00, 0x00, 0x00, 0x03, 0x00,
                                0x83, 0x6a, 0x00, 0x00, 0x00, 0x0d, 0x01, 0x63, 0x6f, 0x6e,
                                0x74, 0x65, 0x6e, 0x74, 0x2d, 0x74, 0x79, 0x70, 0x65, 0x00,
                                0x00, 0x00, 0x26, 0x00, 0x83, 0x6b, 0x00, 0x21, 0x61, 0x70,
                                0x70, 0x6c, 0x69, 0x63, 0x61, 0x74, 0x69, 0x6f, 0x6e, 0x2f,
                                0x78, 0x2d, 0x77, 0x77, 0x77, 0x2d, 0x66, 0x6f, 0x72, 0x6d,
                                0x2d, 0x75, 0x72, 0x6c, 0x65, 0x6e, 0x63, 0x6f, 0x64, 0x65,
                                0x64, 0x00, 0x00, 0x00, 0x06, 0x01, 0x4c, 0x69, 0x6e, 0x6b,
                                0x73, 0x00, 0x00, 0x00, 0x03, 0x00, 0x83, 0x6a};
    Slice text_date_slice(text_date_val, sizeof(text_date_val));

    ret_flag=ValueGetLastModTime(text_date_slice, ret_time);
    ASSERT_TRUE(ret_flag);
    ASSERT_TRUE(1479405368976515==ret_time);


    // X-Riak-Meta-AA and X-Riak-Meta-ZZ in list with X-Riak-Meta-Expiry-Base-Sec: 1478342700
    //  (date -d "2016-11-05 06:45:00" +%s)
    const char before_after_val[]={0x35, 0x01, 0x00, 0x00, 0x00, 0x22, 0x83, 0x6c, 0x00, 0x00,
                                   0x00, 0x01, 0x68, 0x02, 0x6d, 0x00, 0x00, 0x00, 0x08, 0x23,
                                   0x09, 0xfe, 0xf9, 0xef, 0x4a, 0xf0, 0x11, 0x68, 0x02, 0x61,
                                   0x01, 0x6e, 0x05, 0x00, 0xbf, 0x7f, 0xa2, 0xd1, 0x0e, 0x6a,
                                   0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x0e, 0x01, 0x2d,
                                   0x2d, 0x2c, 0x62, 0x75, 0x63, 0x6b, 0x30, 0x2c, 0x6b, 0x65,
                                   0x79, 0x30, 0x00, 0x00, 0x00, 0xfa, 0x00, 0x00, 0x05, 0xc7,
                                   0x00, 0x06, 0x43, 0xff, 0x00, 0x07, 0x10, 0xbe, 0x16, 0x33,
                                   0x55, 0x6d, 0x33, 0x4c, 0x74, 0x70, 0x4d, 0x48, 0x4a, 0x72,
                                   0x33, 0x50, 0x45, 0x64, 0x38, 0x44, 0x79, 0x62, 0x76, 0x58,
                                   0x58, 0x00, 0x00, 0x00, 0x00, 0x0c, 0x01, 0x58, 0x2d, 0x52,
                                   0x69, 0x61, 0x6b, 0x2d, 0x4d, 0x65, 0x74, 0x61, 0x00, 0x00,
                                   0x00, 0x65, 0x00, 0x83, 0x6c, 0x00, 0x00, 0x00, 0x03, 0x68,
                                   0x02, 0x6b, 0x00, 0x0e, 0x58, 0x2d, 0x52, 0x69, 0x61, 0x6b,
                                   0x2d, 0x4d, 0x65, 0x74, 0x61, 0x2d, 0x41, 0x61, 0x6b, 0x00,
                                   0x02, 0x30, 0x30, 0x68, 0x02, 0x6b, 0x00, 0x1b, 0x58, 0x2d,
                                   0x52, 0x69, 0x61, 0x6b, 0x2d, 0x4d, 0x65, 0x74, 0x61, 0x2d,
                                   0x45, 0x78, 0x70, 0x69, 0x72, 0x79, 0x2d, 0x42, 0x61, 0x73,
                                   0x65, 0x2d, 0x53, 0x65, 0x63, 0x6b, 0x00, 0x0a, 0x31, 0x34,
                                   0x37, 0x38, 0x33, 0x34, 0x32, 0x37, 0x30, 0x30, 0x68, 0x02,
                                   0x6b, 0x00, 0x0e, 0x58, 0x2d, 0x52, 0x69, 0x61, 0x6b, 0x2d,
                                   0x4d, 0x65, 0x74, 0x61, 0x2d, 0x58, 0x78, 0x6b, 0x00, 0x02,
                                   0x39, 0x39, 0x6a, 0x00, 0x00, 0x00, 0x06, 0x01, 0x69, 0x6e,
                                   0x64, 0x65, 0x78, 0x00, 0x00, 0x00, 0x03, 0x00, 0x83, 0x6a,
                                   0x00, 0x00, 0x00, 0x0d, 0x01, 0x63, 0x6f, 0x6e, 0x74, 0x65,
                                   0x6e, 0x74, 0x2d, 0x74, 0x79, 0x70, 0x65, 0x00, 0x00, 0x00,
                                   0x26, 0x00, 0x83, 0x6b, 0x00, 0x21, 0x61, 0x70, 0x70, 0x6c,
                                   0x69, 0x63, 0x61, 0x74, 0x69, 0x6f, 0x6e, 0x2f, 0x78, 0x2d,
                                   0x77, 0x77, 0x77, 0x2d, 0x66, 0x6f, 0x72, 0x6d, 0x2d, 0x75,
                                   0x72, 0x6c, 0x65, 0x6e, 0x63, 0x6f, 0x64, 0x65, 0x64, 0x00,
                                   0x00, 0x00, 0x06, 0x01, 0x4c, 0x69, 0x6e, 0x6b, 0x73, 0x00,
                                   0x00, 0x00, 0x03, 0x00, 0x83, 0x6a};
    Slice before_after_slice(before_after_val, sizeof(before_after_val));

    ret_flag=ValueGetLastModTime(before_after_slice, ret_time);
    ASSERT_TRUE(ret_flag);
    ASSERT_TRUE(1478342700000000==ret_time);

    return;

}   // LastModTest


}   // namespace leveldb

