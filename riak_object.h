// -------------------------------------------------------------------
//
// riak_object.h
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

#ifndef RIAK_OBJECT_H
#define RIAK_OBJECT_H

#include <string>
#include <stdint.h>

#include "leveldb/slice.h"


namespace leveldb
{
    bool KeyGetBucket(const Slice & Key, std::string & BucketType, std::string & Bucket);
    bool KeyGetBucket(const Slice & Key, Slice & CompositeBucket);
    void KeyParseBucket(const Slice & CompositeBucket,
                        std::string & BucketType, std::string & Bucket);

    bool ValueGetLastModTimeMicros(Slice Value, uint64_t & LastModTimeMicros);

    // routines for unit test support
    bool WriteSextString(int Prefix, const char * Text, char * & Cursor);
    bool BuildRiakKey(const char * BucketType, const char * Bucket, const char * Key, std::string & Output);

}  // namespace leveldb

#endif // ifndef
