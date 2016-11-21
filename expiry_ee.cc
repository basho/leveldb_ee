// -------------------------------------------------------------------
//
// expiry_ee.cc
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

#define __STDC_FORMAT_MACROS
#include <inttypes.h>
#include <limits.h>

#include "leveldb/perf_count.h"
#include "leveldb/env.h"
#include "db/dbformat.h"
#include "db/db_impl.h"
#include "db/version_set.h"
#include "leveldb_ee/expiry_ee.h"
#include "leveldb_ee/riak_object.h"
#include "util/logging.h"
#include "util/throttle.h"

namespace leveldb {


/**
 * This is the factory function to create
 *  an enterprise edition version of object expiry
 */
ExpiryModule *
ExpiryModule::CreateExpiryModule()
{

    return(new leveldb::ExpiryModuleEE);

}   // ExpiryModule::CreateExpiryModule()


/**
 * settings information that gets dumped to LOG upon
 *  leveldb start
 */
void
ExpiryModuleEE::Dump(
    Logger * log) const
{
    Log(log," ExpiryModuleEE.expiry_enabled: %s", expiry_enabled ? "true" : "false");
    Log(log," ExpiryModuleEE.expiry_minutes: %" PRIu64, expiry_minutes);
    Log(log,"    ExpiryModuleEE.whole_files: %s", whole_file_expiry ? "true" : "false");

    return;

}   // ExpiryModuleEE::Dump


/**
 * Attempt to retrieve write time from Riak Object
 */
uint64_t
ExpiryModuleEE::GenerateWriteTime(
    const Slice & Key,
    const Slice & Value) const
{
    uint64_t ret_time;

    // attempt retrieval from Riak Object
    if (!ValueGetLastModTime(Value, ret_time))
    {
        // get from derived class instead
        ret_time=ExpiryModuleOS::GenerateWriteTime(Key, Value);
    }   // if

    return(ret_time);

}  // ExpiryModuleEE::GenerateWriteTime()

}  // namespace leveldb
