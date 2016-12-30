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

#include "port/port_posix.h"
#include "leveldb/perf_count.h"
#include "leveldb/env.h"
#include "db/dbformat.h"
#include "db/db_impl.h"
#include "db/version_set.h"
#include "leveldb_ee/expiry_ee.h"
#include "leveldb_ee/prop_cache.h"
#include "leveldb_ee/riak_object.h"
#include "util/logging.h"
#include "util/mutexlock.h"
#include "util/throttle.h"

namespace leveldb {

/**
 * This is the factory function to create
 *  an enterprise edition version of object expiry
 */
ExpiryModule *
ExpiryModule::CreateExpiryModule(
    EleveldbRouter_t Router)
{
    // creating a manual version of pthread_once because
    //  the native does not allow parameters to the init function
    static port::Mutex once_mutex;
    static volatile bool once_done(false);

    {
        MutexLock lock(&once_mutex);

        if (!once_done)
        {
            PropertyCache::InitPropertyCache(Router);
            once_done=true;
        }   // if
    }   // MutexLock

    return(new leveldb::ExpiryModuleEE);

}   // ExpiryModule::CreateExpiryModule()


/**
 * static function to clean up expiry related objects
 *  upon shutdown
 */
void
ExpiryModule::ShutdownExpiryModule()
{

    // kill off the property cache
    PropertyCache::ShutdownPropertyCache();

    return;

}   // ExpiryModule::ShutdownExpiryModule


/**
 * settings information that gets dumped to LOG upon
 *  leveldb start
 */
void
ExpiryModuleEE::Dump(
    Logger * log) const
{
    Log(log,"  ExpiryModuleEE.expiry_enabled: %s", expiry_enabled ? "true" : "false");
    Log(log,"  ExpiryModuleEE.expiry_minutes: %" PRIu64, expiry_minutes);
    Log(log,"     ExpiryModuleEE.whole_files: %s", whole_file_expiry ? "true" : "false");

    return;

}   // ExpiryModuleEE::Dump


/**
 * Review the metadata of one file to see if it is
 *  eligible for file expiry
 */
bool
ExpiryModuleEE::IsFileExpired(
    const FileMetaData & SstFile,
    ExpiryTime Now,
    ExpiryTime Aged) const
{
    bool expired_file(false), good;
    ExpiryTime aged(Aged);
    Slice low_composite, high_composite, temp_key;
    ExpiryPropPtr_t expiry_prop;

    // only delete files with matching buckets for first
    //  and last key.  Do not process / make any assumptions
    //  if first and last have different buckets.
    temp_key=SstFile.smallest.internal_key();
    good=KeyGetBucket(temp_key, low_composite);
    temp_key=SstFile.largest.internal_key();
    good=good && KeyGetBucket(temp_key, high_composite);
    assert(good);

    // smallest & largest bucket names match, file eligible for whole file expiry
    expired_file=(good && low_composite==high_composite);

    if (expired_file)
    {
        // see if properties found
        good=expiry_prop.Lookup(low_composite);

        // yes, use bucket level properties
        if (good)
        {

        }   // if
    }   // if

    // test for bucket properties. Set local aged based upon that
    //  or global aged.



    expired_file = expired_file && ExpiryModuleOS::IsFileExpired(SstFile, Now, aged);
    return(expired_file);

}   // ExpiryModuleEE::IsFileExpired


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
