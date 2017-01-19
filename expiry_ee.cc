// -------------------------------------------------------------------
//
// expiry_ee.cc
//
// Copyright (c) 2016-2017 Basho Technologies, Inc. All Rights Reserved.
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

static RefPtr<class ExpiryModuleEE> gUserExpirySample;

/**
 * This is the factory function to create
 *  an enterprise edition version of object expiry
 *  It can be called for BOTH database objects and
 *  expiry bucket property objects.
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
            gUserExpirySample.reset();
            PropertyCache::InitPropertyCache(Router);
            once_done=true;
        }   // if
    }   // MutexLock

    ExpiryModuleEE * new_mod;

    new_mod=new leveldb::ExpiryModuleEE;

    // in case new_mod is for bucket cache, seed it
    //  with resent user defaults
    if (NULL!=gUserExpirySample.get())
    {
        *new_mod=*gUserExpirySample.get();
    }   // if

    // also in case for bucket cache, 5 minute lifetime
    new_mod->SetExpiryModuleExpiry(GetTimeMinutes()+5*60*port::UINT64_ONE_SECOND);

    return(new_mod);

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
    gUserExpirySample.reset();

    return;

}   // ExpiryModule::ShutdownExpiryModule


ExpiryModuleEE &
ExpiryModuleEE::operator=(
    const ExpiryModuleEE & rhs)
{
    // do not carry forward object expiration (likely none anyway)
    m_ExpiryModuleExpiry=0;

    // maybe this should call an operator= in ExpiryModuleOS some day?
    expiry_enabled=rhs.expiry_enabled;
    expiry_minutes=rhs.expiry_minutes;
    whole_file_expiry=rhs.whole_file_expiry;

    return(*this);

}   // ExpiryModuleEE::operator=


void
ExpiryModuleEE::NoteUserExpirySettings()
{
    gUserExpirySample.assign(this);
}   // ExpiryModuleEE::NoteUserExpirySettings


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
 * Setup expiry environment by bucket, then call
 *  OS version
 */
bool
ExpiryModuleEE::MemTableInserterCallback(
    const Slice & Key,   // input: user's key about to be written
    const Slice & Value, // input: user's value object
    ValueType & ValType,   // input/output: key type. call might change
    ExpiryTime & Expiry)   // input/output: 0 or specific expiry. call might change
    const
{
    const ExpiryModuleOS * module_os(this);

    if (expiry_enabled)
    {
        bool good(true);
        ExpiryPropPtr_t expiry_prop;
        Slice composite_bucket;

        good=KeyGetBucket(Key, composite_bucket);

        // see if properties found
        good=good && expiry_prop.Lookup(composite_bucket);

        // yes, use bucket level properties
        if (good)
            module_os=expiry_prop.get();

    }   // if

    return(module_os->MemTableInserterCallback(Key, Value, ValType, Expiry));

}   // ExpiryModuleEE::MemTableInserterCallback


/**
 * Setup expiry environment by bucket, then call
 *  OS version
 *
 * Note: KeyRetirementCallback could rewrite key to add
 *  expiry info from Riak object ... but storage backing for new key is questionable.
 *  Then call ExpiryModuleOS routine to do work.
 *
 */
bool ExpiryModuleEE::KeyRetirementCallback(
    const ParsedInternalKey & Ikey) const
{
    const ExpiryModuleOS * module_os(this);

    if (expiry_enabled)
    {
        bool good(true);
        ExpiryPropPtr_t expiry_prop;
        Slice composite_bucket;

        good=KeyGetBucket(Ikey.user_key, composite_bucket);

        // see if properties found
        good=good && expiry_prop.Lookup(composite_bucket);

        // yes, use bucket level properties
        if (good)
            module_os=expiry_prop.get();

    }   // if

    return(module_os->KeyRetirementCallback(Ikey));

}   // ExpiryModuleEE::KeyRetirementCallback


/**
 * Setup expiry environment by bucket, then call
 *  OS version
 */
bool ExpiryModuleEE::TableBuilderCallback(
    const Slice & Key,
    SstCounters & Counters) const
{
    const ExpiryModuleOS * module_os(this);

    if (expiry_enabled)
    {
        bool good(true);
        ExpiryPropPtr_t expiry_prop;
        Slice composite_bucket;

        good=KeyGetBucket(Key, composite_bucket);

        // see if properties found
        good=good && expiry_prop.Lookup(composite_bucket);

        // yes, use bucket level properties
        if (good)
            module_os=expiry_prop.get();

    }   // if

    return(module_os->TableBuilderCallback(Key, Counters));

}   // ExpiryModuleEE::TableBuilderCallback

/**
 * MemTableCallback routes through KeyRetirementCallback ... no new code
 */


/**
 * Review the metadata of one file to see if it is
 *  eligible for file expiry
 */
bool
ExpiryModuleEE::IsFileExpired(
    const FileMetaData & SstFile,
    ExpiryTime Now) const
{
    bool expired_file(false), good;
    Slice low_composite, high_composite, temp_key;
    ExpiryPropPtr_t expiry_prop;
    const ExpiryModuleOS * module_os(this);

    // only endure this overhead if expiry active
    if (expiry_enabled)
    {
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
                module_os=expiry_prop.get();
        }   // if

        // call ExpiryModuleOS function using parameters from
        //  bucket or default.  smart pointer releases cache handle
        expired_file = expired_file && module_os->IsFileExpired(SstFile, Now);
    }   // if

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
