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
#include "util/prop_cache.h"
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
    //  with recent user defaults
    if (NULL!=gUserExpirySample.get())
    {
        *new_mod=*gUserExpirySample.get();
    }   // if

    // also in case for bucket cache, 5 minute lifetime
    new_mod->SetExpiryModuleExpiryMicros(GetTimeMinutes()+5*60*port::UINT64_ONE_SECOND_MICROS);

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
    m_ExpiryModuleExpiryMicros=0;

    // maybe this should call an operator= in ExpiryModuleOS some day?
    SetExpiryEnabled(rhs.IsExpiryEnabled());
    SetExpiryMinutes(rhs.GetExpiryMinutes());
    SetExpiryUnlimited(rhs.IsExpiryUnlimited());
    SetWholeFileExpiryEnabled(rhs.IsWholeFileExpiryEnabled());

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
    Log(log,"  ExpiryModuleEE.expiry_enabled: %s", IsExpiryEnabled() ? "true" : "false");
    Log(log,"  ExpiryModuleEE.expiry_minutes: %" PRIu64, GetExpiryMinutes());
    Log(log,"ExpiryModuleEE.expiry_unlimited: %s", IsExpiryUnlimited() ? "true" : "false");
    Log(log,"     ExpiryModuleEE.whole_files: %s", IsWholeFileExpiryEnabled() ? "true" : "false");

    return;

}   // ExpiryModuleEE::Dump


/**
 * Setup expiry environment by bucket, then call
 *  OS version (callback to db/write_batch.cc MemTableInserter())
 *
 * Failed lookup ok to use default since only sets
 *  write time within key.
 */
bool                     // always true, return ignored
ExpiryModuleEE::MemTableInserterCallback(
    const Slice & Key,   // input: user's key about to be written
    const Slice & Value, // input: user's value object
    ValueType & ValType,   // input/output: key type. call might change
    ExpiryTimeMicros & Expiry)   // input/output: 0 or specific expiry. call might change
    const
{
    const ExpiryModuleOS * module_os(this);

    if (IsExpiryEnabled())
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

    return(module_os->ExpiryModuleOS::MemTableInserterCallback(Key, Value, ValType, Expiry));

}   // ExpiryModuleEE::MemTableInserterCallback


/**
 * Attempt to retrieve write time from Riak Object
 */
uint64_t
ExpiryModuleEE::GenerateWriteTimeMicros(
    const Slice & Key,
    const Slice & Value) const
{
    uint64_t ret_micros;

    // attempt retrieval from Riak Object
    if (!ValueGetLastModTimeMicros(Value, ret_micros))
    {
        // get from derived class instead
        ret_micros=ExpiryModuleOS::GenerateWriteTimeMicros(Key, Value);
    }   // if

    return(ret_micros);

}  // ExpiryModuleEE::GenerateWriteTimeMicros()


/**
 * Setup expiry environment by bucket, then call
 *  OS version
 *
 * Note: KeyRetirementCallback could potentially rewrite key to add
 *  expiry info from Riak object ... but storage backing for new key is questionable.
 *  Then call ExpiryModuleOS routine to do work.
 *
 * Failure on Lookup could result in expiring something
 *  that should never expire because bucket says unlimited where
 *  global says expire now, so expiry skipped when Lookup fails.
 *
 */
bool
ExpiryModuleEE::KeyRetirementCallback(
    const ParsedInternalKey & Ikey) const
{
    const ExpiryModuleOS * module_os(this);
    bool is_expired(false);

    if (IsExpiryEnabled())
    {
        bool good(true);
        ExpiryPropPtr_t expiry_prop;
        Slice composite_bucket;

        good=KeyGetBucket(Ikey.user_key, composite_bucket);

        // see if properties found
        good=good && expiry_prop.Lookup(composite_bucket);

        // yes, use bucket level properties
        //  (no, do nothing because no-bucket is error)
        if (good)
        {
            module_os=expiry_prop.get();
            is_expired=module_os->ExpiryModuleOS::KeyRetirementCallback(Ikey);
        }   // if
    }   // if

    return(is_expired);

}   // ExpiryModuleEE::KeyRetirementCallback


/**
 * Setup expiry environment by bucket, then call
 *  OS version
 *
 * The OS routine is setting date range and delete counts, worst
 *  if Lookup fails is that delete counts are wrong a leads to
 *  wasted compactions.  But date range settings are important and
 *  immune to Lookup failure.  Calling OS via default even on Lookup failure.
 */
bool               // return value ignored
ExpiryModuleEE::TableBuilderCallback(
    const Slice & Key,
    SstCounters & Counters) const
{
    const ExpiryModuleOS * module_os(this);

    if (IsExpiryEnabled())
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

    return(module_os->ExpiryModuleOS::TableBuilderCallback(Key, Counters));

}   // ExpiryModuleEE::TableBuilderCallback


/**
 * MemTableCallback routes through KeyRetirementCallback ... no new code for EE required
 */


/**
 * CompactionFinalizeCallback routes through IsFileExpired ... no new code for EE required
 */


/**
 * Review the metadata of one file to see if it is
 *  eligible for file expiry
 *
 * This routine could expiry incorrectly if Lookup fails (returns NULL).  Aborts
 *  test in such case (returns false).
 */
bool
ExpiryModuleEE::IsFileExpired(
    const FileMetaData & SstFile,
    ExpiryTimeMicros Now) const
{
    bool expired_file(false), good;
    Slice low_composite, high_composite, temp_key;
    ExpiryPropPtr_t expiry_prop;
    const ExpiryModuleOS * module_os(this);

    // only endure this overhead if expiry active
    if (IsExpiryEnabled())
    {
        // only delete files with matching buckets for first
        //  and last key.  Do not process / make any assumptions
        //  if first and last have different buckets.
        temp_key=SstFile.smallest.internal_key();
        good=KeyGetBucket(temp_key, low_composite);
        temp_key=SstFile.largest.internal_key();
        good=good && KeyGetBucket(temp_key, high_composite);
        //assert(good);

        // smallest & largest bucket names match, file eligible for whole file expiry
        expired_file=(good && low_composite==high_composite);

        if (expired_file)
        {
            // see if properties found
            good=expiry_prop.Lookup(low_composite);

            // yes, use bucket level properties
            if (good)
            {
                // call ExpiryModuleOS function using parameters from bucket
                module_os=expiry_prop.get();
                expired_file = module_os->ExpiryModuleOS::IsFileExpired(SstFile, Now);
            }   // if
            else
            {
                // assume worst case of bad match of default expiry and bucket
                //  if Lookup fails
                expired_file=false;
            }   // else
        }   // if
    }   // if

    return(expired_file);

}   // ExpiryModuleEE::IsFileExpired


}  // namespace leveldb
