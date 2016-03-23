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

#include "leveldb/perf_count.h"
#include "leveldb/env.h"
#include "db/dbformat.h"
#include "db/db_impl.h"
#include "db/version_set.h"
#include "leveldb_ee/expiry_ee.h"
#include "util/logging.h"
#include "util/throttle.h"

namespace leveldb {
void
ExpiryModuleEE::Dump(
    Logger * log) const
{
    Log(log," ExpiryModuleEE.expiry_enabled: %s", m_ExpiryEnabled ? "true" : "false");
    Log(log," ExpiryModuleEE.expiry_minutes: %" PRIu64, m_ExpiryMinutes);
    Log(log,"    ExpiryModuleEE.whole_files: %s", m_WholeFileExpiry ? "true" : "false");

    return;

}   // ExpiryModuleEE::Dump

bool ExpiryModuleEE::MemTableInserterCallback(
    const Slice & Key,   // input: user's key about to be written
    const Slice & Value, // input: user's value object
    ValueType & ValType,   // input/output: key type. call might change
    ExpiryTime & Expiry)   // input/output: 0 or specific expiry. call might change
    const
{
    bool good(true);

    // only update the expiry time if explicit type
    //  without expiry, or ExpiryMinutes set
    if ((kTypeValueWriteTime==ValType && 0==Expiry)
        || (kTypeValue==ValType && 0!=m_ExpiryMinutes && m_ExpiryEnabled))
    {
        ValType=kTypeValueWriteTime;
        Expiry=GetTimeMinutes();
    }   // if

    return(good);

}   // ExpiryModuleEE::MemTableInserterCallback


/**
 * Returns true if key is expired.  False if key is NOT expired
 *  (used by MemtableCallback() too)
 */
bool ExpiryModuleEE::KeyRetirementCallback(
    const ParsedInternalKey & Ikey) const
{
    bool is_expired(false);
    uint64_t now, expires;

    if (m_ExpiryEnabled)
    {
        switch(Ikey.type)
        {
            case kTypeDeletion:
            case kTypeValue:
            default:
                is_expired=false;
                break;

            case kTypeValueWriteTime:
                if (0!=m_ExpiryMinutes && 0!=Ikey.expiry)
                {
                    now=GetTimeMinutes();
                    expires=m_ExpiryMinutes*60000000ULL+Ikey.expiry;
                    is_expired=(expires<=now);
                }   // if
                break;

            case kTypeValueExplicitExpiry:
                if (0!=Ikey.expiry)
                {
                    now=GetTimeMinutes();
                    is_expired=(Ikey.expiry<=now);
                }   // if
                break;
        }   // switch
    }   // if

    return(is_expired);

}   // ExpiryModuleEE::KeyRetirementCallback


/**
 *  - Sets low/high date range for aged expiry.
 *     (low for possible time series optimization)
 *  - Sets high date range for explicit expiry.
 *  - Increments delete counter for things already
 *     expired (to aid in starting compaction for
 *     keys tombstoning for higher levels).
 */
bool ExpiryModuleEE::TableBuilderCallback(
    const Slice & Key,
    SstCounters & Counters) const
{
    bool good(true);
    ExpiryTime expires, temp;

    expires=ExtractExpiry(Key);

    // only updating counters.  do this even if
    //  expiry disabled
    switch(ExtractValueType(Key))
    {
        case kTypeValueWriteTime:
            temp=Counters.Value(eSstCountExpiry1);
            if (expires<temp || 0==temp)
                Counters.Set(eSstCountExpiry1, expires);
            if (Counters.Value(eSstCountExpiry2)<expires)
                Counters.Set(eSstCountExpiry2, expires);
            // add to delete count if expired already
            //   i.e. acting as a tombstone
            if (0!=m_ExpiryMinutes && MemTableCallback(Key))
                Counters.Inc(eSstCountDeleteKey);
            break;

        case kTypeValueExplicitExpiry:
            if (Counters.Value(eSstCountExpiry3)<expires)
                Counters.Set(eSstCountExpiry3, expires);
            // add to delete count if expired already
            //   i.e. acting as a tombstone
            if (0!=m_ExpiryMinutes && MemTableCallback(Key))
                Counters.Inc(eSstCountDeleteKey);
            break;

        default:
            assert(0);
            break;
    }   // switch

    return(good);

}   // ExpiryModuleEE::TableBuilderCallback


/**
 * Returns true if key is expired.  False if key is NOT expired
 */
bool ExpiryModuleEE::MemTableCallback(
    const Slice & InternalKey) const
{
    bool is_expired(false), good;
    ParsedInternalKey parsed;

    good=ParseInternalKey(InternalKey, &parsed);

    if (good)
        is_expired=KeyRetirementCallback(parsed);

    return(is_expired);

}   // ExpiryModuleEE::MemTableCallback


/**
 * Returns true if at least one file on this level
 *  is eligible for full file expiry
 */

/**
Notes:

for Finalize():  find an eligible file, test if base for all keys (key range)
                 - good if one found

find first .sst with smallest greater than largest, then test prior to see its largest
   is less smallest.


for compaction:  find eligible file, test if base, add to list if base
                 - find all files
                 put into version_edit's delete, process version_edit


Test if base:  use Compaction::IsBaseLevelForKey as model.  Has "history vector"
               to help non-overlapped levels.  Must reset vector on overlapped levels

??? smallest_snapshot concerns? ... if iterator started before expiration, should
                                    it remain for life of iterator.

 */

bool ExpiryModuleEE::CompactionFinalizeCallback(
    bool WantAll,                  // input: true - examine all expired files
    const Version & Ver,           // input: database state for examination
    int Level,                     // input: level to review for expiry
    VersionEdit * Edit) const      // output: NULL or destination of delete list
{
    bool expired_file(false);

    if (m_ExpiryEnabled && m_WholeFileExpiry)
    {
        ExpiryTime now, aged;
        const std::vector<FileMetaData*> & files(Ver.GetFileList(Level));
        std::vector<FileMetaData*>::const_iterator it;
        size_t old_index[config::kNumLevels];

        now=GetTimeMinutes();
        aged=now - m_ExpiryMinutes*60000000;
        for (it=files.begin(); (!expired_file || WantAll) && files.end()!=it; ++it)
        {
            // aged above highest aged, or now above highest explicit
            expired_file = ((0!=(*it)->expiry2 && (*it)->expiry2<=aged && 0!=m_ExpiryMinutes)
                            || (0!=(*it)->expiry3 && (*it)->expiry3<=now));

            // identified an expired file, do any higher levels overlap
            //  its key range?
            if (expired_file)
            {
                int test;
                Slice small, large;

                for (test=Level+1;
                     test<config::kNumLevels && expired_file;
                     ++test)
                {
                    small=(*it)->smallest.user_key();
                    large=(*it)->largest.user_key();
                    expired_file=!Ver.OverlapInLevel(test, &small,
                                                     &large);
                }   // for
            }   // if

            // expired_file and no overlap? mark it for delete
            if (expired_file && NULL!=Edit)
            {
                Edit->DeleteFile((*it)->level, (*it)->number);
            }   // if
        }   // for
    }   // if

    return(expired_file);

}   // ExpiryModuleEE::CompactionFinalizeCallback

#if 1
/**
 * Riak specific routine to process whole file expiry.
 *  Code here derived from DBImpl::CompactMemTable() in db/db_impl.cc
 */
Status
DBImpl::BackgroundExpiry(
    Compaction * Compact)
{
    Status s;

    mutex_.AssertHeld();
    assert(NULL != Compact && NULL!=options_.expiry_module);

    if (NULL!=Compact)
    {
        VersionEdit edit;
        Version* base = versions_->current();
        base->Ref();
        options_.expiry_module->CompactionFinalizeCallback(true, *base, Compact->level(),
                                                      Compact->edit());
        Status s = WriteLevel0Table(imm_, &edit, base);
        base->Unref();

        if (s.ok() && shutting_down_.Acquire_Load()) {
            s = Status::IOError("Deleting DB during expiry compaction");
        }

        // push expired list to manifest
        if (s.ok())
        {
            s = versions_->LogAndApply(&edit, &mutex_);
            if (!s.ok())
                s = Status::IOError("LogAndApply error during expiry compaction");
        }   // if

        // Commit to the new state
        if (s.ok())
            DeleteObsoleteFiles();
    }   // if

    return s;

}   // DBImpl:BackgroundExpiry

#endif

}  // namespace leveldb
