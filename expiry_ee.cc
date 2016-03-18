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
#include "leveldb_ee/expiry_ee.h"
#include "util/logging.h"
#include "util/throttle.h"

namespace leveldb {
void
ExpiryModuleEE::Dump(
    Logger * log) const
{
    Log(log,"    ExpiryModuleEE.whole_files: %s", m_WholeFiles ? "true" : "false");
    Log(log," ExpiryModuleEE.expiry_minutes: %" PRIu64, m_ExpiryMinutes);

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

    if ((kTypeValueWriteTime==ValType && 0==Expiry)
        || (kTypeValue==ValType && 0!=m_ExpiryMinutes))
    {
        ValType=kTypeValueWriteTime;
        Expiry=GetTimeMinutes();
    }   // if

    return(good);

}   // ExpiryModuleEE::MemTableInserterCallback


/**
 * Returns true if key is expired.  False if key is NOT expired
 */
bool ExpiryModuleEE::KeyRetirementCallback(
    const ParsedInternalKey & Ikey) const
{
    bool is_expired(false);
    uint64_t now, expires;

    // m_ExpiryMinutes must be non-zero for any expiry
    //  to work.  It is an override switch
    if (0!=m_ExpiryMinutes)
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


bool ExpiryModuleEE::TableBuilderCallback(
    const Slice & Key,
    SstCounters & Counters) const
{
    bool good(true);
    ExpiryTime expires, temp;

    expires=ExtractExpiry(Key);

    switch(ExtractValueType(Key))
    {
        case kTypeValueWriteTime:
            temp=Counters.Value(eSstCountExpiry1);
            if (expires<temp || 0==temp)
                Counters.Set(eSstCountExpiry1, expires);
            if (Counters.Value(eSstCountExpiry2)<expires)
                Counters.Set(eSstCountExpiry2, expires);
            break;

        case kTypeValueExplicitExpiry:
            if (Counters.Value(eSstCountExpiry3)<expires)
                Counters.Set(eSstCountExpiry3, expires);
            break;

        default:
            assert(0);
            break;
    }   // switch

    return(good);

}   // ExpiryModuleEE::TableBuilderCallback


/**
 * Returns true if key is expired.  False if key is NOT expired
 *  (used by KeyRetirementCallback too)
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

bool ExpiryModuleEE::CompactionFinalizeCallback(
    const std::vector<FileMetaData*> & Level) const
{
    bool expired_file(false);

    // All expiry disabled if m_Expiry set to zero.
    if (0!=m_ExpiryMinutes && m_WholeFiles)
    {
        ExpiryTime now, aged;
        std::vector<FileMetaData*>::const_iterator it;

        now=GetTimeMinutes();
        aged=now - m_ExpiryMinutes*60000000;
        for (it=Level.begin(); !expired_file && Level.end()!=it; ++it)
        {
            // aged above highest aged, or now above highest explicit
            expired_file = ((0!=(*it)->expiry2 && (*it)->expiry2<=aged)
                            || (0!=(*it)->expiry3 && (*it)->expiry3<=now));
        }   // for
    }   // if

    return(expired_file);

}   // ExpiryModuleEE::CompactionFinalizeCallback

}  // namespace leveldb
