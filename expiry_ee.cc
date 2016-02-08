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

#include "leveldb_ee/expiry_ee.h"
#include "leveldb/env.h"
#include "db/dbformat.h"
#include "util/logging.h"

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
    uint8_t & ValType,   // input/output: key type. call might change
    uint64_t & Expiry)   // input/output: 0 or specific expiry. call might change
{
    bool good(true);

    if ((kTypeValueWriteTime==ValType && 0==Expiry)
        || (kTypeValue==ValType && 0!=m_ExpiryMinutes))
    {
        ValType=kTypeValueWriteTime;
        Expiry=Env::Default()->NowMicros();
    }   // if

    return(good);

}   // ExpiryModuleEE::MemTableInserterCallback

}  // namespace leveldb
