// -------------------------------------------------------------------
//
// expiry_ee.h
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

#ifndef EXPIRY_EE_H
#define EXPIRY_EE_H

#include <vector>

#include "leveldb/cache.h"
#include "leveldb/options.h"
#include "leveldb/expiry.h"
#include "leveldb/perf_count.h"
#include "db/dbformat.h"
#include "db/version_edit.h"
#include "util/expiry_os.h"

namespace leveldb
{

class ExpiryModuleEE : public ExpiryModuleOS
{
public:
    ExpiryModuleEE()
        : m_ExpiryModuleExpiryMicros(0)
    {};

    virtual ~ExpiryModuleEE() {};

    // assignment operator
    ExpiryModuleEE & operator=(const ExpiryModuleEE & rhs);

    // Print expiry options to LOG file
    virtual void Dump(Logger * log) const;

    // db/write_batch.cc MemTableInserter::Put() calls this.
    // returns false on internal error
    virtual bool MemTableInserterCallback(
        const Slice & Key,   // input: user's key about to be written
        const Slice & Value, // input: user's value object
        ValueType & ValType,   // input/output: key type. call might change
        ExpiryTimeMicros & Expiry) const;  // input/output: 0 or specific expiry. call might change

    // db/dbformat.cc KeyRetirement::operator() calls this.
    // db/version_set.cc SaveValue() calls this too.
    // returns true if key is expired, returns false if key not expired
    virtual bool KeyRetirementCallback(
        const ParsedInternalKey & Ikey) const;  // input: key to examine for retirement

    // table/table_builder.cc TableBuilder::Add() calls this.
    // returns false on internal error
    virtual bool TableBuilderCallback(
        const Slice & key,       // input: internal key
        SstCounters & counters) const; // input/output: counters for new sst table

    virtual uint64_t ExpiryModuleExpiryMicros() {return(m_ExpiryModuleExpiryMicros);};

    // Riak EE:  stash a user created module with settings
    virtual void NoteUserExpirySettings();

    // Riak EE:  establish timeout for things going to property cache
    void SetExpiryModuleExpiryMicros(uint64_t Expire) {m_ExpiryModuleExpiryMicros=Expire;};


protected:
    // utility to CompactionFinalizeCallback to review
    //  characteristics of one SstFile to see if entirely expired
    virtual bool IsFileExpired(const FileMetaData & SstFile, ExpiryTimeMicros Now) const;

    // When "creating" write time, chose its source based upon
    //  open source versus enterprise edition
    virtual uint64_t GenerateWriteTimeMicros(const Slice & Key, const Slice & Value) const;



    uint64_t m_ExpiryModuleExpiryMicros; // for bucket settings, when to flush and reload
                                         //  (zero for "unused")
private:
    ExpiryModuleEE(const ExpiryModuleEE &);  // copy blocked

};  // ExpiryModule

}  // namespace leveldb

#endif // ifndef
