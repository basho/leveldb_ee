// -------------------------------------------------------------------
//
// expiry_ee.h
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

#ifndef EXPIRY_EE_H
#define EXPIRY_EE_H

#include "leveldb/expiry.h"
#include "leveldb/perf_count.h"

namespace leveldb
{

class ExpiryModuleEE : public ExpiryModule
{
public:
    ExpiryModuleEE()
        : m_WholeFiles(true), m_ExpiryMinutes(0)
    {};

    ~ExpiryModuleEE() {};

    // Print expiry options to LOG file
    virtual void Dump(Logger * log) const;

    // db/write_batch.cc MemTableInserter::Put() calls this.
    //  using uint8_t and uint64_t types instead of ValType and ExpiryTime
    //  to prevent exposing internal db/dbformat.h
    // returns false on internal error
    virtual bool MemTableInserterCallback(
        const Slice & Key,   // input: user's key about to be written
        const Slice & Value, // input: user's value object
        uint8_t & ValType,   // input/output: key type. call might change
        uint64_t & Expiry);  // input/output: 0 or specific expiry. call might change

    // db/dbformat.cc KeyRetirement::operator() calls this.
    // returns true if key is expired, returns false if key not expired
    virtual bool KeyRetirementCallback(
        const ParsedInternalKey & Ikey);  // input: key to examine for retirement

    // table/table_builder.cc TableBuilder::Add() calls this.
    // returns false on internal error
    virtual bool TableBuilderCallback(
        const Slice & key,       // input: internal key
        SstCounters & counters); // input/output: counters for new sst table

public:
    // configuration values
    // Riak specific option authorizing leveldb to eliminate entire
    // files that contain expired data (delete files instead of
    // removing expired data during compactions).
    bool m_WholeFiles;

    // Riak specific option giving number of minutes a stored key/value
    // may stay within the database before automatic deletion.  Zero
    // disables expiry feature.
    uint64_t m_ExpiryMinutes;

};  // ExpiryModule

}  // namespace leveldb

#endif // ifndef
