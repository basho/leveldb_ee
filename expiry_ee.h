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

#include <vector>

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
    {};

    ~ExpiryModuleEE() {};

    // Print expiry options to LOG file
    virtual void Dump(Logger * log) const;

protected:
    // When "creating" write time, chose its source based upon
    //  open source versus enterprise edition
    virtual uint64_t GenerateWriteTime(const Slice & Key, const Slice & Value) const;

};  // ExpiryModule

}  // namespace leveldb

#endif // ifndef
