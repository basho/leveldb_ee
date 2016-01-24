// -------------------------------------------------------------------
//
// cache_warm.h
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

#include <string>

#include "db/db_impl.h"
#include "db/filename.h"
#include "db/log_reader.h"
#include "db/log_writer.h"
#include "db/table_cache.h"
#include "db/version_edit.h"
#include "leveldb/cache.h"
#include "util/cache2.h"

namespace leveldb {


/**
 * functor that is called once for each cache entry.
 *  used here to accumulate file cache data and write to
 *  log for cache warming.
 */
class WarmingAccumulator : public CacheAccumulator
{
protected:
    size_t m_ValueCount;
    std::string m_Record;
    log::Writer & m_LogFile;
    Status m_Status;

public:
    WarmingAccumulator(
        log::Writer & LogFile)
        : m_LogFile(LogFile)
    {
        m_ValueCount=0;
        m_Record.reserve(4096);
    };

    virtual ~WarmingAccumulator()
    {
        WriteRecord();
        m_LogFile.Close();
    };

    // this operator() function is the "interface" routine
    //  from the base class
    virtual bool operator()(void * Value);

    std::string & GetRecord() {return(m_Record);};

    size_t GetCount() const {return(m_ValueCount);};

    bool WriteRecord();

    const Status & GetStatus() const {return(m_Status);};

};  // class WarmingAccumulator

}  // namespace leveldb
