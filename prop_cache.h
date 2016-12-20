// -------------------------------------------------------------------
//
// prop_cache.h
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

#ifndef PROP_CACHE_H
#define PROP_CACHE_H

#include "leveldb/cache.h"
#include "leveldb_ee/expiry_ee.h"

namespace leveldb
{

// forward declare
class ExpiryProperties;

class PropertyCache
{
public:
    // create global cache object
    static void InitPropertyCache();

    // release global cache object
    static void ShutdownPropertyCache();

    // static entry point
    static bool GetExpiryProperties(const Slice & CompositeBucket,
                                    ExpiryPropPtr_t & PropOut) {return(false);};

    // static Accessor for populating smart cache pointers
    static Cache & GetCache();

    PropertyCache();

    // virtual destructor to facilitate unit tests
    virtual ~PropertyCache();

protected:
    Cache * GetCachePtr() {return(m_Cache);};

    Cache * m_Cache;

private:

}; // class PropertyCache


}  // namespace leveldb

#endif // ifndef
