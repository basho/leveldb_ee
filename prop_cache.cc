// -------------------------------------------------------------------
//
// prop_cache.cc
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


#include "leveldb_ee/prop_cache.h"
#include "util/logging.h"


namespace leveldb {

// initialize the static variable in hopes we do not later
//  attempt a shutdown against an uninitialized value (could still happen)
static PropertyCache * lPropCache(NULL);

/**
 * Create the cache.  Called only once upon
 * leveldb initialization
 */
void
PropertyCache::InitPropertyCache()
{
    lPropCache = new PropertyCache;

    return;

}   // PropertyCache


void
PropertyCache::ShutdownPropertyCache()
{
    delete lPropCache;
    lPropCache=NULL;

}  // PropertyCache::ShutdownPropertyCache


Cache &
PropertyCache::GetCache()
{
    assert(NULL!=lPropCache);

    return(*lPropCache->GetCachePtr());

}   // PropertyCache::GetCache


/**
 * Construct property cache object (likely singleton)
 */
PropertyCache::PropertyCache()
    : m_Cache(NULL)
{
    // 1000 is number of cache entries.  Just pulled
    //  that number out of the air.
    m_Cache = NewLRUCache2(1000);

}   // PopertyCache::PropertyCache


PropertyCache::~PropertyCache()
{
    delete m_Cache;
    m_Cache=NULL;
}   // PropertyCache::~PropertyCache

}  // namespace leveldb

