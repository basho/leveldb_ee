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
#include "leveldb_ee/riak_object.h"
#include "util/logging.h"
#include "util/mutexlock.h"

namespace leveldb {

// initialize the static variable in hopes we do not later
//  attempt a shutdown against an uninitialized value (could still happen)
static PropertyCache * lPropCache(NULL);

/**
 * Create the cache.  Called only once upon
 * leveldb initialization
 */
void
PropertyCache::InitPropertyCache(
    EleveldbRouter_t Router)
{
    lPropCache = new PropertyCache(Router);

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
PropertyCache::PropertyCache(
    EleveldbRouter_t Router)
    : m_Cache(NULL), m_Router(Router),
      m_Cond(&m_Mutex)
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


/**
 * Retrieve property from cache if available,
 *  else call out to Riak to get properties
 */
Cache::Handle *
PropertyCache::Lookup(
    const Slice & CompositeBucket)
{
    Cache::Handle * ret_handle(NULL);
    assert(NULL!=lPropCache);
    assert(NULL!=lPropCache->m_Cache);

    ret_handle=GetCache().Lookup(CompositeBucket);

    // not waiting in the cache already.  Request info
    if (NULL==ret_handle && NULL!=lPropCache->m_Router)
    {
        ret_handle=lPropCache->LookupWait(CompositeBucket);
    }   // if

    return(ret_handle);

}   // PropertyCache::Lookup



static void
DeleteProperty(
    const Slice& key,
    void* value)
{
    ExpiryModuleOS * expiry;

    expiry=(ExpiryModuleOS *)value;

    delete expiry;
}   // static DeleteProperty


/**
 * (static) Add / Overwrite key in property cache.  Manage handle
 *  on caller's behalf
 */
bool
PropertyCache::Insert(
    const Slice & CompositeBucket,
    void * Props,
    Cache::Handle ** OutputPtr)
{
    assert(NULL!=lPropCache);
    bool ret_flag(false);
    Cache::Handle * ret_handle(NULL);

    ret_handle=lPropCache->InsertInternal(CompositeBucket, Props);

    if (NULL!=OutputPtr)
        *OutputPtr=ret_handle;
    else if (NULL!=ret_handle)
        GetCache().Release(ret_handle);

    ret_flag=(NULL!=ret_handle);

    return(ret_flag);

}   // PropertyCache::Insert


Cache::Handle *
PropertyCache::InsertInternal(
    const Slice & CompositeBucket,
    void * Props)
{
    assert(NULL!=m_Cache);

    Cache::Handle * ret_handle(NULL);

    {
        MutexLock lock(&m_Mutex);

        ret_handle=m_Cache->Insert(CompositeBucket, Props, 1, DeleteProperty);
        m_Cond.SignalAll();
    }

    return(ret_handle);

}   // PropertyCache::InsertInternal


/**
 * Internal Lookup function that first requests property
 *  data from Eleveldb Router, then waits for the data
 *  to post to the cache.
 */
Cache::Handle *
PropertyCache::LookupWait(
    const Slice & CompositeBucket)
{
    Cache::Handle * ret_handle(NULL);
    std::string type, bucket;
    const void * params[4];
    bool flag;

    // split composite to pass to Riak
    KeyParseBucket(CompositeBucket, type, bucket);

    params[0]=type.c_str();
    params[1]=bucket.c_str();
    params[2]=(void *)&CompositeBucket;
    params[3]=NULL;
    flag=m_Router(eGetBucketProperties, 3, params);

    // proceed with wait loop if router call successfull
    if (flag)
    {
        do
        {
            // has value populated since last look?
            ret_handle=m_Cache->Lookup(CompositeBucket);

            // is state appropriate to waiting?
            if (NULL==ret_handle && flag)
            {
                timespec ts;
                MutexLock lock(&m_Mutex);

                clock_gettime(CLOCK_REALTIME, &ts);
                ts.tv_sec+=1;
                flag=m_Cond.Wait(&ts);
            }   // if
        } while(NULL==ret_handle && flag);
    }   // if

    return(ret_handle);

}   // PropertyCache::LookupWait

}  // namespace leveldb

