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
#include "port/port.h"


namespace leveldb
{

// forward declare
class ExpiryProperties;

class PropertyCache
{
public:
    // create global cache object
    static void InitPropertyCache(EleveldbRouter_t Router);

    // release global cache object
    static void ShutdownPropertyCache();

    // static lookup, usually from CachePtr
    static Cache::Handle * Lookup(const Slice & CompositeBucket);

    // static insert, usually from eleveldb::property_cache()
    static bool Insert(const Slice & CompositeBucket, void * Props, Cache::Handle ** OutputPtr);

    // static retrieval of active cache
    static Cache & GetCache();

    // virtual destructor to facilitate unit tests
    virtual ~PropertyCache();

protected:
    // only allow creation from InitPropertyCache();
    PropertyCache(EleveldbRouter_t);

    // accessor to m_Cache pointer (really bad if NULL m_Cache)
    Cache * GetCachePtr() {return(m_Cache);};

    // internal routine to launch lookup request via eleveldb router, then wait
    Cache::Handle * LookupWait(const Slice & CompositeBucket);

    // internal routine to insert object and signal condition variable
    Cache::Handle * InsertInternal(const Slice & CompositeBucket, void * Props);


    Cache * m_Cache;
    EleveldbRouter_t m_Router;
    port::Mutex m_Mutex;
    port::CondVar m_Cond;

private:
    PropertyCache();
    PropertyCache(const PropertyCache &);
    PropertyCache operator=(const PropertyCache &);

}; // class PropertyCache


/**
 * This template wraps an object in property cache
 *  to insure it is properly released.
 *  Makes calls to static functions of PropertyCache.
 */
template<typename Object> class CachePtr
{
    /****************************************************************
    *  Member objects
    ****************************************************************/
public:

protected:
    Object * m_Ptr;            // NULL or object in cache

private:

    /****************************************************************
    *  Member functions
    ****************************************************************/
public:
    CachePtr() : m_Ptr(NULL) {};

    ~CachePtr() {Release();};

    void Release()
    {
        if (NULL!=m_Ptr)
            PropertyCache::GetCache().Release((Cache::Handle*)m_Ptr);
        m_Ptr=NULL;
    };

    CachePtr & operator=(Cache::Handle * Hand) {reset(Hand);};

    void assign(Object * Ptr) {reset(Ptr);};

    void reset(Object * ObjectPtr=NULL)
    {
        Release();
        m_Ptr=(Object *)ObjectPtr;
    }

    Object * get() {return(m_Ptr);};

    const Object * get() const {return(m_Ptr);};

    Object * operator->() {return(m_Ptr);};
    const Object * operator->() const {return(m_Ptr);};

    Object & operator*() {return(*get());};
    const Object & operator*() const {return(*get());};

    bool Lookup(const Slice & Key)
    {
        Release();
        m_Ptr=(Object *)PropertyCache::Lookup(Key);
        return(NULL!=m_Ptr);
    };

    bool Insert(const Slice & Key, Object * Value)
    {
        Release();
        m_Ptr=(Object *)PropertyCache::GetCache().Insert(Key, Value, 1, &Deleter);
        return(NULL!=m_Ptr);
    };

    static void Deleter(const Slice& Key, void * Value)
    {
        Object * ptr(Value);
        delete ptr;
    };
protected:

private:
    CachePtr(const CachePtr &);
    CachePtr & operator=(const CachePtr &);

};  // template CachePtr


typedef CachePtr<ExpiryProperties> ExpiryPropPtr_t;


}  // namespace leveldb

#endif // ifndef
