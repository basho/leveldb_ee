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

#include "leveldb/cache.h"
#include "leveldb/options.h"
#include "leveldb/expiry.h"
#include "leveldb/perf_count.h"
#include "db/dbformat.h"
#include "db/version_edit.h"
#include "util/expiry_os.h"

namespace leveldb
{

struct ExpiryProperties
{
    // same options as ExpiryModuleOS (but destined for cache)

    // true: bucket expiry enabled
    bool m_ExpiryEnabled;

    // number of ttl minutes, zero if disabled
    uint64_t m_ExpiryMinutes;

    // true: if ok to erase file instead of compaction
    bool m_WholeFileExpiry;

};  // ExpiryProperties


class ExpiryModuleEE : public ExpiryModuleOS
{
public:
    ExpiryModuleEE()
    {};

    ~ExpiryModuleEE() {};

    // Print expiry options to LOG file
    virtual void Dump(Logger * log) const;

protected:
    // utility to CompactionFinalizeCallback to review
    //  characteristics of one SstFile to see if entirely expired
    virtual bool IsFileExpired(const FileMetaData & SstFile, ExpiryTime Now, ExpiryTime Aged) const;

    // When "creating" write time, chose its source based upon
    //  open source versus enterprise edition
    virtual uint64_t GenerateWriteTime(const Slice & Key, const Slice & Value) const;

};  // ExpiryModule


template<typename Object> class CachePtr
{
    /****************************************************************
    *  Member objects
    ****************************************************************/
public:

protected:
    Cache & m_Cache;
    Object * m_Ptr;            // NULL or object in cache
private:

    /****************************************************************
    *  Member functions
    ****************************************************************/
public:
    CachePtr(Cache & CacheObj) : m_Cache(CacheObj), m_Ptr(NULL) {};

    virtual ~CachePtr() {Release();};

    void Release()
    {
        if (NULL!=m_Ptr)
            m_Cache.Release((Cache::Handle*)m_Ptr);
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
        m_Ptr=(Object *)m_Cache.Lookup(Key);
        return(NULL!=m_Ptr);
    };

    bool Insert(const Slice & Key, Object * Value)
    {
        Release();
        m_Ptr=(Object *)m_Cache.Insert(Key, Value, 1, &Deleter);
        return(NULL!=m_Ptr);
    };

    static void Deleter(const Slice& Key, void * Value)
    {
        Object * ptr(Value);
        delete ptr;
    };
protected:

private:
    CachePtr();
    CachePtr(const CachePtr &);
    CachePtr & operator=(const CachePtr &);

};  // template CachePtr


typedef CachePtr<ExpiryProperties> ExpiryPropPtr_t;

}  // namespace leveldb

#endif // ifndef
