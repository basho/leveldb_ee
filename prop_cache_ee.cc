// -------------------------------------------------------------------
//
// prop_cache_ee.cc
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

#include <sys/time.h>
#include <unistd.h>

#include "util/prop_cache.h"
#include "leveldb_ee/riak_object.h"
#include "util/logging.h"
#include "util/mutexlock.h"
#include "util/throttle.h"

namespace leveldb {


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
            MutexLock lock(&m_Mutex);
            ret_handle=m_Cache->Lookup(CompositeBucket);

            // is state appropriate to waiting?
            if (NULL==ret_handle && flag)
            {
                timespec ts;

                // OSX does not do clock_gettime
#if _POSIX_TIMERS >= 200801L
                clock_gettime(CLOCK_REALTIME, &ts);
#else
                struct timeval tv;
                gettimeofday(&tv, NULL);
                ts.tv_sec=tv.tv_sec;
                ts.tv_nsec=tv.tv_usec*1000;
#endif

                ts.tv_sec+=1;
                flag=m_Cond.Wait(&ts);
            }   // if
        } while(NULL==ret_handle && flag);
    }   // if

    return(ret_handle);

}   // PropertyCache::LookupWait

}  // namespace leveldb
