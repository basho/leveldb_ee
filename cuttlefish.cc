// -------------------------------------------------------------------
//
// cuttlefish.cc
//
// Copyright (c) 2017 Basho Technologies, Inc. All Rights Reserved.
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

#include <ctype.h>
#include <stdlib.h>

#include "util/expiry_os.h"

namespace leveldb {

uint64_t
CuttlefishDurationMinutes(
    const char * Buffer)
{
    uint64_t seconds, temp;
    const char * cursor;
    char * suffix;

    cursor=Buffer;
    seconds=0;

    if (NULL!=cursor)
    {
        while('\0'!=*cursor)
        {
            temp=strtol(cursor, &suffix, 10);
            if (LONG_MIN!=temp && LONG_MAX!=temp)
            {
                switch(*suffix)
                {
                    // drop through conversion to seconds
                    case 'f': temp*=2;   // fortnight is 2 weeks
                    case 'w': temp*=7;   // week is 7 days
                    case 'd': temp*=24;  // day is 24 hours
                    case 'h': temp*=60;  // hour is 60 minutes
                    case 'm': temp*=60;  // minute is 60 seconds
                    case 's':            // seconds just are
                        // verify it was single char, 'ms' not supported
                        ++suffix;
                        if ('\0'==*suffix || isdigit(*suffix))
                            seconds+=temp;
                        else
                            ++suffix;

                        break;

                    default:
                        ++suffix;
                        break;
                }   // switch

                cursor=suffix;
            }   // if

            // error in strtol, leave
            else
            {
                cursor="";
            }   // else
        }   // while
    }   // if

    // return minutes (a zero return will disable expiry)
    return(seconds/60);

}   // CuttlefishDurationMinutes



}  // namespace leveldb
