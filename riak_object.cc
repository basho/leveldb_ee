// -------------------------------------------------------------------
//
// riak_object.cc
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


#include <arpa/inet.h>
#include <time.h>

#include "leveldb_ee/riak_object.h"
#include "util/logging.h"


namespace leveldb {

// short cut compares and endian concerns via union
union Binary32_t
{
    uint8_t m_Bytes[4];
    uint32_t m_Uint32;
};


union Binary16_t
{
    uint8_t m_Bytes[2];
    uint16_t m_Uint16;
};


// minimum sext encoding is tuple of two binaries
const uint32_t cKeyMinSize=11;
const Binary32_t cSextPrefix={{16,0,0,0}}; // tuple tag and 3 bytes of 4 byte size
const Binary32_t cOKeyPrefix={{0x0c, 0xb7, 0x80, 0x08}};  // atom tag and 'o' atom

// riak object v1 (not v0) starts with these two bytes
const Binary16_t cRiakObjV1={{0x35, 1}};

// Erlang 2 tuple prefix
const Binary16_t cTwoTuplePrefix={{0x68, 0x02}};
const Binary16_t cStringPrefix={{0x6b, 0x00}};

//struct RiakV1

static bool SiblingGetLastModTime(
    const uint8_t * &Cursor,
    const uint8_t * Limit,
    uint64_t & ModTime);

static bool FindDictionaryEntry(
    const char * Key,
    uint32_t KeyLen,
    const uint8_t * &Cursor, // start of sibling, output set to next sibling
    const uint8_t * Limit);  // overrun test

static bool FindMetaEntry(
    const char * Key,
    uint32_t KeyLen,
    const uint8_t * &Cursor, // start of sibling, output set to next sibling
    const uint8_t * Limit);  // overrun test

static bool GetBinaryLength(const uint8_t * Cursor,
                            const uint8_t * Limit,
                            int & Length,
                            bool DecodeLength=true);
static bool GetBinary(const uint8_t * &Cursor,
                      const uint8_t * Limit,
                      uint8_t * Output);



/**
 * Review Key to see if sext encoded Riak key.  If so,
 *  parse out Bucket and potentially Bucket Type
 */
bool                          //< true if bucket found, false if not
KeyGetBucket(
    Slice & Key,              //< input: key to parse
    std::string & BucketType, //< output: bucket type string or clear()
    std::string & Bucket)     //< output: bucket string or clear()
{
    Slice composite_slice;
    bool ret_flag;
    int length;
    const uint8_t * key_end, * tmp_ptr, *cursor;

    key_end=(uint8_t *)Key.data() + Key.size();
    ret_flag=KeyGetBucket(Key, composite_slice);

    BucketType.clear();
    Bucket.clear();

    if (ret_flag)
    {
        // not checking returns other formatting since all checks should
        //  have occurred within prior KeyGetBucket() call
        cursor=(uint8_t *)composite_slice.data();

        // tuple means bucket_type and bucket
        if (16==*cursor)
        {
            // shift cursor to first char of binary
            cursor+=6;
            GetBinaryLength(cursor, key_end, length);
            BucketType.resize(length);
            GetBinary(cursor, key_end, (uint8_t *)BucketType.data());

            ++cursor;
            GetBinaryLength(cursor, key_end, length);
            Bucket.resize(length);
            GetBinary(cursor, key_end, (uint8_t *)Bucket.data());

        }   // if

        // binary name for bucket only
        else
        {
            ++cursor;
            GetBinaryLength(cursor, key_end, length);
            Bucket.resize(length);
            GetBinary(cursor, key_end, (uint8_t *)Bucket.data());
        }   // else
    }   // if

    return(ret_flag);

}   // KeyGetBucket (std::string)


/**
 * Review Key to see if sext encoded Riak key.  If so,
 *  parse out Bucket and potentially Bucket Type
 *  This returns the slice of Bucket Type / Bucket, or just Bucket.
 *  Intent is that the single slice will be lookup key in cache.
 */
bool                          //< true if bucket found, false if not
KeyGetBucket(
    Slice & Key,              //< input: key to parse
    Slice & CompositeBucket)  //< output: entire bucket_type/bucket tuple,
                              //    or binary bucket name
{
    bool ret_flag;
    const uint8_t * cursor, * key_end, *cursor_temp;
    int length;

    ret_flag=false;
    CompositeBucket.clear();

    // hard coded decode for sext prefix of <<bucket>> or {<<bucket type>>,<<bucket>>}
    /// detect prefix
    cursor=(const uint8_t *)Key.data();
    key_end=cursor + Key.size();

    if (cKeyMinSize<=Key.size() && cSextPrefix.m_Uint32==*(uint32_t *)cursor)
    {
        cursor+=sizeof(uint32_t);

        // looking for {o, <<bucket>> | {<<bucket type>>,<<bucket>>}, <<key>>}
        if (3==*cursor)   // tuple size of 3
        {
            ++cursor;

            // 'o' atom?
            if (cOKeyPrefix.m_Uint32==*(uint32_t *)cursor)
            {
                cursor+=sizeof(uint32_t);

                // second tuple is a tuple. its first tuple is binary tag 18: bucket type, bucket
                if ((cursor+5)<key_end && 16==*cursor && 2==*(cursor+4) && 18==*(cursor+5))
                {
                    // return entire {<<bucket type>>, <<bucket>>} slice
                    cursor_temp=cursor;

                    // shift cursor to first char of bucket type's binary
                    cursor+=6;
                    if (GetBinaryLength(cursor, key_end, length, false))
                    {
                        cursor+=length;

                        // test for binary (bucket name)
                        ret_flag=cursor<key_end && 18==*cursor;

                        ++cursor;
                        if (ret_flag && GetBinaryLength(cursor, key_end, length, false))
                        {
                            cursor+=length;
                            Slice temp((const char *)cursor_temp, (cursor-cursor_temp));
                            CompositeBucket=temp;
                        }   // if
                        else
                        {
                            ret_flag=false;
                        }   // else
                    }   // if
                }   // if

                // 18 is binary tag:  bucket only
                else if (18==*cursor)
                {
                    cursor_temp=cursor;
                    ++cursor;
                    if (GetBinaryLength(cursor, key_end, length, false))
                    {
                        cursor+=length;
                        Slice temp((const char *)cursor_temp, (cursor-cursor_temp));
                        CompositeBucket=temp;
                        ret_flag=true;
                    }   // if
                }   // else if
            }   // if
        }   // if
    }   // if

    return(ret_flag);

}   // KeyGetBucket (slice)


// from riak_kv/src/riak_object.erl
//
// Definition of Riak Object version 1 (in Erlangese)
//   header:  <<?MAGIC:8/integer, ?V1_VERS:8/integer, VclockLen:32/integer, VclockBin/binary, SibCount:32/integer, SibsBin/binary>>.
//   sibling:  <<ValLen:32/integer, ValBin:ValLen/binary, MetaLen:32/integer, MetaBin:MetaLen/binary>>.
//   meta:  <<LastModBin/binary, VTagLen:8/integer, VTagBin:VTagLen/binary, Deleted:1/binary-unit:8, RestBin/binary>>.


/**
 * The Value is likely a Riak version 0 or version 1 "Riak Object".
 *  Initial implementation only decodes version 1.  Everything else
 *  is ignored.
 */
bool
ValueGetLastModTime(
    Slice Value,
    uint64_t & LastModTime)
{
    bool ret_flag, good;
    const uint8_t * cursor, * limit;
    int sib_count, vclock_len, loop;
    uint64_t most_recent, sib_time;

    ret_flag=false;
    LastModTime=0;
    cursor=(const uint8_t *)Value.data();
    limit=cursor + Value.size();


    // does this value object start like a Riak v1 object
    if (cRiakObjV1.m_Uint16==*(uint16_t *)cursor && (cursor+1)<limit)
    {
        cursor+=sizeof(cRiakObjV1.m_Uint16);;

        // skip over vclock
        vclock_len=ntohl(*(uint32_t *)cursor);
        cursor+=sizeof(uint32_t);
        cursor+=vclock_len;

        // get sibling count
        sib_count=ntohl(*(uint32_t *)cursor);
        cursor+=sizeof(uint32_t);

        most_recent=0;
        for (loop=0, good=true; loop<sib_count && good && cursor<limit; ++loop)
        {
            good=SiblingGetLastModTime(cursor, limit, sib_time);
            if (good && most_recent<sib_time)
                most_recent=sib_time;
        }   // for

        ret_flag=good && 0!=most_recent;

        if (ret_flag)
            LastModTime=most_recent;
    }   // if

    return(ret_flag);

}   // ValueGetLastModTime


bool
SiblingGetLastModTime(
    const uint8_t * &Cursor, // start of sibling, output set to next sibling
    const uint8_t * Limit,   // overrun test
    uint64_t & ModTime)
{
    bool ret_flag;
    const uint8_t * cursor;
    uint32_t field_size;

    ret_flag=false;

    // process (skip) the first field pair which are value size and value
    cursor=Cursor;
    if ((cursor+sizeof(uint32_t))<Limit)
    {
        // bytes within the value
        field_size=ntohl(*(uint32_t *)cursor);

        // and skip value
        cursor+=sizeof(uint32_t);
        cursor+=field_size;
    }   // if

    // process Metadata fields
    if ((cursor+sizeof(uint32_t))<Limit)
    {
        // bytes within the value
        field_size=ntohl(*(uint32_t *)cursor);

        // and skip value
        cursor+=sizeof(uint32_t);

        // set external cursor to next sibling
        Cursor+=(cursor - Cursor) + field_size;

        // read Metadata's LastModTime, an Erlang timestamp
        //  (three uint32_t integers:  "megaseconds", seconds, microseconds)
        if ((cursor+sizeof(uint32_t)*3)<Limit)
        {
            uint64_t temp;

            temp=ntohl(*(uint32_t *)cursor);
            temp*=1000000;
            cursor+=sizeof(uint32_t);

            temp+=ntohl(*(uint32_t *)cursor);
            temp*=1000000;
            cursor+=sizeof(uint32_t);

            temp+=ntohl(*(uint32_t *)cursor);
            cursor+=sizeof(uint32_t);

            ModTime=temp;
            ret_flag=true;
        }   // if

        //
        // secondary search of X-Riak-Meta for user supplied mod time
        //

        // skip vtag
        if (cursor<Limit)
        {
            field_size=*cursor;
            cursor+= 1 + field_size;
        }   // if

        // skip deleted tag
        ++cursor;

        // now at series of key/value pairs
        //  <<KeyLen:32/integer, KeyBin/binary, ValueLen:32/integer, ValueBin/binary>>
        //  11 is strlen("X-Riak-Meta").  do not want to compute it every call.
        if (FindDictionaryEntry("X-Riak-Meta", 11, cursor, Limit))
        {
            // find entry, see if cursor updated to string header
            //  31 is length of string
            if (FindMetaEntry("X-Riak-Meta-Expiry-Base-Seconds", 31, cursor, Limit)
                && 0x6b==*cursor && (cursor+3)<Limit)
            {
                uint64_t temp;

                ++cursor;
                // external term format ... must be short string
                field_size=ntohs(*(uint16_t *)cursor);
                cursor+=sizeof(uint16_t);

                // strtol() like conversion
                temp=0;
                while(field_size && cursor<Limit)
                {
                    // validate that these are digits
                    if (0x30<=*cursor && *cursor<= 0x39)
                    {
                        temp=temp*10 + (*cursor & 0x0f);
                        ++cursor;
                        --field_size;
                    }   // if
                    else
                    {
                        // terminate decode
                        cursor=Limit;
                        temp=0;
                    }   // else

                }   // while

                // look useful: 1980-01-01 < temp < 2080-01-01
                if (315550800 < temp && temp < 3471310800 && cursor<Limit)
                {
                    // ModTime in microseconds
                    ModTime=temp*1000000;
                    ret_flag=true;
                }   // if
            }   // if
        }   // if
    }   // if

    return(ret_flag);

}   // SiblingGetLastModTime


/**
 *
 *  <<KeyLen:32/integer, KeyBin/binary, ValueLen:32/integer, ValueBin/binary>>
 */
bool
FindDictionaryEntry(
    const char * Key,
    uint32_t KeyLen,
    const uint8_t * &Cursor, // first dictionary entry, output is matched value
    const uint8_t * Limit)   // overrun test
{
    bool ret_flag;
    uint32_t key_len, val_len;

    ret_flag=false;

    while((Cursor+sizeof(uint32_t))<Limit && !ret_flag)
    {
        key_len=ntohl(*(uint32_t *)Cursor);
        Cursor+=sizeof(uint32_t);

        // +1 is for "type byte" preamble
        if (key_len==(KeyLen+1) && (Cursor+key_len)<Limit)
            ret_flag=(0==memcmp(Key, (Cursor+1), KeyLen));

        // move to value
        Cursor+=key_len;

        // skip over value if no match
        if (!ret_flag)
        {
            val_len=ntohl(*(uint32_t *)Cursor);
            Cursor+=sizeof(uint32_t);
            Cursor+=val_len;
        }   // if
    }   // while

    return(ret_flag);

}   // FindDictionaryEntry


/**
 * Currently a handmade decode of expected Erlang Term-To-Binary encoding
 *  (magic numbers from here:  http://erlang.org/doc/apps/erts/erl_ext_dist.html)
 *  The encoding is a "list" of "tuple pairs".
 */
bool
FindMetaEntry(
    const char * Key,
    uint32_t KeyLen,
    const uint8_t * &Cursor, // first pair of Meta K/V items, output is value of matched meta
    const uint8_t * Limit)   // overrun test
{
    bool ret_flag, good;
    uint32_t meta_len, key_len, val_len, list_len;
    const uint8_t * meta_limit;
    uint16_t temp16;

    meta_limit=Limit;
    ret_flag=false;
    list_len=0;
    good=((Cursor+sizeof(uint32_t))<Limit);

    if (good)
    {
        meta_len=ntohl(*(uint32_t *)Cursor);
        meta_limit=Cursor+meta_len;
        Cursor+=sizeof(uint32_t);
    }   // if

    // test for list tag, get count of elements
    if (good && Cursor<meta_limit && meta_limit<=Limit)
    {
        good=(0x00==*Cursor);
        ++Cursor;
        // look for "new term" tag
        good=(good && Cursor<meta_limit && 0x83==*Cursor);
        ++Cursor;
        // look for "list term" tag
        good=(good && Cursor<meta_limit && 0x6c==*Cursor);
        ++Cursor;

        good=(good && (Cursor+sizeof(uint32_t))<meta_limit);
        if (good)
        {
            list_len=ntohl(*(uint32_t *)Cursor);
            Cursor+=sizeof(uint32_t);
        }   // if
    }   // if

    // walk each of the meta list elements.  if we hit one
    //  we do not understand, stop.  maybe log to syslog
    while(good && list_len && (Cursor+1)<meta_limit && !ret_flag)
    {
        // element should be a two element tuple
        //  (element NIL_EXT will be last in list and fail prefix test)
        temp16=*(uint16_t *)Cursor;
        Cursor+=sizeof(uint16_t);
        good=cTwoTuplePrefix.m_Uint16==temp16 && (Cursor+sizeof(uint16_t))<meta_limit;

        // decode "key"
        if (good)
        {
            temp16=*(uint16_t *)Cursor;
            Cursor+=sizeof(uint16_t);
            good=cStringPrefix.m_Uint16==temp16 && (Cursor+sizeof(uint16_t))<meta_limit;

            // get string/key length
            if (good)
                key_len=*Cursor;
            else
                key_len=0;
            ++Cursor;

            if (good && key_len==KeyLen && (Cursor+key_len)<meta_limit)
                ret_flag=(0==memcmp(Cursor, Key, key_len));

            Cursor+=key_len;
        }   // if

        // skip over value if wrong key
        if (good && !ret_flag)
        {
            temp16=*(uint16_t *)Cursor;
            Cursor+=sizeof(uint16_t);
            good=cStringPrefix.m_Uint16==temp16 && (Cursor+sizeof(uint16_t))<meta_limit;

            // get string/key length
            if (good)
                key_len=*Cursor;
            else
                key_len=0;
            Cursor+=(1+key_len);
            --list_len;
        }   // if
    }   // while

    return(ret_flag);

}   // FindMetaEntry



bool
GetBinaryLength(
    const uint8_t * Cursor, // first byte of binary (after tag)
    const uint8_t * Limit,  // safety limit / overrun protection
    int & Length,           // output: count of bytes
    bool DecodedLength)
{
    bool good, again;
    uint8_t mask;
    const uint8_t * start;

    mask=0x80;
    Length=0;
    start=Cursor;

    do
    {
        good=Cursor<Limit;

        if (good)
        {
            again=(0!=(*Cursor & mask));
            if (again)
            {
                ++Length;
                ++Cursor;
                mask>>=1;

                if (0==mask)
                {
                    ++Cursor;
                    mask=0x80;
                }   // if
            }   // if
        }   // if
    } while(again && good);

    // +2 -> +1 for Cursor on 0 byte, +1 to next
    if (!DecodedLength)
        Length=(Cursor-start) +2;

    return(good);

}   // GetBinaryLength


bool GetBinary(
    const uint8_t * &Cursor, // first byte of binary (after tag), output: position after binary
    const uint8_t * Limit,  // safety limit / overrun protection
    uint8_t * Output)       // output: guaranteed storage
{
    bool good, again;
    uint8_t mask, temp_char, high_bits, low_bits, shift;

    mask=0x80;
    low_bits=0x7f;
    high_bits=0x80;
    shift=1;

    do
    {
        good=Cursor<Limit;

        if (good)
        {
            again=(0!=(*Cursor & mask));
            if (again)
            {
                temp_char=(*Cursor & low_bits) << shift;
                ++Cursor;
                temp_char+=(*Cursor & high_bits) >> (8 - shift);

                *Output=temp_char;
                ++Output;

                mask>>=1;
                low_bits &=(~mask);
                high_bits |= mask;
                ++shift;

                if (0==mask)
                {
                    ++Cursor;
                    mask=0x80;
                    low_bits=0x7f;
                    high_bits=0x80;
                    shift=1;
                }   // if
            }   // if
        }   // if
    } while(again && good);

    ++Cursor;
    ++Cursor;

    return(good);

}   // GetBinary




}  // namespace leveldb
