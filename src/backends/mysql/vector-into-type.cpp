//
// Copyright (C) 2004-2006 Maciej Sobczak, Stephen Hutton, David Courtney
// Distributed under the Boost Software License, Version 1.0.
// (See accompanying file LICENSE_1_0.txt or copy at
// http://www.boost.org/LICENSE_1_0.txt)
//

#define SOCI_MYSQL_SOURCE
#include "soci/soci-platform.h"
#include "soci/mysql/soci-mysql.h"
#include "soci/type-wrappers.h"
#include "soci-compiler.h"
#include "soci-cstrtoi.h"
#include "soci-mktime.h"
#include "soci-static-assert.h"
#include "soci-vector-helpers.h"
#include <algorithm>
#include <cctype>
#include <cstdio>
#include <cstring>
#include <ctime>
#include <sstream>

using namespace soci;
using namespace soci::details;

void mysql_vector_into_type_backend::define_by_pos(
    int &position, void *data, exchange_type type)
{
    data_ = data; // for future reference
    type_ = type; // for future reference
    position_ = position - 1;

    statement_.intos_.push_back(this);

    const std::size_t vectorSize = get_vector_size(type, data);
    if (vectorSize == 0)
    {
         throw soci_error("Vectors of size 0 are not allowed.");
    }    
    statement_.vectorIntoElementCount_ = vectorSize;
    int size = 0;
    switch (type)
    {
    // simple cases
    case x_short:
        mysqlType_ = MYSQL_TYPE_SHORT;
        size = sizeof(short);
        break;
    case x_integer:
        mysqlType_ = MYSQL_TYPE_LONG;
        size = sizeof(int);
        break;
    case x_long_long:
        mysqlType_ = MYSQL_TYPE_LONGLONG;
        size = sizeof(long long);
        break;
    case x_double:
        mysqlType_ = MYSQL_TYPE_DOUBLE;
        size = sizeof(double);

        break;

    // cases that require adjustments and buffer management

    case x_char:
        mysqlType_ = MYSQL_TYPE_STRING;
        colSize_ = sizeof(char) * 2;
        size = colSize_;
        break;
    case x_stdstring:
    case x_xmltype:
    case x_longstring:
        {
            mysqlType_ = MYSQL_TYPE_STRING;

            colSize_ = statement_.column_size(position);

            colSize_++;
            size = colSize_;
        }
        break;
    case x_stdtm:
        mysqlType_ = MYSQL_TYPE_TIMESTAMP;
        colSize_ = sizeof(MYSQL_TIME);
        size = colSize_;
        break;
    default:
        throw soci_error("Into element used with non-supported type.");
    }

    position++;
    buf_ = new char[size];

    const int pos = static_cast<int>(position_ + 1);

    memset(&bindingInfo_, 0, sizeof(MYSQL_BIND));
    bindingInfo_.buffer_type = mysqlType_;
    bindingInfo_.buffer = buf_;
    bindingInfo_.buffer_length = size;
    bindingInfo_.is_null = &isNull_;
    bindingInfo_.error = &isError_;
    bindingInfo_.length = &length_;

    statement_.addResultBinding(&bindingInfo_);
}


void mysql_vector_into_type_backend::pre_fetch()
{
    // nothing to do for the supported types
}

void mysql_vector_into_type_backend::do_post_fetch_row(
    std::size_t rowNum)
{

    if (isNull_)
    {
        indicators_.push_back(i_null);
        return;
    }
    else
    {
        indicators_.push_back(i_ok);
    }

    if (type_ == x_integer)
    {
        std::vector<int>* vp
                = static_cast<std::vector<int> *>(data_);
        std::vector<int>& v(*vp);

        v[rowNum] = *(int*)buf_;

    }
    else if (type_ == x_char)
    {
        std::vector<char>* vp
            = static_cast<std::vector<char> *>(data_);
        std::vector<char>& v(*vp);

        v[rowNum] = *(char*)buf_;

    }
    else
    {
        throw soci_error("Unhandled type in do_post_fetch_row");
    }
    /*
    else if (type_ == x_char)
    {
        std::vector<char> *vp
            = static_cast<std::vector<char> *>(data_);

        std::vector<char> &v(*vp);
        char *pos = buf_;
        for (std::size_t i = beginRow; i != endRow; ++i)
        {
            v[i] = *pos;
            pos += colSize_;
        }
    }
    if (type_ == x_stdstring || type_ == x_xmltype || type_ == x_longstring)
    {
        const char *pos = buf_;
        for (std::size_t i = beginRow; i != endRow; ++i, pos += colSize_)
        {
            int len = indicator_;

            std::string& value = vector_string_value(type_, data_, i);
            if (len == -1)
            {
                // Value is null.
                value.clear();
                continue;
            }

            // Find the actual length of the string: for a VARCHAR(N)
            // column, it may be right-padded with spaces up to the length
            // of the longest string in the result set. This happens with
            // at least MS SQL (and the exact behaviour depends on the
            // value of the ANSI_PADDING option) and it seems like some
            // other ODBC drivers also have options like "PADVARCHAR", so
            // it's probably not the only case when it does.
            //
            // So deal with this generically by just trimming all the
            // spaces from the right hand-side.
            const char* end = pos + len;
            while (end != pos)
            {
                // Pre-decrement as "end" is one past the end, as usual.
                if (*--end != ' ')
                {
                    // We must count the last non-space character.
                    ++end;
                    break;
                }
            }

            value.assign(pos, end - pos);
        }
    }
    else if (type_ == x_stdtm)
    {
        std::vector<std::tm> *vp
            = static_cast<std::vector<std::tm> *>(data_);

        std::vector<std::tm> &v(*vp);
        char *pos = buf_;
        for (std::size_t i = beginRow; i != endRow; ++i)
        {
            // See comment for the use of this macro in standard-into-type.cpp.
            GCC_WARNING_SUPPRESS(cast-align)

            MYSQL_TIME* ts = reinterpret_cast<MYSQL_TIME*>(pos);

            GCC_WARNING_RESTORE(cast-align)

            details::mktime_from_ymdhms(v[i],
                                        ts->year, ts->month, ts->day,
                                        ts->hour, ts->minute, ts->second);
            pos += colSize_;
        }
    }*/
}

void mysql_vector_into_type_backend::post_fetch(bool gotData, indicator* ind)
{
    // Here we have to set indicators only. Data was exchanged with user
    // buffers during fetch()
    if (gotData)
    {
        std::size_t rows = statement_.numRowsFetched_;

        if (rows > 0 && indicators_.size() != rows)
        {
            throw soci_error("Internal error - indicators not set");
        }

        for (std::size_t i = 0; i < rows; ++i)
        {
            if (indicators_[i] == i_null)
            {
                if (ind == NULL)
                {
                    throw soci_error("Null value fetched and no indicator defined.");
                }

                ind[i] = i_null;
            }
            else if (ind != NULL)
            {
                ind[i] = i_ok;
            }
        }
    }
    indicators_.clear();
}

void mysql_vector_into_type_backend::resize(std::size_t sz)
{
    indicators_.resize(sz);
    resize_vector(type_, data_, sz);
}

std::size_t mysql_vector_into_type_backend::size()
{
    return get_vector_size(type_, data_);
}

void mysql_vector_into_type_backend::clean_up()
{
    if (buf_ != NULL)
    {
        delete [] buf_;
        buf_ = NULL;
    }
    std::vector<mysql_vector_into_type_backend*>::iterator it
        = std::find(statement_.intos_.begin(), statement_.intos_.end(), this);
    if (it != statement_.intos_.end())
        statement_.intos_.erase(it);
}
