//
// Copyright (C) 2004-2006 Maciej Sobczak, Stephen Hutton, David Courtney
// Distributed under the Boost Software License, Version 1.0.
// (See accompanying file LICENSE_1_0.txt or copy at
// http://www.boost.org/LICENSE_1_0.txt)
//

#define SOCI_MYSQL_SOURCE
#include "soci/soci-platform.h"
#include "soci/mysql/soci-mysql.h"
#include "soci-compiler.h"
#include "soci-cstrtoi.h"
#include "soci-exchange-cast.h"
#include "soci-mktime.h"
#include <ctime>

using namespace soci;
using namespace soci::details;

void mysql_standard_into_type_backend::define_by_pos(
    int & position, void * data, exchange_type type)    
{
    data_ = data;
    type_ = type;
    position_ = position++;

    unsigned long size = 0;
    switch (type_)
    {
    case x_char:
        mysqlType_ = MYSQL_TYPE_STRING;
        size = sizeof(char) + 1;
        buf_ = new char[size];
        data = buf_;
        break;
    case x_stdstring:
    case x_longstring:
    case x_xmltype:
        mysqlType_ = MYSQL_TYPE_STRING;
        // For LONGVARCHAR fields the returned size is ODBC_MAX_COL_SIZE
        // (or 0 for some backends), but this doesn't correspond to the actual
        // field size, which can be (much) greater. For now we just used
        // a buffer of huge (100MiB) hardcoded size, which is clearly not
        // ideal, but changing this would require using SQLGetData() and is
        // not trivial, so for now we're stuck with this suboptimal solution.
        size = statement_.column_size(position_);
        size++;
        buf_ = new char[size];
        data = buf_;
        break;
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
    case x_stdtm:
        mysqlType_ = MYSQL_TYPE_TIMESTAMP;
        size = sizeof(MYSQL_TIME);
        buf_ = new char[size];
        data = buf_;
        break;    
    default:
        throw soci_error("Into element used with non-supported type.");
    }

    valueLen_ = 0;
    memset(&bindingInfo_, 0, sizeof(MYSQL_BIND));
    bindingInfo_.buffer_type = mysqlType_;
    bindingInfo_.buffer = data;
    bindingInfo_.buffer_length = size;
    bindingInfo_.is_null = &isNull_;
    bindingInfo_.error = &isError_;

    statement_.addResultBinding(&bindingInfo_);
}

void mysql_standard_into_type_backend::pre_fetch()
{
    //...
}

void mysql_standard_into_type_backend::post_fetch(
    bool gotData, bool calledFromFetch, indicator * ind)
{
    if (calledFromFetch == true && gotData == false)
    {
        // this is a normal end-of-rowset condition,
        // no need to do anything (fetch() will return false)
        return;
    }

    if (gotData)
    {
        // first, deal with indicators
        if (*bindingInfo_.is_null)
        {
            if (ind == NULL)
            {
                throw soci_error(
                    "Null value fetched and no indicator defined.");
            }

            *ind = i_null;
            return;
        }
        else
        {
            if (ind != NULL)
            {
                *ind = i_ok;
            }
        }

        // only std::string and std::tm need special handling
        if (type_ == x_char)
        {
            exchange_type_cast<x_char>(data_) = buf_[0];
        }
        else if (type_ == x_stdstring)
        {
            std::string& s = exchange_type_cast<x_stdstring>(data_);
            s = buf_;
        }
        else if (type_ == x_longstring)
        {
            exchange_type_cast<x_longstring>(data_).value = buf_;
        }
        else if (type_ == x_xmltype)
        {
            exchange_type_cast<x_xmltype>(data_).value = buf_;
        }
        else if (type_ == x_stdtm)
        {
            std::tm& t = exchange_type_cast<x_stdtm>(data_);

            MYSQL_TIME* ts = reinterpret_cast<MYSQL_TIME*>(buf_);


            details::mktime_from_ymdhms(t,
                                        ts->year, ts->month, ts->day,
                                        ts->hour, ts->minute, ts->second);
        }
    }
}

void mysql_standard_into_type_backend::clean_up()
{
    if (buf_)
    {
        delete [] buf_;
        buf_ = 0;
    }
}
