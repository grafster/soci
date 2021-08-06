// Copyright (C) 2004-2006 Maciej Sobczak, Stephen Hutton, David Courtney
// Distributed under the Boost Software License, Version 1.0.
// (See accompanying file LICENSE_1_0.txt or copy at
// http://www.boost.org/LICENSE_1_0.txt)

#define SOCI_MYSQL_SOURCE
#include "soci/soci-platform.h"
#include "soci/mysql/soci-mysql.h"
#include "soci-compiler.h"
#include "soci-exchange-cast.h"
#include <cctype>
#include <cstdio>
#include <cstring>
#include <ctime>
#include <sstream>

using namespace soci;
using namespace soci::details;

void* mysql_standard_use_type_backend::prepare_for_bind(
    unsigned long &size, enum_field_types &sqlType)
{
    switch (type_)
    {
    // simple cases
    case x_short:
        sqlType = MYSQL_TYPE_SHORT;
        size = sizeof(short);
        break;
    case x_integer:
        sqlType = MYSQL_TYPE_LONG;
        size = sizeof(int);
        break;
    case x_long_long:
          sqlType = MYSQL_TYPE_LONGLONG;
          size = sizeof(long long);
        break;
    case x_double:
        sqlType = MYSQL_TYPE_DOUBLE;
        size = sizeof(double);
        break;

    case x_char:
        sqlType = MYSQL_TYPE_STRING;
        size = 2;
        buf_ = new char[size];
        buf_[0] = exchange_type_cast<x_char>(data_);
        buf_[1] = '\0';
        indHolder_ = STMT_INDICATOR_NTS;
        break;
    case x_stdstring:
    {
        std::string const& s = exchange_type_cast<x_stdstring>(data_);

        copy_from_string(s, size, sqlType);
    }
    break;
    case x_stdtm:
    {
        std::tm const& t = exchange_type_cast<x_stdtm>(data_);

        sqlType = MYSQL_TYPE_TIMESTAMP;
        buf_ = new char[sizeof(MYSQL_TIME)];
        size = sizeof(MYSQL_TIME);

        MYSQL_TIME* ts = reinterpret_cast<MYSQL_TIME*>(buf_);


        ts->year = static_cast<unsigned int>(t.tm_year + 1900);
        ts->month = static_cast<unsigned int>(t.tm_mon + 1);
        ts->day = static_cast<unsigned int>(t.tm_mday);
        ts->hour = static_cast<unsigned int>(t.tm_hour);
        ts->minute = static_cast<unsigned int>(t.tm_min);
        ts->second = static_cast<unsigned int>(t.tm_sec);

    }
    break;

    case x_longstring:
        copy_from_string(exchange_type_cast<x_longstring>(data_).value,
                         size, sqlType);
        break;
    case x_xmltype:
        copy_from_string(exchange_type_cast<x_xmltype>(data_).value,
                         size, sqlType);
        break;

    // unsupported types
    default:
        throw soci_error("Use element used with non-supported type.");
    }

    // Return either the pointer to C++ data itself or the buffer that we
    // allocated, if any.
    return buf_ ? buf_ : data_;
}

void mysql_standard_use_type_backend::copy_from_string(
    std::string const& s,
    unsigned long& size,
    enum_field_types& sqlType)
{
    size = s.size();
    sqlType = MYSQL_TYPE_STRING;
    buf_ = new char[size+1];
    memcpy(buf_, s.c_str(), size);
    buf_[size++] = '\0';
    indHolder_ = STMT_INDICATOR_NTS;
}

void mysql_standard_use_type_backend::bind_by_pos(
    int &position, void *data, exchange_type type, bool /* readOnly */)
{
    if (statement_.boundByName_)
    {
        throw soci_error(
         "Binding for use elements must be either by position or by name.");
    }

    position_ = position++;
    data_ = data;
    type_ = type;

    statement_.boundByPos_ = true;
}

void mysql_standard_use_type_backend::bind_by_name(
    std::string const &name, void *data, exchange_type type, bool /* readOnly */)
{
    if (statement_.boundByPos_)
    {
        throw soci_error(
         "Binding for use elements must be either by position or by name.");
    }

    int position = -1;
    int count = 1;

    for (std::vector<std::string>::iterator it = statement_.names_.begin();
         it != statement_.names_.end(); ++it)
    {
        if (*it == name)
        {
            position = count;
            break;
        }
        count++;
    }

    if (position == -1)
    {
        std::ostringstream ss;
        ss << "Unable to find name '" << name << "' to bind to";
        throw soci_error(ss.str());
    }

    position_ = position;
    data_ = data;
    type_ = type;

    statement_.boundByName_ = true;
}

void mysql_standard_use_type_backend::pre_use(indicator const *ind)
{
    // first deal with data
    enum_field_types sqlType;
 
    int bufLen = 0;

    void* const sqlData = prepare_for_bind(size_, sqlType);

    // If the indicator is i_null, we need to pass the corresponding value to
    // the ODBC function, and we have to do it without changing indHolder_
    // itself because we may need to use its original value again when we're
    // called the next when executing a prepared statement multiple times.
    //
    // So use separate holder variables depending on whether we need to insert
    // null or not.
    static char indHolderNull = STMT_INDICATOR_NULL;

    memset(&bindingInfo_, 0, sizeof(MYSQL_BIND));

    bindingInfo_.buffer = sqlData;
    bindingInfo_.buffer_length = bufLen;
    bindingInfo_.buffer_type = sqlType;
    bindingInfo_.length = &size_;
    bindingInfo_.u.indicator = ind && *ind == i_null ? &indHolderNull : reinterpret_cast<char*>(&indHolder_);

    statement_.addBindingInfo(&bindingInfo_);

   
}

void mysql_standard_use_type_backend::post_use(bool gotData, indicator *ind)
{
    if (ind != NULL)
    {
        if (gotData)
        {
            if (indHolder_ == 0)
            {
                *ind = i_ok;
            }
            else if (indHolder_ == STMT_INDICATOR_NULL)
            {
                *ind = i_null;
            }
            else
            {
                *ind = i_truncated;
            }
        }
    }

    clean_up();
}

void mysql_standard_use_type_backend::clean_up()
{
    if (buf_ != NULL)
    {
        delete [] buf_;
        buf_ = NULL;
    }
}
