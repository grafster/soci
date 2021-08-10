//
// Copyright (C) 2004-2021 Maciej Sobczak, Stephen Hutton, David Courtney, Andrew Grafham
// Distributed under the Boost Software License, Version 1.0.
// (See accompanying file LICENSE_1_0.txt or copy at
// http://www.boost.org/LICENSE_1_0.txt)
//

#define SOCI_MYSQL_SOURCE
#include "soci/soci-platform.h"
#include "soci/mysql/soci-mysql.h"
#include "soci-compiler.h"
#include "soci-static-assert.h"
#include "soci-vector-helpers.h"
#include <cctype>
#include <cstdio>
#include <cstring>
#include <ctime>
#include <sstream>


using namespace soci;
using namespace soci::details;

void mysql_vector_use_type_backend::prepare_indicators(std::size_t size)
{
    if (size == 0)
    {
         throw soci_error("Vectors of size 0 are not allowed.");
    }

    indHolderVec_.resize(size);
    statement_.vectorUseElementCount_ = size;
}

void* mysql_vector_use_type_backend::prepare_for_bind(unsigned long &size,
    enum_field_types& sqlType)
{
    void* data = NULL;
    switch (type_)
    {    // simple cases
    case x_short:
        {
            sqlType = MYSQL_TYPE_SHORT;
            size = sizeof(short);
            std::vector<short> *vp = static_cast<std::vector<short> *>(data_);
            std::vector<short> &v(*vp);
            prepare_indicators(v.size());
            data = &v[0];
        }
        break;
    case x_integer:
        {
            sqlType = MYSQL_TYPE_LONG;
            size = sizeof(long);
            std::vector<int> *vp = static_cast<std::vector<int> *>(data_);
            std::vector<int> &v(*vp);
            prepare_indicators(v.size());
            data = &v[0];
        }
        break;
    case x_long_long:
    case x_unsigned_long_long:
        {
            std::vector<long long> *vp =
                static_cast<std::vector<long long> *>(data_);
            std::vector<long long> &v(*vp);
            std::size_t const vsize = v.size();
            prepare_indicators(vsize);

            sqlType = MYSQL_TYPE_LONGLONG;
            size = sizeof(long long);
            data = &v[0];
        }
        break;
      case x_double:
        {
            sqlType = MYSQL_TYPE_DOUBLE;
            size = sizeof(double);
            std::vector<double> *vp = static_cast<std::vector<double> *>(data_);
            std::vector<double> &v(*vp);
            prepare_indicators(v.size());
            data = &v[0];
        }
        break;

    // cases that require adjustments and buffer management
    case x_char:
        {
            std::vector<char> *vp
                = static_cast<std::vector<char> *>(data_);
            std::size_t const vsize = vp->size();

            prepare_indicators(vsize);

            size = sizeof(char);
            // The buffer is an array of pointers to the actual char data
            // size is the size of the data at that pointer, not the size of the pointer itself
            buf_ = new char[sizeof (void*) * vsize];

            char **pos = reinterpret_cast<char**>(buf_);

            // 1 is the default value for all the lengths
            lengths_.resize(vsize, 1);
            for (std::size_t i = 0; i != vsize; ++i)
            {
                *pos++ = &(*vp)[i];
            }

            sqlType = MYSQL_TYPE_STRING;
            data = buf_;
        }
        break;
    case x_stdstring:
    case x_xmltype:
    case x_longstring:
        {
            std::size_t const vecSize = get_vector_size(type_, data_);
            prepare_indicators(vecSize);
            lengths_.resize(vecSize);

            // The buffer contains an array of pointers to the actual text strings
            buf_ = new char[sizeof(void*) * vecSize];

            const char** ptrBuffer = reinterpret_cast<const char**>(buf_);

            for (std::size_t i = 0; i != vecSize; ++i)
            {
                std::string& value = vector_string_value(type_, data_, i);

                std::size_t sz = value.length();

                indHolderVec_[i] = STMT_INDICATOR_NONE;
                lengths_[i] = sz;

                ptrBuffer[i] = value.data();                
                
            }

            data = buf_;

            sqlType = MYSQL_TYPE_STRING;
        }
        break;
    case x_stdtm:
        {
            std::vector<std::tm> *vp
                = static_cast<std::vector<std::tm> *>(data_);

            prepare_indicators(vp->size());

            // The buffer is an array of pointers to the actual MYSQL_TIME structs
            buf_ = new char[sizeof(void*) * vp->size()];

            lengths_.resize(vp->size(), sizeof(MYSQL_TIME));

            sqlType = MYSQL_TYPE_TIMESTAMP;
            data = buf_;
            size = sizeof(MYSQL_TIME);
        }
        break;

    // not supported
    default:
        throw soci_error("Use vector element used with non-supported type.");
    }

    colSize_ = size;

    return data;
}

void mysql_vector_use_type_backend::bind_by_pos(int &position,
        void *data, exchange_type type)
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

void mysql_vector_use_type_backend::bind_by_name(
    std::string const &name, void *data, exchange_type type)
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

void mysql_vector_use_type_backend::pre_use(indicator const *ind)
{
    enum_field_types sqlType;
    unsigned long size(0);

    // Note that data_ is a pointer to C++ data while data returned by
    // prepare_for_bind() is the data to be used by ODBC and doesn't always
    // have the same format.
    void* const data = prepare_for_bind(size, sqlType);

    // first deal with data
    int non_null_indicator = 0;
    switch (type_)
    {
        case x_short:
        case x_integer:
        case x_double:
            // Length of the parameter value is ignored for these types.
            break;

        case x_char:
        case x_stdstring:
        case x_xmltype:
        case x_longstring:
            non_null_indicator = STMT_INDICATOR_NONE;
            break;

        case x_stdtm:
            {
                std::vector<std::tm> *vp
                     = static_cast<std::vector<std::tm> *>(data_);

                std::vector<std::tm> &v(*vp);

                MYSQL_TIME** timePointerArray = reinterpret_cast<MYSQL_TIME**>(buf_);

                std::size_t const vsize = v.size();
                timeVec_.resize(vsize);

                for (std::size_t i = 0; i != vsize; ++i)
                {
                    std::tm t = v[i];

                    MYSQL_TIME& ts = timeVec_[i];

                    ts.year = t.tm_year + 1900;
                    ts.month = t.tm_mon + 1;
                    ts.day = t.tm_mday;
                    ts.hour = t.tm_hour;
                    ts.minute = t.tm_min;
                    ts.second = t.tm_sec;

                    timePointerArray[i] = &timeVec_[i];
                    

                }
            }
            break;

        case x_statement:
        case x_rowid:
        case x_blob:
            // Those are unreachable, we would have thrown from
            // prepare_for_bind() if we we were using one of them, only handle
            // them here to avoid compiler warnings about unhandled enum
            // elements.
            break;
    }

    // then handle indicators
    if (ind != NULL)
    {
        for (std::size_t i = 0; i != indHolderVec_.size(); ++i, ++ind)
        {
            if (*ind == i_null)
            {
                indHolderVec_[i] = STMT_INDICATOR_NULL;
            }
            else
            {
                // for strings we have already set the values
                if (type_ != x_stdstring && type_ != x_xmltype && type_ != x_longstring)
                {
                    indHolderVec_[i] = STMT_INDICATOR_NONE;
                }
            }
        }
    }
    else
    {
        // no indicators - treat all fields as OK
        for (std::size_t i = 0; i != indHolderVec_.size(); ++i)
        {
            // for strings we have already set the values
            if (type_ != x_stdstring && type_ != x_xmltype && type_ != x_longstring)
            {
                indHolderVec_[i] = STMT_INDICATOR_NONE;
            }
        }
    }

    memset(&bindingInfo_, 0, sizeof(MYSQL_BIND));
    bindingInfo_.buffer_type = sqlType;
    bindingInfo_.buffer = data;
    bindingInfo_.buffer_length = size;
    bindingInfo_.is_null = NULL;
    bindingInfo_.error = NULL;
    bindingInfo_.u.indicator = (char*)indHolderVec_.data();
    bindingInfo_.is_unsigned = (type_ == x_unsigned_long_long);

    if (type_ == x_char || type_ == x_stdstring || type_ == x_stdtm)
    {
        bindingInfo_.length = lengths_.data();
    }

    statement_.addParameterBinding(&bindingInfo_);

}

std::size_t mysql_vector_use_type_backend::size()
{
    return get_vector_size(type_, data_);
}

void mysql_vector_use_type_backend::clean_up()
{
    if (buf_ != NULL)
    {
        delete [] buf_;
        buf_ = NULL;
    }
}
