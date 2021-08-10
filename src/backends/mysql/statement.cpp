//
// Copyright (C) 2004-2006 Maciej Sobczak, Stephen Hutton, David Courtney
// Distributed under the Boost Software License, Version 1.0.
// (See accompanying file LICENSE_1_0.txt or copy at
// http://www.boost.org/LICENSE_1_0.txt)
//

#define SOCI_MYSQL_SOURCE
#include "soci/mysql/soci-mysql.h"
#include <cctype>
#include <sstream>
#include <cstring>

using namespace soci;
using namespace soci::details;


mysql_statement_backend::mysql_statement_backend(mysql_session_backend& session)
    : session_(session), hstmt_(0), result_(NULL), rowsAffectedBulk_(0),
    numberOfRows_(0), currentRow_(0), rowsToConsume_(0), justDescribed_(false),
    hasIntoElements_(false), hasVectorIntoElements_(false), vectorIntoElementCount_(0),
    hasUseElements_(false), vectorUseElementCount_(0), hasVectorUseElements_(false),
    numRowsFetched_(0), rowsAffected_(-1LL), fetchVectorByRows_(false),
    boundByName_(false), boundByPos_(false), metadata_(NULL)
{
}

void mysql_statement_backend::alloc()
{

    // Allocate environment handle
    hstmt_ = mysql_stmt_init(session_.conn_);

    if (hstmt_ == NULL)
    {
        throw soci_error(std::string("Error allocating statement - ") + mysql_stmt_error(hstmt_));
    }
}

void mysql_statement_backend::clean_up()
{
    rowsAffected_ = -1LL;

    mysql_stmt_close(hstmt_);
}

void mysql_statement_backend::prepare(std::string const & query,
    statement_type /* eType */)
{
    // rewrite the query by transforming all named parameters into
    // the Mysql ? s

    enum { eNormal, eInQuotes, eInName, eInAccessDate } state = eNormal;

    std::string name;
    query_.reserve(query.length());

    for (std::string::const_iterator it = query.begin(), end = query.end();
         it != end; ++it)
    {
        switch (state)
        {
        case eNormal:
            if (*it == '\'')
            {
                query_ += *it;
                state = eInQuotes;
            }
            else if (*it == '#')
            {
                query_ += *it;
                state = eInAccessDate;
            }
            else if (*it == ':')
            {
                state = eInName;
            }
            else // regular character, stay in the same state
            {
                query_ += *it;
            }
            break;
        case eInQuotes:
            if (*it == '\'')
            {
                query_ += *it;
                state = eNormal;
            }
            else // regular quoted character
            {
                query_ += *it;
            }
            break;
        case eInName:
            if (std::isalnum(*it) || *it == '_')
            {
                name += *it;
            }
            else // end of name
            {
                names_.push_back(name);
                name.clear();
                query_ += "?";
                query_ += *it;
                state = eNormal;
            }
            break;
        case eInAccessDate:
            if (*it == '#')
            {
                query_ += *it;
                state = eNormal;
            }
            else // regular quoted character
            {
                query_ += *it;
            }
            break;
        }
    }

    if (state == eInName)
    {
        names_.push_back(name);
        query_ += "?";
    }
    if (mysql_stmt_prepare(hstmt_, query_.c_str(), static_cast<unsigned long>(query_.size())) != 0)
    {
        std::ostringstream ss;
        ss << "preparing query \"" << query_ << "\"";
        throw soci_error(ss.str() + " -" + mysql_stmt_error(hstmt_));
    }


    // reset any old into buffers, they will be added later if they're used
    // with this query
    intos_.clear();
}

statement_backend::exec_fetch_result
mysql_statement_backend::execute(int number)
{
    // Store the number of rows processed by this call.
    unsigned long cursorType = 0;
    if (hasVectorUseElements_)
    {
        if (mysql_stmt_attr_set(hstmt_, STMT_ATTR_ARRAY_SIZE, &vectorUseElementCount_) != 0)
        {
            throw soci_error(std::string("Statement array attribute set failed - ") + mysql_stmt_error(hstmt_));
        }
    }
    else if (hasVectorIntoElements_)
    {
        // Do we want to use a cursor, or will that be slow???
        cursorType = CURSOR_TYPE_READ_ONLY;
        if (mysql_stmt_attr_set(hstmt_, STMT_ATTR_CURSOR_TYPE, &cursorType) != 0)
        {
            throw soci_error(std::string("Statement cursor attribute set failed - ") + mysql_stmt_error(hstmt_));
        }


        if (mysql_stmt_attr_set(hstmt_, STMT_ATTR_PREFETCH_ROWS, &vectorIntoElementCount_) != 0)
        {
            throw soci_error(std::string("Statement array attribute set failed - ") + mysql_stmt_error(hstmt_));
        }

    }

    std::unique_ptr<MYSQL_BIND[]> parameterBindArray;

    if (parameterBindingList_.size() > 0)
    {
        parameterBindArray = std::make_unique<MYSQL_BIND[]>(parameterBindingList_.size());

        for (size_t bindingI = 0; bindingI < parameterBindingList_.size(); bindingI++)
        {
            MYSQL_BIND* bindInfo = parameterBindingList_.at(bindingI);
            memcpy(&(parameterBindArray.get()[bindingI]), bindInfo, sizeof(MYSQL_BIND));
        }

        if (mysql_stmt_bind_param(hstmt_, parameterBindArray.get()) != 0)
        {
            throw soci_error("Parameter binding error");
        }
    }

    std::unique_ptr<MYSQL_BIND[]> resultBindArray;

    if (resultBindingList_.size() > 0)
    {
        resultBindArray = std::make_unique<MYSQL_BIND[]>(resultBindingList_.size());

        for (size_t bindingI = 0; bindingI < resultBindingList_.size(); bindingI++)
        {
            MYSQL_BIND* bindInfo = resultBindingList_.at(bindingI);
            memcpy(&(resultBindArray.get()[bindingI]), bindInfo, sizeof(MYSQL_BIND));
        }

        if (mysql_stmt_bind_result(hstmt_, resultBindArray.get()) != 0)
        {
            throw soci_error(std::string("Parameter binding error - ") + mysql_stmt_error(hstmt_));
        }
    }



    // if we are called twice for the same statement we need to close the open
    // cursor or an "invalid cursor state" error will occur on execute
    //SQLCloseCursor(hstmt_);

    if (mysql_stmt_execute(hstmt_) != 0)
    {
        throw soci_error(std::string("Statement execute failed - ") + mysql_stmt_error(hstmt_));
    }
    else if (hasVectorUseElements_)
    {
        // Get the number of rows affected from the API
        rowsAffected_ = mysql_stmt_affected_rows(hstmt_);
    }
    else // We need to retrieve the number of rows affected explicitly.
    {
        unsigned long long res = mysql_stmt_affected_rows(hstmt_);
        if (res == -1)
        {
            //throw soci_error(std::string("Error getting number of affected rows - ") + mysql_stmt_error(hstmt_));
        }

        rowsAffected_ = res;
    }

    unsigned int colCount = mysql_stmt_field_count(hstmt_);

    if (number > 0 && colCount > 0)
    {
        return fetch(number);
    }

    return ef_success;
}

statement_backend::exec_fetch_result
mysql_statement_backend::do_fetch(int rowNumber)
{
    int rc = mysql_stmt_fetch(hstmt_);

    if (MYSQL_NO_DATA == rc)
    {
        return ef_no_data;
    }

    
    if (rc != 0)
    {
        if (rc == MYSQL_DATA_TRUNCATED)
        {
            throw soci_error(std::string("Error fetching data (truncation) - ") + mysql_stmt_error(hstmt_));
        }
        else
        {
            throw soci_error(std::string("Error fetching data - ") + mysql_stmt_error(hstmt_));
        }
    }

    for (std::size_t j = 0; j != intos_.size(); ++j)
    {
        intos_[j]->do_post_fetch_row(rowNumber);
    }

    return ef_success;
}

statement_backend::exec_fetch_result
mysql_statement_backend::fetch(int number)
{
    numRowsFetched_ = 0;

    for (std::size_t i = 0; i != intos_.size(); ++i)
    {
        intos_[i]->resize(number);
    }

    statement_backend::exec_fetch_result res SOCI_DUMMY_INIT(ef_success);

    // int curNumRowsFetched = 0;
    //SQLSetStmtAttr(hstmt_, SQL_ATTR_ROWS_FETCHED_PTR, &curNumRowsFetched, 0);

    for (int row = 0; row < number; ++row)
    {

        // Unfortunately we need to redefine all vector intos which
        // were bound to the first element of the vector initially.
        //
        // Note that we need to do it even for row == 0 as this might not
        // be the first call to fetch() and so the current bindings might
        // not be the same as initial ones.

        /*for (std::size_t j = 0; j != intos_.size(); ++j)
        {
            intos_[j]->rebind_row(row);
        }*/

        res = do_fetch(row);
        if (res != ef_success)
            break;

        numRowsFetched_ ++;
    }

    return res;
}

long long mysql_statement_backend::get_affected_rows()
{
    return rowsAffected_;
}

int mysql_statement_backend::get_number_of_rows()
{
    return static_cast<int>(numRowsFetched_);
}

std::string mysql_statement_backend::get_parameter_name(int index) const
{
    return names_.at(index);
}

std::string mysql_statement_backend::rewrite_for_procedure_call(
    std::string const &query)
{
    return query;
}

int mysql_statement_backend::prepare_for_describe()
{
    // For efficiency, we get all the fields now, and then return them from the cached
    // list in describe_column below

    metadata_ = mysql_stmt_result_metadata(hstmt_);
    if (metadata_ == NULL)
    {
        throw soci_error(std::string("Error loading statement data - ") + mysql_stmt_error(hstmt_));
    }

    return static_cast<int>(mysql_stmt_field_count(hstmt_));
}

void mysql_statement_backend::describe_column(int colNum, data_type & type,
                                          std::string & columnName)
{
    if (metadata_ == NULL)
    {
        throw soci_error("Internal error - prepare_for_describe not called before describe_column");
    }

    // The values passed in are 1 indexed, but mysql_fetch_field_direct expects a zero indexed value,
    // so -1
    MYSQL_FIELD* field = mysql_fetch_field_direct(metadata_, static_cast<unsigned int>(colNum-1));

    if (field == NULL)
    {
        std::ostringstream ss;
        ss << "getting description of column at position " << colNum;
        throw soci_error(ss.str());
    }

    columnName.assign(field->name, field->name_length);


    switch (field->type)
    {
    case MYSQL_TYPE_DATE:
    case MYSQL_TYPE_TIME:
    case MYSQL_TYPE_TIMESTAMP:
    case MYSQL_TYPE_DATETIME:
        type = dt_date;
        break;
    case MYSQL_TYPE_DOUBLE:
    case MYSQL_TYPE_DECIMAL:
    case MYSQL_TYPE_FLOAT:
        type = dt_double;
        break;
    case MYSQL_TYPE_TINY:
    case MYSQL_TYPE_SHORT:
    case MYSQL_TYPE_LONG:
        type = dt_integer;
        break;
    case MYSQL_TYPE_LONGLONG:
        type = dt_long_long;
        break;
    case MYSQL_TYPE_STRING:
    case MYSQL_TYPE_VARCHAR:
    case MYSQL_TYPE_VAR_STRING:
    default:
        type = dt_string;
        break;
    }
}

std::size_t mysql_statement_backend::column_size(int colNum)
{

    if (metadata_ == NULL)
    {
        prepare_for_describe();
    }

    MYSQL_FIELD* field = mysql_fetch_field_direct(metadata_, static_cast<unsigned int>(colNum));

    if (field == NULL)
    {
        std::ostringstream ss;
        ss << "getting description of column at position " << colNum;
        throw soci_error(ss.str());
    }

    return field->length;
}

mysql_standard_into_type_backend * mysql_statement_backend::make_into_type_backend()
{
    return new mysql_standard_into_type_backend(*this);
}

mysql_standard_use_type_backend * mysql_statement_backend::make_use_type_backend()
{
    hasUseElements_ = true;
    return new mysql_standard_use_type_backend(*this);
}

mysql_vector_into_type_backend *
mysql_statement_backend::make_vector_into_type_backend()
{
    hasVectorIntoElements_ = true;
    return new mysql_vector_into_type_backend(*this);
}

mysql_vector_use_type_backend * mysql_statement_backend::make_vector_use_type_backend()
{
    hasVectorUseElements_ = true;
    return new mysql_vector_use_type_backend(*this);
}
