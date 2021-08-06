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
    : session_(session), hstmt_(0), numRowsFetched_(0), fetchVectorByRows_(false),
    hasVectorUseElements_(false), boundByName_(false), boundByPos_(false),
    rowsAffected_(-1LL), metadata_(NULL), hasUseElements_(false), hasIntoElements_(false)
{
}

void mysql_statement_backend::alloc()
{

    // Allocate environment handle
    hstmt_ = mysql_stmt_init(session_.conn_);

    if (hstmt_ == NULL)
    {
        throw soci_error(std::string("Error allocating statement - ") + mysql_error(session_.conn_));
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
        throw soci_error(ss.str() + " -" + mysql_error(session_.conn_));
    }


    // reset any old into buffers, they will be added later if they're used
    // with this query
    intos_.clear();
}

statement_backend::exec_fetch_result
mysql_statement_backend::execute(int number)
{
    // Store the number of rows processed by this call.
    unsigned long long rows_processed = 0;
    if (hasVectorUseElements_)
    {
        mysql_stmt_attr_set(hstmt_, STMT_ATTR_ARRAY_SIZE, &rows_processed);
    }
    else if (hasUseElements_)
    {
        rows_processed = 1;
        mysql_stmt_attr_set(hstmt_, STMT_ATTR_ARRAY_SIZE, &rows_processed);
    }

    std::unique_ptr<MYSQL_BIND[]> bindArray;

    if (hasUseElements_)
    {
        bindArray = std::make_unique<MYSQL_BIND[]>(bindingInfoList_.size());

        for (size_t bindingI = 0; bindingI < bindingInfoList_.size(); bindingI++)
        {
            MYSQL_BIND* bindInfo = bindingInfoList_.at(bindingI);
            memcpy(&(bindArray.get()[bindingI]), bindInfo, sizeof(MYSQL_BIND));
        }



        mysql_stmt_bind_param(hstmt_, bindArray.get());


    }

    // if we are called twice for the same statement we need to close the open
    // cursor or an "invalid cursor state" error will occur on execute
    //SQLCloseCursor(hstmt_);

    if (mysql_stmt_execute(hstmt_) != 0)
    {

        throw soci_error(std::string("Statement execute failed - ") + mysql_error(session_.conn_));
    }
    else if (hasVectorUseElements_)
    {
        // We already have the number of rows, no need to do anything.
        rowsAffected_ = rows_processed;
    }
    else // We need to retrieve the number of rows affected explicitly.
    {
        unsigned long long res = mysql_stmt_affected_rows(hstmt_);
        if (res == -1)
        {
            throw soci_error(std::string("Error getting number of affected rows - ") + mysql_error(session_.conn_));
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
mysql_statement_backend::do_fetch(int beginRow, int endRow)
{
    int rc = mysql_stmt_fetch(hstmt_);

    if (MYSQL_NO_DATA == rc)
    {
        return ef_no_data;
    }

    if (rc == 1)
    {
        throw soci_error(std::string("Error fetching data - ") + mysql_error(session_.conn_));
    }

    for (std::size_t j = 0; j != intos_.size(); ++j)
    {
        intos_[j]->do_post_fetch_rows(beginRow, endRow);
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

    // Is there a MySQL equivalent of this??
    // SQLSetStmtAttr(hstmt_, SQL_ATTR_ROW_BIND_TYPE, SQL_BIND_BY_COLUMN, 0);

    statement_backend::exec_fetch_result res SOCI_DUMMY_INIT(ef_success);

    // Usually we try to fetch the entire vector at once, but if some into
    // string columns are bigger than 8KB (ODBC_MAX_COL_SIZE) then we use
    // 100MB buffer for that columns. So in this case we downgrade to using
    // scalar fetches to hold the buffer only for a single row and not
    // rows_count * 100MB.
    // See mysql_vector_into_type_backend::define_by_pos().
    if (!fetchVectorByRows_)
    {
        // OK, I don't know what to do here yet
        /*
        SQLULEN row_array_size = static_cast<SQLULEN>(number);
        SQLSetStmtAttr(hstmt_, SQL_ATTR_ROW_ARRAY_SIZE, (SQLPOINTER)row_array_size, 0);

        SQLSetStmtAttr(hstmt_, SQL_ATTR_ROWS_FETCHED_PTR, &numRowsFetched_, 0);
        */
        res = do_fetch(0, number);
    }
    else // Use multiple calls to SQLFetch().
    {
        // OK, I don't know what to do here yet
        throw soci_error("Not implemented");
        /*
        SQLULEN curNumRowsFetched = 0;
        SQLSetStmtAttr(hstmt_, SQL_ATTR_ROWS_FETCHED_PTR, &curNumRowsFetched, 0);

        for (int row = 0; row < number; ++row)
        {
            // Unfortunately we need to redefine all vector intos which
            // were bound to the first element of the vector initially.
            //
            // Note that we need to do it even for row == 0 as this might not
            // be the first call to fetch() and so the current bindings might
            // not be the same as initial ones.
            for (std::size_t j = 0; j != intos_.size(); ++j)
            {
                intos_[j]->rebind_row(row);
            }

            res = do_fetch(row, row + 1);
            if (res != ef_success)
                break;

            numRowsFetched_ += curNumRowsFetched;
        }*/
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

    return static_cast<int>(mysql_stmt_field_count(hstmt_));
}

void mysql_statement_backend::describe_column(int colNum, data_type & type,
                                          std::string & columnName)
{
    if (metadata_ == NULL)
    {
        throw soci_error("Internal error - prepare_for_describe not called before describe_column");
    }

    MYSQL_FIELD* field = mysql_fetch_field_direct(metadata_, static_cast<unsigned int>(colNum));

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
        throw soci_error("Internal error - prepare_for_describe not called before column_size");
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
    return new mysql_vector_into_type_backend(*this);
}

mysql_vector_use_type_backend * mysql_statement_backend::make_vector_use_type_backend()
{
    hasVectorUseElements_ = true;
    return new mysql_vector_use_type_backend(*this);
}
