/*
 * JBoss, Home of Professional Open Source.
 * See the COPYRIGHT.txt file distributed with this work for information
 * regarding copyright ownership.  Some portions may be licensed
 * to Red Hat, Inc. under one or more contributor license agreements.
 * 
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 * 
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 * 
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301 USA.
 */

package org.teiid.dialect;

import java.sql.CallableStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;

import org.hibernate.Hibernate;
import org.hibernate.LockMode;
import org.hibernate.dialect.Dialect;
import org.hibernate.dialect.function.NoArgSQLFunction;
import org.hibernate.dialect.function.StandardSQLFunction;
import org.hibernate.dialect.function.VarArgsSQLFunction;

public class TeiidDialect extends Dialect {

    public TeiidDialect() {
        // Register types
        registerColumnType(Types.CHAR, "char"); //$NON-NLS-1$
        registerColumnType(Types.VARCHAR, "string"); //$NON-NLS-1$

        registerColumnType(Types.BIT, "boolean"); //$NON-NLS-1$
        registerColumnType(Types.TINYINT, "byte"); //$NON-NLS-1$
        registerColumnType(Types.SMALLINT, "short"); //$NON-NLS-1$
        registerColumnType(Types.INTEGER, "integer"); //$NON-NLS-1$
        registerColumnType(Types.BIGINT, "long"); //$NON-NLS-1$

        registerColumnType(Types.REAL, "float"); //$NON-NLS-1$
        registerColumnType(Types.FLOAT, "float"); //$NON-NLS-1$
        registerColumnType(Types.DOUBLE, "double"); //$NON-NLS-1$
        registerColumnType(Types.NUMERIC, "bigdecimal"); //$NON-NLS-1$

        registerColumnType(Types.DATE, "date"); //$NON-NLS-1$
        registerColumnType(Types.TIME, "time"); //$NON-NLS-1$
        registerColumnType(Types.TIMESTAMP, "timestamp"); //$NON-NLS-1$

        registerColumnType(Types.BLOB, "blob"); //$NON-NLS-1$
        registerColumnType(Types.VARBINARY, "blob"); //$NON-NLS-1$
        registerColumnType(Types.CLOB, "clob"); //$NON-NLS-1$
        registerColumnType(Types.JAVA_OBJECT, "object"); //$NON-NLS-1$
        
        registerFunction("acos", new StandardSQLFunction("acos", Hibernate.DOUBLE)); //$NON-NLS-1$ //$NON-NLS-2$
        registerFunction("asin", new StandardSQLFunction("asin", Hibernate.DOUBLE)); //$NON-NLS-1$ //$NON-NLS-2$
        registerFunction("atan", new StandardSQLFunction("atan", Hibernate.DOUBLE)); //$NON-NLS-1$ //$NON-NLS-2$
        registerFunction("atan2", new StandardSQLFunction("atan2", Hibernate.DOUBLE)); //$NON-NLS-1$ //$NON-NLS-2$
        registerFunction("ceil", new StandardSQLFunction("ceiling")); //$NON-NLS-1$ //$NON-NLS-2$
        registerFunction("cos", new StandardSQLFunction("cos", Hibernate.DOUBLE)); //$NON-NLS-1$ //$NON-NLS-2$
        registerFunction("cot", new StandardSQLFunction("cot", Hibernate.DOUBLE)); //$NON-NLS-1$ //$NON-NLS-2$
        registerFunction("degrees", new StandardSQLFunction("degrees", Hibernate.DOUBLE)); //$NON-NLS-1$ //$NON-NLS-2$
        registerFunction("exp", new StandardSQLFunction("exp", Hibernate.DOUBLE)); //$NON-NLS-1$ //$NON-NLS-2$
        registerFunction("floor", new StandardSQLFunction("floor")); //$NON-NLS-1$ //$NON-NLS-2$
        registerFunction("formatbigdecimal", new StandardSQLFunction("formatbigdecimal", Hibernate.STRING)); //$NON-NLS-1$ //$NON-NLS-2$
        registerFunction("formatbiginteger", new StandardSQLFunction("formatbiginteger", Hibernate.STRING)); //$NON-NLS-1$ //$NON-NLS-2$
        registerFunction("formatdouble", new StandardSQLFunction("formatdouble", Hibernate.STRING)); //$NON-NLS-1$ //$NON-NLS-2$
        registerFunction("formatfloat", new StandardSQLFunction("formatfloat", Hibernate.STRING)); //$NON-NLS-1$ //$NON-NLS-2$
        registerFunction("formatinteger", new StandardSQLFunction("formatinteger", Hibernate.STRING)); //$NON-NLS-1$ //$NON-NLS-2$
        registerFunction("formatlong", new StandardSQLFunction("formatlong", Hibernate.STRING)); //$NON-NLS-1$ //$NON-NLS-2$
        registerFunction("log", new StandardSQLFunction("log", Hibernate.DOUBLE)); //$NON-NLS-1$ //$NON-NLS-2$
        registerFunction("mod", new StandardSQLFunction("mod")); //$NON-NLS-1$ //$NON-NLS-2$
        registerFunction("parsebigdecimal", new StandardSQLFunction("parsebigdecimal", Hibernate.BIG_DECIMAL)); //$NON-NLS-1$ //$NON-NLS-2$
        registerFunction("parsebiginteger", new StandardSQLFunction("parsebiginteger", Hibernate.BIG_INTEGER)); //$NON-NLS-1$ //$NON-NLS-2$
        registerFunction("parsedouble", new StandardSQLFunction("parsedouble", Hibernate.DOUBLE)); //$NON-NLS-1$ //$NON-NLS-2$
        registerFunction("parsefloat", new StandardSQLFunction("parsefloat", Hibernate.FLOAT)); //$NON-NLS-1$ //$NON-NLS-2$
        registerFunction("parseinteger", new StandardSQLFunction("parseinteger", Hibernate.INTEGER)); //$NON-NLS-1$ //$NON-NLS-2$
        registerFunction("parselong", new StandardSQLFunction("parselong", Hibernate.LONG)); //$NON-NLS-1$ //$NON-NLS-2$
        registerFunction("pi", new StandardSQLFunction("pi", Hibernate.DOUBLE)); //$NON-NLS-1$ //$NON-NLS-2$
        registerFunction("power", new StandardSQLFunction("power", Hibernate.DOUBLE)); //$NON-NLS-1$ //$NON-NLS-2$
        registerFunction("radians", new StandardSQLFunction("radians", Hibernate.DOUBLE)); //$NON-NLS-1$ //$NON-NLS-2$
        registerFunction("round", new StandardSQLFunction("round")); //$NON-NLS-1$ //$NON-NLS-2$
        registerFunction("sign", new StandardSQLFunction("sign", Hibernate.INTEGER)); //$NON-NLS-1$ //$NON-NLS-2$
        registerFunction("sin", new StandardSQLFunction("sin", Hibernate.DOUBLE)); //$NON-NLS-1$ //$NON-NLS-2$
        registerFunction("tan", new StandardSQLFunction("tan", Hibernate.DOUBLE)); //$NON-NLS-1$ //$NON-NLS-2$

        registerFunction("ascii", new StandardSQLFunction("ascii", Hibernate.INTEGER)); //$NON-NLS-1$ //$NON-NLS-2$
        registerFunction("chr", new StandardSQLFunction("chr", Hibernate.CHARACTER)); //$NON-NLS-1$ //$NON-NLS-2$
        registerFunction("char", new StandardSQLFunction("char", Hibernate.CHARACTER)); //$NON-NLS-1$ //$NON-NLS-2$
        registerFunction("concat", new VarArgsSQLFunction(Hibernate.STRING, "", "||", "")); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
        registerFunction("initcap", new StandardSQLFunction("initcap", Hibernate.STRING)); //$NON-NLS-1$ //$NON-NLS-2$
        registerFunction("insert", new StandardSQLFunction("insert", Hibernate.STRING)); //$NON-NLS-1$ //$NON-NLS-2$
        registerFunction("lcase", new StandardSQLFunction("lcase", Hibernate.STRING)); //$NON-NLS-1$ //$NON-NLS-2$
        registerFunction("left", new StandardSQLFunction("left", Hibernate.STRING)); //$NON-NLS-1$ //$NON-NLS-2$
        registerFunction("locate", new StandardSQLFunction("locate", Hibernate.INTEGER)); //$NON-NLS-1$ //$NON-NLS-2$
        registerFunction("lpad", new StandardSQLFunction("lpad", Hibernate.STRING)); //$NON-NLS-1$ //$NON-NLS-2$
        registerFunction("ltrim", new StandardSQLFunction("ltrim", Hibernate.STRING)); //$NON-NLS-1$ //$NON-NLS-2$
        registerFunction("repeat", new StandardSQLFunction("repeat", Hibernate.STRING)); //$NON-NLS-1$ //$NON-NLS-2$
        registerFunction("replace", new StandardSQLFunction("replace", Hibernate.STRING)); //$NON-NLS-1$ //$NON-NLS-2$
        registerFunction("right", new StandardSQLFunction("right", Hibernate.STRING)); //$NON-NLS-1$ //$NON-NLS-2$
        registerFunction("rpad", new StandardSQLFunction("rpad", Hibernate.STRING)); //$NON-NLS-1$ //$NON-NLS-2$
        registerFunction("rtrim", new StandardSQLFunction("rtrim", Hibernate.STRING)); //$NON-NLS-1$ //$NON-NLS-2$
        registerFunction("substring", new StandardSQLFunction("substring", Hibernate.STRING)); //$NON-NLS-1$ //$NON-NLS-2$
        registerFunction("translate", new StandardSQLFunction("translate", Hibernate.STRING)); //$NON-NLS-1$ //$NON-NLS-2$
        registerFunction("ucase", new StandardSQLFunction("ucase", Hibernate.STRING)); //$NON-NLS-1$ //$NON-NLS-2$

        registerFunction("curdate", new NoArgSQLFunction("curdate", Hibernate.DATE)); //$NON-NLS-1$ //$NON-NLS-2$
        registerFunction("curtime", new NoArgSQLFunction("curtime", Hibernate.TIME)); //$NON-NLS-1$ //$NON-NLS-2$
        registerFunction("now", new NoArgSQLFunction("now", Hibernate.TIMESTAMP)); //$NON-NLS-1$ //$NON-NLS-2$
        registerFunction("dayname", new StandardSQLFunction("dayname", Hibernate.STRING)); //$NON-NLS-1$ //$NON-NLS-2$
        registerFunction("dayofmonth", new StandardSQLFunction("dayofmonth", Hibernate.INTEGER)); //$NON-NLS-1$ //$NON-NLS-2$
        registerFunction("dayofweek", new StandardSQLFunction("dayofweek", Hibernate.INTEGER)); //$NON-NLS-1$ //$NON-NLS-2$
        registerFunction("dayofyear", new StandardSQLFunction("dayofyear", Hibernate.INTEGER)); //$NON-NLS-1$ //$NON-NLS-2$
        registerFunction("formatdate", new StandardSQLFunction("formatdate", Hibernate.STRING)); //$NON-NLS-1$ //$NON-NLS-2$
        registerFunction("formattime", new StandardSQLFunction("formattime", Hibernate.STRING)); //$NON-NLS-1$ //$NON-NLS-2$
        registerFunction("formattimestamp", new StandardSQLFunction("formattimestamp", Hibernate.STRING)); //$NON-NLS-1$ //$NON-NLS-2$
        registerFunction("hour", new StandardSQLFunction("hour", Hibernate.INTEGER)); //$NON-NLS-1$ //$NON-NLS-2$
        registerFunction("minute", new StandardSQLFunction("minute", Hibernate.INTEGER)); //$NON-NLS-1$ //$NON-NLS-2$
        registerFunction("monthname", new StandardSQLFunction("monthname", Hibernate.STRING)); //$NON-NLS-1$ //$NON-NLS-2$
        registerFunction("parsedate", new StandardSQLFunction("parsedate", Hibernate.DATE)); //$NON-NLS-1$ //$NON-NLS-2$
        registerFunction("parsetime", new StandardSQLFunction("parsetime", Hibernate.TIME)); //$NON-NLS-1$ //$NON-NLS-2$
        registerFunction("parsetimestamp", new StandardSQLFunction("parsetimestamp", Hibernate.TIMESTAMP)); //$NON-NLS-1$ //$NON-NLS-2$
        registerFunction("second", new StandardSQLFunction("second", Hibernate.INTEGER)); //$NON-NLS-1$ //$NON-NLS-2$
        registerFunction("timestampcreate", new StandardSQLFunction("timestampcreate", Hibernate.TIMESTAMP)); //$NON-NLS-1$ //$NON-NLS-2$
        registerFunction("timestampAdd", new StandardSQLFunction("timestampAdd")); //$NON-NLS-1$ //$NON-NLS-2$
        registerFunction("timestampDiff", new StandardSQLFunction("timestampDiff", Hibernate.LONG)); //$NON-NLS-1$ //$NON-NLS-2$
        registerFunction("week", new StandardSQLFunction("week", Hibernate.INTEGER)); //$NON-NLS-1$ //$NON-NLS-2$
        registerFunction("year", new StandardSQLFunction("year", Hibernate.INTEGER)); //$NON-NLS-1$ //$NON-NLS-2$
        registerFunction("modifytimezone", new StandardSQLFunction("modifytimezone", Hibernate.TIMESTAMP)); //$NON-NLS-1$ //$NON-NLS-2$

        registerFunction("convert", new StandardSQLFunction("convert")); //$NON-NLS-1$ //$NON-NLS-2$
        
        registerFunction("to_bytes", new StandardSQLFunction("to_bytes", Hibernate.BLOB)); //$NON-NLS-1$ //$NON-NLS-2$
        registerFunction("to_chars", new StandardSQLFunction("to_chars", Hibernate.CLOB)); //$NON-NLS-1$ //$NON-NLS-2$
        registerFunction("from_unittime", new StandardSQLFunction("from_unittime", Hibernate.TIMESTAMP)); //$NON-NLS-1$ //$NON-NLS-2$
        registerFunction("session_id", new StandardSQLFunction("session_id", Hibernate.STRING)); //$NON-NLS-1$ //$NON-NLS-2$
        
        registerFunction("uuid", new StandardSQLFunction("uuid", Hibernate.STRING)); //$NON-NLS-1$ //$NON-NLS-2$
        registerFunction("unescape", new StandardSQLFunction("unescape", Hibernate.STRING)); //$NON-NLS-1$ //$NON-NLS-2$
        
        registerFunction("array_get", new StandardSQLFunction("array_get", Hibernate.OBJECT)); //$NON-NLS-1$ //$NON-NLS-2$
        registerFunction("array_length", new StandardSQLFunction("array_length", Hibernate.INTEGER)); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public boolean dropConstraints() {
        return false;
    }

    public boolean hasAlterTable() {
        return false;
    }

    public boolean supportsColumnCheck() {
        return false;
    }

    public boolean supportsCascadeDelete() {
        return false;
    }

    public String getCurrentTimestampSQLFunctionName() {
        return "now"; //$NON-NLS-1$
    }

    public boolean isCurrentTimestampSelectStringCallable() {
        return false;
    }

    public boolean supportsCurrentTimestampSelection() {
        return true;
    }

    public boolean supportsLimit() {
        return true;
    }

    public boolean supportsOuterJoinForUpdate() {
        return false;
    }

    public boolean supportsTableCheck() {
        return false;
    }

    public boolean supportsUnionAll() {
        return true;
    }

    public boolean supportsUnique() {
        return false;
    }

    public String toBooleanValueString(boolean arg0) {
        if (arg0) {
            return "{b'true'}"; //$NON-NLS-1$
        }
        return "{b'false'}"; //$NON-NLS-1$
    }

    /**
     * @see org.hibernate.dialect.Dialect#getLimitString(java.lang.String, boolean)
     */
    public String getLimitString(String querySelect,
                                 boolean hasOffset) {
        return new StringBuffer(querySelect.length() + 20).append(querySelect).append(hasOffset ? " limit ?, ?" : " limit ?") //$NON-NLS-1$ //$NON-NLS-2$
                                                          .toString();
    }

    /**
     * @see org.hibernate.dialect.Dialect#getResultSet(java.sql.CallableStatement)
     */
    public ResultSet getResultSet(CallableStatement ps) throws SQLException {
        boolean isResultSet = ps.execute();
        while (!isResultSet && ps.getUpdateCount() != -1) {
            isResultSet = ps.getMoreResults();
        }
        ResultSet rs = ps.getResultSet();
        return rs;
    }
    
    /** 
     * @see org.hibernate.dialect.Dialect#registerResultSetOutParameter(java.sql.CallableStatement, int)
     */
    public int registerResultSetOutParameter(CallableStatement statement,
                                             int col) throws SQLException {
        return col;
    }

	public String getForUpdateNowaitString() {
		return ""; //$NON-NLS-1$
	}

	public String getForUpdateNowaitString(String aliases) {
		return "";		 //$NON-NLS-1$
	}

	public String getForUpdateString() {
		return ""; //$NON-NLS-1$
	}

	public String getForUpdateString(LockMode lockMode) {
		return ""; //$NON-NLS-1$
	}

	public String getForUpdateString(String aliases) {
		return ""; //$NON-NLS-1$
	}
	
	@Override
	public String getSelectGUIDString() {
		return "select uuid()"; //$NON-NLS-1$
	}
   
}

