/*
 * Copyright Red Hat, Inc. and/or its affiliates
 * and other contributors as indicated by the @author tags and
 * the COPYRIGHT.txt file distributed with this work.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.teiid.dialect;

import java.sql.CallableStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;

import org.hibernate.LockMode;
import org.hibernate.boot.TempTableDdlTransactionHandling;
import org.hibernate.dialect.Dialect;
import org.hibernate.dialect.function.NoArgSQLFunction;
import org.hibernate.dialect.function.StandardSQLFunction;
import org.hibernate.dialect.function.VarArgsSQLFunction;
import org.hibernate.engine.jdbc.env.spi.NameQualifierSupport;
import org.hibernate.hql.spi.id.IdTableSupportStandardImpl;
import org.hibernate.hql.spi.id.MultiTableBulkIdStrategy;
import org.hibernate.hql.spi.id.local.AfterUseAction;
import org.hibernate.hql.spi.id.local.LocalTemporaryTableBulkIdStrategy;
import org.hibernate.type.*;

public class TeiidDialect extends Dialect {
    private static DoubleType DOUBLE = DoubleType.INSTANCE;
    private static StringType STRING = StringType.INSTANCE;
    private static BigDecimalType BIG_DECIMAL = BigDecimalType.INSTANCE;
    private static FloatType FLOAT = FloatType.INSTANCE;
    private static IntegerType INTEGER = IntegerType.INSTANCE;
    private static LongType LONG = LongType.INSTANCE;
    private static CharacterType CHARACTER = CharacterType.INSTANCE;
    private static BigIntegerType BIG_INTEGER = BigIntegerType.INSTANCE;
    private static DateType DATE = DateType.INSTANCE;
    private static TimeType TIME = TimeType.INSTANCE;
    private static TimestampType TIMESTAMP = TimestampType.INSTANCE;
    private static BlobType BLOB = BlobType.INSTANCE;
    private static ClobType CLOB = ClobType.INSTANCE;
    private static ObjectType OBJECT = ObjectType.INSTANCE;

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

        registerFunction("acos", new StandardSQLFunction("acos", DOUBLE)); //$NON-NLS-1$ //$NON-NLS-2$
        registerFunction("asin", new StandardSQLFunction("asin", DOUBLE)); //$NON-NLS-1$ //$NON-NLS-2$
        registerFunction("atan", new StandardSQLFunction("atan", DOUBLE)); //$NON-NLS-1$ //$NON-NLS-2$
        registerFunction("atan2", new StandardSQLFunction("atan2", DOUBLE)); //$NON-NLS-1$ //$NON-NLS-2$
        registerFunction("ceil", new StandardSQLFunction("ceiling")); //$NON-NLS-1$ //$NON-NLS-2$
        registerFunction("cos", new StandardSQLFunction("cos", DOUBLE)); //$NON-NLS-1$ //$NON-NLS-2$
        registerFunction("cot", new StandardSQLFunction("cot", DOUBLE)); //$NON-NLS-1$ //$NON-NLS-2$
        registerFunction("degrees", new StandardSQLFunction("degrees", DOUBLE)); //$NON-NLS-1$ //$NON-NLS-2$
        registerFunction("exp", new StandardSQLFunction("exp", DOUBLE)); //$NON-NLS-1$ //$NON-NLS-2$
        registerFunction("floor", new StandardSQLFunction("floor")); //$NON-NLS-1$ //$NON-NLS-2$
        registerFunction("formatbigdecimal", new StandardSQLFunction("formatbigdecimal", STRING)); //$NON-NLS-1$ //$NON-NLS-2$
        registerFunction("formatbiginteger", new StandardSQLFunction("formatbiginteger", STRING)); //$NON-NLS-1$ //$NON-NLS-2$
        registerFunction("formatdouble", new StandardSQLFunction("formatdouble", STRING)); //$NON-NLS-1$ //$NON-NLS-2$
        registerFunction("formatfloat", new StandardSQLFunction("formatfloat", STRING)); //$NON-NLS-1$ //$NON-NLS-2$
        registerFunction("formatinteger", new StandardSQLFunction("formatinteger", STRING)); //$NON-NLS-1$ //$NON-NLS-2$
        registerFunction("formatlong", new StandardSQLFunction("formatlong", STRING)); //$NON-NLS-1$ //$NON-NLS-2$
        registerFunction("log", new StandardSQLFunction("log", DOUBLE)); //$NON-NLS-1$ //$NON-NLS-2$
        registerFunction("mod", new StandardSQLFunction("mod")); //$NON-NLS-1$ //$NON-NLS-2$
        registerFunction("parsebigdecimal", new StandardSQLFunction("parsebigdecimal", BIG_DECIMAL)); //$NON-NLS-1$ //$NON-NLS-2$
        registerFunction("parsebiginteger", new StandardSQLFunction("parsebiginteger", BIG_INTEGER)); //$NON-NLS-1$ //$NON-NLS-2$
        registerFunction("parsedouble", new StandardSQLFunction("parsedouble", DOUBLE)); //$NON-NLS-1$ //$NON-NLS-2$
        registerFunction("parsefloat", new StandardSQLFunction("parsefloat", FLOAT)); //$NON-NLS-1$ //$NON-NLS-2$
        registerFunction("parseinteger", new StandardSQLFunction("parseinteger", INTEGER)); //$NON-NLS-1$ //$NON-NLS-2$
        registerFunction("parselong", new StandardSQLFunction("parselong", LONG)); //$NON-NLS-1$ //$NON-NLS-2$
        registerFunction("pi", new StandardSQLFunction("pi", DOUBLE)); //$NON-NLS-1$ //$NON-NLS-2$
        registerFunction("power", new StandardSQLFunction("power", DOUBLE)); //$NON-NLS-1$ //$NON-NLS-2$
        registerFunction("radians", new StandardSQLFunction("radians", DOUBLE)); //$NON-NLS-1$ //$NON-NLS-2$
        registerFunction("round", new StandardSQLFunction("round")); //$NON-NLS-1$ //$NON-NLS-2$
        registerFunction("sign", new StandardSQLFunction("sign", INTEGER)); //$NON-NLS-1$ //$NON-NLS-2$
        registerFunction("sin", new StandardSQLFunction("sin", DOUBLE)); //$NON-NLS-1$ //$NON-NLS-2$
        registerFunction("tan", new StandardSQLFunction("tan", DOUBLE)); //$NON-NLS-1$ //$NON-NLS-2$

        registerFunction("ascii", new StandardSQLFunction("ascii", INTEGER)); //$NON-NLS-1$ //$NON-NLS-2$
        registerFunction("chr", new StandardSQLFunction("chr", CHARACTER)); //$NON-NLS-1$ //$NON-NLS-2$
        registerFunction("char", new StandardSQLFunction("char", CHARACTER)); //$NON-NLS-1$ //$NON-NLS-2$
        registerFunction("concat", new VarArgsSQLFunction(STRING, "", "||", "")); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
        registerFunction("initcap", new StandardSQLFunction("initcap", STRING)); //$NON-NLS-1$ //$NON-NLS-2$
        registerFunction("insert", new StandardSQLFunction("insert", STRING)); //$NON-NLS-1$ //$NON-NLS-2$
        registerFunction("lcase", new StandardSQLFunction("lcase", STRING)); //$NON-NLS-1$ //$NON-NLS-2$
        registerFunction("left", new StandardSQLFunction("left", STRING)); //$NON-NLS-1$ //$NON-NLS-2$
        registerFunction("locate", new StandardSQLFunction("locate", INTEGER)); //$NON-NLS-1$ //$NON-NLS-2$
        registerFunction("lpad", new StandardSQLFunction("lpad", STRING)); //$NON-NLS-1$ //$NON-NLS-2$
        registerFunction("ltrim", new StandardSQLFunction("ltrim", STRING)); //$NON-NLS-1$ //$NON-NLS-2$
        registerFunction("repeat", new StandardSQLFunction("repeat", STRING)); //$NON-NLS-1$ //$NON-NLS-2$
        registerFunction("replace", new StandardSQLFunction("replace", STRING)); //$NON-NLS-1$ //$NON-NLS-2$
        registerFunction("right", new StandardSQLFunction("right", STRING)); //$NON-NLS-1$ //$NON-NLS-2$
        registerFunction("rpad", new StandardSQLFunction("rpad", STRING)); //$NON-NLS-1$ //$NON-NLS-2$
        registerFunction("rtrim", new StandardSQLFunction("rtrim", STRING)); //$NON-NLS-1$ //$NON-NLS-2$
        registerFunction("substring", new StandardSQLFunction("substring", STRING)); //$NON-NLS-1$ //$NON-NLS-2$
        registerFunction("translate", new StandardSQLFunction("translate", STRING)); //$NON-NLS-1$ //$NON-NLS-2$
        registerFunction("ucase", new StandardSQLFunction("ucase", STRING)); //$NON-NLS-1$ //$NON-NLS-2$

        registerFunction("curdate", new NoArgSQLFunction("curdate", DATE)); //$NON-NLS-1$ //$NON-NLS-2$
        registerFunction("curtime", new NoArgSQLFunction("curtime", TIME)); //$NON-NLS-1$ //$NON-NLS-2$
        registerFunction("now", new NoArgSQLFunction("now", TIMESTAMP)); //$NON-NLS-1$ //$NON-NLS-2$
        registerFunction("dayname", new StandardSQLFunction("dayname", STRING)); //$NON-NLS-1$ //$NON-NLS-2$
        registerFunction("dayofmonth", new StandardSQLFunction("dayofmonth", INTEGER)); //$NON-NLS-1$ //$NON-NLS-2$
        registerFunction("dayofweek", new StandardSQLFunction("dayofweek", INTEGER)); //$NON-NLS-1$ //$NON-NLS-2$
        registerFunction("dayofyear", new StandardSQLFunction("dayofyear", INTEGER)); //$NON-NLS-1$ //$NON-NLS-2$
        registerFunction("formatdate", new StandardSQLFunction("formatdate", STRING)); //$NON-NLS-1$ //$NON-NLS-2$
        registerFunction("formattime", new StandardSQLFunction("formattime", STRING)); //$NON-NLS-1$ //$NON-NLS-2$
        registerFunction("formattimestamp", new StandardSQLFunction("formattimestamp", STRING)); //$NON-NLS-1$ //$NON-NLS-2$
        registerFunction("hour", new StandardSQLFunction("hour", INTEGER)); //$NON-NLS-1$ //$NON-NLS-2$
        registerFunction("minute", new StandardSQLFunction("minute", INTEGER)); //$NON-NLS-1$ //$NON-NLS-2$
        registerFunction("monthname", new StandardSQLFunction("monthname", STRING)); //$NON-NLS-1$ //$NON-NLS-2$
        registerFunction("parsedate", new StandardSQLFunction("parsedate", DATE)); //$NON-NLS-1$ //$NON-NLS-2$
        registerFunction("parsetime", new StandardSQLFunction("parsetime", TIME)); //$NON-NLS-1$ //$NON-NLS-2$
        registerFunction("parsetimestamp", new StandardSQLFunction("parsetimestamp", TIMESTAMP)); //$NON-NLS-1$ //$NON-NLS-2$
        registerFunction("second", new StandardSQLFunction("second", INTEGER)); //$NON-NLS-1$ //$NON-NLS-2$
        registerFunction("timestampcreate", new StandardSQLFunction("timestampcreate", TIMESTAMP)); //$NON-NLS-1$ //$NON-NLS-2$
        registerFunction("timestampAdd", new StandardSQLFunction("timestampAdd")); //$NON-NLS-1$ //$NON-NLS-2$
        registerFunction("timestampDiff", new StandardSQLFunction("timestampDiff", LONG)); //$NON-NLS-1$ //$NON-NLS-2$
        registerFunction("week", new StandardSQLFunction("week", INTEGER)); //$NON-NLS-1$ //$NON-NLS-2$
        registerFunction("year", new StandardSQLFunction("year", INTEGER)); //$NON-NLS-1$ //$NON-NLS-2$
        registerFunction("modifytimezone", new StandardSQLFunction("modifytimezone", TIMESTAMP)); //$NON-NLS-1$ //$NON-NLS-2$

        registerFunction("convert", new StandardSQLFunction("convert")); //$NON-NLS-1$ //$NON-NLS-2$

        registerFunction("to_bytes", new StandardSQLFunction("to_bytes", BLOB)); //$NON-NLS-1$ //$NON-NLS-2$
        registerFunction("to_chars", new StandardSQLFunction("to_chars", CLOB)); //$NON-NLS-1$ //$NON-NLS-2$
        registerFunction("from_unittime", new StandardSQLFunction("from_unittime", TIMESTAMP)); //$NON-NLS-1$ //$NON-NLS-2$
        registerFunction("session_id", new StandardSQLFunction("session_id", STRING)); //$NON-NLS-1$ //$NON-NLS-2$

        registerFunction("uuid", new StandardSQLFunction("uuid", STRING)); //$NON-NLS-1$ //$NON-NLS-2$
        registerFunction("unescape", new StandardSQLFunction("unescape", STRING)); //$NON-NLS-1$ //$NON-NLS-2$

        registerFunction("array_get", new StandardSQLFunction("array_get", OBJECT)); //$NON-NLS-1$ //$NON-NLS-2$
        registerFunction("array_length", new StandardSQLFunction("array_length", INTEGER)); //$NON-NLS-1$ //$NON-NLS-2$
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
        return "";         //$NON-NLS-1$
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

    @Override
    public boolean supportsSequences() {
        return true;
    }

    @Override
    public boolean supportsPooledSequences() {
        return true;
    }

    @Override
    public String getSequenceNextValString(String sequenceName) {
        return "select " + getSelectSequenceNextValString( sequenceName );
    }

    @Override
    public String getSelectSequenceNextValString(String sequenceName) {
        return sequenceName + "_nextval()";
    }

    public MultiTableBulkIdStrategy getDefaultMultiTableBulkIdStrategy() {
        return new LocalTemporaryTableBulkIdStrategy(
            new IdTableSupportStandardImpl() {
                @Override
                public String getCreateIdTableCommand() {
                    return "create local temporary table";
                }
                @Override
                public String getDropIdTableCommand() {
                    return "drop table";
                }
            },
            AfterUseAction.DROP,
            TempTableDdlTransactionHandling.NONE
        );
    }

    @Override
    public NameQualifierSupport getNameQualifierSupport() {
        return NameQualifierSupport.SCHEMA;
    }
}

