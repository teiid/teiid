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

package com.metamatrix.jdbc;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
//## JDBC4.0-begin ##
import java.sql.RowIdLifetime;
//## JDBC4.0-end ##
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import org.teiid.jdbc.TeiidDriver;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.api.exception.query.QueryMetadataException;
import com.metamatrix.common.types.DataTypeManager;
import com.metamatrix.common.types.MMJDBCSQLTypeInfo;
import com.metamatrix.common.util.SqlUtil;
import com.metamatrix.core.MetaMatrixRuntimeException;
import com.metamatrix.dqp.message.ResultsMessage;
import com.metamatrix.dqp.metadata.ResultsMetadataConstants;
import com.metamatrix.dqp.metadata.ResultsMetadataDefaults;

/**
 * <p>This provides metadata information about the different datasources to which
 * metamatrix connects. Many of the methods in this class return ResultSets, Strings
 * and boolean values. This class makes queries against <code>RuntimeMetadata</code>
 * to support methods returning ResultSets. The methods on this class which
 * return ResultSets need to return JDBC specific columns, whose information is not
 * available in the results obtained on querying, this JDBC specific information is
 * in this class before returning ResultSets. All the information returned
 * by methods returning String,booleans and int.</p>
 */

public class MMDatabaseMetaData extends WrapperImpl implements com.metamatrix.jdbc.api.DatabaseMetaData {
	private static Logger logger = Logger.getLogger("org.teiid.jdbc"); //$NON-NLS-1$
        
    /** CONSTANTS */
    private static final String PERCENT = "%"; //$NON-NLS-1$    
    // constant value indicating that there is not limit
    private final static int NO_LIMIT = 0;
    // constant value giving metamatrix preferred name for a schema
    private final static String SCHEMA_TERM = "VirtualDatabase"; //$NON-NLS-1$
    // constant value giving an empty string value
    private final static String EMPTY_STRING = ""; //$NON-NLS-1$
    // constant value giving a string used to escape search strings
    private final static String ESCAPE_SEARCH_STRING = "\\"; //$NON-NLS-1$
    // constant value giving an identifier quoting char
    //private final static String SINGLE_QUOTE = "\'";
    // constant value giving an identifier quoting string
    private final static String DOUBLE_QUOTE = "\""; //$NON-NLS-1$
    // constant value giving extra name charchters used in Identifiers
    private final static String EXTRA_CHARS = ".@"; //$NON-NLS-1$
    // constant value giving the key words used in metamatrix not in SQL-92
    private final static String KEY_WORDS = "OPTION, SHOWPLAN, DEBUG"; //$NON-NLS-1$
    // constant value giving metamatrix preferred name for a procedure
    private final static String PROCEDURE_TERM = "StoredProcedure"; //$NON-NLS-1$
    // constant value giving the names of numeric functions supported
    final static String NUMERIC_FUNCTIONS =
        "ABS, ACOS, ASIN, ATAN, ATAN2, BITAND, BITNOT, BITOR, BITXOR, CEILING" //$NON-NLS-1$
        + ", COS, COT, DEGREES, EXP, FLOOR, FORMATBIGDECIMAL, FORMATBIGINTEGER" //$NON-NLS-1$
        + ", FORMATDOUBLE, FORMATFLOAT, FORMATINTEGER, FORMATLONG, LOG, LOG10" //$NON-NLS-1$
        + ", MOD, PARSEBIGDECIMAL, PARSEBIGINTEGER, PARSEDOUBLE, PARSEFLOAT" //$NON-NLS-1$
        + ", PARSEINTEGER, PARSELONG, PI, POWER, RADIANS, RAND, ROUND, SIGN, SIN, SQRT, TAN"; //$NON-NLS-1$
    // constant value giving the names of string functions supported
    final static String STRING_FUNCTIONS =
        "ASCII, CHR, CHAR, CONCAT, CONCAT2, INITCAP, INSERT, LCASE, LEFT, LENGTH, LOCATE, LOWER, LPAD, LTRIM, " + //$NON-NLS-1$
        "REPEAT, REPLACE, RIGHT, RPAD, RTRIM, SUBSTRING, TRANSLATE, UCASE, UPPER"; //$NON-NLS-1$
    // constant value giving the names of date/time functions supported
    final static String DATE_FUNCTIONS =
        "CURDATE, CURTIME, NOW, DAYNAME, DAYOFMONTH, DAYOFWEEK, DAYOFYEAR, FORMATDATE, " + //$NON-NLS-1$
        "FORMATTIME, FORMATTIMESTAMP, FROM_UNIXTIME, HOUR, MINUTE, MONTH, MONTHNAME, PARSEDATE, PARSETIME, " + //$NON-NLS-1$
        "PARSETIMESTAMP, QUARTER, SECOND, TIMESTAMPADD, TIMESTAMPDIFF, WEEK, YEAR"; //$NON-NLS-1$
    // constant value giving the names of system functions supported
    final static String SYSTEM_FUNCTIONS =
        "CAST, COALESCE, CONVERT, DECODESTRING, DECODEINTEGER, IFNULL, NULLIF, NVL, LOOKUP"; //$NON-NLS-1$
    // constant value giving max length of a catalog name allowed in MetaMatrix
    private final static int MAX_CATALOG_NAME_LENGTH = 0;
    // constant value giving max length of a procedure name allowed in MetaMatrix
    private final static int MAX_PROCEDURE_NAME_LENGTH = 255;
    // constant value giving max length of a table name allowed in MetaMatrix
    private final static int MAX_TABLE_NAME_LENGTH = 255;
    // constant value giving max length of a column name allowed in MetaMatrix
    private final static int MAX_COLUMN_NAME_LENGTH = 255;
    // constant value giving max length of a user name allowed in MetaMatrix
    private final static int MAX_USER_NAME_LENGTH = 255;
    // constant value giving min value of a columns scale
    //private final static short MIN_SCALE = 0;
    // constant value giving max value of a columns scale
    //private final static short MAX_SCALE = 256;
    
    private final static String LIKE_ESCAPE = " LIKE ? ESCAPE '" + ESCAPE_SEARCH_STRING + "' ";//$NON-NLS-1$//$NON-NLS-2$
    
    final private static class RUNTIME_MODEL{
        public final static String VIRTUAL_MODEL_NAME = "System"; //$NON-NLS-1$
        public final static String ODBC_SYSTEM_MODEL_NAME = "System.ODBC"; //$NON-NLS-1$
        public final static String WSDL_SYSTEM_MODEL_NAME = "DataServiceSystemModel"; //$NON-NLS-1$
        public final static String JDBC_SYSTEM_MODEL_NAME = "System.JDBC"; //$NON-NLS-1$
    }

    private static final String TYPE_MAPPING;
    
    private static final String PRECISION_MAPPING;

    static {
        String[] internalTypes = MMJDBCSQLTypeInfo.getMMTypeNames();
        StringBuffer typeMapping = new StringBuffer();
        StringBuffer precisionMapping = new StringBuffer();
        for (int i = 0; i < internalTypes.length; i++) {
            if (i != 0) {
                typeMapping.append(","); //$NON-NLS-1$
                precisionMapping.append(","); //$NON-NLS-1$
            }
            typeMapping.append(internalTypes[i]).append(",").append(MMJDBCSQLTypeInfo.getSQLType(internalTypes[i])); //$NON-NLS-1$
            precisionMapping.append(internalTypes[i]).append(",").append(ResultsMetadataDefaults.getDefaultPrecision(internalTypes[i])); //$NON-NLS-1$
        }
        TYPE_MAPPING = typeMapping.toString();
        PRECISION_MAPPING = precisionMapping.toString();
    }

    private static final String NULLABILITY_MAPPING =
        new StringBuffer("No Nulls, ").append(DatabaseMetaData.columnNoNulls) //$NON-NLS-1$
          .append(     ", Nullable, ").append(DatabaseMetaData.columnNullable) //$NON-NLS-1$
          .append(     ", Unknown, ") .append(DatabaseMetaData.columnNullableUnknown) //$NON-NLS-1$
          .toString();
      
    private static final String PROC_COLUMN_NULLABILITY_MAPPING =
        new StringBuffer("No Nulls, ").append(DatabaseMetaData.procedureNoNulls) //$NON-NLS-1$
          .append(     ", Nullable, ").append(DatabaseMetaData.procedureNullable) //$NON-NLS-1$
          .append(     ", Unknown, ") .append(DatabaseMetaData.procedureNullableUnknown) //$NON-NLS-1$
          .toString();
      
    private static final String PARAM_DIRECTION_MAPPING = 
      new StringBuffer("In,")         .append(DatabaseMetaData.procedureColumnIn) //$NON-NLS-1$
      .append(       ", Out,")        .append(DatabaseMetaData.procedureColumnOut) //$NON-NLS-1$
      .append(       ", InOut,")      .append(DatabaseMetaData.procedureColumnInOut) //$NON-NLS-1$
      .append(       ", ReturnValue,").append(DatabaseMetaData.procedureColumnReturn) //$NON-NLS-1$
      .append(       ", ResultSet,")  .append(DatabaseMetaData.procedureColumnResult) //$NON-NLS-1$
      .toString();
    
//    private static final String UDT_NAME_MAPPING =
//      new StringBuffer("JAVA_OBJECT, ").append(Types.JAVA_OBJECT) //$NON-NLS-1$
//        .append(     ", DISTINCT, ")   .append(Types.DISTINCT) //$NON-NLS-1$
//        .append(     ", STRUCT, ")     .append(Types.STRUCT) //$NON-NLS-1$
//        .append(     ", null, ")       .append(Types.JAVA_OBJECT).toString(); //$NON-NLS-1$
    
    private static final String DATATYPES_WITH_NO_PRECISION =
      new StringBuffer("'").append(DataTypeManager.DefaultDataTypes.STRING).append("', '") //$NON-NLS-1$ //$NON-NLS-2$
        .append(DataTypeManager.DefaultDataTypes.CHAR).append("', '") //$NON-NLS-1$
        .append(DataTypeManager.DefaultDataTypes.CLOB).append("', '") //$NON-NLS-1$
        .append(DataTypeManager.DefaultDataTypes.BLOB).append("', '") //$NON-NLS-1$
        .append(DataTypeManager.DefaultDataTypes.BOOLEAN).append("', '") //$NON-NLS-1$
        .append(DataTypeManager.DefaultDataTypes.DATE).append("', '") //$NON-NLS-1$
        .append(DataTypeManager.DefaultDataTypes.TIME).append("', '") //$NON-NLS-1$
        .append(DataTypeManager.DefaultDataTypes.TIMESTAMP).append("', '") //$NON-NLS-1$
        .append(DataTypeManager.DefaultDataTypes.OBJECT).append("'").toString(); //$NON-NLS-1$

    // Queries
    private final static String QUERY_REFERENCE_KEYS =
      new StringBuffer("SELECT PKTABLE_CAT, PKTABLE_SCHEM, PKTABLE_NAME, PKCOLUMN_NAME, FKTABLE_CAT, FKTABLE_SCHEM") //$NON-NLS-1$
        .append(", FKTABLE_NAME, FKCOLUMN_NAME, KEY_SEQ, UPDATE_RULE, DELETE_RULE, FK_NAME, PK_NAME, DEFERRABILITY FROM ") //$NON-NLS-1$
        .append(RUNTIME_MODEL.JDBC_SYSTEM_MODEL_NAME).append(".ReferenceKeyColumns").toString(); //$NON-NLS-1$
    
    private final static String QUERY_CROSS_REFERENCES = new StringBuffer(QUERY_REFERENCE_KEYS)
    	.append(",").append(RUNTIME_MODEL.VIRTUAL_MODEL_NAME).append(".VirtualDatabases v WHERE UCASE(v.Name)").append(LIKE_ESCAPE).append("AND UCASE(v.Name) LIKE ?") //$NON-NLS-1$ //$NON-NLS-2$//$NON-NLS-3$
    	.append(" AND UCASE(PKTABLE_SCHEM) = UCASE(v.Name) AND ") //$NON-NLS-1$
        .append(" UCASE(PKTABLE_NAME)").append(LIKE_ESCAPE).append("AND UCASE(FKTABLE_NAME)").append(LIKE_ESCAPE).append("ORDER BY FKTABLE_NAME, KEY_SEQ").toString(); //$NON-NLS-1$//$NON-NLS-2$//$NON-NLS-3$
    
    private final static String QUERY_EXPORTED_KEYS = new StringBuffer(QUERY_REFERENCE_KEYS)
    	.append(",").append(RUNTIME_MODEL.VIRTUAL_MODEL_NAME).append(".VirtualDatabases v WHERE UCASE(v.Name)").append(LIKE_ESCAPE) //$NON-NLS-1$ //$NON-NLS-2$
    	.append(" AND UCASE(PKTABLE_SCHEM) = UCASE(v.Name) AND ") //$NON-NLS-1$
        .append(" UCASE(PKTABLE_NAME)").append(LIKE_ESCAPE).append("ORDER BY FKTABLE_NAME, KEY_SEQ").toString(); //$NON-NLS-1$//$NON-NLS-2$
    
    private final static String QUERY_IMPORTED_KEYS = new StringBuffer(QUERY_REFERENCE_KEYS)
    	.append(",").append(RUNTIME_MODEL.VIRTUAL_MODEL_NAME).append(".VirtualDatabases v WHERE UCASE(v.Name)").append(LIKE_ESCAPE) //$NON-NLS-1$ //$NON-NLS-2$
    	.append(" AND UCASE(PKTABLE_SCHEM) = UCASE(v.Name) AND ") //$NON-NLS-1$    
        .append(" UCASE(FKTABLE_NAME)").append(LIKE_ESCAPE).append("ORDER BY PKTABLE_NAME, KEY_SEQ").toString(); //$NON-NLS-1$//$NON-NLS-2$
    
    private final static String QUERY_COLUMNS = new StringBuffer("SELECT NULL AS TABLE_CAT") //$NON-NLS-1$
        .append(", v.Name AS TABLE_SCHEM, GroupFullName AS TABLE_NAME, e.Name AS COLUMN_NAME") //$NON-NLS-1$
        .append(", convert(decodeString(DataType, '").append(TYPE_MAPPING).append("', ','), short) AS DATA_TYPE") //$NON-NLS-1$ //$NON-NLS-2$
        .append(", DataType AS TYPE_NAME") //$NON-NLS-1$
        .append(", CASE WHEN (DataType IN (").append(DATATYPES_WITH_NO_PRECISION) //$NON-NLS-1$
        .append(")) THEN CASE WHEN ElementLength <= 0 THEN convert(decodeString(DataType,'").append(PRECISION_MAPPING) //$NON-NLS-1$
        .append("',','), integer) ELSE ElementLength END ELSE CASE WHEN Precision <= 0 THEN convert(decodeString(DataType,'") //$NON-NLS-1$
        .append(PRECISION_MAPPING).append("',','), integer) ELSE Precision END END AS COLUMN_SIZE") //$NON-NLS-1$
        .append(", NULL AS BUFFER_LENGTH, Scale AS DECIMAL_DIGITS, Radix AS NUM_PREC_RADIX") //$NON-NLS-1$
        .append(", convert(decodeString(NullType, '").append(NULLABILITY_MAPPING).append("', ','), integer) AS NULLABLE") //$NON-NLS-1$ //$NON-NLS-2$
        .append(", Description AS REMARKS, DefaultValue AS COLUMN_DEF, NULL AS SQL_DATA_TYPE, NULL AS SQL_DATETIME_SUB") //$NON-NLS-1$
        .append(", CharOctetLength AS CHAR_OCTET_LENGTH, Position AS ORDINAL_POSITION") //$NON-NLS-1$
        .append(", decodeString(NullType, 'No Nulls, YES, Nullable, NO, Unknown, '' ''', ',') AS IS_NULLABLE") //$NON-NLS-1$
    	.append(", NULL AS SCOPE_CATALOG, NULL AS SCOPE_SCHEMA, NULL AS SCOPE_TABLE, NULL AS SOURCE_DATA_TYPE, CASE WHEN e.IsAutoIncremented = 'true' THEN 'YES' ELSE 'NO' END AS IS_AUTOINCREMENT") //$NON-NLS-1$
        .append(" FROM ").append(RUNTIME_MODEL.VIRTUAL_MODEL_NAME) //$NON-NLS-1$
        .append(".Elements e CROSS JOIN ").append(RUNTIME_MODEL.VIRTUAL_MODEL_NAME).append(".VirtualDatabases v") //$NON-NLS-1$ //$NON-NLS-2$
        .append(" WHERE UCASE(v.Name)").append(LIKE_ESCAPE)//$NON-NLS-1$
        .append("AND UCASE(GroupFullName)") .append(LIKE_ESCAPE) //$NON-NLS-1$
        .append("AND UCASE(e.Name)").append(LIKE_ESCAPE) //$NON-NLS-1$
        .append(" ORDER BY TABLE_NAME, ORDINAL_POSITION").toString(); //$NON-NLS-1$

    private static final String QUERY_INDEX_INFO =
      new StringBuffer("SELECT NULL AS TABLE_CAT, v.Name AS TABLE_SCHEM, GroupFullName AS TABLE_NAME") //$NON-NLS-1$
        .append(", convert(0, boolean) AS NON_UNIQUE, NULL AS INDEX_QUALIFIER, KeyName AS INDEX_NAME") //$NON-NLS-1$
        .append(", 0 AS TYPE, convert(Position, short) AS ORDINAL_POSITION, k.Name AS COLUMN_NAME") //$NON-NLS-1$
        .append(", NULL AS ASC_OR_DESC, 0 AS CARDINALITY, 1 AS PAGES, NULL AS FILTER_CONDITION") //$NON-NLS-1$
        .append(" FROM ").append(RUNTIME_MODEL.VIRTUAL_MODEL_NAME).append(".KeyElements k CROSS JOIN ") //$NON-NLS-1$ //$NON-NLS-2$
        .append(RUNTIME_MODEL.VIRTUAL_MODEL_NAME).append(".VirtualDatabases v")  //$NON-NLS-1$
        .append(" WHERE UCASE(v.Name)").append(LIKE_ESCAPE).append("AND KeyType LIKE 'Index' AND UCASE(GroupFullName) LIKE ?") //$NON-NLS-1$//$NON-NLS-2$
        .append(" ORDER BY NON_UNIQUE, TYPE, INDEX_NAME, ORDINAL_POSITION").toString(); //$NON-NLS-1$
    
    private static final String QUERY_MODELS =
      new StringBuffer("SELECT NULL AS MODEL_CAT, v.name AS MODEL_SCHEM, m.Name AS MODEL_NAME,") //$NON-NLS-1$
        .append(" Description AS DESCRIPTION, IsPhysical AS IS_PHYSICAL, SupportsWhereAll AS SUP_WHERE_ALL, ") //$NON-NLS-1$
        .append(" SupportsDistinct AS SUP_DISTINCT, SupportsJoin AS SUP_JOIN, SupportsOuterJoin AS SUP_OUTER_JOIN, ") //$NON-NLS-1$
        .append(" SupportsOrderBy AS SUP_ORDER_BY ") //$NON-NLS-1$
        .append(" FROM ").append(RUNTIME_MODEL.VIRTUAL_MODEL_NAME).append(".Models m CROSS JOIN ") //$NON-NLS-1$ //$NON-NLS-2$
        .append(RUNTIME_MODEL.VIRTUAL_MODEL_NAME).append(".VirtualDatabases v ")  //$NON-NLS-1$
        .append("WHERE UCASE(m.Name)").append(LIKE_ESCAPE).append("ORDER BY MODEL_NAME").toString(); //$NON-NLS-1$//$NON-NLS-2$
    
    private static final String QUERY_PRIMARY_KEYS =
      new StringBuffer("SELECT NULL AS TABLE_CAT, v.Name AS TABLE_SCHEM, GroupFullName AS TABLE_NAME") //$NON-NLS-1$
        .append(", k.Name AS COLUMN_NAME, convert(Position, short) AS KEY_SEQ, KeyName AS PK_NAME") //$NON-NLS-1$
        .append(" FROM ").append(RUNTIME_MODEL.VIRTUAL_MODEL_NAME).append(".KeyElements k CROSS JOIN ") //$NON-NLS-1$ //$NON-NLS-2$
        .append(RUNTIME_MODEL.VIRTUAL_MODEL_NAME).append(".VirtualDatabases v ")  //$NON-NLS-1$
        .append(" WHERE UCASE(v.Name)").append(LIKE_ESCAPE).append("AND KeyType LIKE 'Primary' AND UCASE(GroupFullName) LIKE ?") //$NON-NLS-1$//$NON-NLS-2$
        .append(" ORDER BY COLUMN_NAME, KEY_SEQ").toString(); //$NON-NLS-1$
    
    private final static String QUERY_PROCEDURES =
      new StringBuffer("SELECT convert(null, string) AS PROCEDURE_CAT, v.Name AS PROCEDURE_SCHEM") //$NON-NLS-1$
        .append(", p.FullName AS PROCEDURE_NAME, convert(null, string) AS RESERVED_1") //$NON-NLS-1$
        .append(", convert(null, string) AS RESERVED_2, convert(null, string) AS RESERVED_3, p.Description AS REMARKS") //$NON-NLS-1$
        .append(", convert(decodeString(p.ReturnsResults, 'true, ").append(DatabaseMetaData.procedureReturnsResult) //$NON-NLS-1$
        .append(", false, ").append(DatabaseMetaData.procedureNoResult).append("'), short) AS PROCEDURE_TYPE, p.FullName AS SPECIFIC_NAME FROM ") //$NON-NLS-1$ //$NON-NLS-2$
        .append(RUNTIME_MODEL.VIRTUAL_MODEL_NAME)
        .append(".Procedures as p CROSS JOIN ") //$NON-NLS-1$
        .append(RUNTIME_MODEL.VIRTUAL_MODEL_NAME)
        .append(".VirtualDatabases v WHERE UCASE(v.Name)").append(LIKE_ESCAPE).append("AND UCASE(p.FullName)").append(LIKE_ESCAPE) //$NON-NLS-1$//$NON-NLS-2$
        .append(" ORDER BY PROCEDURE_SCHEM, PROCEDURE_NAME").toString(); //$NON-NLS-1$

    private final static String QUERY_PROCEDURE_COLUMNS =
      new StringBuffer("SELECT convert(null, string) AS PROCEDURE_CAT, v.Name AS PROCEDURE_SCHEM") //$NON-NLS-1$
        .append(", ProcedureName AS PROCEDURE_NAME, p.Name AS COLUMN_NAME") //$NON-NLS-1$
        .append(", convert(decodeString(TYPE, '").append(PARAM_DIRECTION_MAPPING).append("', ','), short) AS COLUMN_TYPE") //$NON-NLS-1$ //$NON-NLS-2$
        .append(", convert(decodeString(DataType, '").append(TYPE_MAPPING).append("', ','), integer) AS DATA_TYPE") //$NON-NLS-1$ //$NON-NLS-2$
        .append(", DataType AS TYPE_NAME, CASE WHEN Precision <= 0 THEN convert(decodeString(DataType,'").append(PRECISION_MAPPING) //$NON-NLS-1$
        .append("',','), integer) ELSE Precision END AS PRECISION, CASE WHEN TypeLength <= 0 THEN convert(decodeString(DataType,'") //$NON-NLS-1$
        .append(PRECISION_MAPPING).append("',','), integer) ELSE TypeLength END AS LENGTH, convert(Scale, short) AS SCALE") //$NON-NLS-1$
        .append(", Radix AS RADIX, convert(decodeString(NullType, '") //$NON-NLS-1$
        .append(PROC_COLUMN_NULLABILITY_MAPPING).append("', ','), integer) AS NULLABLE") //$NON-NLS-1$
        .append(", convert(null, string) AS REMARKS, NULL AS COLUMN_DEF") //$NON-NLS-1$
        .append(", NULL AS SQL_DATA_TYPE, NULL AS SQL_DATETIME_SUB, NULL AS CHAR_OCTET_LENGTH, p.Position AS ORDINAL_POSITION") //$NON-NLS-1$
        .append(", CASE NullType WHEN 'Nullable' THEN 'YES' WHEN 'No Nulls' THEN 'NO' ELSE '' END AS IS_NULLABLE, p.ProcedureName || '.' || p.Name as SPECIFIC_NAME FROM ") //$NON-NLS-1$
        .append(RUNTIME_MODEL.VIRTUAL_MODEL_NAME)
        .append(".ProcedureParams as p CROSS JOIN ") //$NON-NLS-1$
        .append(RUNTIME_MODEL.VIRTUAL_MODEL_NAME)
        .append(".VirtualDatabases v WHERE UCASE(v.Name)").append(LIKE_ESCAPE).append("AND UCASE(p.ProcedureName)").append(LIKE_ESCAPE).append("AND UCASE(p.Name) LIKE ?") //$NON-NLS-1$//$NON-NLS-2$//$NON-NLS-3$
        .append(" ORDER BY PROCEDURE_SCHEM, PROCEDURE_NAME, COLUMN_TYPE, POSITION OPTION MAKEDEP SystemPhysical.COLUMNS").toString(); //$NON-NLS-1$

    private static final String QUERY_SCHEMAS =
      new StringBuffer("SELECT Name AS TABLE_SCHEM, NULL AS TABLE_CATALOG") //$NON-NLS-1$
        .append(" FROM ").append(RUNTIME_MODEL.VIRTUAL_MODEL_NAME).append(".VirtualDatabases ORDER BY TABLE_SCHEM").toString(); //$NON-NLS-1$ //$NON-NLS-2$
    
    private final static String QUERY_TABLES =
      new StringBuffer("SELECT NULL AS TABLE_CAT, v.Name AS TABLE_SCHEM, FullName AS TABLE_NAME") //$NON-NLS-1$
        .append(", CASE WHEN IsSystem = 'true' and UCASE(Type) = 'TABLE' THEN 'SYSTEM TABLE' ELSE UCASE(Type) END AS TABLE_TYPE, Description AS REMARKS, NULL AS TYPE_CAT, NULL AS TYPE_SCHEM") //$NON-NLS-1$
        .append(", NULL AS TYPE_NAME, NULL AS SELF_REFERENCING_COL_NAME, NULL AS REF_GENERATION, IsPhysical AS ISPHYSICAL") //$NON-NLS-1$
        .append(" FROM ").append(RUNTIME_MODEL.VIRTUAL_MODEL_NAME).append(".Groups g CROSS JOIN ")  //$NON-NLS-1$ //$NON-NLS-2$
        .append(RUNTIME_MODEL.VIRTUAL_MODEL_NAME).append(".VirtualDatabases v")  //$NON-NLS-1$
        .append(" WHERE UCASE(v.Name)").append(LIKE_ESCAPE).append("AND UCASE(FullName) LIKE ?").toString(); //$NON-NLS-1$//$NON-NLS-2$
    
//    private static final String QUERY_UDT =
//      new StringBuffer("SELECT NULL AS TYPE_CAT, v.Name AS TYPE_SCHEM, TypeName AS TYPE_NAME") //$NON-NLS-1$
//        .append(", JavaClass AS CLASS_NAME, decodeString(JavaClass, '").append(UDT_NAME_MAPPING).append("', ',') AS DATA_TYPE") //$NON-NLS-1$ //$NON-NLS-2$
//        .append(", Description AS REMARKS") //$NON-NLS-1$
//        .append(", decodeString(BaseType, '").append(UDT_NAME_MAPPING).append("', ',') AS BASE_TYPE ") //$NON-NLS-1$ //$NON-NLS-2$
//        .append(" FROM ").append(RUNTIME_MODEL.VIRTUAL_MODEL_NAME).append(".DataTypes CROSS JOIN ") //$NON-NLS-1$ //$NON-NLS-2$
//        .append(RUNTIME_MODEL.VIRTUAL_MODEL_NAME).append(".VirtualDatabases v")  //$NON-NLS-1$
//        .append(" WHERE UCASE(v.Name)").append(LIKE_ESCAPE).append("AND UCASE(TypeName)").append(LIKE_ESCAPE).append("ORDER BY DATA_TYPE, TYPE_SCHEM, TYPE_NAME ").toString(); //$NON-NLS-1$
    
    /** ATTRIBUTES */

    // driver's connection object used in constructin this object.
    private MMConnection driverConnection;

    /**
     * <p>Constructor which initializes with the connection object on which metadata
     * is sought
     * @param driver's connection object.
     * @throws SQLException if the connection is already closed.
     */
    MMDatabaseMetaData(MMConnection connection) {
        this.driverConnection = connection;
    }

    /**
     * <p>Checks whether the current user has the required security rights to call
     * all the procedures returned by the method getProcedures.</p>
     * @return true if the precedures are selectable else return false
     * @throws SQLException. Should never occur.
     */
    public boolean allProceduresAreCallable() throws SQLException {
        return true;
    }

    /**
     * <p>Checks whether the current user can use SELECT statement with all of the
     * tables returned by the method getTables. Selectability is column level in
     * metamatrix, hence return true</p>
     * @return true if tables are selectable else return false
     * @throws SQLException. Should never occur.
     */
    public boolean allTablesAreSelectable() throws SQLException {
        return true;
    }

    /**
     * <p>Checks whether a DDL statement within a transaction forces the transaction
     * to commit.</p>
     * @return if so return true else return false.
     * @throws SQLException Should never occur.
     */
    public boolean dataDefinitionCausesTransactionCommit() throws SQLException {
        return false;
    }

    /**
     * <p>Checks whether a DDL statement within a transaction is ignored.</p>
     * @return if so return true, else false
     * @throw SQLException Should never occur.
     */
    public boolean dataDefinitionIgnoredInTransactions() throws SQLException {
        return false;
    }

    /**
     * <p>Indicates whether or not a visible row delete can be detected by
     * calling ResultSet.rowDeleted().  If deletesAreDetected()
     * returns false, then deleted rows are removed from the result set.
     * @param result set type, i.e. ResultSet.TYPE_XXX
     * @return true if changes are detected by the resultset type
     * @throws SQLException, should never occur
     */
    public boolean deletesAreDetected(int type) throws SQLException {
        return false;
    }

    /**
     * <p>Did getMaxRowSize() include LONGVARCHAR and LONGVARBINARY
     * blobs?</p>
     * @return <code>true</code> if so; <code>false</code> otherwise
     * @throws SQLException, should never occur
     */
    public boolean doesMaxRowSizeIncludeBlobs() throws SQLException {
        return false;
    }

    /**
     * <p>Gets a description of a table's optimal set of columns that uniquely identifies a row.</p>
     * @param name of the catalog from which metadata needs
     * @param catalog name in which the table is present.
     * @param schema name in which this table is present.
     * @param table name whose best row identifier info is sought.
     * @param int indicating the scope of the result.
     * @param boolean indicating whether the nullable columns can be included.
     * @return ResultSet object containing the bestrow indetifier info.
     */
    public ResultSet getBestRowIdentifier(String catalog, String schema, String table, int scope, boolean nullable) throws SQLException {

        // here it always returns a empty result set, when this functionality 
        // is being changed make sure that we check the catelog & schema here
        // to filter.
        
        // list containing records/rows in the ResultSet
        List records = new ArrayList (0);

        /***********************************************************************
        * Hardcoding JDBC column names for the columns returned in results object
        ***********************************************************************/
        Map[] metadataList = new Map[8];

        // HardCoding metadata details for SCOPE column
        metadataList[0] = getColumnMetadata(null, JDBCColumnNames.BEST_ROW.SCOPE,
                            MMJDBCSQLTypeInfo.SHORT,  ResultsMetadataConstants.NULL_TYPES.NOT_NULL);

        // HardCoding metadata details for COLUMN_NAME column
        metadataList[1] = getColumnMetadata(null, JDBCColumnNames.BEST_ROW.COLUMN_NAME,
                            MMJDBCSQLTypeInfo.STRING,  ResultsMetadataConstants.NULL_TYPES.NOT_NULL);

        // HardCoding metadata details for DATA_TYPE column
        metadataList[2] = getColumnMetadata(null, JDBCColumnNames.BEST_ROW.DATA_TYPE,
                            MMJDBCSQLTypeInfo.SHORT, ResultsMetadataConstants.NULL_TYPES.NOT_NULL);

        // HardCoding metadata details for TYPE_NAME column
        metadataList[3] = getColumnMetadata(null, JDBCColumnNames.BEST_ROW.TYPE_NAME,
                            MMJDBCSQLTypeInfo.STRING, ResultsMetadataConstants.NULL_TYPES.NOT_NULL);

        // HardCoding metadata details for COLUMN_SIZE column
        metadataList[4] = getColumnMetadata(null, JDBCColumnNames.BEST_ROW.COLUMN_SIZE,
                            MMJDBCSQLTypeInfo.INTEGER, ResultsMetadataConstants.NULL_TYPES.NOT_NULL);

        // HardCoding metadata details for BUFFER_LENGTH column
        metadataList[5] = getColumnMetadata(null, JDBCColumnNames.BEST_ROW.BUFFER_LENGTH,
                            MMJDBCSQLTypeInfo.INTEGER, ResultsMetadataConstants.NULL_TYPES.NULLABLE);

        // HardCoding metadata details for DECIMAL_DIGITS column
        metadataList[6] = getColumnMetadata(null, JDBCColumnNames.BEST_ROW.DECIMAL_DIGITS,
                            MMJDBCSQLTypeInfo.SHORT, ResultsMetadataConstants.NULL_TYPES.NOT_NULL);

        // HardCoding metadata details for PSEUDO_COLUMN column
        metadataList[7] = getColumnMetadata(null, JDBCColumnNames.BEST_ROW.PSEUDO_COLUMN,
                            MMJDBCSQLTypeInfo.SHORT, ResultsMetadataConstants.NULL_TYPES.NOT_NULL);

        // logging
        String logMsg = JDBCPlugin.Util.getString("MMDatabaseMetadata.Best_row_sucess", table); //$NON-NLS-1$
        logger.fine(logMsg);

        // construct results object from column values and their metadata
        return createResultSet(records, metadataList);
    }

    /**
     * <p>Gets the catalog names available to metamatrix. The results are ordered by
     * catalog name. There is no concept of catalogs in a metamatrix driver and this
     * returns an empty resultSet.
     * @return ResultSet object containing metadata info of the catalog on this connection.
     * @throws SQLException if there is an error obtaining server results
     */
    public ResultSet getCatalogs() throws SQLException {
        // list containing records/rows in the ResultSet
        List records = new ArrayList (0);

        /***********************************************************************
        * Hardcoding JDBC column names for the columns returned in results object
        ***********************************************************************/
        Map[] metadataList = new Map[1];

        // HardCoding metadata details for TABLE_CAT column
        metadataList[0] = getColumnMetadata(null, JDBCColumnNames.CATALOGS.TABLE_CAT, 
                            MMJDBCSQLTypeInfo.STRING, ResultsMetadataConstants.NULL_TYPES.NULLABLE);

        // logging
        String logMsg = JDBCPlugin.Util.getString("MMDatabaseMetadata.Catalog_success"); //$NON-NLS-1$
        logger.fine(logMsg);

        // construct results object from column values and their metadata
        return createResultSet(records, metadataList);
    }

    private ResultSet createResultSet(List records, Map[] columnMetadata) throws SQLException {
        ResultSetMetaData rsmd = ResultsMetadataWithProvider.newInstance(StaticMetadataProvider.createWithData(columnMetadata, 0));

        return createResultSet(records, rsmd);
    }

    private ResultSet createResultSet(List records, ResultSetMetaData rsmd) throws SQLException {
        ResultsMessage resultsMsg = createDummyResultsMessage(null, null, records);
        MMStatement stmt = MMStatement.newInstance(this.driverConnection, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
        return new MMResultSet(resultsMsg, stmt, (com.metamatrix.jdbc.api.ResultSetMetaData) rsmd, 0);
    }

    private ResultSet createEmptyResultSet(String[] columnNames, String[] dataTypes) throws SQLException {
        ResultsMessage resultsMsg = createDummyResultsMessage(columnNames, dataTypes, Collections.EMPTY_LIST);
        MMStatement stmt = MMStatement.newInstance(this.driverConnection, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
        try {
            stmt.setFetchSize(500);
        } catch(SQLException e) {
            // will never happen but throw a runtime if it does
            throw new MetaMatrixRuntimeException(e);
        }
        Map[] metadata = new Map[columnNames.length];
        for (int i = 0; i < columnNames.length; i++) {
            metadata[i] = getColumnMetadata(null, columnNames[i], dataTypes[i], ResultsMetadataConstants.NULL_TYPES.UNKNOWN);
        }
        return new MMResultSet(resultsMsg, stmt, ResultsMetadataWithProvider.newInstance(StaticMetadataProvider.createWithData(metadata, 0)), 0);
    }

    private ResultsMessage createDummyResultsMessage(String[] columnNames, String[] dataTypes, List records) {
        ResultsMessage resultsMsg = new ResultsMessage();
        resultsMsg.setColumnNames(columnNames);
        resultsMsg.setDataTypes(dataTypes);
        resultsMsg.setPartialResults(false);
        resultsMsg.setFirstRow(1);
        resultsMsg.setLastRow(records.size());
        resultsMsg.setFinalRow(records.size());
        resultsMsg.setResults((List[])records.toArray(new List[records.size()]));
        resultsMsg.setFetchSize(500);

        return resultsMsg;
    }

    /**
     * <p>Gets the String object used to separate a catalog name and a table name
     * @return String delimiter
     * @throws SQLException, should never occur.
     */
    public String getCatalogSeparator() throws SQLException {
        return EMPTY_STRING;
    }

    /**
     * <p>Get the metamatrix term for catalog
     * @return String representing catalog name
     * @throws SQLException, should never occur.
     */
    public String getCatalogTerm() throws SQLException {
        return EMPTY_STRING;
    }

    /**
     * <p>Gets a description of the access rights for a column of the given name.
     * Catalog, schema and table names are not used to narrow down the search,
     * but the schema name should match the virtualdatabasename used to obtain
     * this driver connection.</p>
     * @param name of the catalog to which columns belong.
     * @param name of the schema to which columns belong.
     * @param name of the table to which columns belong.
     * @param name pattern to be matched by column names.
     * @return ResultSet containing column privilage information.
     * @throws SQLException if there is an error obtaining server results
     */
    public ResultSet getColumnPrivileges(String catalog, String schema, String table, String columnName) throws SQLException {
        
        List records = new ArrayList (0);
        /***********************************************************************
        * Hardcoding JDBC column names for the columns returned in results object
        ***********************************************************************/
        Map[] metadataList = new Map[8];

        metadataList[0] = getColumnMetadata(null, JDBCColumnNames.PRIVILEGES.TABLE_CAT,
                            MMJDBCSQLTypeInfo.STRING,  ResultsMetadataConstants.NULL_TYPES.NULLABLE);
        metadataList[1] = getColumnMetadata(null, JDBCColumnNames.PRIVILEGES.TABLE_SCHEM,
                            MMJDBCSQLTypeInfo.STRING,  ResultsMetadataConstants.NULL_TYPES.NULLABLE);
        metadataList[2] = getColumnMetadata(null, JDBCColumnNames.PRIVILEGES.TABLE_NAME,
                            MMJDBCSQLTypeInfo.STRING, ResultsMetadataConstants.NULL_TYPES.NOT_NULL);
        metadataList[3] = getColumnMetadata(null, JDBCColumnNames.PRIVILEGES.COLUMN_NAME,
                             MMJDBCSQLTypeInfo.STRING, ResultsMetadataConstants.NULL_TYPES.NOT_NULL);
        metadataList[4] = getColumnMetadata(null, JDBCColumnNames.PRIVILEGES.GRANTOR,
                            MMJDBCSQLTypeInfo.STRING, ResultsMetadataConstants.NULL_TYPES.NULLABLE);
        metadataList[5] = getColumnMetadata(null, JDBCColumnNames.PRIVILEGES.GRANTEE,
                            MMJDBCSQLTypeInfo.STRING, ResultsMetadataConstants.NULL_TYPES.NOT_NULL);
        metadataList[6] = getColumnMetadata(null, JDBCColumnNames.PRIVILEGES.PRIVILEGE,
                            MMJDBCSQLTypeInfo.STRING, ResultsMetadataConstants.NULL_TYPES.NOT_NULL);
        metadataList[7] = getColumnMetadata(null, JDBCColumnNames.PRIVILEGES.IS_GRANTABLE,
                            MMJDBCSQLTypeInfo.STRING, ResultsMetadataConstants.NULL_TYPES.NULLABLE);
       
        return createResultSet(records, metadataList);
        
    }

    /**
     * <p>Get's the metadata information about the columns whose names match the given
     * columnNamePattern. Catalog, schema and tableNamePattern are not used to
     * narrow down the search, but Catalog and schema names should match the
     * virtualdatabasename and version used to obtain this driver connection.</p>
     * <p> The ResultSet returned by this method contains the following additional
     * columns that are not specified in the JDBC specification.</p>
     * <OL>
     *   <LI><B>Format</B> String => format of the column
     *   <LI><B>MinRange</B> String => minimum range value of the column
     *   <LI><B>MaxRange</B> String => maximum range value of the column
     * <OL>
     * @param catalog name to which the columns belong.
     * @param schema name to which the columns belong.
     * @param pattern name to be matched by table name.
     * @param pattern name to be matched by column name.
     * @return ResultSet containing column metadata info.
     * @throws SQLException if there is an error obtaining server results.
     */
    public ResultSet getColumns(String catalog, String schema, String tableNamePattern, String columnNamePattern) throws SQLException {

        // Since catelog is allways null with MM, if nay supplied send empty
        //result set
        if ((catalog != null) && (catalog.trim().length() > 0)) {
            return emptyColumnsResultSet();
        }
        
        // hard wire the schema to the current connection's VDB name, if one
        // not supplied
        if (schema == null) {
            schema = PERCENT;
        }
        
        // Get columns in all the tables if tableNamePattern is null
        if(tableNamePattern == null) {
            tableNamePattern = PERCENT; 
        }
        // Get all columns if columnNamePattern is null
        if(columnNamePattern == null) {
            columnNamePattern = PERCENT; 
        }

        // list which represent records containing column info
        List records = new ArrayList ();
        ResultSetMetaData rmetadata = null;
        MMResultSet results = null;
        PreparedStatement prepareQuery = null;

        try {
            prepareQuery = driverConnection.prepareStatement(QUERY_COLUMNS); 
            prepareQuery.setObject(1, schema.toUpperCase());
            prepareQuery.setObject(2, tableNamePattern.toUpperCase());
            prepareQuery.setObject(3, columnNamePattern.toUpperCase());

            // make a query against runtimemetadata and get results
            results = (MMResultSet) prepareQuery.executeQuery();

            // build the list of records of column description
            while (results.next ()) {
                // list represents a record on the Results object.
                List currentRow = new ArrayList(JDBCColumnPositions.COLUMNS.MAX_COLUMNS);

                // add values in the current record on the Results object to the list
                // number of values to be fetched from each row is MAX_COLUMNS.
                for(int i=0; i < JDBCColumnPositions.COLUMNS.MAX_COLUMNS; i++) {
                    // get the value at the current index add it to currentRow
                    currentRow.add(results.getObject(i+1));
                }
                records.add(currentRow);
            }// end of while

            // get the metadata for the results
            rmetadata = results.getMetaData();

        } catch(Exception e) {
            // logging
            String logMsg = JDBCPlugin.Util.getString("MMDatabaseMetadata.getCols_error", columnNamePattern, tableNamePattern, e.getMessage()); //$NON-NLS-1$
            throw MMSQLException.create(e, logMsg);
        }

        // logging
        String logMsg = JDBCPlugin.Util.getString("MMDatabaseMetadata.getCols_success", columnNamePattern, tableNamePattern); //$NON-NLS-1$
        logger.fine(logMsg);

        // construct results object from column values and their metadata
        return createResultSet(records, rmetadata);
    }

    /**
     * This method returns a empty result for getColumns() request.
     * @return ResultSet
     */
    private ResultSet emptyColumnsResultSet() throws SQLException {
        String[] columnNames = new String[] {
                JDBCColumnNames.COLUMNS.TABLE_CAT, 
                JDBCColumnNames.COLUMNS.TABLE_SCHEM,  
                JDBCColumnNames.COLUMNS.TABLE_NAME,
                JDBCColumnNames.COLUMNS.COLUMN_NAME, 
                JDBCColumnNames.COLUMNS.DATA_TYPE,
                JDBCColumnNames.COLUMNS.TYPE_NAME, 
                JDBCColumnNames.COLUMNS.COLUMN_SIZE,
                JDBCColumnNames.COLUMNS.BUFFER_LENGTH,
                JDBCColumnNames.COLUMNS.DECIMAL_DIGITS,
                JDBCColumnNames.COLUMNS.NUM_PREC_RADIX,
                JDBCColumnNames.COLUMNS.NULLABLE,
                JDBCColumnNames.COLUMNS.REMARKS,
                JDBCColumnNames.COLUMNS.COLUMN_DEF,
                JDBCColumnNames.COLUMNS.SQL_DATA_TYPE,
                JDBCColumnNames.COLUMNS.SQL_DATETIME_SUB,
                JDBCColumnNames.COLUMNS.CHAR_OCTET_LENGTH,
                JDBCColumnNames.COLUMNS.ORDINAL_POSITION,
                JDBCColumnNames.COLUMNS.IS_NULLABLE,
                // These are added in 1.4
                //JDBCColumnNames.COLUMNS.SCOPE_CATLOG,
                //JDBCColumnNames.COLUMNS.SCOPE_SCHEMA,
                //JDBCColumnNames.COLUMNS.SCOPE_TABLE,
                //JDBCColumnNames.COLUMNS.SOURCE_DATA_TYPE
        };
        String[] dataTypes = new String[] {
                MMJDBCSQLTypeInfo.STRING,
                MMJDBCSQLTypeInfo.STRING,
                MMJDBCSQLTypeInfo.STRING,
                MMJDBCSQLTypeInfo.STRING,
                MMJDBCSQLTypeInfo.INTEGER,
                MMJDBCSQLTypeInfo.STRING,
                MMJDBCSQLTypeInfo.INTEGER,
                MMJDBCSQLTypeInfo.INTEGER,
                MMJDBCSQLTypeInfo.INTEGER,
                MMJDBCSQLTypeInfo.INTEGER,
                MMJDBCSQLTypeInfo.INTEGER,
                MMJDBCSQLTypeInfo.STRING,
                MMJDBCSQLTypeInfo.STRING,
                MMJDBCSQLTypeInfo.INTEGER,
                MMJDBCSQLTypeInfo.INTEGER,
                MMJDBCSQLTypeInfo.INTEGER,
                MMJDBCSQLTypeInfo.INTEGER,
                MMJDBCSQLTypeInfo.STRING
                //,MMJDBCSQLTypeInfo.STRING,MMJDBCSQLTypeInfo.STRING,MMJDBCSQLTypeInfo.STRING,MMJDBCSQLTypeInfo.SHORT
        };                    
        return createEmptyResultSet(columnNames, dataTypes);
    }
    
    /**
     * <p>Gets the description of the foreign key columns in the table foreignTable.
     * These foreign key columns reference primary key columns of primaryTable.
     * Catalog and schema information is not used to narrow down the search, but
     * Catalog and schema names(primary and foreign) should match the
     * virtualdatabasename and version used to obtain this driver connection.
     * @param name of the catalog containing primary keys.
     * @param name of the schema containing primary keys.
     * @param name of the table containing primary keys.
     * @param name of the catalog containing foreign keys.
     * @param name of the schema containing foreign keys.
     * @param name of the table containing foreign keys.
     * @return ResultSet giving description of foreign key columns.
     * @throws SQLException if there is an error obtaining server results
     */
    public ResultSet getCrossReference(String primaryCatalog, String primarySchema,String primaryTable,String foreignCatalog,String foreignSchema, String foreignTable) throws SQLException {
        
        if (primaryCatalog != null || foreignCatalog != null) {
            return emptyCrossReference();
        }
        
        if (primarySchema == null) {
            primarySchema = PERCENT;
        }
        
        if (foreignSchema == null) {
            foreignSchema = PERCENT;
        }
        
        if (primaryTable == null) {
            primaryTable = PERCENT; 
        }

        if (foreignTable == null) {
            foreignTable = PERCENT; 
        }

        MMResultSet results = null;
        try {
            PreparedStatement prepareQuery = driverConnection.prepareStatement(QUERY_CROSS_REFERENCES);

            prepareQuery.setObject(1, primarySchema.toUpperCase());
            prepareQuery.setObject(2, foreignSchema.toUpperCase());
            prepareQuery.setObject(3, primaryTable.toUpperCase());
            prepareQuery.setObject(4, foreignTable.toUpperCase());

            // make a query against runtimemetadata and get results
            results = (MMResultSet) prepareQuery.executeQuery();
        } catch(Exception e) {
            // logging
            String logMsg = JDBCPlugin.Util.getString("MMDatabaseMetadata.getCrossRef_error", primaryTable, foreignTable, e.getMessage()); //$NON-NLS-1$
            throw MMSQLException.create(e, logMsg);
        }

        ResultSet resultSet = getReferenceKeys(results);

        // logging
        String logMsg = JDBCPlugin.Util.getString("MMDatabaseMetadata.getCrossRef_success", primaryTable, foreignTable); //$NON-NLS-1$
        logger.fine(logMsg);

        return resultSet;
    }

    /**
     * Generate empty result set for Cross Reference
     * @return
     */
    private ResultSet emptyCrossReference() throws SQLException {
        String[] columnNames = new String[] {
                JDBCColumnNames.REFERENCE_KEYS.PKTABLE_CAT,
                JDBCColumnNames.REFERENCE_KEYS.PKTABLE_SCHEM,
                JDBCColumnNames.REFERENCE_KEYS.PKTABLE_NAME,
                JDBCColumnNames.REFERENCE_KEYS.PKCOLUMN_NAME,
                JDBCColumnNames.REFERENCE_KEYS.FKTABLE_CAT,
                JDBCColumnNames.REFERENCE_KEYS.FKTABLE_SCHEM,
                JDBCColumnNames.REFERENCE_KEYS.FKTABLE_NAME,
                JDBCColumnNames.REFERENCE_KEYS.FKCOLUMN_NAME,
                JDBCColumnNames.REFERENCE_KEYS.KEY_SEQ,
                JDBCColumnNames.REFERENCE_KEYS.UPDATE_RULE,
                JDBCColumnNames.REFERENCE_KEYS.DELETE_RULE,
                JDBCColumnNames.REFERENCE_KEYS.FK_NAME,
                JDBCColumnNames.REFERENCE_KEYS.PK_NAME,
                JDBCColumnNames.REFERENCE_KEYS.DEFERRABILITY
        };
        String[] dataTypes = new String[] {
                MMJDBCSQLTypeInfo.STRING,
                MMJDBCSQLTypeInfo.STRING,
                MMJDBCSQLTypeInfo.STRING,
                MMJDBCSQLTypeInfo.STRING,
                MMJDBCSQLTypeInfo.STRING,
                MMJDBCSQLTypeInfo.STRING,
                MMJDBCSQLTypeInfo.STRING,
                MMJDBCSQLTypeInfo.STRING,
                MMJDBCSQLTypeInfo.SHORT,
                MMJDBCSQLTypeInfo.SHORT,
                MMJDBCSQLTypeInfo.SHORT,
                MMJDBCSQLTypeInfo.STRING,
                MMJDBCSQLTypeInfo.STRING,
                MMJDBCSQLTypeInfo.SHORT
        };
        return createEmptyResultSet(columnNames, dataTypes);        
    }

    /**
     * Retrieves the minor version number of the underlying database.
     * @return intValue of database's minor version
     * @throws SQLException if a database access error occurs.
     */
    public int getDatabaseMinorVersion() throws SQLException {
        return TeiidDriver.getInstance().getMinorVersion();
    }

    /**
     * Retrieves the major version number of the underlying database.
     * @return intValue of database's minor version
     * @throws SQLException if a database access error occurs.
     */
    public int getDatabaseMajorVersion() throws SQLException {
        return TeiidDriver.getInstance().getMajorVersion();
    }

    /**
     * Retrieves the major JDBC version number for this driver.
     * @return intValue JDBC major version number
     * @throws SQLException should never occur.
     */
    public int getJDBCMajorVersion()  throws SQLException {
        return 3;
    }

    /**
     * Retrieves the minor JDBC version number for this driver.
     * @return intValue JDBC major version number
     * @throws SQLException should never occur.
     */
    public int getJDBCMinorVersion()  throws SQLException {
        return 0;
    }

    /**
     * <p>Gets the product name for this database
     * @return String representing the product name
     * @throws SQLException should never occur.
     */
    public String getDatabaseProductName() throws SQLException {
        return this.driverConnection.getDatabaseName();
    }

    /**
     * <p>Gets the version of metamatrix server to which this driver connects
     * @return String representing the product version
     * @throws SQLException if there is an error accessing product release info.
     */
    public String getDatabaseProductVersion() throws SQLException {
        return TeiidDriver.getInstance().getMajorVersion() + "." + TeiidDriver.getInstance().getMinorVersion(); //$NON-NLS-1$
    }

    /**
     * <p>Gets metamatrix default transaction isolation level.
     * @return intvalue giving the transaction isolation level
     * @throws SQLException never
     */
    public int getDefaultTransactionIsolation() throws SQLException {
        return Connection.TRANSACTION_NONE;
    }

    /**
     * <p>Gets this drivers major version number
     * @return int representing the driver's major version
     */
    public int getDriverMajorVersion() {
        return TeiidDriver.getInstance().getMajorVersion();
    }

    /**
     * <p>Gets this drivers minor version number
     * @return int representing the driver's minor version
     */
    public int getDriverMinorVersion() {
        return TeiidDriver.getInstance().getMinorVersion();
    }

    /**
     * <p>Get the name of this JDBC driver
     * @return String representing the driver's name
     * @throws SQLException, if the connection is already closed.
     */
    public String getDriverName() throws SQLException {
        return TeiidDriver.getInstance().getDriverName();
    }

    /**
     * <p>This method gets the version of this JDBC driver. It combines the major
     * and minor version numbers
     * @return String representing the driver's version
     * @throws SQLException, should never occur.
     */
    public String getDriverVersion() throws SQLException {
        return getDriverMajorVersion()+"."+getDriverMinorVersion (); //$NON-NLS-1$
    }

    /**
     * <p>This method gets a description of the forignkey columns that reference the
     * primary key columns in the given table.  Catalog and schema names are not
     * used to narrow down the search, but they should match the virtualdatabasename
     * and version used to obtain this driver connection.
     * @param name of the catalog which contains the given table.
     * @param schema name which contains the given table.
     * @param table name which contains the primary keys.
     * @return ResultSet object giving the exported key info.
     * @throws SQLException if there is an error obtaining server results
     */
    public ResultSet getExportedKeys(String catalog, String schema, String table) throws SQLException {
        if ((catalog != null) && (catalog.trim().length() > 0)) {
            return emptyExportedKeys();
        }
        
        if (schema == null) {
            schema = PERCENT;
        }
        
        if (table == null) {
            table = PERCENT; 
        }

        MMResultSet results = null;
        try {
            PreparedStatement prepareQuery = driverConnection.prepareStatement(QUERY_EXPORTED_KEYS);
            
            prepareQuery.setObject(1, schema.toUpperCase());
            prepareQuery.setObject(2, table.toUpperCase());

            // make a query against runtimemetadata and get results
            results = (MMResultSet) prepareQuery.executeQuery();
        } catch(Exception e) {
            // logging
            String logMsg = JDBCPlugin.Util.getString("MMDatabaseMetadata.getExpKey_error", table, e.getMessage()); //$NON-NLS-1$
            throw MMSQLException.create(e, logMsg);
        }

        ResultSet resultSet = getReferenceKeys(results);

        logger.fine(JDBCPlugin.Util.getString("MMDatabaseMetadata.getExpKey_success", table));//$NON-NLS-1$

        return resultSet;
    }

    /**
     * @return
     */
    private ResultSet emptyExportedKeys() throws SQLException {
        // Same as cross reference.
        return emptyCrossReference();
    }

    /**
     * <p>Gets the extra characters that can be used in unquoted identifier names
     * (those beyond a-z, 0-9, and _)
     * @return String representing extra charachters that can be used in identifier names.
     * @throws SQLException, should never occur
     */
    public String getExtraNameCharacters() throws SQLException {
        return EXTRA_CHARS;// ".@" is use in fully qualified identifier names
    }

    /**
     * <p>Get's the string used to quote SQL identifiers. This returns a " " if identifier
     * quoting is not supported.
     * @return string used to quote SQL identifiers.
     * @throws SQLException, should never occur
     */
    public String getIdentifierQuoteString() throws SQLException {
        return DOUBLE_QUOTE;
    }

    /**
     * <p>Gets a description of the primary key columns that are referenced by the
     * foreign key columns in the given table. Catalog and schema names are not
     * used to narrow down the search, but they should match the virtualdatabasename
     * and version used to obtain this driver connection.
     * @param name of the catalog which contains the given table.
     * @param schema name which contains the given table.
     * @param table name which contains the foreign keys.
     * @return ResultSet object giving the imported key info.
     * @throws SQLException if there is an error obtaining server results
     */
    public ResultSet getImportedKeys(String catalog, String schema, String table) throws SQLException {
        if ((catalog != null) && (catalog.trim().length() > 0)) {
            return emptyImportedKeys();
        }
        
        if (schema == null) {
            schema = PERCENT;
        }
        
        if (table == null) {
            table = PERCENT; 
        }

        MMResultSet results = null;
        try {
            PreparedStatement prepareQuery = driverConnection.prepareStatement(QUERY_IMPORTED_KEYS);

            prepareQuery.setObject(1, schema.toUpperCase());
            prepareQuery.setObject(2, table.toUpperCase());


            // make a query against runtimemetadata and get results
            results = (MMResultSet) prepareQuery.executeQuery();
        } catch(Exception e) {
            String logMsg = JDBCPlugin.Util.getString("MMDatabaseMetadata.getImpKey_error", table, e.getMessage()); //$NON-NLS-1$
            throw MMSQLException.create(e, logMsg);
        }

        ResultSet resultSet = getReferenceKeys(results);

        logger.fine(JDBCPlugin.Util.getString("MMDatabaseMetadata.getImpKey_success", table)); //$NON-NLS-1$

        return resultSet;
    }

    /**
     * @return
     */
    private ResultSet emptyImportedKeys() throws SQLException {
        return emptyCrossReference();
    }

    /**
     * <p>Gets a description of the indexes that are present on a given table.
     *
     * @param name of the catalog which contains the given table.
     * @param schema name which contains the given table.
     * @param table name which contains the indexes.
     * @param boolean indicating if unique key info needs to be returned.
     * @param boolean indicating if approximate value are to be allowed.
     * @return ResultSet object containing metadata info of index columns.
     * @throws SQLException if catalog/schema info does not match for this connection.
     */
    public ResultSet getIndexInfo(String catalog, String schema, String table, boolean unique, boolean approximate) throws SQLException {
        if ((catalog != null) && (catalog.trim().length() > 0)) {
            return emptyIndexInfo();
        }
        
        if (schema == null) {
            schema = PERCENT;
        }
        
        if (table == null) {
            table = PERCENT; 
        }
        // list which represent records containing primary key info
        List records = new ArrayList ();
        ResultSetMetaData rmetadata = null;
        MMResultSet results = null;
        PreparedStatement prepareQuery = null;

        try {
            prepareQuery = driverConnection.prepareStatement(QUERY_INDEX_INFO); 

            prepareQuery.setObject(1, schema.toUpperCase());
            prepareQuery.setObject(2, table.toUpperCase());

            // make a query against runtimemetadata and get results
            results = (MMResultSet) prepareQuery.executeQuery();

            // build the list of records from server's Results object.
            while (results.next ()) {
                // list represents a record on the Results object.
                List currentRow = new ArrayList (13);
                // add values in the current record on the Results object to the list
                // number of values to be fetched from each row is MAX_COLUMNS.
                for(int i=0; i < JDBCColumnPositions.INDEX_INFO.MAX_COLUMNS; i++) {
                    // get the value at the current index add it to currentRow
                    currentRow.add(results.getObject(i+1));
                }

                // add the current row to the list of records.
                records.add(currentRow);
            }// end of while

            // get the metadata for the results
            rmetadata = results.getMetaData();

        } catch (Exception e) {
            throw MMSQLException.create(e, JDBCPlugin.Util.getString("MMDatabaseMetadata.getIndex_error", table, e.getMessage())); //$NON-NLS-1$
        }

        logger.fine(JDBCPlugin.Util.getString("MMDatabaseMetadata.getIndex_success", table)); //$NON-NLS-1$

        // construct results object from column values and their metadata
        return createResultSet(records, rmetadata);
    }

    /**
     * @return
     */
    private ResultSet emptyIndexInfo() throws SQLException {
        String[] columnNames = new String[] {
                JDBCColumnNames.INDEX_INFO.TABLE_CAT,
                JDBCColumnNames.INDEX_INFO.TABLE_SCHEM,                
                JDBCColumnNames.INDEX_INFO.TABLE_NAME,
                JDBCColumnNames.INDEX_INFO.NON_UNIQUE,
                JDBCColumnNames.INDEX_INFO.INDEX_QUALIFIER,
                JDBCColumnNames.INDEX_INFO.INDEX_NAME,
                JDBCColumnNames.INDEX_INFO.TYPE,
                JDBCColumnNames.INDEX_INFO.ORDINAL_POSITION,
                JDBCColumnNames.INDEX_INFO.COLUMN_NAME,
                JDBCColumnNames.INDEX_INFO.ASC_OR_DESC,
                JDBCColumnNames.INDEX_INFO.CARDINALITY,
                JDBCColumnNames.INDEX_INFO.PAGES,
                JDBCColumnNames.INDEX_INFO.FILTER_CONDITION
        };
        String[] dataTypes = new String[] {
                MMJDBCSQLTypeInfo.STRING,
                MMJDBCSQLTypeInfo.STRING,
                MMJDBCSQLTypeInfo.STRING,
                MMJDBCSQLTypeInfo.BOOLEAN,
                MMJDBCSQLTypeInfo.STRING,
                MMJDBCSQLTypeInfo.STRING,
                MMJDBCSQLTypeInfo.SHORT,
                MMJDBCSQLTypeInfo.SHORT,
                MMJDBCSQLTypeInfo.STRING,
                MMJDBCSQLTypeInfo.STRING,
                MMJDBCSQLTypeInfo.INTEGER,
                MMJDBCSQLTypeInfo.INTEGER,
                MMJDBCSQLTypeInfo.STRING
        };
        return createEmptyResultSet(columnNames, dataTypes);   
    }

    /**
     * <p>Gets the maximum number of hexadecimal characters allowed in an inline
     * binary literal
     * @return int value giving maximum length of a binary literal
     * @throws SQLException, should never occur
     */
    public int getMaxBinaryLiteralLength() throws SQLException {
        return NO_LIMIT;
    }

    /**
     * <p>Gets the maximum number of characters allowed in a catalog name
     * @return int value giving maximum length of a catalog name
     * @throws SQLException, should never occur
     */
    public int getMaxCatalogNameLength() throws SQLException {
        return MAX_CATALOG_NAME_LENGTH;
    }

    /**
     * <p>Gets the maximum number of characters allowed in a character literal
     * @return int value giving maximum length of a charachter literal
     * @throws SQLException, should never occur
     */
    public int getMaxCharLiteralLength() throws SQLException {
        return NO_LIMIT;
    }

    /**
     * <p>Gets the maximum number of characters allowed in a column name
     * @return int value giving maximum length of the column name
     * @throws SQLException, should never occur
     */
    public int getMaxColumnNameLength() throws SQLException {
        return MAX_COLUMN_NAME_LENGTH;
    }

    /**
     * <p>Gets the maximum number of columns allowed in a GROUP BY clause
     * @return int values giving max columns in GROUP BY
     * @throws SQLException, should never occur
     */
    public int getMaxColumnsInGroupBy() throws SQLException {
        return NO_LIMIT;
    }

    /**
     * <p>Gets the maximum number of columns allowed in an index
     * @return int gives maximum columns in an index.
     * @throws SQLException, should never occur
     */
    public int getMaxColumnsInIndex() throws SQLException {
        return NO_LIMIT;
    }

    /**
     * <p>Gets the maximum number of columns allowed in a ORDER BY clause
     * @return int gives maximum columns in an order by.
     * @throws SQLException, should never occur
     */
    public int getMaxColumnsInOrderBy() throws SQLException {
        return NO_LIMIT;
    }

    /**
     * <p>Gets the maximum number of columns allowed in a SELECT clause
     * @return int gives maximum columns in a select.
     * @throws SQLException, should never occur
     */
    public int getMaxColumnsInSelect() throws SQLException {
        return NO_LIMIT;
    }

    /**
     * <p>Gets the maximum number of columns allowed in a table
     * @return int gives maximum columns in a table.
     * @throws SQLException, should never occur
     */
    public int getMaxColumnsInTable() throws SQLException {
        return NO_LIMIT;
    }

    /**
     * <p>Gets the maximum number of active connections to metamatrix that can be
     * maintained through this driver instance
     * @return int gives maximum connections.
     * @throws SQLException, should never occur
     */
    public int getMaxConnections() throws SQLException {
        return NO_LIMIT;
    }

    /**
     * <p>Gets the maximum number of characters allowed in a cursor name
     * @return int gives maximum max charachters allowed in a cursor name.
     * @throws SQLException, should never occur
     */
    public int getMaxCursorNameLength() throws SQLException {
        return NO_LIMIT;
    }

    /**
     * <p>Gets the maximum number of bytes allowed in an index
     * @return int gives maximum bytes.
     * @throws SQLException, should never occur
     */
    public int getMaxIndexLength() throws SQLException {
        return NO_LIMIT;
    }

    /**
     * <p>Gets the maximum number of characters allowed in a procedure name
     * @return int gives maximum length of procedure name.
     * @throws SQLException, should never occur
     */
    public int getMaxProcedureNameLength() throws SQLException {
        return MAX_PROCEDURE_NAME_LENGTH;
    }

    /**
     * <p>Gets the maximum number of bytes allowed in a single row
     * @return int max row size in the result set.
     * @throws SQLException, should never occur
     */
    public int getMaxRowSize() throws SQLException {
        return NO_LIMIT;
    }

    /**
     * <p>Gets the maximum number of characters allowed in a schema name
     * @return int maximum length of a schema.
     * @throws SQLException, should never occur
     */
    public int getMaxSchemaNameLength() throws SQLException {
        return NO_LIMIT;
    }

    /**
     * <p>Gets the maximum number of characters allowed in an SQL statement
     * @return maximum length of a statement
     * @throws SQLException, should never occur
     */
    public int getMaxStatementLength() throws SQLException {
        return NO_LIMIT;
    }

    /**
     * <p>Gets the maximum number of active statements that may be open on one
     * connection at any time
     * @return max number of open statements on a connection.
     * @throws SQLException, should never occur
     */
    public int getMaxStatements() throws SQLException {
        return NO_LIMIT;
    }

    /**
     * <p>Gets the maximum number of characters allowed in a table name
     * @return max length of table name.
     * @throws SQLException, should never occur
     */
    public int getMaxTableNameLength() throws SQLException {
        return MAX_TABLE_NAME_LENGTH;
    }

    /**
     * <p>Gets the maximum number of tables allowed in a SELECT clause
     * @return max tables in a select.
     * @throws SQLException, should never occur
     */
    public int getMaxTablesInSelect() throws SQLException {
        return NO_LIMIT;
    }

    /**
     * <p>Gets the maximum number of characters allowed in a user name
     * @return max length of user name.
     * @throws SQLException, should never occur
     */
    public int getMaxUserNameLength() throws SQLException {
        return MAX_USER_NAME_LENGTH;
    }

    /**
     * <p>Gets the OPEN GROUP CLI names of metamatrix math functions
     * @return string giving numeric functions supported.
     * @throws SQLException, should never occur
     */
    public String getNumericFunctions() throws SQLException {
        return NUMERIC_FUNCTIONS;
    }

    /**
     * <p>Get's a description of the primary key columns in a table. The descriptions
     * are ordered by column name. Catalog and schema names are not used to narrow
     * down the search, but they should match the virtualdatabasename and version
     * used to obtain this driver connection.
     * @param name of the catalog which contains the given table.
     * @param schema name which contains the given table.
     * @param table name which contains the primary keys.
     * @return ResultSet object containing primary keys of the given table.
     * @throws SQLException if there is an error obtaining metamatrix results.
     */
    public ResultSet getPrimaryKeys(String catalog, String schema, String table) throws SQLException {
        if ((catalog != null) && (catalog.trim().length() > 0)) {
            return emptyPrimaryKeys();
        }
        
        if (schema == null) {
            schema = PERCENT;
        }
        
        if (table == null) {
            table = PERCENT; 
        }

        // list which represent records containing primary key info
        List records = new ArrayList ();
        ResultSetMetaData rmetadata = null;
        MMResultSet results = null;
        PreparedStatement prepareQuery = null;
        try {
            prepareQuery = driverConnection.prepareStatement(QUERY_PRIMARY_KEYS);

            prepareQuery.setObject(1, schema.toUpperCase());
            prepareQuery.setObject(2, table.toUpperCase());

            // make a query against runtimemetadata and get results
            results = (MMResultSet) prepareQuery.executeQuery();

            // build the list of records from server's Results object.
            while (results.next ()) {
                // list represents a record on the Results object.
                List currentRow = new ArrayList (7);
                // add values in the current record on the Results object to the list
                // number of values to be fetched from each row is MAX_COLUMNS.
                for(int i=0; i < JDBCColumnPositions.PRIMARY_KEYS.MAX_COLUMNS; i++) {
                    // get the value at the current index add it to currentRow
                    currentRow.add(results.getObject(i+1));
                }

                // add the current row to the list of records.
                records.add(currentRow);
            }// end of while

            // get the metadata for the results
            rmetadata = results.getMetaData();

        } catch (Exception e) {
            throw MMSQLException.create(e, JDBCPlugin.Util.getString("MMDatabaseMetadata.getPrimaryKey_error", table, e.getMessage())); //$NON-NLS-1$
        }

        logger.fine(JDBCPlugin.Util.getString("MMDatabaseMetadata.getPrimaryKey_success")); //$NON-NLS-1$

        // construct results object from column values and their metadata
        return createResultSet(records, rmetadata);
    }

    /**
     * create a empty primary keys result set.
     * @return
     */
    private ResultSet emptyPrimaryKeys() throws SQLException {
        String[] columnNames = new String[] {
                JDBCColumnNames.PRIMARY_KEYS.TABLE_CAT,
                JDBCColumnNames.PRIMARY_KEYS.TABLE_SCHEM,                
                JDBCColumnNames.PRIMARY_KEYS.TABLE_NAME,
                JDBCColumnNames.PRIMARY_KEYS.COLUMN_NAME,
                JDBCColumnNames.PRIMARY_KEYS.KEY_SEQ,
                JDBCColumnNames.PRIMARY_KEYS.PK_NAME
        };
        String[] dataTypes = new String[] {
                MMJDBCSQLTypeInfo.STRING,
                MMJDBCSQLTypeInfo.STRING,
                MMJDBCSQLTypeInfo.STRING,
                MMJDBCSQLTypeInfo.STRING,
                MMJDBCSQLTypeInfo.SHORT,
                MMJDBCSQLTypeInfo.STRING
        };
        return createEmptyResultSet(columnNames, dataTypes);
    }

    /**
     * <p>Gets a description of the input, output and results associated with certain
     * stored procedures matching the given procedureNamePattern. Catalog and
     * schema names are not used to narrow down the search, but they should match
     * the virtualdatabasename and version used to obtain this driver connection.</p>
     * @param name of the catalog the procedure is present in.
     * @param pattern of schama name the procedure is present in.
     * @param pattern which is to be matched by the procedureNames.
     * @param pattern to be matched by the column names.
     * @return ResultSet containing the metadata info for procedure parameters.
     * @throws SQLException if there is an error obtaining metamatrix results.
     */
    public ResultSet getProcedureColumns(String catalog, String schemaPattern, String procedureNamePattern, String columnNamePattern) throws SQLException {
        if ((catalog != null) && (catalog.trim().length() > 0)) {
            return emptyProcedureColumns();
        }
        if (schemaPattern == null) {
            schemaPattern = PERCENT;
        }
        
        // Get all columns in all procedures if procedureNamePattern is null
        if(procedureNamePattern == null) {
            procedureNamePattern = PERCENT; 
        }
        // Get all columns if columnNamePattern is null
        if(columnNamePattern == null) {
            columnNamePattern = PERCENT; 
        }

        // list which represent records containing procedure column info
        List records = new ArrayList ();

        ResultSetMetaData rmetadata = null;
        MMResultSet results = null;
        PreparedStatement prepareQuery = null;
        try {
            prepareQuery = driverConnection.prepareStatement(QUERY_PROCEDURE_COLUMNS);

            prepareQuery.setObject(1, schemaPattern.toUpperCase());
            prepareQuery.setObject(2, procedureNamePattern.toUpperCase());
            prepareQuery.setObject(3, columnNamePattern.toUpperCase());

            // make a query against runtimemetadata and get results
            results = (MMResultSet) prepareQuery.executeQuery();
            // build the list of records from server's Results object.
            while (results.next ()) {
                // list represents a record on the Results object.
                List currentRow = new ArrayList (13);
                // add values in the current record on the Results object to the list
                // number of values to be fetched from each row is MAX_COLUMNS.
                for(int i=0; i < JDBCColumnPositions.PROCEDURE_COLUMNS.MAX_COLUMNS; i++) {
                    // get the value at the current index add it to currentRow
                    currentRow.add(results.getObject(i+1));
                }

                // add the current row to the list of records.
                records.add(currentRow);
            }// end of while
            rmetadata = results.getMetaData();

        } catch (Exception e) {
           throw MMSQLException.create(e, JDBCPlugin.Util.getString("MMDatabaseMetadata.getProcCol_error", columnNamePattern, e.getMessage())); //$NON-NLS-1$
        }

        logger.fine(JDBCPlugin.Util.getString("MMDatabaseMetadata.getProcCol_success", columnNamePattern, procedureNamePattern)); //$NON-NLS-1$

        // construct results object from column values and their metadata
        return createResultSet(records, rmetadata);
    }

    /**
     * @return
     */
    private ResultSet emptyProcedureColumns() throws SQLException {
        String[] columnNames = new String[] {
                JDBCColumnNames.PROCEDURE_COLUMNS.PROCEDURE_CAT,
                JDBCColumnNames.PROCEDURE_COLUMNS.PROCEDURE_SCHEM, 
                JDBCColumnNames.PROCEDURE_COLUMNS.PROCEDURE_NAME, 
                JDBCColumnNames.PROCEDURE_COLUMNS.COLUMN_NAME, 
                JDBCColumnNames.PROCEDURE_COLUMNS.COLUMN_TYPE, 
                JDBCColumnNames.PROCEDURE_COLUMNS.DATA_TYPE, 
                JDBCColumnNames.PROCEDURE_COLUMNS.TYPE_NAME, 
                JDBCColumnNames.PROCEDURE_COLUMNS.PRECISION, 
                JDBCColumnNames.PROCEDURE_COLUMNS.LENGTH, 
                JDBCColumnNames.PROCEDURE_COLUMNS.SCALE, 
                JDBCColumnNames.PROCEDURE_COLUMNS.RADIX, 
                JDBCColumnNames.PROCEDURE_COLUMNS.NULLABLE, 
                JDBCColumnNames.PROCEDURE_COLUMNS.REMARKS, 
                JDBCColumnNames.PROCEDURE_COLUMNS.POSITION                
        };
        String[] dataTypes = new String[] {
                MMJDBCSQLTypeInfo.STRING,
                MMJDBCSQLTypeInfo.STRING,
                MMJDBCSQLTypeInfo.STRING,
                MMJDBCSQLTypeInfo.STRING,
                MMJDBCSQLTypeInfo.SHORT,
                MMJDBCSQLTypeInfo.INTEGER,
                MMJDBCSQLTypeInfo.STRING,
                MMJDBCSQLTypeInfo.INTEGER,
                MMJDBCSQLTypeInfo.INTEGER,
                MMJDBCSQLTypeInfo.SHORT,
                MMJDBCSQLTypeInfo.SHORT,
                MMJDBCSQLTypeInfo.SHORT,
                MMJDBCSQLTypeInfo.STRING,
                MMJDBCSQLTypeInfo.SHORT
        };
        return createEmptyResultSet(columnNames, dataTypes);
    }

    /**
     * <p>Gets description of all the available stored procedures whose names match
     * the given pattern. Catalog and schemaPattern are not used to narrow down
     * the search, but they should match the virtualdatabasename and version used
     * to obtain this driver connection.
     * @param name of the catalog.
     * @param name of the schema.
     * @param pattern which is to be matched by the procedureNames.
     * @return ResultSet object which gives the metadata information about procedures.
     * @throws SQLException if there is an error obtaining metamatrix results.
     */
    public ResultSet getProcedures(String catalog, String schemaPattern, String procedureNamePattern) throws SQLException {
        if ((catalog != null) && (catalog.trim().length() > 0)) {
            return emptyProcedures();
        }
        if (schemaPattern == null) {
            schemaPattern = PERCENT;
        }
        
        // Get all procedures if procedureNamePattern is null
        if(procedureNamePattern == null) {
            procedureNamePattern = PERCENT; 
        }

        // list which represent records containing procedure info
        List records = new ArrayList ();
        ResultSetMetaData rmetadata = null;
        MMResultSet results = null;
        PreparedStatement prepareQuery = null;

        try {
            prepareQuery = driverConnection.prepareStatement(QUERY_PROCEDURES);

            prepareQuery.setObject(1, schemaPattern.toUpperCase());
            prepareQuery.setObject(2, procedureNamePattern.toUpperCase());

            // make a query against runtimemetadata and get results
            results = (MMResultSet) prepareQuery.executeQuery();

            // build the list of records from server's Results object.
            while (results.next ()) {
                // list represents a record on the Results object.
                List currentRow = new ArrayList (JDBCColumnPositions.PROCEDURES.MAX_COLUMNS);
                // add values in the current record on the Results object to the list
                // number of values to be fetched from each row is MAX_COLUMNS.
                // there are 3 columns are reserved for future use
                for(int i=0; i < JDBCColumnPositions.PROCEDURES.MAX_COLUMNS; i++) {
                    // get the value at the current index add it to currentRow
                    currentRow.add(results.getObject(i+1));
                }

                // add the current row to the list of records.
                records.add(currentRow);

            }// end of while
            // get the metadata for the results
            rmetadata = results.getMetaData();

        } catch (Exception e) {
            throw MMSQLException.create(e, JDBCPlugin.Util.getString("MMDatabaseMetadata.getProc_error", procedureNamePattern, e.getMessage())); //$NON-NLS-1$
        }

        logger.fine(JDBCPlugin.Util.getString("MMDatabaseMetadata.getProc_success", procedureNamePattern)); //$NON-NLS-1$

        // construct results object from column values and their metadata
        return createResultSet(records, rmetadata);
    }

    /**
     * @return
     */
    private ResultSet emptyProcedures() throws SQLException {
        String[] columnNames = new String[] {
                JDBCColumnNames.PROCEDURES.PROCEDURE_CAT,
                JDBCColumnNames.PROCEDURES.PROCEDURE_SCHEM, 
                JDBCColumnNames.PROCEDURES.PROCEDURE_NAME, 
                JDBCColumnNames.PROCEDURES.RESERVED_1,
                JDBCColumnNames.PROCEDURES.RESERVED_2, 
                JDBCColumnNames.PROCEDURES.RESERVED_3, 
                JDBCColumnNames.PROCEDURES.REMARKS, 
                JDBCColumnNames.PROCEDURES.PROCEDURE_TYPE 
        };
        String[] dataTypes = new String[] {
                MMJDBCSQLTypeInfo.STRING,
                MMJDBCSQLTypeInfo.STRING,
                MMJDBCSQLTypeInfo.STRING,
                MMJDBCSQLTypeInfo.STRING,
                MMJDBCSQLTypeInfo.STRING,
                MMJDBCSQLTypeInfo.STRING,
                MMJDBCSQLTypeInfo.STRING,
                MMJDBCSQLTypeInfo.SHORT
        };
        return createEmptyResultSet(columnNames, dataTypes);
    }

    /**
     * <p>Gets MetaMatrix's preferred term for procedures
     * @return String representing metamatrix procedure term.
     * @throws SQLException, should never occur
     */
    public String getProcedureTerm() throws SQLException {
        return PROCEDURE_TERM;
    }

    /**
     * <p>Gets the schema names available for this connection. The results are ordered
     * by schema name. Schema information retreived only for the schema to which
     * used in obtaining this driver connection.
     * @return ResultsSet object containing schema and catalog names.
     * @throws SQLException if there is an error obtaining metamatrix results.
     */
    public ResultSet getSchemas() throws SQLException {

        // list which represent records containing schema info
        List records = new ArrayList ();

        ResultSetMetaData rmetadata = null;
        MMResultSet results = null;
        PreparedStatement prepareQuery = null;
        try {
            prepareQuery = driverConnection.prepareStatement(QUERY_SCHEMAS);
            // make a query against runtimemetadata and get results
            results = (MMResultSet) prepareQuery.executeQuery();

            while (results.next ()) {
                // each row will have only one column(Virtual database name)
                List currentRow = new ArrayList (2);

                for(int i = 0; i < JDBCColumnPositions.SCHEMAS.MAX_COLUMNS; i++) {
                    // get the value at the current index add it to currentRow
                    currentRow.add(results.getObject(i+1));
                }

                records.add(currentRow);
            }

            // Get the metadata for the results
            rmetadata = results.getMetaData();

        } catch(Exception e) {
            throw MMSQLException.create(e, JDBCPlugin.Util.getString("MMDatabaseMetadata.getschema_error", e.getMessage())); //$NON-NLS-1$
        }

        logger.fine(JDBCPlugin.Util.getString("MMDatabaseMetadata.getschema_success")); //$NON-NLS-1$

        // construct results object from column values and their metadata
        return createResultSet(records, rmetadata);
    }

    /**
     * <p>Gets MetaMatrix's preferred term for schema
     * @return String object giving schema term
     * @throws SQLException, should never occur
     */
    public String getSchemaTerm() throws SQLException {
        return SCHEMA_TERM;
    }

    /**
     * <p>Gets the string that can be used to escape "_" or "%" wildcards in the
     * string search pattern used for search parameters
     * @return String that is used for escaping wildcards
     * @throws SQLException, should never occur
     */
    public String getSearchStringEscape() throws SQLException {
        return ESCAPE_SEARCH_STRING;
    }

    /**
     * <p>Get metamatrix keywords that are not SQL-92 keyword
     * @return String object giving non SQL-92 keywords
     * @throws SQLException, should never occur
     */
    public String getSQLKeywords() throws SQLException {
        return KEY_WORDS;
    }

    /**
     * Indicates whether the SQLSTATE returned by SQLException.getSQLState is X/Open
     * (now known as Open Group) SQL CLI or SQL99.
     * @return intValue, the type of SQLSTATE; one of: sqlStateXOpen or sqlStateSQL99
     */
    public int getSQLStateType() throws SQLException {
        //return sqlStateSQL99;
        return 2;
    }

    /**
     * <p>Gets the Open Group CLI names for metamatrix string functions
     * @return String containing string function names
     * @throws SQLException, should never occur
     */
    public String getStringFunctions() throws SQLException {
        return STRING_FUNCTIONS;
    }

    /**
     * Retrieves a description of the table hierarchies defined in a
     * particular schema in this database.
     * @param catalog A catalog name; "" retrieves those without a catalog;
     * null means drop catalog name from the selection criteria.
     * @param schemaPattern A schema name pattern; "" retrieves those without a schema.
     * @param tableNamePattern A table name pattern; may be a fully-qualified name.
     * @throws SQLException since not supported
     */
    public ResultSet getSuperTables(String catalog, String schemaPattern,
        String tableNamePattern) throws SQLException {
        List records = new ArrayList (0);

        /***********************************************************************
        * Hardcoding JDBC column names for the columns returned in results object
        ***********************************************************************/
        Map[] metadataList = new Map[4];

        // HardCoding metadata details for TABLE_CAT column
        metadataList[0] = getColumnMetadata(null, JDBCColumnNames.SUPER_TABLES.TABLE_CAT,
                            MMJDBCSQLTypeInfo.STRING,  ResultsMetadataConstants.NULL_TYPES.NULLABLE);

        // HardCoding metadata details for TABLE_SCHEM column
        metadataList[1] = getColumnMetadata(null, JDBCColumnNames.SUPER_TABLES.TABLE_SCHEM,
                            MMJDBCSQLTypeInfo.STRING,  ResultsMetadataConstants.NULL_TYPES.NULLABLE);

        // HardCoding metadata details for TABLE_NAME column
        metadataList[2] = getColumnMetadata(null, JDBCColumnNames.SUPER_TABLES.TABLE_NAME,
                            MMJDBCSQLTypeInfo.STRING, ResultsMetadataConstants.NULL_TYPES.NOT_NULL);

        // HardCoding metadata details for SUPERTABLE_NAME column
        metadataList[3] = getColumnMetadata(null, JDBCColumnNames.SUPER_TABLES.SUPERTABLE_NAME,
                            MMJDBCSQLTypeInfo.STRING, ResultsMetadataConstants.NULL_TYPES.NOT_NULL);

        // construct results object from column values and their metadata
        return createResultSet(records, metadataList);

    }

    /**
     * Retrieves a description of the user-defined type (UDT) hierarchies
     * defined in a particular schema in this database.
     * @param catalog A catalog name; "" retrieves those without a catalog;
     * null means drop catalog name from the selection criteria.
     * @param schemaPattern A schema name pattern; "" retrieves those without a schema.
     * @param tableNamePattern A table name pattern; may be a fully-qualified name.
     */
    public ResultSet getSuperTypes(String catalog, String schemaPattern,
        String tableNamePattern) throws SQLException {
        List records = new ArrayList (0);

        /***********************************************************************
        * Hardcoding JDBC column names for the columns returned in results object
        ***********************************************************************/
        Map[] metadataList = new Map[6];

        // HardCoding metadata details for TYPE_CAT column
        metadataList[0] = getColumnMetadata(null, JDBCColumnNames.SUPER_TYPES.TYPE_CAT,
                            MMJDBCSQLTypeInfo.STRING,  ResultsMetadataConstants.NULL_TYPES.NULLABLE);

        // HardCoding metadata details for TYPE_SCHEM column
        metadataList[1] = getColumnMetadata(null, JDBCColumnNames.SUPER_TYPES.TYPE_SCHEM,
                            MMJDBCSQLTypeInfo.STRING,  ResultsMetadataConstants.NULL_TYPES.NULLABLE);

        // HardCoding metadata details for TYPE_NAME column
        metadataList[2] = getColumnMetadata(null, JDBCColumnNames.SUPER_TYPES.TYPE_NAME,
                            MMJDBCSQLTypeInfo.STRING, ResultsMetadataConstants.NULL_TYPES.NOT_NULL);

        // HardCoding metadata details for SUPERTYPE_CAT column
        metadataList[3] = getColumnMetadata(null, JDBCColumnNames.SUPER_TYPES.SUPERTYPE_CAT,
                            MMJDBCSQLTypeInfo.STRING, ResultsMetadataConstants.NULL_TYPES.NULLABLE);

        // HardCoding metadata details for SUPERTYPE_SCHEM column
        metadataList[4] = getColumnMetadata(null, JDBCColumnNames.SUPER_TYPES.SUPERTYPE_SCHEM,
                            MMJDBCSQLTypeInfo.STRING, ResultsMetadataConstants.NULL_TYPES.NULLABLE);
        
        // HardCoding metadata details for SUPERTYPE_NAME column
        metadataList[5] = getColumnMetadata(null, JDBCColumnNames.SUPER_TYPES.SUPERTYPE_NAME,
                            MMJDBCSQLTypeInfo.STRING, ResultsMetadataConstants.NULL_TYPES.NOT_NULL);
        
        // construct results object from column values and their metadata
        return createResultSet(records, metadataList);
    }

    /**
     * <p>Gets the Open Group CLI names for metamatrix system functions
     * @return String containing system function names
     * @throws SQLException, should never occur
     */
    public String getSystemFunctions() throws SQLException {
        return SYSTEM_FUNCTIONS; // there are no system functions
    }

    /**
     * <p>Gets a description of access rights for the table of the given name.
     * <p>Catalog and schemaPattern are not used to narrow down the search, but they
     * should match the virtualdatabasename and version used to obtain this driver
     * connection.
     * @param name of the catalog the table is present in.
     * @param pattern of schama name the table is present in.
     * @param pattern of table names whose privilage info is needed.
     * @return ResultSet containing table privilages info.
     * @throws SQLException if there is an error obtaining metamatrix results.
     */
    public ResultSet getTablePrivileges(String catalog, String schemaPattern, String tableName) throws SQLException {
        List records = new ArrayList (0);
        /***********************************************************************
        * Hardcoding JDBC column names for the columns returned in results object
        ***********************************************************************/
        Map[] metadataList = new Map[7];

        metadataList[0] = getColumnMetadata(null, JDBCColumnNames.PRIVILEGES.TABLE_CAT,
                            MMJDBCSQLTypeInfo.STRING,  ResultsMetadataConstants.NULL_TYPES.NULLABLE);
        metadataList[1] = getColumnMetadata(null, JDBCColumnNames.PRIVILEGES.TABLE_SCHEM,
                            MMJDBCSQLTypeInfo.STRING,  ResultsMetadataConstants.NULL_TYPES.NULLABLE);
        metadataList[2] = getColumnMetadata(null, JDBCColumnNames.PRIVILEGES.TABLE_NAME,
                            MMJDBCSQLTypeInfo.STRING, ResultsMetadataConstants.NULL_TYPES.NOT_NULL);
        metadataList[3] = getColumnMetadata(null, JDBCColumnNames.PRIVILEGES.GRANTOR,
                            MMJDBCSQLTypeInfo.STRING, ResultsMetadataConstants.NULL_TYPES.NULLABLE);
        metadataList[4] = getColumnMetadata(null, JDBCColumnNames.PRIVILEGES.GRANTEE,
                            MMJDBCSQLTypeInfo.STRING, ResultsMetadataConstants.NULL_TYPES.NOT_NULL);
        metadataList[5] = getColumnMetadata(null, JDBCColumnNames.PRIVILEGES.PRIVILEGE,
                            MMJDBCSQLTypeInfo.STRING, ResultsMetadataConstants.NULL_TYPES.NOT_NULL);
        metadataList[6] = getColumnMetadata(null, JDBCColumnNames.PRIVILEGES.IS_GRANTABLE,
                            MMJDBCSQLTypeInfo.STRING, ResultsMetadataConstants.NULL_TYPES.NULLABLE);
       
        return createResultSet(records, metadataList);
        
    }

    /**
     * <p>Gets a description of tables whose table name matches tableNamePattern.
     * <p>Catalog, schemaPattern and types[] are not used to narrow down the search,
     * but catalog and schemaPattern should match the virtualdatabasename and
     * version respectively used to obtain this driver connection.
     * Note:
     * supports 1.4 API
     * @param name of the catalog in which tables are present.
     * @param pattern of schema names which in which the tables are present.
     * @param pattern of tables names.
     * @param list of possible table types.
     * @return ResultSet containing Table metadata information.
     * @throws SQLException if there is an error obtaining metamatrix results.
     */
    public ResultSet getTables(String catalog, String schemaPattern, String tableNamePattern, String types[]) throws SQLException {
        if ((catalog != null) && (catalog.trim().length() > 0)) {
            return emptyTablesResultSet();
        }
        
        if (schemaPattern == null) {
            schemaPattern = PERCENT;    
        }
        
        // Get all tables if tableNamePattern is null
        if(tableNamePattern == null) {
            tableNamePattern = PERCENT; 
        }

        // list which represent records containing tables info
        List records = new ArrayList ();

        // query string to be submitted to get table metadata info
        StringBuffer sqlQuery = new StringBuffer(QUERY_TABLES);

        StringBuffer typesString = new StringBuffer(); // criteria string for different table types

        if (types != null) {
            // construct the criteria string
            for(int i=0; i < types.length; i++) {
                if (types[i] != null && types[i].length() > 0) {
                    if (i > 0) {
                        typesString.append(" OR CASE WHEN IsSystem = 'true' and UCASE(Type) = 'TABLE' THEN 'SYSTEM TABLE' ELSE UCASE(Type) END LIKE ?"); //$NON-NLS-1$
                    } else {
                        typesString.append("(CASE WHEN IsSystem = 'true' and UCASE(Type) = 'TABLE' THEN 'SYSTEM TABLE' ELSE UCASE(Type) END LIKE ?"); //$NON-NLS-1$
                    }
                }
            }
        }

        if (typesString.length() != 0) {
            typesString.append(")"); //$NON-NLS-1$
            sqlQuery.append(" AND ").append(typesString.toString()).append(" AND Type IS NOT NULL"); //$NON-NLS-1$ //$NON-NLS-2$

        }
        sqlQuery.append(" ORDER BY TABLE_TYPE, TABLE_SCHEM, TABLE_NAME"); //$NON-NLS-1$


        ResultSetMetaData rmetadata = null;
        MMResultSet results = null;
        PreparedStatement prepareQuery = null;

        try {
            prepareQuery = driverConnection.prepareStatement(sqlQuery.toString());
            int columnIndex = 0;
            prepareQuery.setObject(++columnIndex, schemaPattern.toUpperCase());
            prepareQuery.setObject(++columnIndex, tableNamePattern.toUpperCase());

            if(types != null) {
                for(int i=0; i < types.length; i++) {
                    if (types[i] != null && types[i].length() > 0) {
                        prepareQuery.setObject(++columnIndex, types[i].toUpperCase());
                    }
                }
            }

            // make a query against runtimemetadata and get results
            results = (MMResultSet) prepareQuery.executeQuery();

            // build the list of records from server's Results object.
            while (results.next ()) {
                // list represents a record on the Results object.
                List currentRow = new ArrayList (11);
                // add values in the current record on the Results object to the list
                // number of values to be fetched from each row is MAX_COLUMNS.
                for(int i=0; i < JDBCColumnPositions.TABLES.MAX_COLUMNS; i++) {
                    // get the value at the current index add it to currentRow
                    currentRow.add(results.getObject(i+1));
                }

                // add the current row to the list of records.
                records.add(currentRow);
            }// end of while

            // get the metadata for the results
            rmetadata = results.getMetaData();
        } catch (SQLException e) {
            throw e;
        } catch (Exception e) {
            throw MMSQLException.create(e, JDBCPlugin.Util.getString("MMDatabaseMetadata.getTable_error", tableNamePattern, e.getMessage())); //$NON-NLS-1$
        }

        // close the results (need to close case results exceed cursor size)
        //results.close();

        // close the PreparedStatement, too. Because of the way of closing request in current framework,
        // manually send out a close request is very necessary for PreparedStatement.
        //prepareQuery.close();

        logger.fine(JDBCPlugin.Util.getString("MMDatabaseMetadata.getTable_success", tableNamePattern)); //$NON-NLS-1$

        // construct results object from column values and their metadata
        return createResultSet(records, rmetadata);
    }

    /**
     * Return a empty tables resultset
     * @return
     */
    private ResultSet emptyTablesResultSet() throws SQLException {
        String[] columnNames = {
                JDBCColumnNames.TABLES.TABLE_CAT,
                JDBCColumnNames.TABLES.TYPE_SCHEM,
                JDBCColumnNames.TABLES.TABLE_NAME,
                JDBCColumnNames.TABLES.TABLE_TYPE,
                JDBCColumnNames.TABLES.REMARKS,
                JDBCColumnNames.TABLES.TYPE_CAT,
                JDBCColumnNames.TABLES.TYPE_SCHEM,
                JDBCColumnNames.TABLES.TYPE_NAME,
                JDBCColumnNames.TABLES.SELF_REFERENCING_COL_NAME,
                JDBCColumnNames.TABLES.REF_GENERATION,
                JDBCColumnNames.TABLES.ISPHYSICAL
        };
        String[] dataTypes = {  
                MMJDBCSQLTypeInfo.STRING,
                MMJDBCSQLTypeInfo.STRING,
                MMJDBCSQLTypeInfo.STRING,
                MMJDBCSQLTypeInfo.STRING,
                MMJDBCSQLTypeInfo.STRING,
                MMJDBCSQLTypeInfo.STRING,
                MMJDBCSQLTypeInfo.STRING,
                MMJDBCSQLTypeInfo.STRING,
                MMJDBCSQLTypeInfo.STRING,
                MMJDBCSQLTypeInfo.STRING,
                MMJDBCSQLTypeInfo.BOOLEAN
        };
        return createEmptyResultSet(columnNames, dataTypes);
    }
    /**
     * <p>Gets the table types available to metamatrix. The results are ordered by
     * table type
     * @return ResultSet object containing hardcoded table type info.
     * @throws SQLException, should never occur.
     */
    public ResultSet getTableTypes() throws SQLException {

        // list which represent records containing Table Type info
        List records = new ArrayList (5);
        /********************************
        * HardCoding JDBC specific values
        *********************************/

        records.add(Arrays.asList(new String[] {"DOCUMENT"})); //$NON-NLS-1$
        records.add(Arrays.asList(new String[] {"TABLE"})); //$NON-NLS-1$
        records.add(Arrays.asList(new String[] {"VIEW"})); //$NON-NLS-1$
        records.add(Arrays.asList(new String[] {"XMLSTAGINGTABLE"})); //$NON-NLS-1$
        records.add(Arrays.asList(new String[] {"SYSTEM TABLE"})); //$NON-NLS-1$

        /***********************************************************************
        * Hardcoding JDBC column names for the columns returned in results object
        ***********************************************************************/

        Map[] metadataList = new Map[1];

        metadataList[0] = getColumnMetadata(null, JDBCColumnNames.TABLE_TYPES.TABLE_TYPE, 
                            MMJDBCSQLTypeInfo.STRING, ResultsMetadataConstants.NULL_TYPES.NOT_NULL);

        logger.fine(JDBCPlugin.Util.getString("MMDatabaseMetadata.getTableType_success")); //$NON-NLS-1$

        // construct results object from column values and their metadata
        return createResultSet(records, metadataList);
    }

    /**
     * <p>Gets the Open Group CLI names for metamatrix time and date functions
     * @return String representing time and date functions in MetaMatrix
     * @throws SQLException, should never occur
     */
    public String getTimeDateFunctions() throws SQLException {
        return DATE_FUNCTIONS;
    }

    /**
     * <p>Gets the description of all datatypes supported by metamatrix.
     * @return ResultSet object containing info about the datatypes supported by MetaMatrix.
     * @throws SQLException if there is an error obtaining metamatrix results.
     */
    public ResultSet getTypeInfo() throws SQLException {

        // list which represent records containing data type info
        List records = new ArrayList ();

        records.add(Arrays.asList(createTypeInfoRow("boolean",  "{b'", "}", Boolean.TRUE, Boolean.TRUE, 0))); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        records.add(Arrays.asList(createTypeInfoRow("byte", null, null, Boolean.TRUE, Boolean.TRUE, 0))); //$NON-NLS-1$ 
        records.add(Arrays.asList(createTypeInfoRow("long", null, null, Boolean.FALSE, Boolean.FALSE, 10))); //$NON-NLS-1$ 
        records.add(Arrays.asList(createTypeInfoRow("char", "'", "'", Boolean.TRUE, Boolean.TRUE, 0))); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        records.add(Arrays.asList(createTypeInfoRow("bigdecimal",null, null, Boolean.FALSE, Boolean.TRUE, 10))); //$NON-NLS-1$ 
        records.add(Arrays.asList(createTypeInfoRow("biginteger", null, null, Boolean.FALSE, Boolean.FALSE, 10))); //$NON-NLS-1$ 
        records.add(Arrays.asList(createTypeInfoRow("integer",  null, null, Boolean.FALSE, Boolean.FALSE, 10))); //$NON-NLS-1$ 
        records.add(Arrays.asList(createTypeInfoRow("short", null, null, Boolean.FALSE, Boolean.FALSE, 10))); //$NON-NLS-1$ 
        records.add(Arrays.asList(createTypeInfoRow("float", null, null, Boolean.FALSE, Boolean.FALSE, 10))); //$NON-NLS-1$ 
        records.add(Arrays.asList(createTypeInfoRow("double",  null, null, Boolean.FALSE, Boolean.FALSE, 10))); //$NON-NLS-1$ 
        records.add(Arrays.asList(createTypeInfoRow("string", "'", "'", Boolean.TRUE, Boolean.TRUE, 0))); //$NON-NLS-1$ //$NON-NLS-2$//$NON-NLS-3$
        records.add(Arrays.asList(createTypeInfoRow("xml", null, null, Boolean.TRUE, Boolean.TRUE, 0))); //$NON-NLS-1$ 
        records.add(Arrays.asList(createTypeInfoRow("date", "{d'", "}", Boolean.TRUE, Boolean.TRUE, 0))); //$NON-NLS-1$ //$NON-NLS-2$//$NON-NLS-3$
        records.add(Arrays.asList(createTypeInfoRow("time", "{t'", "}", Boolean.TRUE, Boolean.TRUE, 0))); //$NON-NLS-1$ //$NON-NLS-2$//$NON-NLS-3$
        records.add(Arrays.asList(createTypeInfoRow("timestamp",  "{ts'", "}", Boolean.TRUE, Boolean.TRUE, 0))); //$NON-NLS-1$ //$NON-NLS-2$//$NON-NLS-3$
        records.add(Arrays.asList(createTypeInfoRow("object", null, null, Boolean.TRUE, Boolean.TRUE, 0))); //$NON-NLS-1$ 
        records.add(Arrays.asList(createTypeInfoRow("blob", null, null, Boolean.TRUE, Boolean.TRUE, 0))); //$NON-NLS-1$ 
        records.add(Arrays.asList(createTypeInfoRow("clob", null, null, Boolean.TRUE, Boolean.TRUE, 0))); //$NON-NLS-1$ 

        Map[] metadataList = new Map[18];

        metadataList[0] = getColumnMetadata("System.DataTypes", JDBCColumnNames.TYPE_INFO.TYPE_NAME, MMJDBCSQLTypeInfo.STRING,  ResultsMetadataConstants.NULL_TYPES.NOT_NULL, ResultsMetadataConstants.SEARCH_TYPES.SEARCHABLE, Boolean.FALSE, Boolean.FALSE, Boolean.FALSE);//$NON-NLS-1$ 
        metadataList[1] = getColumnMetadata("System.DataTypes", JDBCColumnNames.TYPE_INFO.DATA_TYPE, MMJDBCSQLTypeInfo.INTEGER,  ResultsMetadataConstants.NULL_TYPES.NULLABLE, ResultsMetadataConstants.SEARCH_TYPES.SEARCHABLE, Boolean.TRUE, Boolean.TRUE, Boolean.FALSE);//$NON-NLS-1$ 
        metadataList[2] = getColumnMetadata("System.DataTypes", JDBCColumnNames.TYPE_INFO.PRECISION, MMJDBCSQLTypeInfo.INTEGER,  ResultsMetadataConstants.NULL_TYPES.NULLABLE, ResultsMetadataConstants.SEARCH_TYPES.SEARCHABLE, Boolean.TRUE, Boolean.TRUE, Boolean.FALSE);//$NON-NLS-1$ 
        metadataList[3] = getColumnMetadata("System.DataTypes", JDBCColumnNames.TYPE_INFO.LITERAL_PREFIX, MMJDBCSQLTypeInfo.STRING,  ResultsMetadataConstants.NULL_TYPES.NULLABLE,  ResultsMetadataConstants.SEARCH_TYPES.SEARCHABLE, Boolean.TRUE, Boolean.TRUE, Boolean.FALSE);//$NON-NLS-1$ 
        metadataList[4] = getColumnMetadata("System.DataTypes", JDBCColumnNames.TYPE_INFO.LITERAL_SUFFIX, MMJDBCSQLTypeInfo.STRING,  ResultsMetadataConstants.NULL_TYPES.NULLABLE,  ResultsMetadataConstants.SEARCH_TYPES.SEARCHABLE, Boolean.TRUE, Boolean.TRUE, Boolean.FALSE);//$NON-NLS-1$ 
        metadataList[5] = getColumnMetadata("System.DataTypes", JDBCColumnNames.TYPE_INFO.CREATE_PARAMS, MMJDBCSQLTypeInfo.STRING,  ResultsMetadataConstants.NULL_TYPES.NULLABLE,  ResultsMetadataConstants.SEARCH_TYPES.SEARCHABLE, Boolean.TRUE, Boolean.TRUE, Boolean.FALSE);//$NON-NLS-1$ 
        metadataList[6] = getColumnMetadata("System.DataTypes", JDBCColumnNames.TYPE_INFO.NULLABLE, MMJDBCSQLTypeInfo.SHORT,  ResultsMetadataConstants.NULL_TYPES.NULLABLE,  ResultsMetadataConstants.SEARCH_TYPES.SEARCHABLE, Boolean.TRUE, Boolean.TRUE, Boolean.FALSE);//$NON-NLS-1$ 
        metadataList[7] = getColumnMetadata("System.DataTypes", JDBCColumnNames.TYPE_INFO.CASE_SENSITIVE, MMJDBCSQLTypeInfo.BOOLEAN,  ResultsMetadataConstants.NULL_TYPES.NOT_NULL, ResultsMetadataConstants.SEARCH_TYPES.SEARCHABLE, Boolean.FALSE, Boolean.FALSE, Boolean.TRUE);//$NON-NLS-1$ 
        metadataList[8] = getColumnMetadata("System.DataTypes", JDBCColumnNames.TYPE_INFO.SEARCHABLE, MMJDBCSQLTypeInfo.SHORT,  ResultsMetadataConstants.NULL_TYPES.NULLABLE, ResultsMetadataConstants.SEARCH_TYPES.SEARCHABLE, Boolean.TRUE, Boolean.TRUE, Boolean.FALSE);//$NON-NLS-1$ 
        metadataList[9] = getColumnMetadata("System.DataTypes", JDBCColumnNames.TYPE_INFO.UNSIGNED_ATTRIBUTE, MMJDBCSQLTypeInfo.BOOLEAN,  ResultsMetadataConstants.NULL_TYPES.NULLABLE, ResultsMetadataConstants.SEARCH_TYPES.SEARCHABLE, Boolean.TRUE, Boolean.TRUE, Boolean.FALSE);//$NON-NLS-1$ 
        metadataList[10] = getColumnMetadata("System.DataTypes", JDBCColumnNames.TYPE_INFO.FIXED_PREC_SCALE, MMJDBCSQLTypeInfo.BOOLEAN,  ResultsMetadataConstants.NULL_TYPES.NULLABLE, ResultsMetadataConstants.SEARCH_TYPES.SEARCHABLE, Boolean.TRUE, Boolean.TRUE, Boolean.FALSE);//$NON-NLS-1$
        metadataList[11] = getColumnMetadata("System.DataTypes", JDBCColumnNames.TYPE_INFO.AUTOINCREMENT, MMJDBCSQLTypeInfo.BOOLEAN,  ResultsMetadataConstants.NULL_TYPES.NOT_NULL, ResultsMetadataConstants.SEARCH_TYPES.SEARCHABLE, Boolean.FALSE, Boolean.TRUE, Boolean.TRUE);//$NON-NLS-1$ 
        metadataList[12] = getColumnMetadata("System.DataTypes", JDBCColumnNames.TYPE_INFO.LOCAL_TYPE_NAME, MMJDBCSQLTypeInfo.STRING,  ResultsMetadataConstants.NULL_TYPES.NOT_NULL, ResultsMetadataConstants.SEARCH_TYPES.SEARCHABLE, Boolean.FALSE, Boolean.FALSE, Boolean.FALSE);//$NON-NLS-1$ 
        metadataList[13] = getColumnMetadata("System.DataTypes", JDBCColumnNames.TYPE_INFO.MINIMUM_SCALE, MMJDBCSQLTypeInfo.SHORT,  ResultsMetadataConstants.NULL_TYPES.NULLABLE, ResultsMetadataConstants.SEARCH_TYPES.SEARCHABLE, Boolean.TRUE, Boolean.TRUE, Boolean.FALSE);//$NON-NLS-1$ 
        metadataList[14] = getColumnMetadata("System.DataTypes", JDBCColumnNames.TYPE_INFO.MAXIMUM_SCALE, MMJDBCSQLTypeInfo.SHORT,  ResultsMetadataConstants.NULL_TYPES.NULLABLE, ResultsMetadataConstants.SEARCH_TYPES.SEARCHABLE, Boolean.TRUE, Boolean.TRUE, Boolean.FALSE);//$NON-NLS-1$ 
        metadataList[15] = getColumnMetadata("System.DataTypes", JDBCColumnNames.TYPE_INFO.SQL_DATA_TYPE, MMJDBCSQLTypeInfo.INTEGER,  ResultsMetadataConstants.NULL_TYPES.NULLABLE, ResultsMetadataConstants.SEARCH_TYPES.SEARCHABLE, Boolean.TRUE, Boolean.TRUE, Boolean.FALSE);//$NON-NLS-1$ 
        metadataList[16] = getColumnMetadata("System.DataTypes", JDBCColumnNames.TYPE_INFO.SQL_DATETIME_SUB, MMJDBCSQLTypeInfo.INTEGER,  ResultsMetadataConstants.NULL_TYPES.NULLABLE, ResultsMetadataConstants.SEARCH_TYPES.SEARCHABLE, Boolean.TRUE, Boolean.TRUE, Boolean.FALSE);//$NON-NLS-1$ 
        metadataList[17] = getColumnMetadata("System.DataTypes", JDBCColumnNames.TYPE_INFO.NUM_PREC_RADIX, MMJDBCSQLTypeInfo.INTEGER,  ResultsMetadataConstants.NULL_TYPES.NULLABLE, ResultsMetadataConstants.SEARCH_TYPES.SEARCHABLE, Boolean.FALSE, Boolean.FALSE, Boolean.FALSE);//$NON-NLS-1$ 

        ResultSetMetaData rmetadata = ResultsMetadataWithProvider.newInstance(StaticMetadataProvider.createWithData(metadataList, 0));

        logger.fine(JDBCPlugin.Util.getString("MMDatabaseMetadata.getTypes_success")); //$NON-NLS-1$

        // construct results object from column values and their metadata
        return createResultSet(records, rmetadata);
    }

    private Object[] createTypeInfoRow(String typeName, String prefix, String suffix, Boolean unsigned, Boolean fixedPrecScale, int radix){
        return new Object[] {typeName, new Integer(MMJDBCSQLTypeInfo.getSQLType(typeName)), ResultsMetadataDefaults.getDefaultPrecision(typeName), prefix, suffix, null, new Short((short)DatabaseMetaData.typeNullable), Boolean.FALSE, new Short((short)DatabaseMetaData.typeSearchable), unsigned, fixedPrecScale, Boolean.FALSE, typeName, new Short((short)0), new Short((short)255), null, null, new Integer(radix)};
    }
    
    /**
     * <p>Gets a description of the user-defined types defined in a particular
     * schema.  Schema-specific UDTs may have type JAVA_OBJECT, STRUCT, or DISTINCT.
     * Supports 1.4
     * @param catalog a catalog name
     * @param schemaPattern a schema name pattern
     * @param typeNamePattern a type name pattern
     * @param types a list of user-named types to include (JAVA_OBJECT, STRUCT,
     * or DISTINCT); null returns all types
     * @return ResultSet. Empty ResultSet object as this method is not supported.
     * @throws SQLException if catalog/schema info does not match for the given connection.
     */
    public ResultSet getUDTs(String catalog, String schemaPattern,
        String typeNamePattern, int[] types) throws SQLException {
        return emptyUDTSResultSet();
    }
    
    /**
     * Return a empty result set for to aid getUDTS() functions. 
     * @return ResultSet.
     */
    private ResultSet emptyUDTSResultSet() throws SQLException {
        String[] columnNames = new String[] {
                JDBCColumnNames.UDTS.TYPE_CAT,
                JDBCColumnNames.UDTS.TYPE_SCHEM,
                JDBCColumnNames.UDTS.TYPE_NAME,
                JDBCColumnNames.UDTS.CLASS_NAME,
                JDBCColumnNames.UDTS.DATA_TYPE,
                JDBCColumnNames.UDTS.REMARKS,
                JDBCColumnNames.UDTS.BASE_TYPE
        };
        String[] dataTypes = new String[] {
                MMJDBCSQLTypeInfo.STRING,
                MMJDBCSQLTypeInfo.STRING,
                MMJDBCSQLTypeInfo.STRING,
                MMJDBCSQLTypeInfo.STRING,
                MMJDBCSQLTypeInfo.STRING, 
                MMJDBCSQLTypeInfo.STRING,
                MMJDBCSQLTypeInfo.SHORT                
        };       
        return createEmptyResultSet(columnNames, dataTypes);
    }
    
    /**
     * <p>Gets the URL used to connect to metamatrix
     * @return URL used to connect.
     * @throws SQLException
     */
    public String getURL() throws SQLException {
        return driverConnection.getUrl();
    }

    /**
     * <p>Gets the user name as known to metamatrix
     * @return name if the user.
     */
    public String getUserName() throws SQLException {
        return driverConnection.getUserName();
    }

    /**
     * <p>Gets the description of the columns in a table that are automatically updated
     * when any value in a row is updated. The column descriptions are not ordered.
     * @param name of the catalog in which the table is present.
     * @param name of the schema in which the table is present.
     * @param name of the table which has the version columns.
     * @return ResultSet object containing version column information.
     * @throws SQLException, should never occur
     */
    public ResultSet getVersionColumns(String catalog, String schema, String table) throws SQLException {
        
        // ResultSet returned has the same columns as best row identifier
        // Method not supported, retuning empty ResultSet
        ResultSet resultSet = getBestRowIdentifier(catalog, schema, table, 0, true);

        logger.fine(JDBCPlugin.Util.getString("MMDatabaseMetadata.getVersionCols_success")); //$NON-NLS-1$

       return resultSet;
    }


    /**
     * <p>Checks whether a catalog name appears at the start of a fully qualified table
     * name. If it is not at the beginning, it appears at the end.</p>
     * @return if so return true, else false.
     * @throws SQLException, should never occur.
     */
    public boolean isCatalogAtStart() throws SQLException {
        return false;
    }

    /**
     * <p>Indicates whether or not a visible row insert can be detected
     * by calling ResultSet.rowInserted().</p>
     * @param result set type, i.e. ResultSet.TYPE_XXX
     * @return true if changes are detected by the resultset type
     * @throws SQLException, should never occur
     */
    public boolean insertsAreDetected(int type) throws SQLException {
        return false;
    }

    /**
     * <p>Checks whether the databases used by metamatrix are in read-only mode</p>
     * @return if so return true, else false.
     * @throws SQLException, should never occur.
     */
    public boolean isReadOnly() throws SQLException {
        return false;
    }

    /**
     * <p>Indicates whether updates made to a LOB are made on a copy or directly to the LOB. </p>
     * @return if so return true, else false.
     * @throws SQLException, should never occur.
     */
    public boolean locatorsUpdateCopy() throws SQLException {
        return false;
    }

    /**
     * <p>Checks whether the concatenation of a NULL vaule and a non-NULL value results
     * in a NULL value.</p>
     * @return if so return true, else false.
     * @throws SQLException, should never occur.
     */
    public boolean nullPlusNonNullIsNull() throws SQLException {
        return true;
    }

    /**
     * <p>Checks whether NULL vaules are sorted at the end regardless of sort order.</p>
     * @return if so return true, else false.
     * @throws SQLException, should never occur.
     */
    public boolean nullsAreSortedAtEnd() throws SQLException {
        return false;
    }

    /**
     * <p>Checks whether NULL vaules are sorted at the start regardless of sort order.</p>
     * @return if so return true, else false.
     * @throws SQLException, should never occur.
     */
    public boolean nullsAreSortedAtStart() throws SQLException {
        return false;
    }

    /**
     * <p>Checks whether NULL vaules are sorted high.</p>
     * @return if so return true, else false.
     * @throws SQLException, should never occur.
     */
    public boolean nullsAreSortedHigh() throws SQLException {
        return false;
    }

    /**
     * <p>Checks whether NULL vaules are sorted low.</p>
     * @return if so return true, else false.
     * @throws SQLException, should never occur.
     */
    public boolean nullsAreSortedLow() throws SQLException {
        return true;
    }

    /**
     * <p>Indicates whether a result set's own updates are visible.</p>
     * @param result set type, i.e. ResultSet.TYPE_XXX
     * @return <code>true</code> if updates are visible for the result set type;
     *        <code>false</code> otherwise
     * @throws SQLException, should never occur
     */
    public boolean ownUpdatesAreVisible(int type) throws SQLException {
        return false;
    }

    /**
     * <p>Indicates whether a result set's own deletes are visible.</p>
     * @param result set type, i.e. ResultSet.TYPE_XXX
     * @return <code>true</code> if deletes are visible for the result set type;
     *        <code>false</code> otherwise
     * @throws SQLException, should never occur
     */
    public boolean ownDeletesAreVisible(int type) throws SQLException {
        return false;
    }

    /**
     * <p>Indicates whether a result set's own inserts are visible.</p>
     * @param result set type, i.e. ResultSet.TYPE_XXX
     * @return <code>true</code> if inserts are visible for the result set type;
     *        <code>false</code> otherwise
     * @throws SQLException, should never occur
     */
    public boolean ownInsertsAreVisible(int type) throws SQLException {
        return false;
    }

    /**
     * <p>Indicates whether updates made by others are visible.</p>
     * @param result set type, i.e. ResultSet.TYPE_XXX
     * @return <code>true</code> if updates made by others
     * are visible for the result set type;
     *        <code>false</code> otherwise
     * @throws SQLException, should never occur
     */
    public boolean othersUpdatesAreVisible(int type) throws SQLException {
        return false;
    }

    /**
     * <p>Indicates whether deletes made by others are visible.</p>
     * @param result set type, i.e. ResultSet.TYPE_XXX
     * @return <code>true</code> if deletes made by others
     * are visible for the result set type;
     *        <code>false</code> otherwise
     * @throws SQLException, should never occur
     */
    public boolean othersDeletesAreVisible(int type) throws SQLException {
        return false;
    }
    /**
     * <p>Indicates whether inserts made by others are visible.</p>
     * @param result set type, i.e. ResultSet.TYPE_XXX
     * @return true if updates are visible for the result set type <code>true</code>
     * if inserts made by others are visible for the result set type;
     * <code>false</code> otherwise
     * @throws SQLException, should never occur
     */
    public boolean othersInsertsAreVisible(int type) throws SQLException {
        return false;
    }

    /**
     * <p>This method checks whether metamatrix treats mixed-case unquoted SQL identifiers
     * used in SQL statements as case-insensitive and stores them as all lowercase
     * in its metadata tables.</p>
     * @return if so return true, else false.
     * @throws SQLException, should never occur.
     */
    public boolean storesLowerCaseIdentifiers() throws SQLException {
        return false;
    }

    /**
     * <p>This method checks whether metamatrix treats mixed-case quoted SQL identifiers
     * used in SQL statements as case-insensitive and stores them as all lowercase
     * in its metadata tables.</p>
     * @return if so return true, else false.
     * @throws SQLException, should never occur.
     */
    public boolean storesLowerCaseQuotedIdentifiers() throws SQLException {
        return false;
    }

    /**
     * <p>This method checks whether metamatrix treats mixed-case unquoted SQL identifiers
     * used in SQL statements as case-insensitive and stores them as all mixedcase
     * in its metadata tables.</p>
     * @return if so return true, else false.
     * @throws SQLException, should never occur.
     */
    public boolean storesMixedCaseIdentifiers() throws SQLException {
        return true;
    }

    /**
     * <p>This method checks whether metamatrix treats mixed-case quoted SQL identifiers
     * used in SQL statements as case-insensitive and stores them in mixedcase
     * in its metadata tables.</p>
     * @return if so return true, else false.
     * @throws SQLException, should never occur.
     */
    public boolean storesMixedCaseQuotedIdentifiers() throws SQLException {
        return true;
    }

    /**
     * <p>This method checks whether metamatrix treats mixed-case unquoted SQL identifiers
     * used in SQL statements as case-insensitive and stores them as all uppercase
     * in its metadata tables.</p>
     * @return if so return true, else false.
     * @throws SQLException, should never occur.
     */
    public boolean storesUpperCaseIdentifiers() throws SQLException {
        return false;
    }

    /**
     * <p>This method checks whether metamatrix treats mixed-case quoted SQL identifiers
     * used in SQL statements as case-insensitive and stores them as all uppercase
     * in its metadata tables.</p>
     * @return if so return true, else false.
     * @throws SQLException, should never occur.
     */
    public boolean storesUpperCaseQuotedIdentifiers() throws SQLException {
        return false;
    }

    /**
     * <p>Checks whether metamatrix supports ALTER TABLE with add column.</p>
     * @return if so return true, else false.
     * @throws SQLException, should never occur.
     */
    public boolean supportsAlterTableWithAddColumn() throws SQLException {
        return false;
    }

    /**
     * <p>Checks whether metamatrix supports ALTER TABLE with drop column.</p>
     * @return if so return true, else false.
     * @throws SQLException, should never occur.
     */
    public boolean supportsAlterTableWithDropColumn() throws SQLException {
        return false;
    }

    /**
     * <p>Checks whether metamatrix supports ANSI-92 entry level SQL grammer.</p>
     * @return if so return true, else false.
     * @throws SQLException, should never occur.
     */
    public boolean supportsANSI92EntryLevelSQL() throws SQLException {
        return false;
    }

    /**
     * <p>Checks whether metamatrix supports ANSI-92 full SQL grammer.</p>
     * @return if so return true, else false.
     * @throws SQLException, should never occur.
     */
    public boolean supportsANSI92FullSQL() throws SQLException {
        return false;
    }

    /**
     * <p>Checks whether metamatrix supports ANSI-92 intermediate SQL grammer.</p>
     * @return if so return true, else false.
     * @throws SQLException, should never occur.
     */
    public boolean supportsANSI92IntermediateSQL() throws SQLException {
        return false;
    }

    /**
     * <p>Indicates whether the driver supports batch updates.</p>
     * @return true if the driver supports batch updates; false otherwise
     * @throws SQLException, should never occur
     */
    public boolean supportsBatchUpdates() throws SQLException {
        return true;
    }

    /**
     * <p>Checks whether metamatrix supports using a catalog name in a data manipulation
     * statement.</p>
     * @return if so return true, else false.
     * @throws SQLException, should never occur.
     */
    public boolean supportsCatalogsInDataManipulation() throws SQLException {
        return false;
    }

    /**
     * <p>Checks whether metamatrix supports using a catalog name in an index definition
     * statement.</p>
     * @return if so return true, else false.
     * @throws SQLException, should never occur.
     */
    public boolean supportsCatalogsInIndexDefinitions() throws SQLException {
        return false;
    }

    /**
     * <p>Checks whether metamatrix supports using a catalog name in a privilage
     * definition statement.</p>
     * @return if so return true, else false.
     * @throws SQLException, should never occur.
     */
    public boolean supportsCatalogsInPrivilegeDefinitions() throws SQLException {
        return false;
    }

    /**
     * <p>Checks whether metamatrix supports using a catalog name in a procedure call
     * statement.</p>
     * @return if so return true, else false.
     * @throws SQLException, should never occur.
     */
    public boolean supportsCatalogsInProcedureCalls() throws SQLException {
        return false;
    }

    /**
     * <p>Checks whether metamatrix supports using a catalog name in a table definition
     * statement.</p>
     * @return if so return true, else false.
     * @throws SQLException, should never occur.
     */
    public boolean supportsCatalogsInTableDefinitions() throws SQLException {
        return false;
    }

    /**
     * <p>Checks whether metamatrix supports column aliasing. If true the SQL AS clause
     * can be used to provide names for alias names for columns.</p>
     * @return if so return true, else false.
     * @throws SQLException, should never occur.
     */
    public boolean supportsColumnAliasing() throws SQLException {
        return true;
   }

    /**
     * <p>Checks whether metamatrix supports scalar function CONVERT for the conversion
     * of one JDBC type to another.</p>
     * @return if so return true, else false.
     * @throws SQLException, should never occur.
     */
    public boolean supportsConvert() throws SQLException {
        return true;
    }

    /**
     * <p>Checks whether metamatrix supports scalar function CONVERT for the conversion
     * of fromType to toType.</p>
     * @param fromType SQL type from which to convert
     * @param toType SQL type to convert to
     * @return if so return true, else false.
     * @throws SQLException, should never occur.
     */
    public boolean supportsConvert(int fromType, int toType) throws SQLException {

        if(fromType == Types.CHAR || fromType == Types.VARCHAR || fromType == Types.LONGVARCHAR) {
            if(toType == Types.CHAR || toType == Types.VARCHAR || toType == Types.LONGVARCHAR || toType == Types.BIT
                || toType == Types.SMALLINT || toType == Types.TINYINT|| toType == Types.INTEGER || toType == Types.BIGINT
                || toType == Types.FLOAT  || toType == Types.REAL || toType == Types.DOUBLE || toType == Types.NUMERIC
                || toType == Types.DECIMAL  || toType == Types.DATE || toType == Types.TIME || toType == Types.TIMESTAMP) {
                return true;
            }
            return false;
        } else if(fromType == Types.INTEGER || fromType == Types.TINYINT || fromType == Types.SMALLINT
                 || fromType == Types.BIT || fromType == Types.BIGINT || fromType == Types.FLOAT || fromType == Types.REAL
                 || fromType == Types.DOUBLE || fromType == Types.NUMERIC || fromType == Types.DECIMAL) {
            if(toType == Types.CHAR || toType == Types.VARCHAR || toType == Types.LONGVARCHAR || toType == Types.BIT
                || toType == Types.SMALLINT || toType == Types.TINYINT || toType == Types.INTEGER || toType == Types.BIGINT
                || toType == Types.FLOAT || toType == Types.REAL || toType == Types.DOUBLE || toType == Types.NUMERIC
                || toType == Types.DECIMAL) {
                return true;
            }
            return false;
        } else if(fromType == Types.DATE) {
            if(toType == Types.DATE || toType == Types.TIMESTAMP || toType == Types.CHAR
                || toType == Types.VARCHAR || toType == Types.LONGVARCHAR) {
                return true;
            }
            return false;
        } else if(fromType == Types.TIME) {
            if(toType == Types.TIME || toType == Types.TIMESTAMP || toType == Types.CHAR
                || toType == Types.VARCHAR || toType == Types.LONGVARCHAR) {
                return true;
            }
            return false;
        } else if(fromType == Types.TIMESTAMP) {
            if(toType == Types.DATE || toType == Types.TIME || toType == Types.TIMESTAMP
                || toType == Types.CHAR || toType == Types.VARCHAR || toType == Types.LONGVARCHAR) {
                return true;
            }
            return false;
        } else if(fromType == Types.JAVA_OBJECT) {
            if(toType == Types.JAVA_OBJECT) {
                return true;
            }
            return false;
        } else {
            return false;
        }
    }

    /**
     * <p>Checks whether metamatrix supports correlated subqueries.</p>
     * @return if so return true, else false.
     * @throws SQLException, should never occur.
     */
    public boolean supportsCorrelatedSubqueries() throws SQLException {
        return true;
    }

    /**
     * <p>Checks whether metamatrix supports ODBC Core SQL grammer.</p>
     * @return if so return true, else false.
     * @throws SQLException, should never occur.
     */
    public boolean supportsCoreSQLGrammar() throws SQLException {
        return false;
    }

    /**
     * <p>Checks whether metamatric supports both data definition and data manipulation
     * statements within a transaction.</p>
     * @return if so return true, else false.
     * @throws SQLException, should never occur.
     */
    public boolean supportsDataDefinitionAndDataManipulationTransactions() throws SQLException {
        return false;
    }

    /**
     * <p>Are only data manipulation statements within a transaction
     * supported?</p>
     * @return <code>true</code> if so; <code>false</code> otherwise
     * @throws SQLException, should never occur
     */
    public boolean supportsDataManipulationTransactionsOnly() throws SQLException {
        return false;
    }

    /**
     * <p>Checks whether metamatric supports only data manipulation statements within
     * a transaction.</p>
     * @return if so return true, else false.
     * @throws SQLException, should never occur.
     */
    public boolean supportsDifferentTableCorrelationNames() throws SQLException {
        return false;
    }

    /**
     * <p>Checks whether metamatrix supports expressions in ORDER BY lists.</p>
     * @return if so return true, else false.
     * @throws SQLException, should never occur.
     */
    public boolean supportsExpressionsInOrderBy() throws SQLException {
        return false;
    }

    /**
     * <p>Checks whether metamatrix supports ODBC Extended SQL grammer.</p>
     * @return if so return true, else false.
     * @throws SQLException, should never occur.
     */
    public boolean supportsExtendedSQLGrammar() throws SQLException {
        return false;
    }

    /**
     * <p>Checks whether metamatrix supports full outer joins</p>
     * @return if so return true, else false.
     * @throws SQLException, should never occur.
     */
    public boolean supportsFullOuterJoins() throws SQLException {
        return true;
    }

    /**
     * Retrieves whether auto-generated keys can be retrieved after a
     * statement has been executed.
     * @return boolean true if auto-generated keys can be retrieved
     * after a statement has executed; false otherwise
     * @throws SQLException, should never occur
     */
    public boolean supportsGetGeneratedKeys() throws SQLException {
        return false;
    }

    /**
     * <p>Checks whether metamatrix supports GROUP BY</p>
     * @return if so return true, else false.
     * @throws SQLException, should never occur.
     */
    public boolean supportsGroupBy() throws SQLException {
        return true;
    }

    /**
     * <p>Checks whether a GROUP BY clause can use columns that are not in the SELECT
     * clause, provided that it specifies all the columns in the SELECT clause.</p>
     * @return if so return true, else false.
     * @throws SQLException, should never occur.
     */
    public boolean supportsGroupByBeyondSelect() throws SQLException {
        return true;
    }

    /**
     * <p>Checks whether a GROUP BY clause can use columns that are not in the SELECT
     * clause.</p>
     * @return if so return true, else false.
     * @throws SQLException, should never occur.
     */
    public boolean supportsGroupByUnrelated() throws SQLException {
        return false;
    }

    /**
     * <p>Checks whether metamatrix supports the SQL Integrity Enhancement Facility.</p>
     * @return if so return true, else false.
     * @throws SQLException, should never occur.
     */
    public boolean supportsIntegrityEnhancementFacility() throws SQLException {
        return false;
    }

    /**
     * <p>Checks whether metamatrix supports specifying a LIKE escape clause.</p>
     * @return if so return true, else false.
     * @throws SQLException, should never occur.
     */
    public boolean supportsLikeEscapeClause() throws SQLException {
        return true;
    }

    /**
     * <p>Checks whether metamatrix provides limited support for outer joins.</p>
     * @return if so return true, else false.
     * @throws SQLException, should never occur.
     */
    public boolean supportsLimitedOuterJoins() throws SQLException {
        return true;
    }

    /**
     * <p>Checks whether metamatrix supports the ODBC minimum SQL grammer.</p>
     * @return if so return true, else false.
     * @throws SQLException, should never occur.
     */
    public boolean supportsMinimumSQLGrammar() throws SQLException {
        return true;
    }

    /**
     * <p>Checks whether mixed-case unquoted SQL identifiers used in SQL statements are
     * case sensitive</p>
     * @return if so return true, else false.
     * @throws SQLException, should never occur.
     */
    public boolean supportsMixedCaseIdentifiers() throws SQLException {
        return false;
    }

    /**
     * <p>Checks whether mixed-case quoted SQL identifiers used in SQL statements are
     * case sensitive</p>
     * @return if so return true, else false.
     * @throws SQLException, should never occur.
     */
    public boolean supportsMixedCaseQuotedIdentifiers() throws SQLException {
        return false;
    }

    /**
     * <p>Checks whether metamatrix supports having cursors open across commits.
     * @return if so return true, else false.</p>
     * @throws SQLException, should never occur.
     */
    public boolean supportsOpenCursorsAcrossCommit() throws SQLException {
        return false;
    }

    /**
     * <p>Are multiple ResultSets from a single execute supported?</p>
     * @return <code>true</code> if so; <code>false</code> otherwise
     * @throws SQLException, should never occur
     */
    public boolean supportsMultipleResultSets() throws SQLException {
        return false;
    }

    /**
     * Retrieves whether it is possible to have multiple ResultSet objects
     * returned from a CallableStatement object simultaneously.
     * @return <code>true</code> if so; <code>false</code> otherwise
     * @throws SQLException, should never occur
     */
    public boolean supportsMultipleOpenResults() throws SQLException {
        return false;
    }

    /**
     * <p>Checks whether multiple transactions open at once on different connectons</p>
     * @return if so return true, else false.
     * @throws SQLException, should never occur.
     */
    public boolean supportsMultipleTransactions() throws SQLException {
        return true;
    }

    /**
     * Retrieves whether metamatrix supports named parameters to callable statements.
     * @return boolean true if named parameters are supported; false otherwise.
     * @throws SQLException, should never occur
     */
    public boolean supportsNamedParameters() throws SQLException {
        return false;
    }

    /**
     * <p>Checks whether metamatrix supports defining columns as nonnullable.</p>
     * @return if so return true, else false.
     * @throws SQLException, should never occur.
     */
    public boolean supportsNonNullableColumns() throws SQLException {
        return true;
    }

    /**
     * <p>Checks whether metamatrix supports having cursors open across rollbacks.</p>
     * @return if so return true, else false.
     * @throws SQLException, should never occur.
     */
    public boolean supportsOpenCursorsAcrossRollback() throws SQLException {
        return false;
    }

    /**
     * <p>Checks whether metamatrix supports having cursors open across commits.</p>
     * @return if so return true, else false.
     * @throws SQLException, should never occur.
     */
    public boolean supportsOpenStatementsAcrossCommit() throws SQLException {
        return true;
    }

    /**
     * <p>Checks whether metamatrix supports having cursors open across rollbacks.</p>
     * @return if so return true, else false.
     * @throws SQLException, should never occur.
     */
    public boolean supportsOpenStatementsAcrossRollback() throws SQLException {
        return true;
    }

    /**
     * <p>Checks whether an ORDER BY clause can use columns that are not in the SELECT clause.</p>
     * @return if so return true, else false.
     * @throws SQLException, should never occur.
     */
    public boolean supportsOrderByUnrelated() throws SQLException {
        return true;
    }

    /**
     * <p>Checks whether metamatrix supports some form of outer joins.</p>
     * @return if so return true, else false.
     * @throws SQLException, should never occur.
     */
    public boolean supportsOuterJoins() throws SQLException {
        return true;
    }

    /**
     * <p>Checks whether metamatrix supports positioned DELETEs on resultsets</p>
     * @return if so return true, else false.
     * @throws SQLException, should never occur.
     */
    public boolean supportsPositionedDelete() throws SQLException {
        return false;
    }

    /**
     * <p>Checks whether metamatrix supports positioned UPDATEs on result sets.</p>
     * @return if so return true, else false.
     * @throws SQLException, should never occur.
     */
    public boolean supportsPositionedUpdate() throws SQLException {
        return false;
    }

    /**
     * <p>Does the database support the concurrency type in combination
     * with the given result set type?</p>
     * @param type defined in <code>java.sql.ResultSet</code>
     * @param concurrency type defined in <code>java.sql.ResultSet</code>
     * @return <code>true</code> if so; <code>false</code> otherwise
     * @throws SQLException, should never occur
     * @see Connection
     */
    public boolean supportsResultSetConcurrency(int type, int concurrency)
      throws SQLException {
        if(type == java.sql.ResultSet.TYPE_FORWARD_ONLY || type == java.sql.ResultSet.TYPE_SCROLL_INSENSITIVE) {
            if(concurrency == java.sql.ResultSet.CONCUR_READ_ONLY) {
                return true;
              }
              return false;
        }
        return false;
    }

    /**
     * Retrieves whether database supports the given result set holdability.
     * Holdability - one of the following constants:
     * ResultSet.HOLD_CURSORS_OVER_COMMIT or ResultSet.CLOSE_CURSORS_AT_COMMIT.
     * @param intValue holdability
     * @return boolean true if so; false otherwise
     * @throws SQLException, should never occur

     */
    public boolean supportsResultSetHoldability(int holdability) throws SQLException {
        return false;
    }

    /**
     * <p>Does the database support the given result set type?</p>
     * @param type defined in <code>java.sql.ResultSet</code>
     * @return <code>true</code> if so; <code>false</code> otherwise
     * @throws SQLException, should never occur
     * @see Connection
     */
    public boolean supportsResultSetType(int type) throws SQLException {

        if(type == java.sql.ResultSet.TYPE_FORWARD_ONLY || type == java.sql.ResultSet.TYPE_SCROLL_INSENSITIVE) {
            return true;
        }
        return false;
    }

    /**
     * Retrieves whether metamatrix supports savepoints.
     * @return if so return true, else false.
     * @throws SQLException, should never occur.
     */
    public boolean supportsSavepoints() throws SQLException {
        return false;
    }

    /**
     * <p>Checks whether metamatrix supports using a schema name in a data manipulation
     * statement</p>
     * @return if so return true, else false.
     * @throws SQLException, should never occur.
     */
    public boolean supportsSchemasInDataManipulation() throws SQLException {
        return true;
    }

    /**
     * <p>Checks whether metamatrix supports using a schema name in an index definition
     * statement</p>
     * @return if so return true, else false.
     * @throws SQLException, should never occur.
     */
    public boolean supportsSchemasInIndexDefinitions() throws SQLException {
        return false;
    }

    /**
     * <p>Checks whether metamatrix supports using a schema name in a privilage
     * definition statement</p>
     * @return if so return true, else false.
     * @throws SQLException, should never occur.
     */
    public boolean supportsSchemasInPrivilegeDefinitions() throws SQLException {
        return false;
    }

    /**
     * <p>Checks whether metamatrix supports using a schema name in an procedure
     * call statement</p>
     * @return if so return true, else false.
     * @throws SQLException, should never occur.
     */
    public boolean supportsSchemasInProcedureCalls() throws SQLException {
        return true;
    }

    /**
     * <p>Checks whether metamatrix supports using a schema name in an table
     * definition statement</p>
     * @return if so return true, else false.
     * @throws SQLException, should never occur.
     */
    public boolean supportsSchemasInTableDefinitions() throws SQLException {
        return false;
    }

    /**
     * <p>Checks whether metamatrix supports SELECT FOR UPDATE statements</p>
     * @return if so return true, else false.
     * @throws SQLException, should never occur.
     */
    public boolean supportsSelectForUpdate() throws SQLException {
        return false;
    }

    /**
     * Retrieves whether this metamatrix supports statement pooling.
     * @return if so return true, else false.
     * @throws SQLException, should never occur.
     */
    public boolean supportsStatementPooling() throws SQLException {
        return false;
    }

    /**
     * <p>Checks whether metamatrix supports stored procedure calls using the stored
     * procedure escape syntax</p>
     * @return if so return true, else false.
     * @throws SQLException, should never occur.
     */
    public boolean supportsStoredProcedures() throws SQLException {
        return true;
    }

    /**
     * <p>Checks whether metamatrix supports subqueries in comparision expressions.</p>
     * @return if so return true, else false.
     * @throws SQLException, should never occur.
     */
    public boolean supportsSubqueriesInComparisons() throws SQLException {
        return true;
    }

    /**
     * <p>Checks whether metamatrix supports subqueries in EXISTS expressions.</p>
     * @return if so return true, else false.
     * @throws SQLException, should never occur.
     */
    public boolean supportsSubqueriesInExists() throws SQLException {
        return true;
    }

    /**
     * <p>Checks whether metamatrix supports subqueries in IN statements.</p>
     * @return if so return true, else false.
     * @throws SQLException, should never occur.
     */
    public boolean supportsSubqueriesInIns() throws SQLException {
        return true;
    }

    /**
     * <p>Checks whether metamatrix supports subqueries in qualified expressions.</p>
     * @return if so return true, else false.
     * @throws SQLException, should never occur.
     */
    public boolean supportsSubqueriesInQuantifieds() throws SQLException {
        return true;
    }

    /**
     * <p>Checks whether metamatrix supports table correlation names</p>
     * @return if so return true, else false.
     * @throws SQLException, should never occur.
     */
    public boolean supportsTableCorrelationNames() throws SQLException {
        return true;
    }

    /**
     * <p>Checks whether metamatrix supports the given transaction isolation level</p>
     * @return if so return true, else false.
     * @throws SQLException, should never occur.
     */
    public boolean supportsTransactionIsolationLevel(int level) throws SQLException {
        return false;
    }

    /**
     * <p>Checks whether metamatrix supports transactions</p>
     * @return if so return true, else false.
     * @throws SQLException, should never occur.
     */
    public boolean supportsTransactions() throws SQLException {
        return true;
    }

    /**
     * <p>Checks whether metamatrix supports SQL UNION</p>
     * @return if so return true, else false.
     * @throws SQLException, should never occur.
     */
    public boolean supportsUnion() throws SQLException {
        return true;
    }

    /**
     * <p>Checks whether metamatrix supports SQL UNION ALL</p>
     * @return if so return true, else false.
     * @throws SQLException, should never occur.
     */
    public boolean supportsUnionAll() throws SQLException {
        return true;
    }

    /**
     * <p>Indicates whether or not a visible row update can be detected by
     * calling the method <code>ResultSet.rowUpdated</code>.</p>
     * @param result set type, i.e. ResultSet.TYPE_XXX
     * @return <code>true</code> if changes are detected by the result set type;
     *         <code>false</code> otherwise
     * @throws SQLException, should never occur
     */
    public boolean updatesAreDetected(int type) throws SQLException {
        return false;
    }

    /**
     * <p>Checks whether metamatrix uses a separate local file to store each table</p>
     * @return if so return true, else false.
     * @throws SQLException, should never occur.
     */
    public boolean usesLocalFilePerTable() throws SQLException {
        return false;
    }

    /**
     * <p>Checks whether metamatrix stores tables in a local file</p>
     * @return if so return true, else false.
     * @throws SQLException, should never occur.
     */
    public boolean usesLocalFiles() throws SQLException {
        return false;
    }

    /**
     * <p>This method is used to produce ResultSets from server's Results objects for
     * getCrossReference, getExportedKeys and getImportedKeys methods.
     * @param server's Results object.
     * @return ResultSet object giving the reference key info.
     * @throws SQLException if there is an accesing server results
     */
    private ResultSet getReferenceKeys(MMResultSet results) throws SQLException {

        // list which represent records containing reference key info
        List records = new ArrayList ();
        ResultSetMetaData rmetadata = null;
        try {
            // build the list of records from Results object.
            while (results.next()) {
                // list represents a record on the Results object.
                List currentRow = new ArrayList (15);
                // add values in the current record on the Results object to the list
                // number of values to be fetched from each row is MAX_COLUMNS.
                for(int i=0; i < JDBCColumnPositions.REFERENCE_KEYS.MAX_COLUMNS; i++) {
                    // get the value at the current index add it to currentRow
                    currentRow.add(results.getObject(i+1));
                }

                // add the current row to the list of records.
                records.add(currentRow);
            }// end of while

            // get the metadata for the results
            rmetadata = results.getMetaData();

        } catch (Exception e) {
            String msg = JDBCPlugin.Util.getString("MMDatabaseMetadata.Err_getting_primary_keys"); //$NON-NLS-1$
            throw MMSQLException.create(e, msg);
        }

        // close the resultset and driver connection
        //results.close();

        logger.fine(JDBCPlugin.Util.getString("MMDatabaseMetadata.getRefKey_success")); //$NON-NLS-1$

        // construct results object from column values and their metadata
        return createResultSet(records, rmetadata);
    }

    /**
     * <p>This method is used to hardcode metadata details for a given column into
     * a map object. While some of the details are obtained as parameters, most
     * are hardcoded.
     * @param tableName The group/table name in which the column is present
     * @param columnName The name of the column/element
     * @param dataType The MetaMatrix datatype of the column
     * @param nullable An int value indicating nallability of the column
     * @return a map containing metadata details for any given column
     * @throws SQLException, should never occur
     */
    private Map getColumnMetadata(String tableName, String columnName, String dataType, Integer nullable) throws SQLException {
            return getColumnMetadata(tableName, columnName, dataType, nullable, ResultsMetadataConstants.SEARCH_TYPES.UNSEARCHABLE, Boolean.FALSE, Boolean.FALSE, Boolean.FALSE);
    }
    
    private Map getColumnMetadata(String tableName, String columnName, String dataType, Integer nullable, Integer searchable, Boolean writable, Boolean signed, Boolean caseSensitive) throws SQLException {

        // map that would contain metadata details
        Map metadataMap = new HashMap();

        /*******************************************************
         HardCoding Column metadata details for the given column
        ********************************************************/

        metadataMap.put(ResultsMetadataConstants.VIRTUAL_DATABASE_NAME, driverConnection.getSchema());
        metadataMap.put(ResultsMetadataConstants.GROUP_NAME, tableName);
        metadataMap.put(ResultsMetadataConstants.ELEMENT_NAME, columnName);
        metadataMap.put(ResultsMetadataConstants.DATA_TYPE, dataType);
        metadataMap.put(ResultsMetadataConstants.PRECISION, ResultsMetadataDefaults.getDefaultPrecision(dataType));
        metadataMap.put(ResultsMetadataConstants.RADIX, new Integer(10));
        metadataMap.put(ResultsMetadataConstants.SCALE, new Integer(0));
        metadataMap.put(ResultsMetadataConstants.AUTO_INCREMENTING, Boolean.FALSE);
        metadataMap.put(ResultsMetadataConstants.CASE_SENSITIVE, caseSensitive);
        metadataMap.put(ResultsMetadataConstants.NULLABLE, nullable);
        metadataMap.put(ResultsMetadataConstants.SEARCHABLE, searchable);
        metadataMap.put(ResultsMetadataConstants.SIGNED, signed);
        metadataMap.put(ResultsMetadataConstants.WRITABLE, writable);
        metadataMap.put(ResultsMetadataConstants.CURRENCY, Boolean.FALSE);
        metadataMap.put(ResultsMetadataConstants.DISPLAY_SIZE, ResultsMetadataDefaults.getMaxDisplaySize(dataType));

        return metadataMap;
    }

    /**
     * Gets a description of models available in a catalog.
     *
     * <P>Only model descriptions matching the catalog, schema, and
     * model are returned.  They are ordered by MODEL_NAME.
     *
     * <P>Each model description has the following columns:
     *  <OL>
     *  <LI><B>MODEL_CAT</B> String => model catalog (may be null)
     *  <LI><B>MODEL_SCHEM</B> String => model schema (may be null)
     *  <LI><B>MODEL_NAME</B> String => model name
     *  <LI><B>DESCRIPTION</B> String => explanatory comment on the model (may be null)
     *  <LI><B>IS_PHYSICAL</B> Boolean => true if the model is a physical model
     *  <LI><B>SUP_WHERE_ALL</B> Boolean => true if queries without a criteria are allowed
     *  <LI><B>SUP_DISTINCT</B> Boolean => true if distinct clause can be used
     *  <LI><B>SUP_JOIN</B> Boolean => true if joins are supported
     *  <LI><B>SUP_OUTER_JOIN</B> Boolean => true if outer joins are supported
     *  <LI><B>SUP_ORDER_BY</B> Boolean => true if order by is supported
     * </OL>
     *
     * <P><B>Note:</B> Some databases may not return information for
     * all models.
     *
     * @param catalog a catalog name; "" retrieves those without a
     * catalog; null means drop catalog name from the selection criteria
     * @param schemaPattern a schema name pattern; "" retrieves those
     * without a schema
     * @param modelNamePattern a model name pattern
     * @return <code>ResultSet</code> - each row is a model description
     * @exception SQLException if a database access error occurs
     */
    public ResultSet getModels(String catalog, String schemaPattern, String modelNamePattern) throws SQLException {
        // Get all models if modelNamePattern is null
        if(modelNamePattern == null) {
            modelNamePattern = PERCENT; 
        }
        try {
            PreparedStatement prepareQuery = driverConnection.prepareStatement(QUERY_MODELS);
            prepareQuery.setObject(1, modelNamePattern.toUpperCase());

            // make a query against runtimemetadata and get results
            MMResultSet results = (MMResultSet) prepareQuery.executeQuery();

            return results;
        } catch (Exception e) {
            throw MMSQLException.create(e, JDBCPlugin.Util.getString("MMDatabaseMetadata.getModels_error", modelNamePattern, e.getMessage())); //$NON-NLS-1$
        }
    }

    /**
     * <p> This is a different method from #getSchemas(). It only extracts the XML schemas.</p>
     * @param documentName of the Group specified in the XML model
     * @return a list of xml schemas
     */
    public List getXMLSchemas(String documentName) throws SQLException {
        try {
			return driverConnection.getDQP().getXmlSchemas(documentName);
		} catch (QueryMetadataException e) {
			throw MMSQLException.create(e);
		} catch (MetaMatrixComponentException e) {
			throw MMSQLException.create(e);
		}
    }

	public boolean autoCommitFailureClosesAllResultSets() throws SQLException {
		return false;
	}

	public int getResultSetHoldability() throws SQLException {
		return ResultSet.HOLD_CURSORS_OVER_COMMIT;
	}
	
	public Connection getConnection() throws SQLException {
		return driverConnection;
	}
	
	public boolean supportsStoredFunctionsUsingCallSyntax() throws SQLException {
		return false;
	}

	public ResultSet getAttributes(String catalog, String schemaPattern, String typeNamePattern, String attributeNamePattern)
			throws SQLException {
		throw SqlUtil.createFeatureNotSupportedException();	
	}

	public ResultSet getClientInfoProperties() throws SQLException {
		throw SqlUtil.createFeatureNotSupportedException();
	}

	public ResultSet getFunctionColumns(String catalog, String schemaPattern,
			String functionNamePattern, String columnNamePattern)
			throws SQLException {
		throw SqlUtil.createFeatureNotSupportedException();
	}

	public ResultSet getFunctions(String catalog, String schemaPattern,
			String functionNamePattern) throws SQLException {
		throw SqlUtil.createFeatureNotSupportedException();
	}

	//## JDBC4.0-begin ##
	public RowIdLifetime getRowIdLifetime() throws SQLException {
		throw SqlUtil.createFeatureNotSupportedException();
	}
	//## JDBC4.0-end ##

	public ResultSet getSchemas(String catalog, String schemaPattern)
			throws SQLException {
		throw SqlUtil.createFeatureNotSupportedException();
	}
}
