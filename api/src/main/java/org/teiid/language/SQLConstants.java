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

package org.teiid.language;

import java.lang.reflect.Field;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * SQL Constants for Teiid.
 */
public class SQLConstants {
	
	public interface Tokens {
		public static final String ALL_COLS = "*"; //$NON-NLS-1$
		public static final String SPACE = " "; //$NON-NLS-1$
		public static final String COMMA = ","; //$NON-NLS-1$
		public static final String DOT = "."; //$NON-NLS-1$
		public static final String QUOTE = "'"; //$NON-NLS-1$
		public static final String EQ = "="; //$NON-NLS-1$
		public static final String NE = "<>"; //$NON-NLS-1$
		public static final String LT = "<"; //$NON-NLS-1$
		public static final String GT = ">"; //$NON-NLS-1$
		public static final String LE = "<="; //$NON-NLS-1$
		public static final String GE = ">="; //$NON-NLS-1$
		public static final String LPAREN = "("; //$NON-NLS-1$
		public static final String RPAREN = ")"; //$NON-NLS-1$
	}
	
	public interface NonReserved {
		public static final String SQL_TSI_FRAC_SECOND = "SQL_TSI_FRAC_SECOND"; //$NON-NLS-1$
		public static final String SQL_TSI_SECOND = "SQL_TSI_SECOND"; //$NON-NLS-1$
		public static final String SQL_TSI_MINUTE = "SQL_TSI_MINUTE"; //$NON-NLS-1$
		public static final String SQL_TSI_HOUR = "SQL_TSI_HOUR"; //$NON-NLS-1$
		public static final String SQL_TSI_DAY = "SQL_TSI_DAY"; //$NON-NLS-1$
		public static final String SQL_TSI_WEEK = "SQL_TSI_WEEK"; //$NON-NLS-1$
		public static final String SQL_TSI_MONTH = "SQL_TSI_MONTH"; //$NON-NLS-1$
		public static final String SQL_TSI_QUARTER = "SQL_TSI_QUARTER"; //$NON-NLS-1$
		public static final String SQL_TSI_YEAR = "SQL_TSI_YEAR"; //$NON-NLS-1$
		public static final String TIMESTAMPADD = "TIMESTAMPADD"; //$NON-NLS-1$
		public static final String TIMESTAMPDIFF = "TIMESTAMPDIFF"; //$NON-NLS-1$
		//aggregate functions
		public static final String MAX = "MAX"; //$NON-NLS-1$
		public static final String MIN = "MIN"; //$NON-NLS-1$
		public static final String COUNT = "COUNT"; //$NON-NLS-1$
		public static final String AVG = "AVG"; //$NON-NLS-1$
		public static final String SUM = "SUM"; //$NON-NLS-1$
		//texttable
		public static final String WIDTH = "WIDTH"; //$NON-NLS-1$
		public static final String DELIMITER = "DELIMITER"; //$NON-NLS-1$
		public static final String HEADER = "HEADER"; //$NON-NLS-1$
		public static final String QUOTE = "QUOTE"; //$NON-NLS-1$
		public static final String COLUMNS = "COLUMNS"; //$NON-NLS-1$
		//xmltable
		public static final String ORDINALITY = "ORDINALITY"; //$NON-NLS-1$
		public static final String PASSING = "PASSING"; //$NON-NLS-1$
		public static final String PATH = "PATH"; //$NON-NLS-1$
		//xmlserialize
		public static final String DOCUMENT = "DOCUMENT"; //$NON-NLS-1$
		public static final String CONTENT = "CONTENT"; //$NON-NLS-1$
		//xmlquery
		public static final String RETURNING = "RETURNING"; //$NON-NLS-1$
		public static final String SEQUENCE = "SEQUENCE"; //$NON-NLS-1$
		public static final String EMPTY = "EMPTY"; //$NON-NLS-1$
		//querystring function
		public static final String QUERYSTRING = "QUERYSTRING"; //$NON-NLS-1$
		//xmlparse
		public static final String WELLFORMED = "WELLFORMED"; //$NON-NLS-1$
		//agg
		public static final String EVERY = "EVERY"; //$NON-NLS-1$
		public static final String STDDEV_POP = "STDDEV_POP"; //$NON-NLS-1$
		public static final String STDDEV_SAMP = "STDDEV_SAMP"; //$NON-NLS-1$
		public static final String VAR_SAMP = "VAR_SAMP"; //$NON-NLS-1$
		public static final String VAR_POP = "VAR_POP"; //$NON-NLS-1$
		
		public static final String NULLS = "NULLS"; //$NON-NLS-1$
		public static final String FIRST = "FIRST"; //$NON-NLS-1$
		public static final String LAST = "LAST"; //$NON-NLS-1$
		
		public static final String KEY = "KEY"; //$NON-NLS-1$
		
		public static final String SERIAL = "SERIAL"; //$NON-NLS-1$
		
		public static final String ENCODING = "ENCODING"; //$NON-NLS-1$
		public static final String TEXTAGG = "TEXTAGG"; //$NON-NLS-1$
		
		public static final String ARRAYTABLE = "ARRAYTABLE"; //$NON-NLS-1$
		
		public static final String VIEW = "VIEW"; //$NON-NLS-1$
		public static final String INSTEAD = "INSTEAD"; //$NON-NLS-1$
		public static final String ENABLED = "ENABLED"; //$NON-NLS-1$
		public static final String DISABLED = "DISABLED"; //$NON-NLS-1$
	}
	
	public interface Reserved {
		//Teiid specific
		public static final String BIGDECIMAL = "BIGDECIMAL"; //$NON-NLS-1$
		public static final String BIGINTEGER = "BIGINTEGER"; //$NON-NLS-1$
	    public static final String BREAK = "BREAK"; //$NON-NLS-1$
	    public static final String BYTE = "BYTE"; //$NON-NLS-1$
	    public static final String CRITERIA = "CRITERIA"; //$NON-NLS-1$
	    public static final String ERROR = "ERROR";	 //$NON-NLS-1$
	    public static final String LIMIT = "LIMIT"; //$NON-NLS-1$
	    public static final String LONG = "LONG"; //$NON-NLS-1$
	    public static final String LOOP = "LOOP"; //$NON-NLS-1$
	    public static final String MAKEDEP = "MAKEDEP"; //$NON-NLS-1$
	    public static final String MAKENOTDEP = "MAKENOTDEP"; //$NON-NLS-1$
		public static final String NOCACHE = "NOCACHE"; //$NON-NLS-1$
		public static final String STRING = "STRING"; //$NON-NLS-1$
	    public static final String VIRTUAL = "VIRTUAL"; //$NON-NLS-1$
	    public static final String WHILE = "WHILE"; //$NON-NLS-1$
	    
	    //SQL2003 keywords
	    public static final String ADD = "ADD"; //$NON-NLS-1$
		public static final String ANY = "ANY"; //$NON-NLS-1$
	    public static final String ALL = "ALL"; //$NON-NLS-1$
	    public static final String ALLOCATE = "ALLOCATE"; //$NON-NLS-1$
	    public static final String ALTER = "ALTER"; //$NON-NLS-1$
	    public static final String AND = "AND"; //$NON-NLS-1$
	    public static final String ARE = "ARE"; //$NON-NLS-1$
	    public static final String ARRAY = "ARRAY"; //$NON-NLS-1$s
	    public static final String AS = "AS"; //$NON-NLS-1$
	    public static final String ASC = "ASC"; //$NON-NLS-1$
	    public static final String ASENSITIVE = "ASENSITIVE"; //$NON-NLS-1$
	    public static final String ASYMETRIC = "ASYMETRIC"; //$NON-NLS-1$
	    public static final String ATOMIC = "ATOMIC"; //$NON-NLS-1$
	    public static final String AUTHORIZATION = "AUTHORIZATION"; //$NON-NLS-1$
		public static final String BEGIN = "BEGIN"; //$NON-NLS-1$
	    public static final String BETWEEN = "BETWEEN"; //$NON-NLS-1$
	    public static final String BIGINT = "BIGINT"; //$NON-NLS-1$
	    public static final String BINARY = "BINARY"; //$NON-NLS-1$
		public static final String BLOB = "BLOB"; //$NON-NLS-1$
		public static final String BOTH = "BOTH"; //$NON-NLS-1$
		public static final String BY = "BY"; //$NON-NLS-1$
	    public static final String CALL = "CALL"; //$NON-NLS-1$
	    public static final String CALLED = "CALLED"; //$NON-NLS-1$
	    public static final String CASE = "CASE"; //$NON-NLS-1$
		public static final String CAST = "CAST"; //$NON-NLS-1$
	    public static final String CASCADED = "CASCADED"; //$NON-NLS-1$
		public static final String CHAR = "CHAR"; //$NON-NLS-1$
		public static final String CHARACTER = "CHARACTER"; //$NON-NLS-1$
	    public static final String CHECK = "CHECK"; //$NON-NLS-1$
	    public static final String CLOB = "CLOB"; //$NON-NLS-1$
	    public static final String CLOSE = "CLOSE"; //$NON-NLS-1$
	    public static final String COLLATE = "COLLATE"; //$NON-NLS-1$
	    public static final String COLUMN = "COLUMN"; //$NON-NLS-1$
	    public static final String COMMIT = "COMMIT"; //$NON-NLS-1$
	    public static final String CONNECT = "CONNECT"; //$NON-NLS-1$
	    public static final String CONVERT = "CONVERT"; //$NON-NLS-1$
	    public static final String CONSTRAINT = "CONSTRAINT"; //$NON-NLS-1$
	    public static final String CONTINUE = "CONTINUE"; //$NON-NLS-1$
		public static final String CORRESPONDING = "CORRESPONDING"; //$NON-NLS-1$
	    public static final String CREATE = "CREATE"; //$NON-NLS-1$
	    public static final String CROSS = "CROSS"; //$NON-NLS-1$
	    public static final String CURRENT_DATE = "CURRENT_DATE"; //$NON-NLS-1$
	    public static final String CURRENT_TIME = "CURRENT_TIME"; //$NON-NLS-1$
	    public static final String CURRENT_TIMESTAMP = "CURRENT_TIMESTAMP"; //$NON-NLS-1$
	    public static final String CURRENT_USER = "CURRENT_USER"; //$NON-NLS-1$
	    public static final String CURSOR = "CURSOR"; //$NON-NLS-1$
	    public static final String CYCLE = "CYCLE"; //$NON-NLS-1$
	    public static final String DATE = "DATE"; //$NON-NLS-1$
	    public static final String DAY = "DAY"; //$NON-NLS-1$
	    public static final String DEALLOCATE = "DEALLOCATE"; //$NON-NLS-1$
	    public static final String DEC = "DEC"; //$NON-NLS-1$
	    public static final String DECIMAL = "DECIMAL"; //$NON-NLS-1$
	    public static final String DECLARE = "DECLARE";     //$NON-NLS-1$
	    public static final String DEFAULT = "DEFAULT"; //$NON-NLS-1$
		public static final String DELETE = "DELETE"; //$NON-NLS-1$
		public static final String DEREF = "DEREF"; //$NON-NLS-1$
	    public static final String DESC = "DESC"; //$NON-NLS-1$
	    public static final String DESCRIBE = "DESCRIBE"; //$NON-NLS-1$
	    public static final String DETERMINISTIC = "DETERMINISTIC"; //$NON-NLS-1$
	    public static final String DISCONNECT = "DISCONNECT"; //$NON-NLS-1$
		public static final String DISTINCT = "DISTINCT"; //$NON-NLS-1$
		public static final String DOUBLE = "DOUBLE"; //$NON-NLS-1$
	    public static final String DROP = "DROP"; //$NON-NLS-1$
	    public static final String DYNAMIC = "DYNAMIC"; //$NON-NLS-1$
	    public static final String EACH = "EACH"; //$NON-NLS-1$
	    public static final String ELEMENT = "ELEMENT"; //$NON-NLS-1$
		public static final String ELSE = "ELSE";	 //$NON-NLS-1$
		public static final String END = "END"; //$NON-NLS-1$
	    public static final String ESCAPE = "ESCAPE"; //$NON-NLS-1$
	    public static final String EXCEPT = "EXCEPT"; //$NON-NLS-1$
	    public static final String EXEC = "EXEC"; //$NON-NLS-1$
	    public static final String EXECUTE = "EXECUTE"; //$NON-NLS-1$
	    public static final String EXISTS = "EXISTS"; //$NON-NLS-1$
	    public static final String EXTERNAL = "EXTERNAL"; //$NON-NLS-1$
	    public static final String FALSE = "FALSE"; //$NON-NLS-1$
	    public static final String FETCH = "FETCH"; //$NON-NLS-1$
	    public static final String FILTER = "FILTER"; //$NON-NLS-1$
	    public static final String FLOAT = "FLOAT"; //$NON-NLS-1$
	    public static final String FOR = "FOR";     //$NON-NLS-1$
	    public static final String FOREIGN = "FOREIGN"; //$NON-NLS-1$
	    public static final String FREE = "FREE"; //$NON-NLS-1$
		public static final String FROM = "FROM"; //$NON-NLS-1$
		public static final String FULL = "FULL"; //$NON-NLS-1$
		public static final String FUNCTION = "FUNCTION"; //$NON-NLS-1$
		public static final String GET = "GET"; //$NON-NLS-1$
		public static final String GLOBAL = "GLOBAL"; //$NON-NLS-1$
		public static final String GRANT = "GRANT"; //$NON-NLS-1$
		public static final String GROUP = "GROUP"; //$NON-NLS-1$
		public static final String GROUPING = "GROUPING"; //$NON-NLS-1$
		public static final String HAS = "HAS";	 //$NON-NLS-1$
	    public static final String HAVING = "HAVING"; //$NON-NLS-1$
	    public static final String HOLD = "HOLD"; //$NON-NLS-1$
	    public static final String HOUR = "HOUR"; //$NON-NLS-1$
	    public static final String IDENTITY = "IDENTITY"; //$NON-NLS-1$
	    public static final String INDICATOR = "INDICATOR"; //$NON-NLS-1$
	    public static final String IF = "IF";     //$NON-NLS-1$
	    public static final String IMMEDIATE = "IMMEDIATE"; //$NON-NLS-1$
	    public static final String IN = "IN"; //$NON-NLS-1$
	    public static final String INOUT = "INOUT"; //$NON-NLS-1$
	    public static final String INNER = "INNER"; //$NON-NLS-1$
	    public static final String INPUT = "INPUT"; //$NON-NLS-1$
	    public static final String INSENSITIVE = "INSENSITIVE"; //$NON-NLS-1$
	    public static final String INSERT = "INSERT"; //$NON-NLS-1$
	    public static final String INTEGER = "INTEGER"; //$NON-NLS-1$
	    public static final String INTERSECT = "INTERSECT"; //$NON-NLS-1$
	    public static final String INTERVAL = "INTERVAL"; //$NON-NLS-1$
	    public static final String INT = "INT"; //$NON-NLS-1$
	    public static final String INTO = "INTO"; //$NON-NLS-1$
	    public static final String IS = "IS";     //$NON-NLS-1$
	    public static final String ISOLATION = "ISOLATION"; //$NON-NLS-1$
	    public static final String JOIN = "JOIN"; //$NON-NLS-1$
	    public static final String LANGUAGE = "LANGUAGE"; //$NON-NLS-1$
	    public static final String LARGE = "LARGE"; //$NON-NLS-1$
	    public static final String LATERAL = "LATERAL"; //$NON-NLS-1$
	    public static final String LEADING = "LEADING"; //$NON-NLS-1$
	    public static final String LEFT = "LEFT"; //$NON-NLS-1$
	    public static final String LIKE = "LIKE"; //$NON-NLS-1$
	    public static final String LOCAL = "LOCAL"; //$NON-NLS-1$
	    public static final String LOCALTIME = "LOCALTIME"; //$NON-NLS-1$
	    public static final String LOCALTIMESTAMP = "LOCALTIMESTAMP"; //$NON-NLS-1$
	    public static final String MATCH = "MATCH"; //$NON-NLS-1$
	    public static final String MEMBER = "MEMBER"; //$NON-NLS-1$
		public static final String MERGE = "MERGE"; //$NON-NLS-1$
		public static final String METHOD = "METHOD"; //$NON-NLS-1$
		public static final String MINUTE = "MINUTE"; //$NON-NLS-1$
		public static final String MODIFIES = "MODIFIES"; //$NON-NLS-1$
		public static final String MODULE = "MODULE"; //$NON-NLS-1$
		public static final String MONTH = "MONTH"; //$NON-NLS-1$
		public static final String MULTISET = "MULTISET"; //$NON-NLS-1$
		public static final String NATIONAL = "NATIONAL"; //$NON-NLS-1$
		public static final String NATURAL = "NATURAL"; //$NON-NLS-1$
		public static final String NCHAR = "NCHAR"; //$NON-NLS-1$
		public static final String NCLOB = "NCLOB"; //$NON-NLS-1$
		public static final String NEW = "NEW"; //$NON-NLS-1$
		public static final String NO = "NO"; //$NON-NLS-1$
	    public static final String NONE = "NONE"; //$NON-NLS-1$
	    public static final String NOT = "NOT"; //$NON-NLS-1$
	    public static final String NULL = "NULL"; //$NON-NLS-1$
	    public static final String NUMERIC = "NUMERIC"; //$NON-NLS-1$
	    public static final String OBJECT = "OBJECT"; //$NON-NLS-1$
		public static final String OF = "OF"; //$NON-NLS-1$
		public static final String OLD = "OLD"; //$NON-NLS-1$
		public static final String ON = "ON"; //$NON-NLS-1$
		public static final String ONLY = "ONLY"; //$NON-NLS-1$
		public static final String OPEN = "OPEN"; //$NON-NLS-1$
	    public static final String OR = "OR"; //$NON-NLS-1$
		public static final String ORDER = "ORDER"; //$NON-NLS-1$
		public static final String OUT = "OUT"; //$NON-NLS-1$
	    public static final String OUTER = "OUTER"; //$NON-NLS-1$
		public static final String OUTPUT = "OUTPUT"; //$NON-NLS-1$
		public static final String OPTION = "OPTION"; //$NON-NLS-1$
	    public static final String OVER = "OVER"; //$NON-NLS-1$
	    public static final String OVERLAPS = "OVERLAPS"; //$NON-NLS-1$
	    public static final String PARAMETER = "PARAMETER"; //$NON-NLS-1$
	    public static final String PARTITION = "PARTITION"; //$NON-NLS-1$
	    public static final String PRECISION = "PRECISION"; //$NON-NLS-1$
	    public static final String PREPARE = "PREPARE"; //$NON-NLS-1$
	    public static final String PRIMARY = "PRIMARY"; //$NON-NLS-1$
	    public static final String PROCEDURE = "PROCEDURE"; //$NON-NLS-1$
	    public static final String RANGE = "RANGE"; //$NON-NLS-1$
	    public static final String READS = "READS"; //$NON-NLS-1$
	    public static final String REAL = "REAL"; //$NON-NLS-1$
	    public static final String RECURSIVE = "RECURSIVE"; //$NON-NLS-1$
	    public static final String REFERENCES = "REFERENCES"; //$NON-NLS-1$
	    public static final String REFERENCING = "REFERENCING"; //$NON-NLS-1$
	    public static final String RELEASE = "RELEASE"; //$NON-NLS-1$
	    public static final String RETURN = "RETURN"; //$NON-NLS-1$
	    public static final String RETURNS = "RETURNS"; //$NON-NLS-1$
	    public static final String REVOKE = "REVOKE"; //$NON-NLS-1$
	    public static final String RIGHT = "RIGHT"; //$NON-NLS-1$
	    public static final String ROLLBACK = "ROLLBACK"; //$NON-NLS-1$
	    public static final String ROLLUP = "ROLLUP"; //$NON-NLS-1$
	    public static final String ROW = "ROW"; //$NON-NLS-1$
	    public static final String ROWS = "ROWS"; //$NON-NLS-1$
	    public static final String SAVEPOINT = "SAVEPOINT"; //$NON-NLS-1$
	    public static final String SCROLL = "SCROLL"; //$NON-NLS-1$
	    public static final String SEARCH = "SEARCH"; //$NON-NLS-1$
	    public static final String SECOND = "SECOND"; //$NON-NLS-1$
		public static final String SELECT = "SELECT"; //$NON-NLS-1$
	    public static final String SENSITIVE = "SENSITIVE"; //$NON-NLS-1$
	    public static final String SESSION_USER = "SESSION_USER"; //$NON-NLS-1$
	    public static final String SET = "SET"; //$NON-NLS-1$
	    public static final String SHORT = "SHORT"; //$NON-NLS-1$
	    public static final String SIILAR = "SIMILAR"; //$NON-NLS-1$
	    public static final String SMALLINT = "SMALLINT"; //$NON-NLS-1$
	    public static final String SOME = "SOME"; //$NON-NLS-1$
	    public static final String SPECIFIC = "SPECIFIC"; //$NON-NLS-1$
	    public static final String SPECIFICTYPE = "SPECIFICTYPE"; //$NON-NLS-1$
	    public static final String SQL = "SQL"; //$NON-NLS-1$
	    public static final String SQLEXCEPTION = "SQLEXCEPTION"; //$NON-NLS-1$
	    public static final String SQLSTATE = "SQLSTATE"; //$NON-NLS-1$
	    public static final String SQLWARNING = "SQLWARNING"; //$NON-NLS-1$
	    public static final String SUBMULTILIST = "SUBMULTILIST"; //$NON-NLS-1$
	    public static final String START = "START"; //$NON-NLS-1$
	    public static final String STATIC = "STATIC"; //$NON-NLS-1$
	    public static final String SYMETRIC = "SYMETRIC"; //$NON-NLS-1$
	    public static final String SYSTEM = "SYSTEM"; //$NON-NLS-1$
	    public static final String SYSTEM_USER = "SYSTEM_USER"; //$NON-NLS-1$
		public static final String TABLE = "TABLE"; //$NON-NLS-1$
	    public static final String TEMPORARY = "TEMPORARY"; //$NON-NLS-1$
	    public static final String THEN = "THEN"; //$NON-NLS-1$
	    public static final String TIME = "TIME"; //$NON-NLS-1$
		public static final String TIMESTAMP = "TIMESTAMP"; //$NON-NLS-1$
	    public static final String TIMEZONE_HOUR = "TIMEZONE_HOUR"; //$NON-NLS-1$
	    public static final String TIMEZONE_MINUTE = "TIMEZONE_MINUTE"; //$NON-NLS-1$
	    public static final String TO = "TO"; //$NON-NLS-1$
	    public static final String TREAT = "TREAT"; //$NON-NLS-1$
	    public static final String TRAILING = "TRAILING"; //$NON-NLS-1$
		public static final String TRANSLATE = "TRANSLATE";	 //$NON-NLS-1$
		public static final String TRANSLATION = "TRANSLATION";	 //$NON-NLS-1$
		public static final String TRIGGER = "TRIGGER"; //$NON-NLS-1$
	    public static final String TRUE = "TRUE"; //$NON-NLS-1$
	    public static final String UNION = "UNION"; //$NON-NLS-1$
	    public static final String UNIQUE = "UNIQUE"; //$NON-NLS-1$
	    public static final String UNKNOWN = "UNKNOWN"; //$NON-NLS-1$
		public static final String UPDATE = "UPDATE"; //$NON-NLS-1$
		public static final String USER = "USER"; //$NON-NLS-1$
		public static final String USING = "USING";	 //$NON-NLS-1$
	    public static final String VALUE = "VALUE"; //$NON-NLS-1$
		public static final String VALUES = "VALUES"; //$NON-NLS-1$
	    public static final String VARCHAR = "VARCHAR"; //$NON-NLS-1$
	    public static final String VARYING = "VARYING"; //$NON-NLS-1$
	    public static final String WHEN = "WHEN";     //$NON-NLS-1$
	    public static final String WHENEVER = "WHENEVER";     //$NON-NLS-1$
	    public static final String WHERE = "WHERE"; //$NON-NLS-1$
	    public static final String WINDOW = "WINDOW"; //$NON-NLS-1$
	    public static final String WITH = "WITH";     //$NON-NLS-1$
	    public static final String WITHIN = "WITHIN"; //$NON-NLS-1$
	    public static final String WITHOUT = "WITHOUT"; //$NON-NLS-1$
		public static final String YEAR = "YEAR"; //$NON-NLS-1$
		
		// SQL 2008 words
		public static final String ARRAY_AGG= "ARRAY_AGG"; //$NON-NLS-1$
	    
		//SQL/XML
		
		public static final String XML = "XML"; //$NON-NLS-1$
	    public static final String XMLAGG = "XMLAGG"; //$NON-NLS-1$
	    public static final String XMLATTRIBUTES = "XMLATTRIBUTES"; //$NON-NLS-1$
	    public static final String XMLBINARY = "XMLBINARY"; //$NON-NLS-1$
	    public static final String XMLCAST = "XMLCAST"; //$NON-NLS-1$
	    public static final String XMLCOMMENT = "XMLCOMMENT"; //$NON-NLS-1$
	    public static final String XMLCONCAT = "XMLCONCAT"; //$NON-NLS-1$
	    public static final String XMLDOCUMENT = "XMLDOCUMENT"; //$NON-NLS-1$
	    public static final String XMLELEMENT = "XMLELEMENT"; //$NON-NLS-1$
	    public static final String XMLEXISTS = "XMLEXISTS"; //$NON-NLS-1$
	    public static final String XMLFOREST = "XMLFOREST"; //$NON-NLS-1$
	    public static final String XMLITERATE = "XMLITERATE"; //$NON-NLS-1$
	    public static final String XMLNAMESPACES = "XMLNAMESPACES"; //$NON-NLS-1$
	    public static final String XMLPARSE = "XMLPARSE"; //$NON-NLS-1$
	    public static final String XMLPI = "XMLPI"; //$NON-NLS-1$
	    public static final String XMLQUERY = "XMLQUERY"; //$NON-NLS-1$
	    public static final String XMLSERIALIZE = "XMLSERIALIZE"; //$NON-NLS-1$
	    public static final String XMLTABLE = "XMLTABLE"; //$NON-NLS-1$
	    public static final String XMLTEXT = "XMLTEXT"; //$NON-NLS-1$
	    public static final String XMLVALIDATE = "XMLVALIDATE"; //$NON-NLS-1$
	    
	    //SQL/MED
	    
	    public static final String DATALINK = "DATALINK"; //$NON-NLS-1$
	    public static final String DLNEWCOPY = "DLNEWCOPY"; //$NON-NLS-1$
	    public static final String DLPREVIOUSCOPY = "DLPREVIOUSCOPY"; //$NON-NLS-1$
	    public static final String DLURLCOMPLETE = "DLURLCOMPLETE"; //$NON-NLS-1$
	    public static final String DLURLCOMPLETEWRITE = "DLURELCOMPLETEWRITE"; //$NON-NLS-1$
	    public static final String DLURLCOMPLETEONLY = "DLURLCOMPLETEONLY"; //$NON-NLS-1$
	    public static final String DLURLPATH = "DLURLPATH"; //$NON-NLS-1$
	    public static final String DLURLPATHWRITE = "DLURLPATHWRITE"; //$NON-NLS-1$
	    public static final String DLURLPATHONLY = "DLURLPATHONLY"; //$NON-NLS-1$
	    public static final String DLURLSCHEME = "DLURLSCHEME"; //$NON-NLS-1$
	    public static final String DLURLSERVER = "DLURLSEVER"; //$NON-NLS-1$
	    public static final String DLVALUE = "DLVALUE"; //$NON-NLS-1$
	    public static final String IMPORT = "IMPORT"; //$NON-NLS-1$
	}
        
    /**
 	 * Set of CAPITALIZED reserved words for checking whether a string is a reserved word.
 	 */
    private static final Set<String> RESERVED_WORDS = extractFieldNames(SQLConstants.Reserved.class);
    private static final Set<String> NON_RESERVED_WORDS = extractFieldNames(SQLConstants.NonReserved.class);

    /**
     * @throws AssertionError
     */
    private static Set<String> extractFieldNames(Class<?> clazz) throws AssertionError {
        HashSet<String> result = new HashSet<String>();
        Field[] fields = clazz.getDeclaredFields();
 		for (Field field : fields) {
 			if (field.getType() == String.class) {
				try {
					if (!result.add((String)field.get(null))) {
						throw new AssertionError("Duplicate value for " + field.getName()); //$NON-NLS-1$
					}
				} catch (Exception e) {
				}
 			}
 		}
 		return Collections.unmodifiableSet(result);
    }
    
    /**
     * @return nonReservedWords
     */
    public static Set<String> getNonReservedWords() {
        return NON_RESERVED_WORDS;
    }
    
    /**
     * @return reservedWords
     */
    public static Set<String> getReservedWords() {
        return RESERVED_WORDS;
    }
    
 	/** Can't construct */
 	private SQLConstants() {}   

 	/**
 	 * Check whether a string is a reserved word.  
 	 * @param str String to check
 	 * @return True if reserved word, false if not or null
 	 */
 	public static final boolean isReservedWord(String str) {
 		if(str == null) { 
 			return false;    
 		}
 		return RESERVED_WORDS.contains(str.toUpperCase());    
 	}
}
