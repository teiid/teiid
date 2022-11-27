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

package org.teiid.language;

import java.lang.reflect.Field;
import java.util.Collections;
import java.util.Set;
import java.util.TreeSet;

/**
 * SQL Constants for Teiid.
 */
public class SQLConstants {

    public interface Tokens {
        static final String ALL_COLS = "*"; //$NON-NLS-1$
        static final String SPACE = " "; //$NON-NLS-1$
        static final String COMMA = ","; //$NON-NLS-1$
        static final String DOT = "."; //$NON-NLS-1$
        static final String QUOTE = "'"; //$NON-NLS-1$
        static final String EQ = "="; //$NON-NLS-1$
        static final String NE = "<>"; //$NON-NLS-1$
        static final String LT = "<"; //$NON-NLS-1$
        static final String GT = ">"; //$NON-NLS-1$
        static final String LE = "<="; //$NON-NLS-1$
        static final String GE = ">="; //$NON-NLS-1$
        static final String LPAREN = "("; //$NON-NLS-1$
        static final String RPAREN = ")"; //$NON-NLS-1$
        static final String LSBRACE = "["; //$NON-NLS-1$
        static final String RSBRACE = "]"; //$NON-NLS-1$
        static final String COLON = ":"; //$NON-NLS-1$
        static final String TICK = "'"; //$NON-NLS-1$
        static final String SEMICOLON = ";"; //$NON-NLS-1$
        static final String DOUBLE_AMP = "&&"; //$NON-NLS-1$
        static final String PLUS = "+"; //$NON-NLS-1$
        static final String MINUS = "-"; //$NON-NLS-1$
        static final String SLASH = "/"; //$NON-NLS-1$
        static final String CONCAT = "||"; //$NON-NLS-1$
    }

    public interface NonReserved {
        static final String SQL_TSI_FRAC_SECOND = "SQL_TSI_FRAC_SECOND"; //$NON-NLS-1$
        static final String SQL_TSI_SECOND = "SQL_TSI_SECOND"; //$NON-NLS-1$
        static final String SQL_TSI_MINUTE = "SQL_TSI_MINUTE"; //$NON-NLS-1$
        static final String SQL_TSI_HOUR = "SQL_TSI_HOUR"; //$NON-NLS-1$
        static final String SQL_TSI_DAY = "SQL_TSI_DAY"; //$NON-NLS-1$
        static final String SQL_TSI_WEEK = "SQL_TSI_WEEK"; //$NON-NLS-1$
        static final String SQL_TSI_MONTH = "SQL_TSI_MONTH"; //$NON-NLS-1$
        static final String SQL_TSI_QUARTER = "SQL_TSI_QUARTER"; //$NON-NLS-1$
        static final String SQL_TSI_YEAR = "SQL_TSI_YEAR"; //$NON-NLS-1$
        static final String TIMESTAMPADD = "TIMESTAMPADD"; //$NON-NLS-1$
        static final String TIMESTAMPDIFF = "TIMESTAMPDIFF"; //$NON-NLS-1$
        //aggregate functions
        static final String MAX = "MAX"; //$NON-NLS-1$
        static final String MIN = "MIN"; //$NON-NLS-1$
        static final String COUNT = "COUNT"; //$NON-NLS-1$
        static final String COUNT_BIG = "COUNT_BIG"; //$NON-NLS-1$
        static final String AVG = "AVG"; //$NON-NLS-1$
        static final String SUM = "SUM"; //$NON-NLS-1$
        //texttable
        static final String WIDTH = "WIDTH"; //$NON-NLS-1$
        static final String DELIMITER = "DELIMITER"; //$NON-NLS-1$
        static final String HEADER = "HEADER"; //$NON-NLS-1$
        static final String QUOTE = "QUOTE"; //$NON-NLS-1$
        static final String COLUMNS = "COLUMNS"; //$NON-NLS-1$
        static final String SELECTOR = "SELECTOR"; //$NON-NLS-1$
        static final String SKIP = "SKIP"; //$NON-NLS-1$
        //xmltable
        static final String ORDINALITY = "ORDINALITY"; //$NON-NLS-1$
        static final String PASSING = "PASSING"; //$NON-NLS-1$
        static final String PATH = "PATH"; //$NON-NLS-1$
        //xmlserialize
        static final String DOCUMENT = "DOCUMENT"; //$NON-NLS-1$
        static final String CONTENT = "CONTENT"; //$NON-NLS-1$
        //xmlquery
        static final String RETURNING = "RETURNING"; //$NON-NLS-1$
        static final String SEQUENCE = "SEQUENCE"; //$NON-NLS-1$
        static final String EMPTY = "EMPTY"; //$NON-NLS-1$
        //querystring function
        static final String QUERYSTRING = "QUERYSTRING"; //$NON-NLS-1$
        //xmlparse
        static final String WELLFORMED = "WELLFORMED"; //$NON-NLS-1$
        //agg
        static final String EVERY = "EVERY"; //$NON-NLS-1$
        static final String STDDEV_POP = "STDDEV_POP"; //$NON-NLS-1$
        static final String STDDEV_SAMP = "STDDEV_SAMP"; //$NON-NLS-1$
        static final String VAR_SAMP = "VAR_SAMP"; //$NON-NLS-1$
        static final String VAR_POP = "VAR_POP"; //$NON-NLS-1$

        static final String NULLS = "NULLS"; //$NON-NLS-1$
        static final String FIRST = "FIRST"; //$NON-NLS-1$
        static final String LAST = "LAST"; //$NON-NLS-1$

        static final String KEY = "KEY"; //$NON-NLS-1$

        static final String SERIAL = "SERIAL"; //$NON-NLS-1$

        static final String ENCODING = "ENCODING"; //$NON-NLS-1$
        static final String TEXTAGG = "TEXTAGG"; //$NON-NLS-1$

        static final String ARRAYTABLE = "ARRAYTABLE"; //$NON-NLS-1$

        static final String VIEW = "VIEW"; //$NON-NLS-1$
        static final String INSTEAD = "INSTEAD"; //$NON-NLS-1$
        static final String ENABLED = "ENABLED"; //$NON-NLS-1$
        static final String DISABLED = "DISABLED"; //$NON-NLS-1$

        static final String TRIM = "TRIM"; //$NON-NLS-1$
        static final String POSITION = "POSITION"; //$NON-NLS-1$
        static final String RESULT = "RESULT"; //$NON-NLS-1$
        static final String OBJECTTABLE = "OBJECTTABLE"; //$NON-NLS-1$
        static final String VERSION = "VERSION"; //$NON-NLS-1$
        static final String INCLUDING = "INCLUDING"; //$NON-NLS-1$
        static final String EXCLUDING = "EXCLUDING"; //$NON-NLS-1$
        static final String XMLDECLARATION = "XMLDECLARATION"; //$NON-NLS-1$
        static final String VARIADIC = "VARIADIC"; //$NON-NLS-1$
        static final String INDEX = "INDEX"; //$NON-NLS-1$
        static final String EXCEPTION = "EXCEPTION"; //$NON-NLS-1$
        static final String RAISE = "RAISE"; //$NON-NLS-1$
        static final String CHAIN = "CHAIN"; //$NON-NLS-1$
        static final String JSONTABLE = "JSONTABLE"; //$NON-NLS-1$
        static final String JSONOBJECT = "JSONOBJECT"; //$NON-NLS-1$
        static final String JSONARRAY_AGG = "JSONARRAY_AGG"; //$NON-NLS-1$
        static final String JSON = "JSON"; //$NON-NLS-1$

        static final String AUTO_INCREMENT = "AUTO_INCREMENT"; //$NON-NLS-1$

        static final String PRESERVE = "PRESERVE"; //$NON-NLS-1$

        static final String GEOMETRY = "GEOMETRY"; //$NON-NLS-1$
        static final String GEOGRAPHY = "GEOGRAPHY"; //$NON-NLS-1$
        static final String UPSERT = "UPSERT"; //$NON-NLS-1$
        static final String AFTER = "AFTER"; //$NON-NLS-1$

        //ddl
        static final String AUTHENTICATED = "AUTHENTICATED"; //$NON-NLS-1$
        static final String TYPE = "TYPE"; //$NON-NLS-1$$
        static final String TRANSLATOR = "TRANSLATOR"; //$NON-NLS-1$
        static final String JAAS = "JAAS"; //$NON-NLS-1$
        static final String CONDITION= "CONDITION"; //$NON-NLS-1$
        static final String MASK = "MASK"; //$NON-NLS-1$
        static final String ACCESS = "ACCESS"; //$NON-NLS-1$
        static final String CONTROL = "CONTROL"; //$NON-NLS-1$
        static final String DATABASE = "DATABASE"; //$NON-NLS-1$
        static final String DATA = "DATA"; //$NON-NLS-1$
        static final String PRIVILEGES = "PRIVILEGES"; //$NON-NLS-1$
        static final String ROLE = "ROLE"; //$NON-NLS-1$
        static final String SCHEMA = "SCHEMA"; //$NON-NLS-1$
        static final String USE = "USE"; //$NON-NLS-1$
        static final String SERVER = "SERVER"; //$NON-NLS-1$
        static final String WRAPPER = "WRAPPER"; //$NON-NLS-1$
        static final String NONE = "NONE"; //$NON-NLS-1$
        static final String REPOSITORY= "REPOSITORY"; //$NON-NLS-1$
        static final String RENAME = "RENAME"; //$NON-NLS-1$
        static final String DOMAIN = "DOMAIN"; //$NON-NLS-1$
        static final String USAGE = "USAGE"; //$NON-NLS-1$
        static final String ROW_NUMBER = "ROW_NUMBER"; //$NON-NLS-1$
        static final String RANK = "RANK"; //$NON-NLS-1$
        static final String DENSE_RANK = "DENSE_RANK"; //$NON-NLS-1$
        static final String PERCENT_RANK = "PERCENT_RANK"; //$NON-NLS-1$
        static final String CUME_DIST = "CUME_DIST"; //$NON-NLS-1$
        static final String CURRENT = "CURRENT"; //$NON-NLS-1$
        static final String UNBOUNDED = "UNBOUNDED"; //$NON-NLS-1$
        static final String PRECEDING = "PRECEDING"; //$NON-NLS-1$
        static final String FOLLOWING = "FOLLOWING"; //$NON-NLS-1$

        static final String LISTAGG = "LISTAGG"; //$NON-NLS-1$

        static final String OBJECT = "OBJECT"; //$NON-NLS-1$

        //explain
        static final String EXPLAIN = "EXPLAIN"; //$NON-NLS-1$
        static final String FORMAT = "FORMAT"; //$NON-NLS-1$
        static final String YAML = "YAML"; //$NON-NLS-1$
        static final String ANALYZE = "ANALYZE"; //$NON-NLS-1$
        static final String TEXT = "TEXT"; //$NON-NLS-1$

        //fdw
        static final String HANDLER = "HANDLER"; //$NON-NLS-1$

        static final String EPOCH = "EPOCH"; //$NON-NLS-1$
        static final String QUARTER = "QUARTER"; //$NON-NLS-1$
        static final String DOW = "DOW"; //$NON-NLS-1$
        static final String DOY = "DOY"; //$NON-NLS-1$

        //policy
        static final String POLICY = "POLICY"; //$NON-NLS-1$
    }

    public interface Reserved {
        //Teiid specific
        static final String BIGDECIMAL = "BIGDECIMAL"; //$NON-NLS-1$
        static final String BIGINTEGER = "BIGINTEGER"; //$NON-NLS-1$
        static final String BREAK = "BREAK"; //$NON-NLS-1$
        static final String BYTE = "BYTE"; //$NON-NLS-1$
        static final String CRITERIA = "CRITERIA"; //$NON-NLS-1$
        static final String ERROR = "ERROR";  //$NON-NLS-1$
        static final String LIMIT = "LIMIT"; //$NON-NLS-1$
        static final String LONG = "LONG"; //$NON-NLS-1$
        static final String LOOP = "LOOP"; //$NON-NLS-1$
        static final String MAKEDEP = "MAKEDEP"; //$NON-NLS-1$
        static final String MAKEIND = "MAKEIND"; //$NON-NLS-1$
        static final String MAKENOTDEP = "MAKENOTDEP"; //$NON-NLS-1$
        static final String NOCACHE = "NOCACHE"; //$NON-NLS-1$
        static final String STRING = "STRING"; //$NON-NLS-1$
        static final String VIRTUAL = "VIRTUAL"; //$NON-NLS-1$
        static final String WHILE = "WHILE"; //$NON-NLS-1$

        //SQL2003 keywords
        static final String ADD = "ADD"; //$NON-NLS-1$
        static final String ANY = "ANY"; //$NON-NLS-1$
        static final String ALL = "ALL"; //$NON-NLS-1$
        static final String ALLOCATE = "ALLOCATE"; //$NON-NLS-1$
        static final String ALTER = "ALTER"; //$NON-NLS-1$
        static final String AND = "AND"; //$NON-NLS-1$
        static final String ARE = "ARE"; //$NON-NLS-1$
        static final String ARRAY = "ARRAY"; //$NON-NLS-1$s
        static final String AS = "AS"; //$NON-NLS-1$
        static final String ASC = "ASC"; //$NON-NLS-1$
        static final String ASENSITIVE = "ASENSITIVE"; //$NON-NLS-1$
        static final String ASYMETRIC = "ASYMETRIC"; //$NON-NLS-1$
        static final String ATOMIC = "ATOMIC"; //$NON-NLS-1$
        static final String AUTHORIZATION = "AUTHORIZATION"; //$NON-NLS-1$
        static final String BEGIN = "BEGIN"; //$NON-NLS-1$
        static final String BETWEEN = "BETWEEN"; //$NON-NLS-1$
        static final String BIGINT = "BIGINT"; //$NON-NLS-1$
        static final String BINARY = "BINARY"; //$NON-NLS-1$
        static final String BLOB = "BLOB"; //$NON-NLS-1$
        static final String BOTH = "BOTH"; //$NON-NLS-1$
        static final String BY = "BY"; //$NON-NLS-1$
        static final String CALL = "CALL"; //$NON-NLS-1$
        static final String CALLED = "CALLED"; //$NON-NLS-1$
        static final String CASE = "CASE"; //$NON-NLS-1$
        static final String CAST = "CAST"; //$NON-NLS-1$
        static final String CASCADED = "CASCADED"; //$NON-NLS-1$
        static final String CHAR = "CHAR"; //$NON-NLS-1$
        static final String CHARACTER = "CHARACTER"; //$NON-NLS-1$
        static final String CHECK = "CHECK"; //$NON-NLS-1$
        static final String CLOB = "CLOB"; //$NON-NLS-1$
        static final String CLOSE = "CLOSE"; //$NON-NLS-1$
        static final String COLLATE = "COLLATE"; //$NON-NLS-1$
        static final String COLUMN = "COLUMN"; //$NON-NLS-1$
        static final String COMMIT = "COMMIT"; //$NON-NLS-1$
        static final String CONNECT = "CONNECT"; //$NON-NLS-1$
        static final String CONVERT = "CONVERT"; //$NON-NLS-1$
        static final String CONSTRAINT = "CONSTRAINT"; //$NON-NLS-1$
        static final String CONTINUE = "CONTINUE"; //$NON-NLS-1$
        static final String CORRESPONDING = "CORRESPONDING"; //$NON-NLS-1$
        static final String CREATE = "CREATE"; //$NON-NLS-1$
        static final String CROSS = "CROSS"; //$NON-NLS-1$
        static final String CURRENT_DATE = "CURRENT_DATE"; //$NON-NLS-1$
        static final String CURRENT_TIME = "CURRENT_TIME"; //$NON-NLS-1$
        static final String CURRENT_TIMESTAMP = "CURRENT_TIMESTAMP"; //$NON-NLS-1$
        static final String CURRENT_USER = "CURRENT_USER"; //$NON-NLS-1$
        static final String CURSOR = "CURSOR"; //$NON-NLS-1$
        static final String CYCLE = "CYCLE"; //$NON-NLS-1$
        static final String DATE = "DATE"; //$NON-NLS-1$
        static final String DAY = "DAY"; //$NON-NLS-1$
        static final String DEALLOCATE = "DEALLOCATE"; //$NON-NLS-1$
        static final String DEC = "DEC"; //$NON-NLS-1$
        static final String DECIMAL = "DECIMAL"; //$NON-NLS-1$
        static final String DECLARE = "DECLARE";     //$NON-NLS-1$
        static final String DEFAULT = "DEFAULT"; //$NON-NLS-1$
        static final String DELETE = "DELETE"; //$NON-NLS-1$
        static final String DEREF = "DEREF"; //$NON-NLS-1$
        static final String DESC = "DESC"; //$NON-NLS-1$
        static final String DESCRIBE = "DESCRIBE"; //$NON-NLS-1$
        static final String DETERMINISTIC = "DETERMINISTIC"; //$NON-NLS-1$
        static final String DISCONNECT = "DISCONNECT"; //$NON-NLS-1$
        static final String DISTINCT = "DISTINCT"; //$NON-NLS-1$
        static final String DOUBLE = "DOUBLE"; //$NON-NLS-1$
        static final String DROP = "DROP"; //$NON-NLS-1$
        static final String DYNAMIC = "DYNAMIC"; //$NON-NLS-1$
        static final String EACH = "EACH"; //$NON-NLS-1$
        static final String ELEMENT = "ELEMENT"; //$NON-NLS-1$
        static final String ELSE = "ELSE";  //$NON-NLS-1$
        static final String END = "END"; //$NON-NLS-1$
        static final String ESCAPE = "ESCAPE"; //$NON-NLS-1$
        static final String EXCEPT = "EXCEPT"; //$NON-NLS-1$
        static final String EXEC = "EXEC"; //$NON-NLS-1$
        static final String EXECUTE = "EXECUTE"; //$NON-NLS-1$
        static final String EXISTS = "EXISTS"; //$NON-NLS-1$
        static final String EXTERNAL = "EXTERNAL"; //$NON-NLS-1$
        static final String FALSE = "FALSE"; //$NON-NLS-1$
        static final String FETCH = "FETCH"; //$NON-NLS-1$
        static final String FILTER = "FILTER"; //$NON-NLS-1$
        static final String FLOAT = "FLOAT"; //$NON-NLS-1$
        static final String FOR = "FOR";     //$NON-NLS-1$
        static final String FOREIGN = "FOREIGN"; //$NON-NLS-1$
        static final String FREE = "FREE"; //$NON-NLS-1$
        static final String FROM = "FROM"; //$NON-NLS-1$
        static final String FULL = "FULL"; //$NON-NLS-1$
        static final String FUNCTION = "FUNCTION"; //$NON-NLS-1$
        static final String GET = "GET"; //$NON-NLS-1$
        static final String GLOBAL = "GLOBAL"; //$NON-NLS-1$
        static final String GRANT = "GRANT"; //$NON-NLS-1$
        static final String GROUP = "GROUP"; //$NON-NLS-1$
        static final String GROUPING = "GROUPING"; //$NON-NLS-1$
        static final String HAS = "HAS";  //$NON-NLS-1$
        static final String HAVING = "HAVING"; //$NON-NLS-1$
        static final String HOLD = "HOLD"; //$NON-NLS-1$
        static final String HOUR = "HOUR"; //$NON-NLS-1$
        static final String IDENTITY = "IDENTITY"; //$NON-NLS-1$
        static final String INDICATOR = "INDICATOR"; //$NON-NLS-1$
        static final String IF = "IF";     //$NON-NLS-1$
        static final String IMMEDIATE = "IMMEDIATE"; //$NON-NLS-1$
        static final String IN = "IN"; //$NON-NLS-1$
        static final String INOUT = "INOUT"; //$NON-NLS-1$
        static final String INNER = "INNER"; //$NON-NLS-1$
        static final String INPUT = "INPUT"; //$NON-NLS-1$
        static final String INSENSITIVE = "INSENSITIVE"; //$NON-NLS-1$
        static final String INSERT = "INSERT"; //$NON-NLS-1$
        static final String INTEGER = "INTEGER"; //$NON-NLS-1$
        static final String INTERSECT = "INTERSECT"; //$NON-NLS-1$
        static final String INTERVAL = "INTERVAL"; //$NON-NLS-1$
        static final String INT = "INT"; //$NON-NLS-1$
        static final String INTO = "INTO"; //$NON-NLS-1$
        static final String IS = "IS";     //$NON-NLS-1$
        static final String ISOLATION = "ISOLATION"; //$NON-NLS-1$
        static final String JOIN = "JOIN"; //$NON-NLS-1$
        static final String LANGUAGE = "LANGUAGE"; //$NON-NLS-1$
        static final String LARGE = "LARGE"; //$NON-NLS-1$
        static final String LATERAL = "LATERAL"; //$NON-NLS-1$
        static final String LEADING = "LEADING"; //$NON-NLS-1$
        static final String LEAVE = "LEAVE"; //$NON-NLS-1$
        static final String LEFT = "LEFT"; //$NON-NLS-1$
        static final String LIKE = "LIKE"; //$NON-NLS-1$
        static final String LIKE_REGEX = "LIKE_REGEX"; //$NON-NLS-1$
        static final String LOCAL = "LOCAL"; //$NON-NLS-1$
        static final String LOCALTIME = "LOCALTIME"; //$NON-NLS-1$
        static final String LOCALTIMESTAMP = "LOCALTIMESTAMP"; //$NON-NLS-1$
        static final String MATCH = "MATCH"; //$NON-NLS-1$
        static final String MEMBER = "MEMBER"; //$NON-NLS-1$
        static final String MERGE = "MERGE"; //$NON-NLS-1$
        static final String METHOD = "METHOD"; //$NON-NLS-1$
        static final String MINUTE = "MINUTE"; //$NON-NLS-1$
        static final String MODIFIES = "MODIFIES"; //$NON-NLS-1$
        static final String MODULE = "MODULE"; //$NON-NLS-1$
        static final String MONTH = "MONTH"; //$NON-NLS-1$
        static final String MULTISET = "MULTISET"; //$NON-NLS-1$
        static final String NATIONAL = "NATIONAL"; //$NON-NLS-1$
        static final String NATURAL = "NATURAL"; //$NON-NLS-1$
        static final String NCHAR = "NCHAR"; //$NON-NLS-1$
        static final String NCLOB = "NCLOB"; //$NON-NLS-1$
        static final String NEW = "NEW"; //$NON-NLS-1$
        static final String NO = "NO"; //$NON-NLS-1$
        static final String NOT = "NOT"; //$NON-NLS-1$
        static final String NULL = "NULL"; //$NON-NLS-1$
        static final String NUMERIC = "NUMERIC"; //$NON-NLS-1$
        static final String OF = "OF"; //$NON-NLS-1$
        static final String OFFSET = "OFFSET"; //$NON-NLS-1$
        static final String OLD = "OLD"; //$NON-NLS-1$
        static final String ON = "ON"; //$NON-NLS-1$
        static final String ONLY = "ONLY"; //$NON-NLS-1$
        static final String OPEN = "OPEN"; //$NON-NLS-1$
        static final String OR = "OR"; //$NON-NLS-1$
        static final String ORDER = "ORDER"; //$NON-NLS-1$
        static final String OUT = "OUT"; //$NON-NLS-1$
        static final String OUTER = "OUTER"; //$NON-NLS-1$
        static final String OUTPUT = "OUTPUT"; //$NON-NLS-1$
        static final String OPTION = "OPTION"; //$NON-NLS-1$
        static final String OPTIONS = "OPTIONS"; //$NON-NLS-1$
        static final String OVER = "OVER"; //$NON-NLS-1$
        static final String OVERLAPS = "OVERLAPS"; //$NON-NLS-1$
        static final String PARAMETER = "PARAMETER"; //$NON-NLS-1$
        static final String PARTITION = "PARTITION"; //$NON-NLS-1$
        static final String PRECISION = "PRECISION"; //$NON-NLS-1$
        static final String PREPARE = "PREPARE"; //$NON-NLS-1$
        static final String PRIMARY = "PRIMARY"; //$NON-NLS-1$
        static final String PROCEDURE = "PROCEDURE"; //$NON-NLS-1$
        static final String RANGE = "RANGE"; //$NON-NLS-1$
        static final String READS = "READS"; //$NON-NLS-1$
        static final String REAL = "REAL"; //$NON-NLS-1$
        static final String RECURSIVE = "RECURSIVE"; //$NON-NLS-1$
        static final String REFERENCES = "REFERENCES"; //$NON-NLS-1$
        static final String REFERENCING = "REFERENCING"; //$NON-NLS-1$
        static final String RELEASE = "RELEASE"; //$NON-NLS-1$
        static final String RETURN = "RETURN"; //$NON-NLS-1$
        static final String RETURNS = "RETURNS"; //$NON-NLS-1$
        static final String REVOKE = "REVOKE"; //$NON-NLS-1$
        static final String RIGHT = "RIGHT"; //$NON-NLS-1$
        static final String ROLLBACK = "ROLLBACK"; //$NON-NLS-1$
        static final String ROLLUP = "ROLLUP"; //$NON-NLS-1$
        static final String ROW = "ROW"; //$NON-NLS-1$
        static final String ROWS = "ROWS"; //$NON-NLS-1$
        static final String SAVEPOINT = "SAVEPOINT"; //$NON-NLS-1$
        static final String SCROLL = "SCROLL"; //$NON-NLS-1$
        static final String SEARCH = "SEARCH"; //$NON-NLS-1$
        static final String SECOND = "SECOND"; //$NON-NLS-1$
        static final String SELECT = "SELECT"; //$NON-NLS-1$
        static final String SENSITIVE = "SENSITIVE"; //$NON-NLS-1$
        static final String SESSION_USER = "SESSION_USER"; //$NON-NLS-1$
        static final String SET = "SET"; //$NON-NLS-1$
        static final String SHORT = "SHORT"; //$NON-NLS-1$
        static final String SIMILAR = "SIMILAR"; //$NON-NLS-1$
        static final String SMALLINT = "SMALLINT"; //$NON-NLS-1$
        static final String SOME = "SOME"; //$NON-NLS-1$
        static final String SPECIFIC = "SPECIFIC"; //$NON-NLS-1$
        static final String SPECIFICTYPE = "SPECIFICTYPE"; //$NON-NLS-1$
        static final String SQL = "SQL"; //$NON-NLS-1$
        static final String SQLEXCEPTION = "SQLEXCEPTION"; //$NON-NLS-1$
        static final String SQLSTATE = "SQLSTATE"; //$NON-NLS-1$
        static final String SQLWARNING = "SQLWARNING"; //$NON-NLS-1$
        static final String SUBMULTILIST = "SUBMULTILIST"; //$NON-NLS-1$
        static final String START = "START"; //$NON-NLS-1$
        static final String STATIC = "STATIC"; //$NON-NLS-1$
        static final String SYMETRIC = "SYMETRIC"; //$NON-NLS-1$
        static final String SYSTEM = "SYSTEM"; //$NON-NLS-1$
        static final String SYSTEM_USER = "SYSTEM_USER"; //$NON-NLS-1$
        static final String TABLE = "TABLE"; //$NON-NLS-1$
        static final String TEMPORARY = "TEMPORARY"; //$NON-NLS-1$
        static final String THEN = "THEN"; //$NON-NLS-1$
        static final String TIME = "TIME"; //$NON-NLS-1$
        static final String TIMESTAMP = "TIMESTAMP"; //$NON-NLS-1$
        static final String TIMEZONE_HOUR = "TIMEZONE_HOUR"; //$NON-NLS-1$
        static final String TIMEZONE_MINUTE = "TIMEZONE_MINUTE"; //$NON-NLS-1$
        static final String TO = "TO"; //$NON-NLS-1$
        static final String TREAT = "TREAT"; //$NON-NLS-1$
        static final String TRAILING = "TRAILING"; //$NON-NLS-1$
        static final String TRANSLATE = "TRANSLATE";  //$NON-NLS-1$
        static final String TRANSLATION = "TRANSLATION";  //$NON-NLS-1$
        static final String TRIGGER = "TRIGGER"; //$NON-NLS-1$
        static final String TRUE = "TRUE"; //$NON-NLS-1$
        static final String UNION = "UNION"; //$NON-NLS-1$
        static final String UNIQUE = "UNIQUE"; //$NON-NLS-1$
        static final String UNKNOWN = "UNKNOWN"; //$NON-NLS-1$
        static final String UPDATE = "UPDATE"; //$NON-NLS-1$
        static final String USER = "USER"; //$NON-NLS-1$
        static final String USING = "USING";  //$NON-NLS-1$
        static final String VALUE = "VALUE"; //$NON-NLS-1$
        static final String VALUES = "VALUES"; //$NON-NLS-1$
        static final String VARCHAR = "VARCHAR"; //$NON-NLS-1$
        static final String VARYING = "VARYING"; //$NON-NLS-1$
        static final String WHEN = "WHEN";     //$NON-NLS-1$
        static final String WHENEVER = "WHENEVER";     //$NON-NLS-1$
        static final String WHERE = "WHERE"; //$NON-NLS-1$
        static final String WINDOW = "WINDOW"; //$NON-NLS-1$
        static final String WITH = "WITH";     //$NON-NLS-1$
        static final String WITHIN = "WITHIN"; //$NON-NLS-1$
        static final String WITHOUT = "WITHOUT"; //$NON-NLS-1$
        static final String YEAR = "YEAR"; //$NON-NLS-1$

        // SQL 2008 words
        static final String ARRAY_AGG= "ARRAY_AGG"; //$NON-NLS-1$

        //SQL/XML

        static final String XML = "XML"; //$NON-NLS-1$
        static final String XMLAGG = "XMLAGG"; //$NON-NLS-1$
        static final String XMLATTRIBUTES = "XMLATTRIBUTES"; //$NON-NLS-1$
        static final String XMLBINARY = "XMLBINARY"; //$NON-NLS-1$
        static final String XMLCAST = "XMLCAST"; //$NON-NLS-1$
        static final String XMLCOMMENT = "XMLCOMMENT"; //$NON-NLS-1$
        static final String XMLCONCAT = "XMLCONCAT"; //$NON-NLS-1$
        static final String XMLDOCUMENT = "XMLDOCUMENT"; //$NON-NLS-1$
        static final String XMLELEMENT = "XMLELEMENT"; //$NON-NLS-1$
        static final String XMLEXISTS = "XMLEXISTS"; //$NON-NLS-1$
        static final String XMLFOREST = "XMLFOREST"; //$NON-NLS-1$
        static final String XMLITERATE = "XMLITERATE"; //$NON-NLS-1$
        static final String XMLNAMESPACES = "XMLNAMESPACES"; //$NON-NLS-1$
        static final String XMLPARSE = "XMLPARSE"; //$NON-NLS-1$
        static final String XMLPI = "XMLPI"; //$NON-NLS-1$
        static final String XMLQUERY = "XMLQUERY"; //$NON-NLS-1$
        static final String XMLSERIALIZE = "XMLSERIALIZE"; //$NON-NLS-1$
        static final String XMLTABLE = "XMLTABLE"; //$NON-NLS-1$
        static final String XMLTEXT = "XMLTEXT"; //$NON-NLS-1$
        static final String XMLVALIDATE = "XMLVALIDATE"; //$NON-NLS-1$

        //SQL/MED
        static final String DATALINK = "DATALINK"; //$NON-NLS-1$
        static final String DLNEWCOPY = "DLNEWCOPY"; //$NON-NLS-1$
        static final String DLPREVIOUSCOPY = "DLPREVIOUSCOPY"; //$NON-NLS-1$
        static final String DLURLCOMPLETE = "DLURLCOMPLETE"; //$NON-NLS-1$
        static final String DLURLCOMPLETEWRITE = "DLURELCOMPLETEWRITE"; //$NON-NLS-1$
        static final String DLURLCOMPLETEONLY = "DLURLCOMPLETEONLY"; //$NON-NLS-1$
        static final String DLURLPATH = "DLURLPATH"; //$NON-NLS-1$
        static final String DLURLPATHWRITE = "DLURLPATHWRITE"; //$NON-NLS-1$
        static final String DLURLPATHONLY = "DLURLPATHONLY"; //$NON-NLS-1$
        static final String DLURLSCHEME = "DLURLSCHEME"; //$NON-NLS-1$
        static final String DLURLSERVER = "DLURLSEVER"; //$NON-NLS-1$
        static final String DLVALUE = "DLVALUE"; //$NON-NLS-1$
        static final String IMPORT = "IMPORT"; //$NON-NLS-1$
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
        TreeSet<String> result = new TreeSet<String>(String.CASE_INSENSITIVE_ORDER);
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
         return RESERVED_WORDS.contains(str);
     }
}
