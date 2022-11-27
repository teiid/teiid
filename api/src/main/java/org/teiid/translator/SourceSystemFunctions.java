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

package org.teiid.translator;

import org.teiid.language.SQLConstants;

/**
 * Constants for all the possible standard system push down functions.
 * The names and function forms follow the Open Group CLI functions, with a few exceptions
 * (such as lpad, rpad, bitand, bitor, etc. which are most notably supported by Oracle).
 *
 * Note that not all system functions are listed as some functions will use a common name
 * such as CONCAT vs. the || operator, and other functions will be rewritten and
 * not pushed down, such as SPACE.
 *
 */
public class SourceSystemFunctions {

    //arithmetic
    public static final String MULTIPLY_OP = "*"; //$NON-NLS-1$
    public static final String ADD_OP = "+"; //$NON-NLS-1$
    public static final String SUBTRACT_OP = "-"; //$NON-NLS-1$
    public static final String DIVIDE_OP = "/"; //$NON-NLS-1$

    //String
    public static final String ASCII = "ascii"; //$NON-NLS-1$
    public static final String CHAR = "char"; //$NON-NLS-1$
    public static final String CONCAT = "concat"; //$NON-NLS-1$
    public static final String CONCAT2 = "concat2"; //$NON-NLS-1$
    //public static final String DIFFERENCE = "difference";
    public static final String INITCAP = "initcap"; //$NON-NLS-1$
    public static final String INSERT = "insert"; //$NON-NLS-1$
    public static final String LCASE = "lcase"; //$NON-NLS-1$
    public static final String LPAD = "lpad"; //$NON-NLS-1$
    public static final String LEFT = "left"; //$NON-NLS-1$
    public static final String LENGTH = "length"; //$NON-NLS-1$
    public static final String LOCATE = "locate"; //$NON-NLS-1$
    public static final String LTRIM = "ltrim"; //$NON-NLS-1$
    public static final String REPEAT = "repeat"; //$NON-NLS-1$
    public static final String REGEXP_REPLACE = "regexp_replace"; //$NON-NLS-1$
    public static final String REPLACE = "replace"; //$NON-NLS-1$
    public static final String RIGHT = "right"; //$NON-NLS-1$
    public static final String RPAD = "rpad"; //$NON-NLS-1$
    public static final String RTRIM = "rtrim"; //$NON-NLS-1$
    //public static final String SOUNDEX = "soundex";
    public static final String SUBSTRING = "substring"; //$NON-NLS-1$
    public static final String TO_BYTES = "to_bytes"; //$NON-NLS-1$
    public static final String TO_CHARS = "to_chars"; //$NON-NLS-1$
    public static final String ENDSWITH = "endswith"; //$NON-NLS-1$
    /**
     * The trim function is only used for a non-space trim character
     */
    public static final String TRIM = "trim"; //$NON-NLS-1$
    public static final String UCASE = "ucase"; //$NON-NLS-1$
    public static final String UNESCAPE = "unescape"; //$NON-NLS-1$

    //numeric
    public static final String ABS = "abs"; //$NON-NLS-1$
    public static final String ACOS = "acos"; //$NON-NLS-1$
    public static final String ASIN = "asin"; //$NON-NLS-1$
    public static final String ATAN = "atan"; //$NON-NLS-1$
    public static final String ATAN2 = "atan2"; //$NON-NLS-1$
    public static final String CEILING = "ceiling"; //$NON-NLS-1$
    public static final String COS = "cos"; //$NON-NLS-1$
    public static final String COT = "cot"; //$NON-NLS-1$
    public static final String DEGREES = "degrees"; //$NON-NLS-1$
    public static final String EXP = "exp"; //$NON-NLS-1$
    public static final String FLOOR = "floor"; //$NON-NLS-1$

    @Deprecated public static final String FORMATINTEGER = "formatinteger"; //$NON-NLS-1$
    @Deprecated public static final String FORMATLONG = "formatlong"; //$NON-NLS-1$
    @Deprecated public static final String FORMATDOUBLE = "formatdouble"; //$NON-NLS-1$
    @Deprecated public static final String FORMATFLOAT = "formatfloat"; //$NON-NLS-1$
    @Deprecated public static final String FORMATBIGINTEGER = "formatbiginteger"; //$NON-NLS-1$

    public static final String FORMATBIGDECIMAL = "formatbigdecimal"; //$NON-NLS-1$

    public static final String LOG = "log"; //$NON-NLS-1$
    public static final String LOG10 = "log10"; //$NON-NLS-1$
    public static final String MOD = "mod"; //$NON-NLS-1$

    @Deprecated public static final String PARSEINTEGER = "parseinteger"; //$NON-NLS-1$
    @Deprecated public static final String PARSELONG = "parselong"; //$NON-NLS-1$
    @Deprecated public static final String PARSEDOUBLE = "parsedouble"; //$NON-NLS-1$
    @Deprecated public static final String PARSEFLOAT = "parsefloat"; //$NON-NLS-1$
    @Deprecated public static final String PARSEBIGINTEGER = "parsebiginteger"; //$NON-NLS-1$

    public static final String PARSEBIGDECIMAL = "parsebigdecimal"; //$NON-NLS-1$
    public static final String PI = "pi"; //$NON-NLS-1$
    public static final String POWER = "power"; //$NON-NLS-1$
    public static final String RADIANS = "radians"; //$NON-NLS-1$
    public static final String RAND = "rand"; //$NON-NLS-1$
    public static final String ROUND = "round"; //$NON-NLS-1$
    public static final String SIGN = "sign"; //$NON-NLS-1$
    public static final String SIN = "sin"; //$NON-NLS-1$
    public static final String SQRT = "sqrt"; //$NON-NLS-1$
    public static final String TAN = "tan"; //$NON-NLS-1$
    public static final String TRANSLATE = "translate"; //$NON-NLS-1$
    public static final String TRUNCATE = "truncate"; //$NON-NLS-1$

    //bit
    public static final String BITAND = "bitand"; //$NON-NLS-1$
    public static final String BITOR = "bitor"; //$NON-NLS-1$
    public static final String BITNOT = "bitnot"; //$NON-NLS-1$
    public static final String BITXOR = "bitxor"; //$NON-NLS-1$

    //date functions
    public static final String CURDATE = "curdate"; //$NON-NLS-1$
    public static final String CURTIME = "curtime"; //$NON-NLS-1$
    public static final String DAYNAME = "dayname"; //$NON-NLS-1$
    public static final String DAYOFMONTH = "dayofmonth"; //$NON-NLS-1$
    public static final String DAYOFWEEK = "dayofweek"; //$NON-NLS-1$
    public static final String DAYOFYEAR = "dayofyear"; //$NON-NLS-1$
    public static final String FORMATTIMESTAMP = "formattimestamp"; //$NON-NLS-1$
    public static final String HOUR = "hour"; //$NON-NLS-1$
    public static final String MINUTE = "minute"; //$NON-NLS-1$
    public static final String MODIFYTIMEZONE = "modifytimezone"; //$NON-NLS-1$
    public static final String MONTH = "month"; //$NON-NLS-1$
    public static final String MONTHNAME = "monthname"; //$NON-NLS-1$
    public static final String NOW = "now"; //$NON-NLS-1$
    public static final String PARSETIMESTAMP = "parsetimestamp"; //$NON-NLS-1$
    public static final String QUARTER = "quarter"; //$NON-NLS-1$
    public static final String EPOCH = "epoch"; //$NON-NLS-1$
    public static final String SECOND = "second"; //$NON-NLS-1$
    public static final String TIMESTAMPADD = "timestampadd"; //$NON-NLS-1$
    public static final String TIMESTAMPCREATE = "timestampcreate"; //$NON-NLS-1$
    public static final String TIMESTAMPDIFF = "timestampdiff"; //$NON-NLS-1$
    public static final String WEEK = "week"; //$NON-NLS-1$
    public static final String YEAR = "year"; //$NON-NLS-1$
    public static final String FROM_UNIXTIME = "from_unixtime"; //$NON-NLS-1$
    public static final String UNIX_TIMESTAMP = "unix_timestamp"; //$NON-NLS-1$
    public static final String TO_MILLIS = "to_millis"; //$NON-NLS-1$

    //system functions
    public static final String IFNULL = "ifnull"; //$NON-NLS-1$
    public static final String COALESCE = "coalesce"; //$NON-NLS-1$
    public static final String NULLIF = "nullif"; //$NON-NLS-1$
    public static final String ARRAY_GET = "array_get"; //$NON-NLS-1$
    public static final String ARRAY_LENGTH = "array_length"; //$NON-NLS-1$

    //conversion functions
    public static final String CONVERT = "convert"; //$NON-NLS-1$

    //xml
    public static final String XPATHVALUE = "xpathvalue"; //$NON-NLS-1$
    public static final String XSLTRANSFORM = "xsltransform"; //$NON-NLS-1$
    public static final String XMLCONCAT = "xmlconcat"; //$NON-NLS-1$
    public static final String XMLCOMMENT = "xmlcomment"; //$NON-NLS-1$
    public static final String XMLPI = "xmlpi"; //$NON-NLS-1$

    public static final String JSONTOXML = "jsontoxml"; //$NON-NLS-1$

    //json
    public static final String JSONPATHVALUE = "jsonpathvalue"; //$NON-NLS-1$
    public static final String JSONQUERY = "jsonquery"; //$NON-NLS-1$
    public static final String JSONTOARRAY = "jsontoarray"; //$NON-NLS-1$

    public static final String UUID = "uuid"; //$NON-NLS-1$

    public static final String MD5 = "md5"; //$NON-NLS-1$
    public static final String SHA1 = "sha1"; //$NON-NLS-1$
    public static final String SHA2_256 = "sha2_256"; //$NON-NLS-1$
    public static final String SHA2_512 = "sha2_512"; //$NON-NLS-1$

    public static final String AES_ENCRYPT = "aes_encrypt"; //$NON-NLS-1$
    public static final String AES_DECRYPT = "aes_decrypt"; //$NON-NLS-1$

    public static final String ST_ASTEXT = "st_astext"; //$NON-NLS-1$
    public static final String ST_ASEWKT = "st_asewkt"; //$NON-NLS-1$
    public static final String ST_ASBINARY = "st_asbinary"; //$NON-NLS-1$
    public static final String ST_ASGEOJSON = "st_asgeojson"; //$NON-NLS-1$
    public static final String ST_ASGML = "st_asgml"; //$NON-NLS-1$
    public static final String ST_ASKML = "st_askml"; //$NON-NLS-1$
    public static final String ST_GEOMFROMTEXT = "st_geomfromtext"; //$NON-NLS-1$
    public static final String ST_GEOMFROMWKB = "st_geomfromwkb"; //$NON-NLS-1$
    public static final String ST_GEOMFROMGEOJSON = "st_geomfromgeojson"; //$NON-NLS-1$
    public static final String ST_GEOMFROMGML = "st_geomfromgml"; //$NON-NLS-1$
    public static final String ST_INTERSECTS = "st_intersects"; //$NON-NLS-1$
    public static final String ST_INTERSECTION = "st_intersection"; //$NON-NLS-1$
    public static final String ST_CONTAINS = "st_contains"; //$NON-NLS-1$
    public static final String ST_CROSSES = "st_crosses"; //$NON-NLS-1$
    public static final String ST_DISJOINT = "st_disjoint"; //$NON-NLS-1$
    public static final String ST_DISTANCE = "st_distance"; //$NON-NLS-1$
    public static final String ST_OVERLAPS = "st_overlaps"; //$NON-NLS-1$
    public static final String ST_TOUCHES = "st_touches"; //$NON-NLS-1$
    public static final String ST_SRID = "st_srid"; //$NON-NLS-1$
    public static final String ST_SETSRID = "st_setsrid"; //$NON-NLS-1$
    public static final String ST_EQUALS = "st_equals"; //$NON-NLS-1$
    public static final String ST_TRANSFORM = "st_transform"; //$NON-NLS-1$
    public static final String ST_SIMPLIFY = "st_simplify"; //$NON-NLS-1$
    public static final String ST_SIMPLIFYPRESERVETOPOLOGY = "st_simplifypreservetopology"; //$NON-NLS-1$
    public static final String ST_FORCE_2D = "st_force_2d"; //$NON-NLS-1$
    public static final String ST_ENVELOPE = "st_envelope"; //$NON-NLS-1$
    public static final String ST_WITHIN = "st_within"; //$NON-NLS-1$
    public static final String ST_DWITHIN = "st_dwithin"; //$NON-NLS-1$
    public static final String ST_EXTENT = "st_extent"; //$NON-NLS-1$
    public static final String ST_HASARC = "st_hasarc"; //$NON-NLS-1$
    public static final String DOUBLE_AMP_OP = SQLConstants.Tokens.DOUBLE_AMP;
    public static final String ST_GEOMFROMEWKT = "st_geomfromewkt"; //$NON-NLS-1$
    public static final String ST_ASEWKB = "st_asewkb"; //$NON-NLS-1$
    public static final String ST_GEOMFROMEWKB = "st_geomfromewkb"; //$NON-NLS-1$
    public static final String ST_AREA = "st_area"; //$NON-NLS-1$
    public static final String ST_BOUNDARY = "st_boundary"; //$NON-NLS-1$
    public static final String ST_BUFFER = "st_buffer"; //$NON-NLS-1$
    public static final String ST_CENTROID = "st_centroid"; //$NON-NLS-1$
    public static final String ST_CONVEXHULL = "st_convexhull"; //$NON-NLS-1$
    public static final String ST_COORDDIM = "st_coorddim"; //$NON-NLS-1$
    public static final String ST_CURVETOLINE = "st_curvetoline"; //$NON-NLS-1$
    public static final String ST_DIFFERENCE = "st_difference"; //$NON-NLS-1$
    public static final String ST_DIMENSION = "st_dimension"; //$NON-NLS-1$
    public static final String ST_ENDPOINT = "st_endpoint"; //$NON-NLS-1$
    public static final String ST_EXTERIORRING = "st_exteriorring"; //$NON-NLS-1$
    public static final String ST_GEOMETRYN = "st_geometryn"; //$NON-NLS-1$
    public static final String ST_GEOMETRYTYPE = "st_geometrytype"; //$NON-NLS-1$
    public static final String ST_INTERIORRINGN = "st_interiorringn"; //$NON-NLS-1$
    public static final String ST_ISCLOSED = "st_isclosed"; //$NON-NLS-1$
    public static final String ST_ISEMPTY = "st_isempty"; //$NON-NLS-1$
    public static final String ST_ISRING = "st_isring"; //$NON-NLS-1$
    public static final String ST_ISSIMPLE = "st_issimple"; //$NON-NLS-1$
    public static final String ST_ISVALID = "st_isvalid"; //$NON-NLS-1$
    public static final String ST_LENGTH = "st_length"; //$NON-NLS-1$
    public static final String ST_NUMGEOMETRIES = "st_numgeometries"; //$NON-NLS-1$
    public static final String ST_NUMINTERIORRINGS = "st_numinteriorrings"; //$NON-NLS-1$
    public static final String ST_NUMPOINTS = "st_numpoints"; //$NON-NLS-1$
    public static final String ST_ORDERINGEQUALS = "st_orderingequals"; //$NON-NLS-1$
    public static final String ST_PERIMETER = "st_perimeter"; //$NON-NLS-1$
    public static final String ST_POINT = "st_point"; //$NON-NLS-1$
    public static final String ST_POINTN = "st_pointn"; //$NON-NLS-1$
    public static final String ST_POINTONSURFACE = "st_pointonsurface"; //$NON-NLS-1$
    public static final String ST_POLYGON = "st_polygon"; //$NON-NLS-1$
    public static final String ST_RELATE = "st_relate"; //$NON-NLS-1$
    public static final String ST_STARTPOINT = "st_startpoint"; //$NON-NLS-1$
    public static final String ST_SYMDIFFERENCE = "st_symdifference"; //$NON-NLS-1$
    public static final String ST_UNION = "st_union"; //$NON-NLS-1$
    public static final String ST_X = "st_x"; //$NON-NLS-1$
    public static final String ST_Y = "st_y"; //$NON-NLS-1$
    public static final String ST_Z = "st_z"; //$NON-NLS-1$
    public static final String ST_MAKEENVELOPE = "st_makeenvelope"; //$NON-NLS-1$
    public static final String ST_SNAPTOGRID = "st_snaptogrid"; //$NON-NLS-1$

    //geography
    public static final String ST_GEOGFROMWKB = "st_geogfromwkb"; //$NON-NLS-1$
    public static final String ST_GEOGFROMTEXT = "st_geogfromtext"; //$NON-NLS-1$
}
