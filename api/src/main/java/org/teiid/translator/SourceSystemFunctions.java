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

package org.teiid.translator;

/**
 * Constants for all the possible standard system push down functions.
 * The names and function forms follow the Open Group CLI functions, with a few exceptions
 * (such as lpad, rpad, bitand, bitor, etc. which are most notably supported by Oracle).
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
	public static final String REPLACE = "replace"; //$NON-NLS-1$
	public static final String RIGHT = "right"; //$NON-NLS-1$
	public static final String RPAD = "rpad"; //$NON-NLS-1$
	public static final String RTRIM = "rtrim"; //$NON-NLS-1$
	//public static final String SOUNDEX = "soundex";
	public static final String SUBSTRING = "substring"; //$NON-NLS-1$
	public static final String TO_BYTES = "to_bytes"; //$NON-NLS-1$
	public static final String TO_CHARS = "to_chars"; //$NON-NLS-1$
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
	public static final String SECOND = "second"; //$NON-NLS-1$
	public static final String TIMESTAMPADD = "timestampadd"; //$NON-NLS-1$
	public static final String TIMESTAMPCREATE = "timestampcreate"; //$NON-NLS-1$
	public static final String TIMESTAMPDIFF = "timestampdiff"; //$NON-NLS-1$
	public static final String WEEK = "week"; //$NON-NLS-1$
	public static final String YEAR = "year"; //$NON-NLS-1$
	
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
	
	public static final String UUID = "uuid"; //$NON-NLS-1$

}
