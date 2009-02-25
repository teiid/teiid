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

package org.teiid.connector.api;

/**
 * Constants for all the possible standard system push down functions.
 * The names and function forms follow the Open Group CLI functions, with a few exceptions
 * (such as lpad, rpad, bitand, bitor, etc. which are most notably supported by Oracle).
 * 
 */
public class SourceSystemFunctions {
	
	//arithmetic
	public static final String MULTIPLY_OP = "*";
	public static final String ADD_OP = "+";
	public static final String SUBTRACT_OP = "-";
	public static final String DIVIDE_OP = "/";
	
	//String
	public static final String ASCII = "ascii";
	public static final String CHAR = "char";
	public static final String CONCAT = "concat";
	//public static final String DIFFERENCE = "difference";
	public static final String INITCAP = "initcap";
	public static final String INSERT = "insert";
	public static final String LCASE = "lcase";
	public static final String LPAD = "lpad";
	public static final String LEFT = "left";
	public static final String LENGTH = "length";
	public static final String LOCATE = "locate";
	public static final String LTRIM = "ltrim";
	public static final String REPEAT = "repeat";
	public static final String REPLACE = "replace";
	public static final String RIGHT = "right";
	public static final String RPAD = "rpad";
	public static final String RTRIM = "rtrim";
	//public static final String SOUNDEX = "soundex";
	public static final String SUBSTRING = "substring";
	public static final String UCASE = "ucase";
	
	//numeric
	public static final String ABS = "abs";
	public static final String ACOS = "acos";
	public static final String ASIN = "asin";
	public static final String ATAN = "atan";
	public static final String ATAN2 = "atan2";
	public static final String CEILING = "ceiling";
	public static final String COS = "cos";
	public static final String COT = "cot";
	public static final String DEGREES = "degrees";
	public static final String EXP = "exp";
	public static final String FLOOR = "floor";
	public static final String FORMATINTEGER = "formatinteger";
	public static final String FORMATLONG = "formatlong";
	public static final String FORMATDOUBLE = "formatdouble";
	public static final String FORMATFLOAT = "formatfloat";
	public static final String FORMATBIGINTEGER = "formatbiginteger";
	public static final String FORMATBIGDECIMAL = "formatbigdecimal";
	public static final String LOG = "log";
	public static final String LOG10 = "log10";
	public static final String MOD = "mod";
	public static final String PARSEINTEGER = "parseinteger";
	public static final String PARSELONG = "parselong";
	public static final String PARSEDOUBLE = "parsedouble";
	public static final String PARSEFLOAT = "parsefloat";
	public static final String PARSEBIGINTEGER = "parsebiginteger";
	public static final String PARSEBIGDECIMAL = "parsebigdecimal";
	public static final String PI = "pi";
	public static final String POWER = "power";
	public static final String RADIANS = "radians";
	public static final String RAND = "rand";
	public static final String ROUND = "round";
	public static final String SIGN = "sign";
	public static final String SIN = "sin";
	public static final String SQRT = "sqrt";
	public static final String TAN = "tan";
	public static final String TRANSLATE = "translate";
	public static final String TRUNCATE = "truncate";
	
	//bit
	public static final String BITAND = "bitand";
	public static final String BITOR = "bitor";
	public static final String BITNOT = "bitnot";
	public static final String BITXOR = "bitxor";
	
	//date functions
	public static final String CURDATE = "curdate";
	public static final String CURTIME = "curtime";
	public static final String DAYNAME = "dayname";
	public static final String DAYOFMONTH = "dayofmonth";
	public static final String DAYOFWEEK = "dayofweek";
	public static final String DAYOFYEAR = "dayofyear";
	public static final String FORMATDATE = "formatdate";
	public static final String FORMATTIME = "formattime";
	public static final String FORMATTIMESTAMP = "formattimestamp";
	public static final String HOUR = "hour";
	public static final String MINUTE = "minute";
	public static final String MODIFYTIMEZONE = "modifytimezone";
	public static final String MONTH = "month";
	public static final String MONTHNAME = "monthname";
	public static final String NOW = "now";
	public static final String PARSEDATE = "parsedate";
	public static final String PARSETIME = "parsetime";
	public static final String PARSETIMESTAMP = "parsetimestamp";
	public static final String QUARTER = "quarter";
	public static final String SECOND = "second";
	public static final String TIMESTAMPADD = "timestampadd";
	public static final String TIMESTAMPCREATE = "timestampcreate";
	public static final String TIMESTAMPDIFF = "timestampdiff";
	public static final String WEEK = "week";
	public static final String YEAR = "year";
	
	//system functions
	public static final String IFNULL = "ifnull";
	public static final String COALESCE = "coalesce";
	public static final String NULLIF = "nullif";
	
	//conversion functions
	public static final String CONVERT = "convert";
	
	//xml
	public static final String XPATHVALUE = "xpathvalue";

}
