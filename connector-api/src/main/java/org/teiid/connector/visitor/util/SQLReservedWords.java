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

package org.teiid.connector.visitor.util;

import java.util.HashSet;
import java.util.Set;

public class SQLReservedWords {

    public static final String ANY = "ANY"; //$NON-NLS-1$
    public static final String ALL = "ALL"; //$NON-NLS-1$
    public static final String ALL_COLS = "*"; //$NON-NLS-1$
    public static final String AND = "AND"; //$NON-NLS-1$
    public static final String AS = "AS"; //$NON-NLS-1$
    public static final String ASC = "ASC"; //$NON-NLS-1$
	public static final String AVG = "AVG"; //$NON-NLS-1$
    public static final String BEGIN = "BEGIN"; //$NON-NLS-1$
    public static final String BETWEEN = "BETWEEN"; //$NON-NLS-1$
	public static final String BIGDECIMAL = "BIGDECIMAL"; //$NON-NLS-1$
	public static final String BIGINTEGER = "BIGINTEGER"; //$NON-NLS-1$
    public static final String BREAK = "BREAK"; //$NON-NLS-1$
	public static final String BY = "BY"; //$NON-NLS-1$
    public static final String BYTE = "BYTE"; //$NON-NLS-1$
    public static final String CASE = "CASE"; //$NON-NLS-1$
	public static final String CAST = "CAST"; //$NON-NLS-1$
	public static final String CHAR = "CHAR"; //$NON-NLS-1$
    public static final String CONVERT = "CONVERT"; //$NON-NLS-1$
    public static final String CONTINUE = "CONTINUE"; //$NON-NLS-1$
	public static final String COUNT = "COUNT"; //$NON-NLS-1$
    public static final String CRITERIA = "CRITERIA"; //$NON-NLS-1$
    public static final String CREATE = "CREATE"; //$NON-NLS-1$
    public static final String CROSS = "CROSS"; //$NON-NLS-1$
    public static final String DATE = "DATE"; //$NON-NLS-1$
    public static final String DEBUG = "DEBUG"; //$NON-NLS-1$
    public static final String DECLARE = "DECLARE";     //$NON-NLS-1$
	public static final String DELETE = "DELETE"; //$NON-NLS-1$
    public static final String DESC = "DESC"; //$NON-NLS-1$
	public static final String DISTINCT = "DISTINCT"; //$NON-NLS-1$
	public static final String DOUBLE = "DOUBLE"; //$NON-NLS-1$
	public static final String ELSE = "ELSE";	 //$NON-NLS-1$
	public static final String END = "END"; //$NON-NLS-1$
	public static final String ERROR = "ERROR";	 //$NON-NLS-1$
    public static final String ESCAPE = "ESCAPE"; //$NON-NLS-1$
    public static final String EXCEPT = "EXCEPT"; //$NON-NLS-1$
    public static final String EXEC = "EXEC"; //$NON-NLS-1$
    public static final String EXECUTE = "EXECUTE"; //$NON-NLS-1$
    public static final String EXISTS = "EXISTS"; //$NON-NLS-1$
    public static final String FALSE = "FALSE"; //$NON-NLS-1$
    public static final String FLOAT = "FLOAT"; //$NON-NLS-1$
    public static final String FOR = "FOR";     //$NON-NLS-1$
	public static final String FROM = "FROM"; //$NON-NLS-1$
    public static final String FULL = "FULL"; //$NON-NLS-1$
	public static final String GROUP = "GROUP"; //$NON-NLS-1$
	public static final String HAS = "HAS";	 //$NON-NLS-1$
    public static final String HAVING = "HAVING"; //$NON-NLS-1$
    public static final String IF = "IF";     //$NON-NLS-1$
    public static final String IN = "IN"; //$NON-NLS-1$
    public static final String INNER = "INNER"; //$NON-NLS-1$
    public static final String INSERT = "INSERT"; //$NON-NLS-1$
    public static final String INTEGER = "INTEGER"; //$NON-NLS-1$
    public static final String INTERSECT = "INTERSECT"; //$NON-NLS-1$
    public static final String INTO = "INTO"; //$NON-NLS-1$
    public static final String IS = "IS";     //$NON-NLS-1$
    public static final String JOIN = "JOIN"; //$NON-NLS-1$
    public static final String LEFT = "LEFT"; //$NON-NLS-1$
    public static final String LIKE = "LIKE"; //$NON-NLS-1$
    public static final String LIMIT = "LIMIT"; //$NON-NLS-1$
    public static final String LONG = "LONG"; //$NON-NLS-1$
    public static final String LOOP = "LOOP"; //$NON-NLS-1$
    public static final String MAKEDEP = "MAKEDEP"; //$NON-NLS-1$
	public static final String MIN = "MIN"; //$NON-NLS-1$
	public static final String MAX = "MAX"; //$NON-NLS-1$
    public static final String NOT = "NOT"; //$NON-NLS-1$
    public static final String NULL = "NULL"; //$NON-NLS-1$
    public static final String OBJECT = "OBJECT"; //$NON-NLS-1$
	public static final String ON = "ON"; //$NON-NLS-1$
    public static final String OR = "OR"; //$NON-NLS-1$
	public static final String ORDER = "ORDER"; //$NON-NLS-1$
    public static final String OPTION = "OPTION"; //$NON-NLS-1$
    public static final String OUTER = "OUTER"; //$NON-NLS-1$
    public static final String PROCEDURE = "PROCEDURE"; //$NON-NLS-1$
    public static final String RIGHT = "RIGHT"; //$NON-NLS-1$
	public static final String SELECT = "SELECT"; //$NON-NLS-1$
    public static final String SET = "SET"; //$NON-NLS-1$
    public static final String SHORT = "SHORT"; //$NON-NLS-1$
    public static final String SHOWPLAN = "SHOWPLAN"; //$NON-NLS-1$
    public static final String SOME = "SOME"; //$NON-NLS-1$
    public static final String SQL_TSI_FRAC_SECOND = "SQL_TSI_FRAC_SECOND"; //$NON-NLS-1$
    public static final String SQL_TSI_SECOND = "SQL_TSI_SECOND"; //$NON-NLS-1$
    public static final String SQL_TSI_MINUTE = "SQL_TSI_MINUTE"; //$NON-NLS-1$
    public static final String SQL_TSI_HOUR = "SQL_TSI_HOUR"; //$NON-NLS-1$
    public static final String SQL_TSI_DAY = "SQL_TSI_DAY"; //$NON-NLS-1$
    public static final String SQL_TSI_WEEK = "SQL_TSI_WEEK"; //$NON-NLS-1$
    public static final String SQL_TSI_MONTH = "SQL_TSI_MONTH"; //$NON-NLS-1$
    public static final String SQL_TSI_QUARTER = "SQL_TSI_QUARTER"; //$NON-NLS-1$
    public static final String SQL_TSI_YEAR = "SQL_TSI_YEAR"; //$NON-NLS-1$
    public static final String STRING = "STRING"; //$NON-NLS-1$
	public static final String SUM = "SUM"; //$NON-NLS-1$
    public static final String THEN = "THEN"; //$NON-NLS-1$
    public static final String TIME = "TIME"; //$NON-NLS-1$
	public static final String TIMESTAMP = "TIMESTAMP"; //$NON-NLS-1$
    public static final String TIMESTAMPADD = "TIMESTAMPADD"; //$NON-NLS-1$
    public static final String TIMESTAMPDIFF = "TIMESTAMPDIFF"; //$NON-NLS-1$
	public static final String TRANSLATE = "TRANSLATE";	 //$NON-NLS-1$
    public static final String TRUE = "TRUE"; //$NON-NLS-1$
    public static final String UNION = "UNION"; //$NON-NLS-1$
    public static final String UNKNOWN = "UNKNOWN"; //$NON-NLS-1$
	public static final String UPDATE = "UPDATE"; //$NON-NLS-1$
	public static final String USING = "USING";	 //$NON-NLS-1$
    public static final String VALUES = "VALUES"; //$NON-NLS-1$
    public static final String VIRTUAL = "VIRTUAL"; //$NON-NLS-1$
    public static final String WHEN = "WHEN";     //$NON-NLS-1$
    public static final String WITH = "WITH";     //$NON-NLS-1$
	public static final String WHERE = "WHERE"; //$NON-NLS-1$
    public static final String WHILE = "WHILE"; //$NON-NLS-1$

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
    
    public static final String[] ALL_WORDS = new String[] {ALL, ALL_COLS, AND, ANY, AS, ASC, AVG, BEGIN, BETWEEN, BIGINTEGER,
        BIGDECIMAL, BREAK, BY, BYTE, CASE, CAST, CHAR, CONVERT, CONTINUE, COUNT, CREATE, CRITERIA, CROSS, DATE, DEBUG, DECLARE,
        DELETE, DESC, DISTINCT, DOUBLE, ELSE, END, ERROR, ESCAPE, EXCEPT, EXEC, EXECUTE, EXISTS, FALSE, FLOAT, FOR, FROM, FULL,
        GROUP, HAS, HAVING, IF, IN, INNER, INSERT, INTEGER, INTERSECT, INTO, IS, JOIN, LEFT, LIKE, LONG, LOOP, MAKEDEP, MIN, MAX,
        NOT, NULL, OBJECT, ON, OR, ORDER, OPTION, OUTER, PROCEDURE, RIGHT, SELECT, SET, SHORT, SHOWPLAN, SOME,
        SQL_TSI_FRAC_SECOND, SQL_TSI_SECOND, SQL_TSI_MINUTE, SQL_TSI_HOUR, SQL_TSI_DAY, SQL_TSI_WEEK, SQL_TSI_MONTH,
        SQL_TSI_QUARTER, SQL_TSI_YEAR, STRING, SUM, THEN, TIME, TIMESTAMP, TIMESTAMPADD, TIMESTAMPDIFF, TRANSLATE, TRUE, UNION,
        UNKNOWN, UPDATE, USING, VALUES, VIRTUAL, WHEN, WITH, WHERE, WHILE,};

    /**
     * Set of CAPITALIZED reserved words for checking whether a string is a reserved word.
     */
    private static final Set RESERVED_WORDS = new HashSet();

    // Initialize RESERVED_WORDS set
    static {
        // Iterate through the reserved words and capitalize all of them
        for (int i = 0; i != SQLReservedWords.ALL_WORDS.length; ++i) {
            String reservedWord = SQLReservedWords.ALL_WORDS[i];
            SQLReservedWords.RESERVED_WORDS.add(reservedWord.toUpperCase());
        }
    }

    /** Can't construct */
    private SQLReservedWords() {
    }

    /**
     * Check whether a string is a reserved word.
     * 
     * @param str String to check
     * @return True if reserved word, false if not or null
     */
    public static final boolean isReservedWord( String str ) {
        if (str == null) {
            return false;
        }
        return RESERVED_WORDS.contains(str.toUpperCase());
    }
}
