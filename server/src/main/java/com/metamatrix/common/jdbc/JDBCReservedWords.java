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

package com.metamatrix.common.jdbc;

public interface JDBCReservedWords {

    public static final String INSERT       = "INSERT"; //$NON-NLS-1$
    public static final String UPDATE       = "UPDATE"; //$NON-NLS-1$
    public static final String DELETE       = "DELETE"; //$NON-NLS-1$
    public static final String SELECT       = "SELECT"; //$NON-NLS-1$
	  public static final String DISTINCT     = "DISTINCT"; //$NON-NLS-1$
	  public static final String FROM         = "FROM"; //$NON-NLS-1$
	  public static final String WHERE        = "WHERE"; //$NON-NLS-1$
    public static final String ORDER_BY     = "ORDER BY"; //$NON-NLS-1$
    public static final String DESC         = "DESC"; //$NON-NLS-1$
    public static final String SET          = "SET"; //$NON-NLS-1$
    public static final String ON           = "ON"; //$NON-NLS-1$
    public static final String INTO         = "INTO"; //$NON-NLS-1$
    public static final String INNER_JOIN   = "INNER JOIN"; //$NON-NLS-1$
    public static final String VALUES       = "VALUES"; //$NON-NLS-1$
    public static final String ALL_COLS     = "*"; //$NON-NLS-1$

    // for embedded sql
    public static final String EMB_ENC_CHAR = "{"; //$NON-NLS-1$
    public static final String EMB_DEC_CHAR = "}"; //$NON-NLS-1$
    public static final String EMB_DATE_CHAR= "d "; //$NON-NLS-1$
    public static final String EMB_TIME_CHAR= "t "; //$NON-NLS-1$
    public static final String EMB_TS_CHAR  = "ts "; //$NON-NLS-1$
    public static final String EMB_TIC      = "'"; //$NON-NLS-1$

    // criteria reserved words
    public static final String AND          = "AND"; //$NON-NLS-1$
    public static final String OR           = "OR"; //$NON-NLS-1$
    public static final String IS           = "IS"; //$NON-NLS-1$
    public static final String NOT          = "NOT"; //$NON-NLS-1$
    public static final String NULL         = "NULL"; //$NON-NLS-1$
    public static final String TRUE         = "TRUE"; //$NON-NLS-1$
    public static final String FALSE        = "FALSE"; //$NON-NLS-1$
    public static final String UNKNOWN      = "UNKNOWN"; //$NON-NLS-1$
    public static final String LIKE         = "LIKE"; //$NON-NLS-1$
    public static final String ESCAPE       = "ESCAPE"; //$NON-NLS-1$
    public static final String IN           = "IN"; //$NON-NLS-1$
    
    // These literals are our representation of true and false as 
    // true and false are represented as char(1) fields in our databases.
    public static final String TRUE_CHAR = "1"; //$NON-NLS-1$
    public static final String FALSE_CHAR = "0"; //$NON-NLS-1$
    
    // LIKE characters
	public static final char LIKE_WILDCARD_CHARACTER = '%';
	public static final char LIKE_MATCH_CHARACTER = '_';
	public static final char DEFAULT_ESCAPE_CHARACTER = '\\';

    public static final char   LITERAL_ENCLOSING_CHARACTER = '\'';
    public static final String NULL_LITERAL = "NULL"; //$NON-NLS-1$


    // miscellaneous
    public static final String LEFT_PAREN = "("; //$NON-NLS-1$
    public static final String RIGHT_PAREN = ")"; //$NON-NLS-1$

}


