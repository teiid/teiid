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

package org.teiid.core.util;

import java.sql.SQLException;

import java.sql.SQLFeatureNotSupportedException;

import java.util.regex.Pattern;

/**
 * Utilities for dealing with SQL strings.
 */
public class SqlUtil {
    public static final char CR_CHAR = StringUtil.Constants.CARRIAGE_RETURN_CHAR;
    public static final char NL_CHAR = StringUtil.Constants.NEW_LINE_CHAR;
    public static final char SPACE_CHAR = StringUtil.Constants.SPACE_CHAR;
    public static final char TAB_CHAR = StringUtil.Constants.TAB_CHAR;
	private static Pattern PATTERN = Pattern.compile("^([\\s]|(/\\*.*\\*/))*(insert|update|delete|create|drop|(select([\\s]|(/\\*.*\\*/))+.*into([\\s]|(/\\*.*\\*/))+)).*", Pattern.CASE_INSENSITIVE|Pattern.MULTILINE); //$NON-NLS-1$
    
    private SqlUtil() {
        super();
    }

    /**
     * Determines whether a sql statement is an update (INSERT, UPDATE, or DELETE).
     * Throws exception if SQL statement appears to be invalid (because it's null, has
     * 0 length, etc.
     * @param sql Sql string
     * @return True if INSERT, UPDATE, or DELETE, and false otherwise
     * @throws IllegalArgumentException If sql string is invalid and neither a 
     * query or an update
     */
    public static boolean isUpdateSql(String sql) throws IllegalArgumentException {
        ArgCheck.isNotNull(sql);
        return PATTERN.matcher(sql).matches();
    }
    
    public static SQLException createFeatureNotSupportedException() {
    	return new SQLFeatureNotSupportedException();
    }    
}
