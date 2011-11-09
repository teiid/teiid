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
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Utilities for dealing with SQL strings.
 */
public class SqlUtil {
    public static final char CR_CHAR = StringUtil.Constants.CARRIAGE_RETURN_CHAR;
    public static final char NL_CHAR = StringUtil.Constants.NEW_LINE_CHAR;
    public static final char SPACE_CHAR = StringUtil.Constants.SPACE_CHAR;
    public static final char TAB_CHAR = StringUtil.Constants.TAB_CHAR;
    private static TreeSet<String> updateKeywords = new TreeSet<String>(String.CASE_INSENSITIVE_ORDER);
    static {
    	updateKeywords.add("insert"); //$NON-NLS-1$
    	updateKeywords.add("update"); //$NON-NLS-1$
    	updateKeywords.add("delete"); //$NON-NLS-1$
    	updateKeywords.add("drop"); //$NON-NLS-1$
    	updateKeywords.add("create"); //$NON-NLS-1$
    }
	private static Pattern PATTERN = Pattern.compile("^(?:\\s|(?:/\\*.*\\*/))*(\\w*)\\s", Pattern.CASE_INSENSITIVE|Pattern.DOTALL); //$NON-NLS-1$
	private static Pattern INTO_PATTERN = Pattern.compile("(?:'[^']*')|(\\sinto\\s)", Pattern.CASE_INSENSITIVE|Pattern.DOTALL); //$NON-NLS-1$

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
        String keyWord = getKeyword(sql);
        return updateKeywords.contains(keyWord);
    }
    
    public static String getKeyword(String sql) {
    	Matcher matcher = PATTERN.matcher(sql);
        if (!matcher.find()) {
        	return sql; //shouldn't happen
        }
        String keyword = matcher.group(1);
        if (keyword.equalsIgnoreCase("select")) { //$NON-NLS-1$
        	int end = matcher.end();
        	Matcher intoMatcher = INTO_PATTERN.matcher(sql);
        	while (intoMatcher.find(end)) {
        		if (intoMatcher.group(1) != null) {
        			return "insert"; //$NON-NLS-1$
        		}
        		end = intoMatcher.end();
        	}
        }
        return keyword;
    }
    
    public static SQLException createFeatureNotSupportedException() {
    	return new SQLFeatureNotSupportedException();
    }    
}
