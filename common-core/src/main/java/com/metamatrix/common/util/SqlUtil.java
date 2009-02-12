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

package com.metamatrix.common.util;

import com.metamatrix.core.util.StringUtil;

/**
 * Utilities for dealing with SQL strings.
 */
public class SqlUtil {
    public static final char CR_CHAR = StringUtil.Constants.CARRIAGE_RETURN_CHAR;
    public static final char NL_CHAR = StringUtil.Constants.NEW_LINE_CHAR;
    public static final char SPACE_CHAR = StringUtil.Constants.SPACE_CHAR;
    public static final char TAB_CHAR = StringUtil.Constants.TAB_CHAR;
    
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
        if(sql != null && sql.length() > 0) {
            int length = sql.length();
            
            for(int i=0; i<length; i++) {
                char c = sql.charAt(i);
                if(Character.isWhitespace(c)) {
                    continue;
                } else if(c == 'i' || c =='I' || c == 'u' || c == 'U' || c == 'd' || c == 'D' || c == 'c' || c == 'C') {
                    if(i+6 > length) {
                        return false;
                    }
                    String word = sql.substring(i, i+6);
                    if(word.equalsIgnoreCase("insert") || word.equalsIgnoreCase("update") || word.equalsIgnoreCase("delete")) { //$NON-NLS-2$ //$NON-NLS-1$ //$NON-NLS-3$
                        return true;
                    }
                    return false;
                }else if(c == 's' || c == 'S'){
                    if(i+6 > length) {
                        return false;
                    }
                    String word = sql.substring(i, i+6);
                    if(word.equalsIgnoreCase("select")){ //$NON-NLS-1$
                    	String upperCaseSql = sql.toUpperCase();
                    	int intoIndex = upperCaseSql.indexOf(" INTO ", i); //$NON-NLS-1$
                    	if(intoIndex == -1){
                    		return false;
                    	}

                        int fromIndex = upperCaseSql.indexOf(" FROM ", intoIndex); //$NON-NLS-1$
                        if(fromIndex == -1 || fromIndex > intoIndex){
                        	return true;
                        }                       
                    }
                    return false;
                }else {
                    return false;
                }
            }
        }

        // Bad sql string
        throw new IllegalArgumentException();
    }
    
    /**
     * Simple method which normalizes a SQL string by replacing CR characters, new line characters and Tab characters with spaces,
     * then trimming the string to remove unneeded spaces.
     * 
     * @param inputSqlString
     * @return
     * @since 5.0
     */
    public static String normalize(String inputSqlString) {
        String normalizedString = inputSqlString;
        if (inputSqlString != null && inputSqlString.length() > 0) {
            if (  inputSqlString.indexOf(NL_CHAR) > -1 || 
                  inputSqlString.indexOf(CR_CHAR) > -1 || 
                  inputSqlString.indexOf(TAB_CHAR) > -1 ||
                  inputSqlString.indexOf(StringUtil.Constants.DBL_SPACE) > -1) {
                normalizedString = normalizedString.replace(NL_CHAR, SPACE_CHAR);
                normalizedString = normalizedString.replace(CR_CHAR, SPACE_CHAR);
                normalizedString = normalizedString.replace(TAB_CHAR, SPACE_CHAR);
                normalizedString = StringUtil.replaceAll(normalizedString,
                                                         StringUtil.Constants.DBL_SPACE,
                                                         StringUtil.Constants.SPACE);
                
            }
            normalizedString = StringUtil.collapseWhitespace(normalizedString);
        }
        return normalizedString;
    }
    
    /**
     * determine if the supplied sql Strings are different
     * @param newSql the new SQL String
     * @param oldSql the old SQL String
     * @return 'true' if strings differ, 'false' if same
     */
    public static boolean stringsAreDifferent(String newSql, String oldSql) {
        boolean isDifferent = true;
        if(newSql==null) {
            if(oldSql==null) {
                isDifferent = false;
            }
        } else if(oldSql!=null) {
            String normalizedNewSql = normalize(newSql);
            String normalizedOldSql = normalize(oldSql);
            if(normalizedNewSql.equals(normalizedOldSql)) {
                isDifferent=false;
            }
        }
        return isDifferent;
    }
}
