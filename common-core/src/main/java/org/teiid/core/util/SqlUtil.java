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
        StackTraceElement ste = new Exception().getStackTrace()[1];
        String methodName = ste.getMethodName();
        return new SQLFeatureNotSupportedException(methodName + " is not supported"); //$NON-NLS-1$
    }
}
