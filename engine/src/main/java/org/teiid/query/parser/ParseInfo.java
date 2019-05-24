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

package org.teiid.query.parser;

import java.io.Serializable;

import org.teiid.core.util.PropertiesUtils;


public class ParseInfo implements Serializable{

    private static final long serialVersionUID = -7323683731955992888L;
    private static final boolean ANSI_QUOTED_DEFAULT = PropertiesUtils.getHierarchicalProperty("org.teiid.ansiQuotedIdentifiers", true, Boolean.class); //$NON-NLS-1$
    public static boolean REQUIRE_UNQUALIFIED_NAMES = PropertiesUtils.getHierarchicalProperty("org.teiid.requireUnqualifiedNames", true, Boolean.class); //$NON-NLS-1$

    public int referenceCount = 0;

    public static final ParseInfo DEFAULT_INSTANCE = new ParseInfo();
    static {
        DEFAULT_INSTANCE.ansiQuotedIdentifiers = true;
    }

    // treat a double quoted variable as variable instead of string
    public boolean ansiQuotedIdentifiers=ANSI_QUOTED_DEFAULT;
    boolean backslashDefaultMatchEscape=false;

    public ParseInfo() { }

    public boolean useAnsiQuotedIdentifiers() {
        return ansiQuotedIdentifiers;
    }

    @Override
    public int hashCode() {
        return ansiQuotedIdentifiers?1:0;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        if (!(obj instanceof ParseInfo)) {
            return false;
        }
        ParseInfo other = (ParseInfo)obj;
        return this.ansiQuotedIdentifiers == other.ansiQuotedIdentifiers
                && this.backslashDefaultMatchEscape == other.backslashDefaultMatchEscape;
    }

    public boolean isBackslashDefaultMatchEscape() {
        return backslashDefaultMatchEscape;
    }

    public void setBackslashDefaultMatchEscape(
            boolean backslashDefaultMatchEscape) {
        this.backslashDefaultMatchEscape = backslashDefaultMatchEscape;
    }
}