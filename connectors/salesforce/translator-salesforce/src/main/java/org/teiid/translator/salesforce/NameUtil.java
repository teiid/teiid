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
package org.teiid.translator.salesforce;

import org.teiid.language.SQLConstants;

public class NameUtil {

    public static String normalizeName( String nameIn ) {
        String normal = nameIn.trim();
        normal = removeDuplicate(normal);
        normal = removeSpaces(normal);
        normal = removeIllegalChars(normal);
        normal = removeTrailingUnderscore(normal);
        normal = removeLeadingUnderscore(normal);
        normal = checkReservedWords(normal);
        return normal;

    }

    /**
     * @param normal
     * @return
     */
    private static String checkReservedWords( String normal ) {
        if (SQLConstants.isReservedWord(normal)) {
            normal = normal + "_"; //$NON-NLS-1$
        }
        return normal;
    }

    private static String removeTrailingUnderscore( String normal ) {
        if (normal.endsWith("_")) { //$NON-NLS-1$
            return normal.substring(0, normal.lastIndexOf('_'));
        }
        return normal;
    }

    private static String removeIllegalChars( String normal ) {
        String edit = normal;
        edit = edit.replace('.', '_');
        edit = edit.replace('(', '_');
        edit = edit.replace(')', '_');
        edit = edit.replace('/', '_');
        edit = edit.replace('\\', '_');
        edit = edit.replace(':', '_');
        edit = edit.replace('\'', '_');
        edit = edit.replace('-', '_');
        edit = edit.replace("%", "percentage");//$NON-NLS-1$ //$NON-NLS-2$
        edit = edit.replace("#", "number");//$NON-NLS-1$ //$NON-NLS-2$
        edit = edit.replace("$", "_");//$NON-NLS-1$ //$NON-NLS-2$
        edit = edit.replace("{", "_");//$NON-NLS-1$ //$NON-NLS-2$
        edit = edit.replace("}", "_");//$NON-NLS-1$ //$NON-NLS-2$
        return edit;
    }

    private static String removeSpaces( String normal ) {
        return normal.replace(' ', '_');
    }

    private static String removeDuplicate( String normal ) {
        if (normal.indexOf('(') < 0 || normal.indexOf(')') != normal.length() - 1) return normal;
        String firstPart = normal.substring(0, normal.indexOf('(')).trim();
        String secondPart = normal.substring(normal.indexOf('(') + 1, normal.length() - 1).trim();
        if (firstPart.equals(secondPart) || secondPart.equals("null")) return firstPart; //$NON-NLS-1$
        return normal;
    }

    /**
     * @param normal
     * @return
     */
    private static String removeLeadingUnderscore( String normal ) {
        while (normal.indexOf('_') == 0) {
            normal = normal.substring(1);
        }
        return normal;
    }

}