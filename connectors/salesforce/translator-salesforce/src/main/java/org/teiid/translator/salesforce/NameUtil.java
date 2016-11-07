/*
 * JBoss, Home of Professional Open Source.
 *
 * See the LEGAL.txt file distributed with this work for information regarding copyright ownership and licensing.
 *
 * See the AUTHORS.txt file distributed with this work for a full listing of individual contributors.
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