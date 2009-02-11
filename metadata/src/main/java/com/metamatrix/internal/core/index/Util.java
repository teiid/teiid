/*******************************************************************************
 * Copyright (c) 2000, 2003 IBM Corporation and others.
 * All rights reserved. This program and the accompanying materials 
 * are made available under the terms of the Common Public License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/cpl-v10.html
 * 
 * Contributors:
 *     IBM Corporation - initial API and implementation
 *     MetaMatrix, Inc - repackaging and updates for use as a metadata store
 *******************************************************************************/
package com.metamatrix.internal.core.index;

import java.text.MessageFormat;
import java.util.MissingResourceException;
import java.util.ResourceBundle;

public class Util {

	private Util() {
	}
    
    /* Bundle containing messages */
    protected static ResourceBundle bundle;
    private final static String bundleName = "com.metamatrix.core.index.i18n"; //$NON-NLS-1$    
    
    /**
     * Lookup the message with the given ID in this catalog 
     */
    public static String bind(String id) {
        return bind(id, (String[]) null);
    }
    /**
     * Lookup the message with the given ID in this catalog and bind its
     * substitution locations with the given string values.
     */
    public static String bind(String id, String[] arguments) {
        if (id == null)
            return "No message available"; //$NON-NLS-1$
        String message = null;
        try {
            message = bundle.getString(id);
        } catch (MissingResourceException e) {
            // If we got an exception looking for the message, fail gracefully by just returning
            // the id we were looking for.  In most cases this is semi-informative so is not too bad.
            return "Missing message: " + id + " in: " + bundleName; //$NON-NLS-2$ //$NON-NLS-1$
        }
        return MessageFormat.format(message, (Object[])arguments);
    }

	/**
	 * Compares two strings lexicographically. 
	 * The comparison is based on the Unicode value of each character in
	 * the strings. 
	 *
	 * @return  the value <code>0</code> if the str1 is equal to str2;
	 *          a value less than <code>0</code> if str1
	 *          is lexicographically less than str2; 
	 *          and a value greater than <code>0</code> if str1 is
	 *          lexicographically greater than str2.
	 */
	public static int compare(char[] str1, char[] str2) {
		int len1= str1.length;
		int len2= str2.length;
		int n= Math.min(len1, len2);
		int i= 0;
		while (n-- != 0) {
			char c1= str1[i];
			char c2= str2[i++];
			if (c1 != c2) {
				return c1 - c2;
			}
		}
		return len1 - len2;
	}

	/**
	 * Returns the length of the common prefix between s1 and s2.
	 */
	public static int prefixLength(char[] s1, char[] s2) {
		int len= 0;
		int max= Math.min(s1.length, s2.length);
		for (int i= 0; i < max && s1[i] == s2[i]; ++i)
			++len;
		return len;
	}
	/**
	 * Returns the length of the common prefix between s1 and s2.
	 */
	public static int prefixLength(String s1, String s2) {
		int len= 0;
		int max= Math.min(s1.length(), s2.length());
		for (int i= 0; i < max && s1.charAt(i) == s2.charAt(i); ++i)
			++len;
		return len;
	} 
}
