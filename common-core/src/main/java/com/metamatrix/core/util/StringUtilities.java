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

package com.metamatrix.core.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;

import com.metamatrix.core.MetaMatrixRuntimeException;

/**
 * This class contains static utilities that return strings that are the result of manipulating other strings or objects. 
 * @since 3.1
 * @version 3.1
 * @author <a href="mailto:jverhaeg@metamatrix.com">John P. A. Verhaeg</a>
 */
public class StringUtilities {
    /**
     * The String "\n"
     */
    public static final String NEW_LINE = "\n"; //$NON-NLS-1$

    /**
     * The name of the System property that specifies the string that should be used to separate
     * lines.  This property is a standard environment property that is usually set automatically.
     */
    public static final String LINE_SEPARATOR_PROPERTY_NAME = "line.separator"; //$NON-NLS-1$

    /**
     * The String that should be used to separate lines; defaults to
     * {@link #NEW_LINE}
     */
    public static final String LINE_SEPARATOR = System.getProperty(LINE_SEPARATOR_PROPERTY_NAME, NEW_LINE);

    //############################################################################################################################
	//# Static Methods                                                                                                           #
	//############################################################################################################################
    
    public static String getLineSeparator() {
        return LINE_SEPARATOR;
    }

    /**
     * Returns the path representing the concatenation of the specified path prefix and suffix.  The resulting path is guaranteed
     * to have exactly one file separator between the prefix and suffix.
     * @param prefix The path prefix
     * @param suffix The path suffix
     * @return The concatenated path prefix and suffix 
	 * @since 3.1
	 */
	public static String buildPath(final String prefix, final String suffix) {
        final StringBuffer path = new StringBuffer(prefix);
		if (!prefix.endsWith(File.separator)) {
            path.append(File.separator);
        }
        if (suffix.startsWith(File.separator)) {
            path.append(suffix.substring(File.separator.length()));
        } else {
            path.append(suffix);
        }
        return path.toString();
	}
    
    /**
     * Returns a new string that lowercases the first character in the passed in
     * value String
     * @param value
     * @return String
     */
    public static String lowerCaseFirstChar(final String value){
        if(value == null){
            return null;
        }
        
        //Lower case the first char and try to look-up the SF
        String firstChar = new Character(value.charAt(0) ).toString();
        firstChar = firstChar.toLowerCase();
        return (firstChar + value.substring(1) ); 
    }
    
    /**
     * Returns a new string that uppercases the first character in the passed in
     * value String
     * @param value
     * @return String
     */
    public static String upperCaseFirstChar(final String value){
        if(value == null){
            return null;
        }
        
        //Lower case the first char and try to look-up the SF
        String firstChar = new Character(value.charAt(0) ).toString();
        firstChar = firstChar.toUpperCase();
        return (firstChar + value.substring(1) ); 
    }
    
    /**
     * Returns a new string that represents the last fragment of the original
     * string that begins with an uppercase char.  Ex: "getSuperTypes" would
     * return "Types".
     * @param value
     * @param lastToken - the last token tried... if not null will look
     * backwards from the last token instead of the end of the value param
     * @return String
     */
    public static String getLastUpperCharToken(final String value, final String lastToken){
        if(value == null || lastToken == null){
            return value;
        }
        
        int index = value.lastIndexOf(lastToken);
        if(index == -1){
            return null;
        }
        
        StringBuffer result = new StringBuffer();
        for(int i = index - 1; i >= 0 ; i--){
            result.insert(0,value.charAt(i) );
            if(Character.isUpperCase(value.charAt(i) ) ){
                return result.toString() + lastToken;
            }
        }
        
        return result.toString() + lastToken;
    }
    
    /**
     * Returns a new string that represents the last fragment of the original
     * string that begins with an uppercase char.  Ex: "getSuperTypes" would
     * return "Types".
     * @param value
     * @return String
     */
    public static String getLastUpperCharToken(final String value){
        if(value == null){
            return null;
        }
        
        StringBuffer result = new StringBuffer();
        for(int i = value.length() - 1; i >= 0; i--){
            result.insert(0, value.charAt(i) );
            if(Character.isUpperCase(value.charAt(i) ) ){
                return result.toString();
            }
        }
        
        return result.toString();
    }
    
    public static String[] getLines(final String value) {
        StringReader stringReader = new StringReader(value);
        BufferedReader reader = new BufferedReader(stringReader);
        ArrayList result = new ArrayList();
        try {
            String line = reader.readLine();
            while (line != null) {
                result.add(line);
                line = reader.readLine();
            }
        } catch (IOException e) {
            throw new MetaMatrixRuntimeException(e);
        }
        return (String[]) result.toArray(new String[result.size()]);
    }
    
    public static String removeChars(final String value, final char[] chars) {
        final StringBuffer result = new StringBuffer();
        if (value != null && chars != null && chars.length > 0) {
            final String removeChars = String.valueOf(chars);
            for (int i = 0; i < value.length(); i++) {
                final String character = value.substring(i, i + 1);
                if (removeChars.indexOf(character) == -1) {
                    result.append(character);
                }
            }
        } else {
            result.append(value);
        }
        return result.toString();
    }

    /** Replaces all "whitespace" characters from the specified string with space
      * characters, where whitespace includes \r\t\n and other characters
      * @param value the string to work with
      * @param stripExtras if true, replace multiple whitespace characters with a single character.
      * @see java.util.regex.Pattern
      */
    public static String replaceWhitespace(String value, boolean stripExtras) {
        return replaceWhitespace(value, " ", stripExtras); //$NON-NLS-1$
    }

    /** Replaces all "whitespace" characters from the specified string with space
      * characters, where whitespace includes \r\t\n and other characters
      * @param value the string to work with
      * @param replaceWith the character to replace with
      * @param stripExtras if true, replace multiple whitespace characters with a single character.
      * @see java.util.regex.Pattern
      */
    public static String replaceWhitespace(String value, String replaceWith, boolean stripExtras) {
        String rv = value.replaceAll("\\s+", replaceWith);  //$NON-NLS-1$
        
        if (stripExtras) {
            rv = removeExtraWhitespace(rv);
        } // endif
        
        return rv;
    }

    /** Replaces multiple sequential "whitespace" characters from the specified string with
      *  a single space character, where whitespace includes \r\t\n and other characters
      * @param value the string to work with
      * @see java.util.regex.Pattern
      */
   public static String removeExtraWhitespace(String value) {
       return value.replaceAll("\\s\\s+", " ");  //$NON-NLS-1$//$NON-NLS-2$
   }
   
   /**
    *  
    * @param originalString
    * @param maxLength
    * @param endLength
    * @param middleString
    * @return
    * @since 5.0
    */
   public static String condenseToLength(String originalString, int maxLength, int endLength, String middleString) {
       if( originalString.length() <= maxLength) {
           return originalString;
       }
       int originalLength = originalString.length();
       StringBuffer sb = new StringBuffer(maxLength);
       sb.append(originalString.substring(0, maxLength - endLength - middleString.length()));
       sb.append(middleString);
       sb.append(originalString.substring(originalLength - endLength, originalLength));
       
       return sb.toString();
   }
}
