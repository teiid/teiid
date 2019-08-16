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

import java.io.ByteArrayOutputStream;
import java.io.PrintWriter;
import java.lang.reflect.Array;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.*;


/**
 * This is a common place to put String utility methods.
 */
public final class StringUtil {

    public interface Constants {
        String EMPTY_STRING    = ""; //$NON-NLS-1$
        String SPACE           = " "; //$NON-NLS-1$
    }

    /**
     * Join string pieces and separate with a delimiter.  Similar to the perl function of
     * the same name.  If strings or delimiter are null, null is returned.  Otherwise, at
     * least an empty string will be returned.
     * @see #split
     *
     * @param strings String pieces to join
     * @param delimiter Delimiter to put between string pieces
     * @return One merged string
     */
    public static String join(Collection<String> strings, String delimiter) {
        if(strings == null || delimiter == null) {
            return null;
        }

        StringBuffer str = new StringBuffer();

        Iterator<String> iter = strings.iterator();
        while (iter.hasNext()) {
            str.append(iter.next());
            if (iter.hasNext()) {
                str.append(delimiter);
            }
        }

        return str.toString();
    }

    /**
     * Return a stringified version of the array.
     * @param array the array
     * @param delim the delimiter to use between array components
     * @return the string form of the array
     */
    public static String toString( final Object[] array, final String delim ) {
        return toString(array, delim, true);
    }

    /**
     * Return a stringified version of the array.
     * @param array the array
     * @param delim the delimiter to use between array components
     * @return the string form of the array
     */
    public static String toString( final Object[] array, final String delim, boolean includeBrackets) {
        if ( array == null ) {
            return ""; //$NON-NLS-1$
        }
        final StringBuffer sb = new StringBuffer();
        if (includeBrackets) {
            sb.append('[');
        }
        for (int i = 0; i < array.length; ++i) {
            if ( i != 0 ) {
                sb.append(delim);
            }
            sb.append(array[i]);
        }
        if (includeBrackets) {
            sb.append(']');
        }
        return sb.toString();
    }


    /**
     * Return a stringified version of the array, using a ',' as a delimiter
     * @param array the array
     * @return the string form of the array
     * @see #toString(Object[], String)
     */
    public static String toString( final Object[] array ) {
        return toString(array, ",", true); //$NON-NLS-1$
    }

    /**
     * Split a string into pieces based on delimiters.  Similar to the perl function of
     * the same name.  The delimiters are not included in the returned strings.
     * @see #join
     *
     * @param str Full string
     * @param splitter Characters to split on
     * @return List of String pieces from full string
     */
    public static List<String> split(String str, String splitter) {
        StringTokenizer tokens = new StringTokenizer(str, splitter);
        ArrayList<String> l = new ArrayList<String>(tokens.countTokens());
        while(tokens.hasMoreTokens()) {
            l.add(tokens.nextToken());
        }
        return l;
    }

    /**
     * Replace a single occurrence of the search string with the replace string
     * in the source string. If any of the strings is null or the search string
     * is zero length, the source string is returned.
     * @param source the source string whose contents will be altered
     * @param search the string to search for in source
     * @param replace the string to substitute for search if present
     * @return source string with the *first* occurrence of the search string
     * replaced with the replace string
     */
    public static String replace(String source, String search, String replace) {
        if (source != null && search != null && search.length() > 0 && replace != null) {
            int start = source.indexOf(search);
            if (start > -1) {
                return new StringBuffer(source).replace(start, start + search.length(), replace).toString();
            }
        }
        return source;
    }

    /**
     * Replace all occurrences of the search string with the replace string
     * in the source string. If any of the strings is null or the search string
     * is zero length, the source string is returned.
     * @param source the source string whose contents will be altered
     * @param search the string to search for in source
     * @param replace the string to substitute for search if present
     * @return source string with *all* occurrences of the search string
     * replaced with the replace string
     */
    public static String replaceAll(String source, String search, String replace) {
        if (source == null || search == null || search.length() == 0 || replace == null) {
            return source;
        }
        int start = source.indexOf(search);
        if (start > -1) {
            StringBuffer newString = new StringBuffer(source);
            while (start > -1) {
                int end = start + search.length();
                newString.replace(start, end, replace);
                start = newString.indexOf(search, start + replace.length());
            }
            return newString.toString();
        }
        return source;
    }

    /**
     * Return the tokens in a string in a list. This is particularly
     * helpful if the tokens need to be processed in reverse order. In that case,
     * a list iterator can be acquired from the list for reverse order traversal.
     *
     * @param str String to be tokenized
     * @param delimiter Characters which are delimit tokens
     * @return List of string tokens contained in the tokenized string
     */
    public static List<String> getTokens(String str, String delimiter) {
        ArrayList<String> l = new ArrayList<String>();
        StringTokenizer tokens = new StringTokenizer(str, delimiter);
        while(tokens.hasMoreTokens()) {
            l.add(tokens.nextToken());
        }
        return l;
    }

    /**
     * Return the last token in the string.
     *
     * @param str String to be tokenized
     * @param delimiter Characters which are delimit tokens
     * @return the last token contained in the tokenized string
     */
    public static String getLastToken(String str, String delimiter) {
        if (str == null) {
            return Constants.EMPTY_STRING;
        }
        int beginIndex = 0;
        if (str.lastIndexOf(delimiter) > 0) {
            beginIndex = str.lastIndexOf(delimiter)+1;
        }
        return str.substring(beginIndex,str.length());
    }


    /**
     * Return the first token in the string.
     *
     * @param str String to be tokenized
     * @param delimiter Characters which are delimit tokens
     * @return the first token contained in the tokenized string
     */
    public static String getFirstToken(String str, String delimiter) {
        if (str == null) {
            return Constants.EMPTY_STRING;
        }
        int endIndex = str.indexOf(delimiter);
        if (endIndex < 0) {
            endIndex = str.length();
        }
        return str.substring(0,endIndex);
    }

    public static String getStackTrace( final Throwable t ) {
        final ByteArrayOutputStream bas = new ByteArrayOutputStream();
        final PrintWriter pw = new PrintWriter(bas);
        t.printStackTrace(pw);
        pw.close();
        return bas.toString();
    }

    /**<p>
     * Returns whether the specified text is either empty or null.
     *
     * @param text The text to check; may be null;
     * @return True if the specified text is either empty or null.
     * @since 4.0
     */
    public static boolean isEmpty(final String text) {
        return (text == null  ||  text.length() == 0);
    }

    /**
     * Returns the index within this string of the first occurrence of the
     * specified substring. The integer returned is the smallest value
     * <i>k</i> such that:
     * <blockquote><pre>
     * this.startsWith(str, <i>k</i>)
     * </pre></blockquote>
     * is <code>true</code>.
     *
     * @param   text  any string.
     * @param   str   any string.
     * @return  if the str argument occurs as a substring within text,
     *          then the index of the first character of the first
     *          such substring is returned; if it does not occur as a
     *          substring, <code>-1</code> is returned.  If the text or
     *          str argument is null or empty then <code>-1</code> is returned.
     */
    public static int indexOfIgnoreCase(final String text, final String str) {
        if (isEmpty(text)) {
            return -1;
        }
        if (isEmpty(str)) {
            return -1;
        }
        int len = text.length() - str.length();
        for (int i = 0; i <= len; i++) {
            if (text.regionMatches(true, i, str, 0, str.length())) {
                return i;
            }
        }
        return -1;
    }

    /**
     * Tests if the string starts with the specified prefix.
     *
     * @param   text     the string to test.
     * @param   prefix   the prefix.
     * @return  <code>true</code> if the character sequence represented by the
     *          argument is a prefix of the character sequence represented by
     *          this string; <code>false</code> otherwise.
     *          Note also that <code>true</code> will be returned if the
     *          prefix is an empty string or is equal to the text
     *          <code>String</code> object as determined by the
     *          {@link #equals(Object)} method. If the text or
     *          prefix argument is null <code>false</code> is returned.
     * @since   JDK1. 0
     */
    public static boolean startsWithIgnoreCase(final String text, final String prefix) {
        if (text == null || prefix == null) {
            return false;
        }
        return text.regionMatches(true, 0, prefix, 0, prefix.length());
    }

    /**
     * Tests if the string ends with the specified suffix.
     */
    public static boolean endsWithIgnoreCase(final String text, final String suffix) {
        if (text == null || suffix == null) {
            return false;
        }
        return text.regionMatches(true, text.length() - suffix.length(), suffix, 0, suffix.length());
    }

    /**<p>
     * Prevents instantiation.
     *
     * @since 4.0
     */
    private StringUtil() {
    }

    public static boolean isLetter(char c) {
        return isBasicLatinLetter(c) || Character.isLetter(c);
    }
    public static boolean isDigit(char c) {
        return isBasicLatinDigit(c) || Character.isDigit(c);
    }
    public static boolean isLetterOrDigit(char c) {
        return isBasicLatinLetter(c) || isBasicLatinDigit(c) || Character.isLetterOrDigit(c);
    }
    private static boolean isBasicLatinLetter(char c) {
        return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z');
    }
    private static boolean isBasicLatinDigit(char c) {
        return c >= '0' && c <= '9';
    }

    /**
     * Convert the given value to specified type.
     * @param value
     * @param type
     * @return
     */
    @SuppressWarnings("unchecked")
    public static <T> T valueOf(String value, Class type){
        if (value == null) {
            return null;
        }
        if(type == String.class) {
            return (T) value;
        }
        else if(type == Boolean.class || type == Boolean.TYPE) {
            return (T) Boolean.valueOf(value);
        }
        else if (type == Integer.class || type == Integer.TYPE) {
            return (T) Integer.decode(value);
        }
        else if (type == Float.class || type == Float.TYPE) {
            return (T) Float.valueOf(value);
        }
        else if (type == Double.class || type == Double.TYPE) {
            return (T) Double.valueOf(value);
        }
        else if (type == Long.class || type == Long.TYPE) {
            return (T) Long.decode(value);
        }
        else if (type == Short.class || type == Short.TYPE) {
            return (T) Short.decode(value);
        }
        else if (type.isAssignableFrom(List.class)) {
            return (T)new ArrayList<String>(Arrays.asList(value.split(","))); //$NON-NLS-1$
        }
        else if (type.isAssignableFrom(Set.class)) {
            return (T)new HashSet<String>(Arrays.asList(value.split(","))); //$NON-NLS-1$
        }
        else if (type.isArray()) {
            String[] values = value.split(","); //$NON-NLS-1$
            Object array = Array.newInstance(type.getComponentType(), values.length);
            for (int i = 0; i < values.length; i++) {
                Array.set(array, i, valueOf(values[i], type.getComponentType()));
            }
            return (T)array;
        }
        else if (type == Void.class) {
            return null;
        }
        else if (type.isEnum()) {
            return (T)Enum.valueOf(type, value);
        }
        else if (type == URL.class) {
            try {
                return (T)new URL(value);
            } catch (MalformedURLException e) {
                // fall through and end up in error
            }
        }
        else if (type.isAssignableFrom(Map.class)) {
            List<String> l = Arrays.asList(value.split(",")); //$NON-NLS-1$
            Map m = new HashMap<String, String>();
            for(String key: l) {
                int index = key.indexOf('=');
                if (index != -1) {
                    m.put(key.substring(0, index), key.substring(index+1));
                }
            }
            return (T)m;
        }

        throw new IllegalArgumentException("Conversion from String to "+ type.getName() + " is not supported"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public static boolean equalsIgnoreCase(String s1, String s2) {
        if (s1 != null) {
            return s1.equalsIgnoreCase(s2);
        } else if (s2 != null) {
            return false;
        }
        return true;
    }

    public static <T extends Enum<T>> T caseInsensitiveValueOf(Class<T> enumType, String name) {
        try {
            return Enum.valueOf(enumType, name);
        } catch (IllegalArgumentException e) {
            T[] vals = enumType.getEnumConstants();
            for (T t : vals) {
                if (name.equalsIgnoreCase(t.name())) {
                    return t;
                }
            }
            throw e;
        }
    }

    public static List<String> tokenize(String str, char delim) {
        ArrayList<String> result = new ArrayList<String>();
        StringBuilder current = new StringBuilder();
        boolean escaped = false;
        for (int i = 0; i < str.length(); i++) {
            char c = str.charAt(i);
            if (c == delim) {
                if (escaped) {
                    current.append(c);
                    escaped = false;
                } else {
                    escaped = true;
                }
            } else {
                if (escaped && current.length() > 0) {
                    result.add(current.toString());
                    current.setLength(0);
                    escaped = false;
                }
                current.append(c);
            }
        }
        if (current.length()>0) {
            result.add(current.toString());
        }
        return result;
    }

    /**
     * Unescape the given string
     * @param string
     * @param quoteChar
     * @param useAsciiEscapes
     * @param sb a scratch buffer to use
     * @return
     */
    public static String unescape(CharSequence string, int quoteChar, boolean useAsciiEscapes, StringBuilder sb) {
        boolean escaped = false;

        for (int i = 0; i < string.length(); i++) {
            char c = string.charAt(i);
            if (escaped) {
                switch (c) {
                case 'b':
                    sb.append('\b');
                    break;
                case 't':
                    sb.append('\t');
                    break;
                case 'n':
                    sb.append('\n');
                    break;
                case 'f':
                    sb.append('\f');
                    break;
                case 'r':
                    sb.append('\r');
                    break;
                case 'u':
                    i = parseNumericValue(string, sb, i, 0, 4, 4);
                    //TODO: this should probably be strict about needing 4 digits
                    break;
                default:
                    if (c == quoteChar) {
                        sb.append(quoteChar);
                    } else if (useAsciiEscapes) {
                        int value = Character.digit(c, 8);
                        if (value == -1) {
                            sb.append(c);
                        } else {
                            int possibleDigits = value < 3 ? 2:1;
                            int radixExp = 3;
                            i = parseNumericValue(string, sb, i, value, possibleDigits, radixExp);
                        }
                    }
                }
                escaped = false;
            } else {
                if (c == '\\') {
                    escaped = true;
                } else if (c == quoteChar) {
                    break;
                } else {
                    sb.append(c);
                }
            }
        }
        //TODO: should this be strict?
        //if (escaped) {
            //throw new FunctionExecutionException();
        //}
        return sb.toString();
    }

    private static int parseNumericValue(CharSequence string, StringBuilder sb,
            int i, int value, int possibleDigits, int radixExp) {
        for (int j = 0; j < possibleDigits; j++) {
            if (i + 1 == string.length()) {
                break;
            }
            char digit = string.charAt(i + 1);
            int val = Character.digit(digit, 1 << radixExp);
            if (val == -1) {
                break;
            }
            i++;
            value = (value << radixExp) + val;
        }
        sb.append((char)value);
        return i;
    }

}
