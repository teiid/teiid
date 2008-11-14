/*
 * JBoss, Home of Professional Open Source.
 * Copyright (C) 2008 Red Hat, Inc.
 * Copyright (C) 2000-2007 MetaMatrix, Inc.
 * Licensed to Red Hat, Inc. under one or more contributor 
 * license agreements.  See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
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
import java.util.Enumeration;
import java.util.NoSuchElementException;


/**
 * The string tokenizer class allows an application to break a 
 * string into tokens AND (unlike the JDK tokenizer) access the delimiters
 * separating the tokens.
 * The tokenization method is much simpler than 
 * the one used by the <code>StreamTokenizer</code> class. The 
 * <code>StringTokenizer</code> methods do not distinguish among 
 * identifiers, numbers, and quoted strings, nor do they recognize 
 * and skip comments. 
 * <p>
 * The set of delimiters (the characters that separate tokens) may 
 * be specified either at creation time or on a per-token basis. 
 * <p>
 * An instance of <code>StringTokenizer</code> behaves in one of two 
 * ways, depending on whether it was created with the 
 * <code>returnDelims</code> flag having the value <code>true</code> 
 * or <code>false</code>: 
 * <ul>
 * <li>If the flag is <code>false</code>, delimiter characters serve to 
 *     separate tokens. A token is a maximal sequence of consecutive 
 *     characters that are not delimiters. 
 * <li>If the flag is <code>true</code>, delimiter characters are themselves 
 *     considered to be tokens. A token is thus either one delimiter 
 *     character, or a maximal sequence of consecutive characters that are 
 *     not delimiters.
 * </ul><p>
 * A <tt>StringTokenizer</tt> object internally maintains a current 
 * position within the string to be tokenized. Some operations advance this 
 * current position past the characters processed.<p>
 * A token is returned by taking a substring of the string that was used to 
 * create the <tt>StringTokenizer</tt> object.
 * <p>
 * The following is one example of the use of the tokenizer. The code:
 * <blockquote><pre>
 *     StringTokenizer st = new StringTokenizer("this is a test");
 *     while (st.hasMoreTokens()) {
 *         println(st.nextToken());
 *     }
 * </pre></blockquote>
 * <p>
 * prints the following output:
 * <blockquote><pre>
 *     this
 *     is
 *     a
 *     test
 * </pre></blockquote>
 *
 */
public
class EnhancedStringTokenizer implements Enumeration {
    private int currentPosition;
    private int newPosition;
    private int maxPosition;
    private String str;
    private String delimiters;
    private int lastCurrentPosition;
    private boolean retDelims;
    private boolean delimsChanged;

    /**
     * maxDelimChar stores the value of the delimiter character with the
     * highest value. It is used to optimize the detection of delimiter
     * characters.
     */
    private char maxDelimChar;

    /**
     * Set maxDelimChar to the highest char in the delimiter set.
     */
    private void setMaxDelimChar() {
        if (delimiters == null) {
            maxDelimChar = 0;
            return;
        }

    char m = 0;
    for (int i = 0; i < delimiters.length(); i++) {
        char c = delimiters.charAt(i);
        if (m < c)
        m = c;
    }
    maxDelimChar = m;
    }

    /**
     * Constructs a string tokenizer for the specified string. All  
     * characters in the <code>delim</code> argument are the delimiters 
     * for separating tokens. 
     * <p>
     * If the <code>returnDelims</code> flag is <code>true</code>, then 
     * the delimiter characters are also returned as tokens. Each 
     * delimiter is returned as a string of length one. If the flag is 
     * <code>false</code>, the delimiter characters are skipped and only 
     * serve as separators between tokens. 
     *
     * @param   str            a string to be parsed.
     * @param   delim          the delimiters.
     * @param   returnDelims   flag indicating whether to return the delimiters
     *                         as tokens.
     */
    public EnhancedStringTokenizer(String str, String delim, boolean returnDelims) {
    currentPosition = 0;
    lastCurrentPosition = -1;
    newPosition = -1;
    delimsChanged = false;
    this.str = str;
    maxPosition = str.length();
    delimiters = delim;
    retDelims = returnDelims;
        setMaxDelimChar();
    }

    /**
     * Constructs a string tokenizer for the specified string. The 
     * characters in the <code>delim</code> argument are the delimiters 
     * for separating tokens. Delimiter characters themselves will not 
     * be treated as tokens.
     *
     * @param   str     a string to be parsed.
     * @param   delim   the delimiters.
     */
    public EnhancedStringTokenizer(String str, String delim) {
    this(str, delim, false);
    }

    /**
     * Constructs a string tokenizer for the specified string. The 
     * tokenizer uses the default delimiter set, which is 
     * <code>"&nbsp;&#92;t&#92;n&#92;r&#92;f"</code>: the space character, 
     * the tab character, the newline character, the carriage-return character,
     * and the form-feed character. Delimiter characters themselves will 
     * not be treated as tokens.
     *
     * @param   str   a string to be parsed.
     */
    public EnhancedStringTokenizer(String str) {
    this(str, " \t\n\r\f", false); //$NON-NLS-1$
    }

    /**
     * Skips delimiters starting from the specified position. If retDelims
     * is false, returns the index of the first non-delimiter character at or
     * after startPos. If retDelims is true, startPos is returned.
     */
    private int skipDelimiters(int startPos) {
        if (delimiters == null)
            throw new NullPointerException();

        int position = startPos;
    while (!retDelims && position < maxPosition) {
            char c = str.charAt(position);
            if ((c > maxDelimChar) || (delimiters.indexOf(c) < 0))
                break;
        position++;
    }
        return position;
    }

    /**
     * Skips ahead from startPos and returns the index of the next delimiter
     * character encountered, or maxPosition if no such delimiter is found.
     */
    private int scanToken(int startPos) {
        int position = startPos;
        while (position < maxPosition) {
            char c = str.charAt(position);
            if ((c <= maxDelimChar) && (delimiters.indexOf(c) >= 0))
                break;
            position++;
    }
    if (retDelims && (startPos == position)) {
            char c = str.charAt(position);
        if ((c <= maxDelimChar) && (delimiters.indexOf(c) >= 0))
                position++;
        }
        return position;
    }

    /**
     * Tests if there are more tokens available from this tokenizer's string. 
     * If this method returns <tt>true</tt>, then a subsequent call to 
     * <tt>nextToken</tt> with no argument will successfully return a token.
     *
     * @return  <code>true</code> if and only if there is at least one token 
     *          in the string after the current position; <code>false</code> 
     *          otherwise.
     */
    public boolean hasMoreTokens() {
    /*
     * Temporary store this position and use it in the following
     * nextToken() method only if the delimiters have'nt been changed in
     * that nextToken() invocation.
     */
    newPosition = skipDelimiters(currentPosition);
    return (newPosition < maxPosition);
    }
    
    /**
     * Return the next delimiter character(s) that appear in the String.
     * Typically, this method is called immediately after or immediate
     * before the {@link #nextToken()} method.
     * @return the dlimiter character(s), or null if there is no delimiter
     * @since 4.2
     */
    public String nextDelimiters() {
        if ( currentPosition == 0 && lastCurrentPosition == -1 ) {
            lastCurrentPosition = 0;
            return null;
        }
        final int nextPosition = (newPosition >= 0 && !delimsChanged ) ? newPosition : skipDelimiters(currentPosition);
        if ( nextPosition == lastCurrentPosition ) {
            // There are no more ..
            return null;
        }
        final String result = str.substring(lastCurrentPosition,nextPosition);
        lastCurrentPosition = maxPosition;
        return result;
    }

    /**
     * Returns the next token from this string tokenizer.
     *
     * @return     the next token from this string tokenizer.
     * @exception  NoSuchElementException  if there are no more tokens in this
     *               tokenizer's string.
     */
    public String nextToken() {
    /* 
     * If next position already computed in hasMoreElements() and
     * delimiters have changed between the computation and this invocation,
     * then use the computed value.
     */

    currentPosition = (newPosition >= 0 && !delimsChanged) ?  
        newPosition : skipDelimiters(currentPosition);

    /* Reset these anyway */
    delimsChanged = false;
    newPosition = -1;

    if (currentPosition >= maxPosition)
        throw new NoSuchElementException();
    int start = currentPosition;
    currentPosition = scanToken(currentPosition);
    String result = str.substring(start, currentPosition);
    lastCurrentPosition = currentPosition;
    return result;
    }

    /**
     * Returns the next token in this string tokenizer's string. First, 
     * the set of characters considered to be delimiters by this 
     * <tt>StringTokenizer</tt> object is changed to be the characters in 
     * the string <tt>delim</tt>. Then the next token in the string
     * after the current position is returned. The current position is 
     * advanced beyond the recognized token.  The new delimiter set 
     * remains the default after this call. 
     *
     * @param      delim   the new delimiters.
     * @return     the next token, after switching to the new delimiter set.
     * @exception  NoSuchElementException  if there are no more tokens in this
     *               tokenizer's string.
     */
    public String nextToken(String delim) {
    delimiters = delim;

    /* delimiter string specified, so set the appropriate flag. */
    delimsChanged = true;

        setMaxDelimChar();
    return nextToken();
    }

    /**
     * Returns the same value as the <code>hasMoreTokens</code>
     * method. It exists so that this class can implement the
     * <code>Enumeration</code> interface. 
     *
     * @return  <code>true</code> if there are more tokens;
     *          <code>false</code> otherwise.
     * @see     java.util.Enumeration
     * @see     java.util.StringTokenizer#hasMoreTokens()
     */
    public boolean hasMoreElements() {
    return hasMoreTokens();
    }

    /**
     * Returns the same value as the <code>nextToken</code> method,
     * except that its declared return value is <code>Object</code> rather than
     * <code>String</code>. It exists so that this class can implement the
     * <code>Enumeration</code> interface. 
     *
     * @return     the next token in the string.
     * @exception  NoSuchElementException  if there are no more tokens in this
     *               tokenizer's string.
     * @see        java.util.Enumeration
     * @see        java.util.StringTokenizer#nextToken()
     */
    public Object nextElement() {
    return nextToken();
    }

    /**
     * Calculates the number of times that this tokenizer's 
     * <code>nextToken</code> method can be called before it generates an 
     * exception. The current position is not advanced.
     *
     * @return  the number of tokens remaining in the string using the current
     *          delimiter set.
     * @see     java.util.StringTokenizer#nextToken()
     */
    public int countTokens() {
    int count = 0;
    int currpos = currentPosition;
    while (currpos < maxPosition) {
            currpos = skipDelimiters(currpos);
        if (currpos >= maxPosition)
        break;
            currpos = scanToken(currpos);
        count++;
    }
    return count;
    }
}
