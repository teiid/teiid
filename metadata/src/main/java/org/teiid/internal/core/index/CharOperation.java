/*******************************************************************************
 * Copyright (c) 2000, 2003 IBM Corporation and others.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * which accompanies this distribution, and is available at
 * https://www.eclipse.org/legal/epl-v10.html
 *
 * Contributors:
 *     IBM Corporation - initial API and implementation
 *     MetaMatrix, Inc - repackaging and updates for use as a metadata store
 *******************************************************************************/

package org.teiid.internal.core.index;

/**
 * This class is a collection of helper methods to manipulate char arrays.
 *
 * @since 2.1
 */
public final class CharOperation {

    /**
     * Answers true if the pattern matches the given name, false otherwise. This
     * char[] pattern matching accepts wild-cards '*' and '?'.
     *
     * When not case sensitive, the pattern is assumed to already be lowercased,
     * the name will be lowercased character per character as comparing. If name
     * is null, the answer is false. If pattern is null, the answer is true if
     * name is not null. <br>
     * <br>
     * For example:
     * <ol>
     * <li>
     *
     * <pre>
     *    pattern = { '?', 'b', '*' }
     *    name = { 'a', 'b', 'c' , 'd' }
     *    isCaseSensitive = true
     *    result =&gt; true
     * </pre>
     *
     * </li>
     * <li>
     *
     * <pre>
     *    pattern = { '?', 'b', '?' }
     *    name = { 'a', 'b', 'c' , 'd' }
     *    isCaseSensitive = true
     *    result =&gt; false
     * </pre>
     *
     * </li>
     * <li>
     *
     * <pre>
     *    pattern = { 'b', '*' }
     *    name = { 'a', 'b', 'c' , 'd' }
     *    isCaseSensitive = true
     *    result =&gt; false
     * </pre>
     *
     * </li>
     * </ol>
     *
     * @param pattern
     *            the given pattern
     * @param name
     *            the given name
     * @param isCaseSensitive
     *            flag to know whether or not the matching should be case
     *            sensitive
     * @return true if the pattern matches the given name, false otherwise
     *
     * TODO: this code was derived from eclipse CharOperation.
     * It also lacks the ability to specify an escape character.
     *
     */
    public static final boolean match(char[] pattern, char[] name,
            boolean isCaseSensitive) {

        if (name == null)
            return false; // null name cannot match
        if (pattern == null)
            return true; // null pattern is equivalent to '*'

        int patternEnd = pattern.length;
        int nameEnd = name.length;

        int iPattern = 0;
        int iName = 0;

        /* check first segment */
        char patternChar = 0;
        while ((iPattern < patternEnd)
                && (patternChar = pattern[iPattern]) != '*') {
            if (iName == nameEnd)
                return false;
            if (isCaseSensitive && patternChar != name[iName]
                    && patternChar != '?') {
                return false;
            } else if (!isCaseSensitive
                    && Character.toLowerCase(patternChar) != Character
                            .toLowerCase(name[iName]) && patternChar != '?') {
                return false;
            }
            iName++;
            iPattern++;
        }
        /* check sequence of star+segment */
        int segmentStart;
        if (patternChar == '*') {
            if (patternEnd == 1) {
                return true;
            }
            segmentStart = ++iPattern; // skip star
        } else {
            segmentStart = 0; // force iName check
        }
        int prefixStart = iName;
        checkSegment: while (iName < nameEnd) {
            if (iPattern == patternEnd) {
                iPattern = segmentStart; // mismatch - restart current
                                            // segment
                iName = ++prefixStart;
                continue checkSegment;
            }
            /* segment is ending */
            if ((patternChar = pattern[iPattern]) == '*') {
                segmentStart = ++iPattern; // skip start
                if (segmentStart == patternEnd) {
                    return true;
                }
                prefixStart = iName;
                continue checkSegment;
            }
            /* check current name character */
            char matchChar = isCaseSensitive ? name[iName] : Character
                    .toLowerCase(name[iName]);
            if ((isCaseSensitive ? ((matchChar != patternChar) && patternChar != '?')
                    : (matchChar != Character.toLowerCase(patternChar))
                            && patternChar != '?')) {
                iPattern = segmentStart; // mismatch - restart current
                                            // segment
                iName = ++prefixStart;
                continue checkSegment;
            }
            iName++;
            iPattern++;
        }

        return (segmentStart == patternEnd)
                || (iName == nameEnd && iPattern == patternEnd)
                || (iPattern == patternEnd - 1 && pattern[iPattern] == '*');
    }

    /**
     * Answers true if the given name starts with the given prefix, false otherwise.
     * isCaseSensitive is used to find out whether or not the comparison should be case sensitive.
     * <br>
     * <br>
     * For example:
     * <ol>
     * <li><pre>
     *    prefix = { 'a' , 'B' }
     *    name = { 'a' , 'b', 'b', 'a', 'b', 'a' }
     *    isCaseSensitive = false
     *    result =&gt; true
     * </pre>
     * </li>
     * <li><pre>
     *    prefix = { 'a' , 'B' }
     *    name = { 'a' , 'b', 'b', 'a', 'b', 'a' }
     *    isCaseSensitive = true
     *    result =&gt; false
     * </pre>
     * </li>
     * </ol>
     *
     * @param prefix the given prefix
     * @param name the given name
     * @param isCaseSensitive to find out whether or not the comparison should be case sensitive
     * @return true if the given name starts with the given prefix, false otherwise
     * @exception NullPointerException if the given name is null or if the given prefix is null
     */
    public static final boolean prefixEquals(char[] prefix, char[] name,
            boolean isCaseSensitive) {

        int max = prefix.length;
        if (name.length < max)
            return false;

        for (int i = max; --i >= 0;) {
            if (prefix[i] == name[i]
                    || (isCaseSensitive && Character.toLowerCase(prefix[i]) == Character
                            .toLowerCase(name[i]))) {
                continue;
            }
            return false;
        }
        return true;
    }
}
