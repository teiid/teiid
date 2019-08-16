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

package org.teiid.query.sql.lang;

import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import org.teiid.api.exception.query.ExpressionEvaluationException;
import org.teiid.core.util.EquivalenceUtil;
import org.teiid.core.util.HashCodeUtil;
import org.teiid.core.util.LRUCache;
import org.teiid.core.util.PropertiesUtils;
import org.teiid.language.Like.MatchMode;
import org.teiid.query.QueryPlugin;
import org.teiid.query.sql.LanguageVisitor;
import org.teiid.query.sql.lang.PredicateCriteria.Negatable;
import org.teiid.query.sql.symbol.Expression;


/**
 * This class represents a criteria involving a string expression to be matched
 * against a string expression match value.  The match value may contain a few
 * special characters: % represents 0 or more characters and _ represents a single
 * match character.  The escape character can be used to escape an actual % or _ within a
 * match string.
 */
public class MatchCriteria extends PredicateCriteria implements Negatable {

    /** The default wildcard character - '%' */
    public static final char WILDCARD_CHAR = '%';

    /** The default single match character - '_' */
    public static final char MATCH_CHAR = '_';

    /** The left-hand expression. */
    private Expression leftExpression;

    /** The right-hand expression. */
    private Expression rightExpression;

    /** The internal null escape character */
    public static final char NULL_ESCAPE_CHAR = 0;

    static char DEFAULT_ESCAPE_CHAR = PropertiesUtils.getHierarchicalProperty("org.teiid.backslashDefaultMatchEscape", false, Boolean.class)?'\\':NULL_ESCAPE_CHAR; //$NON-NLS-1$

    /** The escape character or '' if there is none */
    private char escapeChar = DEFAULT_ESCAPE_CHAR;

    /** Negation flag. Indicates whether the criteria expression contains a NOT. */
    private boolean negated;
    private MatchMode mode = MatchMode.LIKE;

    /**
     * Constructs a default instance of this class.
     */
    public MatchCriteria() {}

    /**
     * Constructs an instance of this class from a left and right expression
     *
     * @param leftExpression The expression to check
     * @param rightExpression The match expression
     */
    public MatchCriteria( Expression leftExpression, Expression rightExpression ) {
        setLeftExpression(leftExpression);
        setRightExpression(rightExpression);
    }

    /**
     * Constructs an instance of this class from a left and right expression
     * and an escape character
     *
      * @param leftExpression The expression to check
     * @param rightExpression The match expression
     * @param escapeChar The escape character, to allow literal use of wildcard and single match chars
     */
    public MatchCriteria( Expression leftExpression, Expression rightExpression, char escapeChar ) {
        this(leftExpression, rightExpression);
        setEscapeChar(escapeChar);
    }

    /**
     * Set left expression.
     * @param expression expression
     */
    public void setLeftExpression(Expression expression) {
        this.leftExpression = expression;
    }

    /**
     * Get left expression.
     * @return Left expression
     */
    public Expression getLeftExpression() {
        return this.leftExpression;
    }

    /**
     * Set right expression.
     * @param expression expression
     */
    public void setRightExpression(Expression expression) {
        this.rightExpression = expression;
    }

    /**
     * Get right expression.
     * @return right expression
     */
    public Expression getRightExpression() {
        return this.rightExpression;
    }

    /**
     * Get the escape character, which can be placed before the wildcard or single match
     * character in the expression to prevent it from being used as a wildcard or single
     * match.  The escape character must not be used elsewhere in the expression.
     * For example, to match "35%" without activating % as a wildcard, set the escape character
     * to '$' and use the expression "35$%".
     * @return Escape character, if not set will return {@link #NULL_ESCAPE_CHAR}
     */
    public char getEscapeChar() {
        return this.escapeChar;
    }

    /**
     * Set the escape character which can be used when the wildcard or single
     * character should be used literally.
     * @param escapeChar New escape character
     */
    public void setEscapeChar(char escapeChar) {
        this.escapeChar = escapeChar;
    }

    /**
     * Returns whether this criteria is negated.
     * @return flag indicating whether this criteria contains a NOT
     */
    public boolean isNegated() {
        return negated;
    }

    /**
     * Sets the negation flag for this criteria.
     * @param negationFlag true if this criteria contains a NOT; false otherwise
     */
    public void setNegated(boolean negationFlag) {
        negated = negationFlag;
    }

    @Override
    public void negate() {
        this.negated = !this.negated;
    }

    public void acceptVisitor(LanguageVisitor visitor) {
        visitor.visit(this);
    }

    /**
     * Get hash code.  WARNING: The hash code is based on data in the criteria.
     * If data values are changed, the hash code will change - don't hash this
     * object and change values.
     * @return Hash code
     */
    public int hashCode() {
        int hc = 0;
        hc = HashCodeUtil.hashCode(hc, getLeftExpression());
        hc = HashCodeUtil.hashCode(hc, getRightExpression());
        return hc;
    }

    /**
     * Override equals() method.
     * @param obj Other object
     * @return True if equal
     */
    public boolean equals(Object obj) {
        // Use super.equals() to check obvious stuff and variable
        if(obj == this) {
            return true;
        }

        if(!(obj instanceof MatchCriteria)) {
            return false;
        }

        MatchCriteria mc = (MatchCriteria)obj;

        if (isNegated() != mc.isNegated()) {
            return false;
        }

        if (this.mode != mc.mode) {
            return false;
        }

        return getEscapeChar() == mc.getEscapeChar() &&
        EquivalenceUtil.areEqual(getLeftExpression(), mc.getLeftExpression()) &&
        EquivalenceUtil.areEqual(getRightExpression(), mc.getRightExpression());
    }

    /**
     * Deep copy of object
     * @return Deep copy of object
     */
    public Object clone() {
        Expression leftCopy = null;
        if(getLeftExpression() != null) {
            leftCopy = (Expression) getLeftExpression().clone();
        }
        Expression rightCopy = null;
        if(getRightExpression() != null) {
            rightCopy = (Expression) getRightExpression().clone();
        }
        MatchCriteria criteriaCopy = new MatchCriteria(leftCopy, rightCopy, getEscapeChar());
        criteriaCopy.setNegated(isNegated());
        criteriaCopy.mode = mode;
        return criteriaCopy;
    }

    private final static LRUCache<List<?>, Pattern> patternCache = new LRUCache<List<?>, Pattern>(100);

    public static Pattern getPattern(String newPattern, String originalPattern, int flags) throws ExpressionEvaluationException {
        List<?> key = Arrays.asList(newPattern, flags);
        Pattern p = patternCache.get(key);
        if (p == null) {
            try {
                p = Pattern.compile(newPattern, Pattern.DOTALL);
                patternCache.put(key, p);
            } catch(PatternSyntaxException e) {
                 throw new ExpressionEvaluationException(QueryPlugin.Event.TEIID30448, e, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30448, new Object[]{originalPattern, e.getMessage()}));
            }
        }
        return p;
    }

    /**
     * <p>Utility to convert the pattern into a different match syntax
     */
    public static class PatternTranslator {
        private char[] reserved;
        private char newEscape;
        private char[] toReplace;
        private String[] replacements;
        private int flags;
        private final LRUCache<List<?>, Pattern> cache = new LRUCache<List<?>, Pattern>(100);

        /**
         * @param toReplace meta characters to replace
         * @param replacements the replacements for the meta characters
         * @param reserved sorted array of reserved chars in the new match syntax
         * @param newEscape escape char in the new syntax
         */
        public PatternTranslator(char[] toReplace, String[] replacements, char[] reserved, char newEscape, int flags) {
            this.reserved = reserved;
            this.newEscape = newEscape;
            this.toReplace = toReplace;
            this.replacements = replacements;
            this.flags = flags;
        }

        public Pattern translate(String pattern, char escape) throws ExpressionEvaluationException {
            List<?> key = Arrays.asList(pattern, escape);
            Pattern result = null;
            synchronized (cache) {
                result = cache.get(key);
            }
            if (result == null) {
                String newPattern = getPatternString(pattern, escape);
                result = getPattern(newPattern, pattern, flags);
                synchronized (cache) {
                    cache.put(key, result);
                }
            }
            return result;
        }

        public String getPatternString(String pattern, char escape)
                throws ExpressionEvaluationException {
            int startChar = 0;
            StringBuffer newPattern = new StringBuffer(pattern.length());
            if (pattern.length() > 0 && pattern.charAt(0) == '%') {
                startChar = 1;
            } else {
                newPattern.append('^');
            }

            boolean escaped = false;
            boolean endsWithMatchAny = false;
            for (int i = startChar; i < pattern.length(); i++) {
                char character = pattern.charAt(i);

                if (character == escape && character != NULL_ESCAPE_CHAR) {
                    if (escaped) {
                        appendCharacter(newPattern, character);
                        escaped = false;
                    } else {
                        escaped = true;
                    }
                } else {
                    int index = Arrays.binarySearch(toReplace, character);
                    if (index >= 0) {
                        if (escaped) {
                            appendCharacter(newPattern, character);
                            escaped = false;
                        } else {
                            if (character == '%' && i == pattern.length() - 1) {
                                endsWithMatchAny = true;
                                continue;
                            }
                            newPattern.append(replacements[index]);
                        }
                    } else {
                        if (escaped) {
                             throw new ExpressionEvaluationException(QueryPlugin.Event.TEIID30449, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30449, new Object[] {pattern, new Character(escape)}));
                        }
                        appendCharacter(newPattern, character);
                    }
                }
            }

            if (escaped) {
                 throw new ExpressionEvaluationException(QueryPlugin.Event.TEIID30449, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30449, new Object[] {pattern, new Character(escape)}));
            }

            if (!endsWithMatchAny) {
                newPattern.append('$');
            }
            return newPattern.toString();
        }

        private void appendCharacter(StringBuffer newPattern, char character) {
            if (Arrays.binarySearch(this.reserved, character) >= 0) {
                newPattern.append(this.newEscape);
            }
            newPattern.append(character);
        }

    }

    public MatchMode getMode() {
        return mode;
    }

    public void setMode(MatchMode mode) {
        this.mode = mode;
    }

}  // END CLASS
