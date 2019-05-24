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

package org.teiid.query.processor.eval;

import static org.junit.Assert.*;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Test;
import org.teiid.api.exception.query.ExpressionEvaluationException;
import org.teiid.common.buffer.BlockedException;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidProcessingException;
import org.teiid.query.eval.Evaluator;
import org.teiid.query.sql.lang.*;
import org.teiid.query.sql.symbol.Constant;
import org.teiid.query.sql.symbol.ElementSymbol;
import org.teiid.query.sql.util.ValueIterator;
import org.teiid.query.util.CommandContext;


public class TestCriteriaEvaluator {

    // ################################## TEST HELPERS ################################

    private void helpTestMatch(String value, String pattern, char escape, boolean negated, boolean expectedMatch) throws ExpressionEvaluationException, BlockedException, TeiidComponentException {
        MatchCriteria crit = new MatchCriteria(new Constant(value), new Constant(pattern), escape);
        crit.setNegated(negated);
        boolean actualMatch = Evaluator.evaluate(crit);
        // Compare actual and expected match
        assertEquals("Match criteria test failed for value=[" + value + "], pattern=[" + pattern + "], hasEscape=" + (escape != MatchCriteria.NULL_ESCAPE_CHAR) + ": ", expectedMatch, actualMatch); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
    }

    private void helpTestMatch(String value, String pattern, char escape, boolean expectedMatch) throws ExpressionEvaluationException, BlockedException, TeiidComponentException {
        helpTestMatch(value, pattern, escape, false, expectedMatch);
    }

    private void helpTestIsNull(String value, boolean negated, boolean expectedMatch) throws ExpressionEvaluationException, BlockedException, TeiidComponentException {
        IsNullCriteria criteria = new IsNullCriteria(new Constant(value));
        criteria.setNegated(negated);

        boolean result = Evaluator.evaluate(criteria);
        assertEquals("Result did not match expected value", expectedMatch, result); //$NON-NLS-1$
    }

    private void helpTestSetCriteria(int value, boolean negated, boolean expectedMatch) throws ExpressionEvaluationException, BlockedException, TeiidComponentException {
        helpTestSetCriteria(new Integer(value), negated, expectedMatch);
    }

    private void helpTestSetCriteria(Integer value, boolean negated, boolean expectedMatch) throws ExpressionEvaluationException, BlockedException, TeiidComponentException {
        Collection constants = new ArrayList(2);
        constants.add(new Constant(new Integer(1000)));
        constants.add(new Constant(new Integer(5000)));
        SetCriteria crit = new SetCriteria(new Constant(value), constants);
        crit.setNegated(negated);
        boolean result = Evaluator.evaluate(crit);
        assertEquals("Result did not match expected value", expectedMatch, result); //$NON-NLS-1$
    }

    private void helpTestCompareSubqueryCriteria(Criteria crit, Boolean expectedResult, final Collection<Object> values) throws ExpressionEvaluationException, BlockedException, TeiidComponentException{

        Map elementMap = new HashMap();
        ElementSymbol e1 = new ElementSymbol("e1"); //$NON-NLS-1$
        elementMap.put(e1, new Integer(0));

        List tuple = Arrays.asList(new String[]{"a"}); //$NON-NLS-1$
        CommandContext cc = new CommandContext();
        assertEquals(expectedResult, new Evaluator(elementMap, null, cc) {
            @Override
            protected ValueIterator evaluateSubquery(
                    SubqueryContainer container, List tuple)
                    throws TeiidProcessingException, BlockedException,
                    TeiidComponentException {
                return new CollectionValueIterator(values);
            }
        }.evaluateTVL(crit, tuple));
    }

    private SubqueryCompareCriteria helpGetCompareSubqueryCriteria(int operator, int predicateQuantifier){
        ElementSymbol e1 = new ElementSymbol("e1"); //$NON-NLS-1$
        SubqueryCompareCriteria crit = new SubqueryCompareCriteria(e1, new Query(), operator, predicateQuantifier);
        return crit;
    }

    // ################################## ACTUAL TESTS ################################

    @Test public void testIsNull1() throws Exception {
        helpTestIsNull(null, false, true);
    }

    @Test public void testIsNull2() throws Exception {
        helpTestIsNull(null, true, false);
    }

    @Test public void testIsNull3() throws Exception {
        helpTestIsNull("x", false, false); //$NON-NLS-1$
    }

    @Test public void testIsNull4() throws Exception {
        helpTestIsNull("x", true, true); //$NON-NLS-1$
    }

    @Test public void testMatch1() throws Exception {
        helpTestMatch("", "", MatchCriteria.NULL_ESCAPE_CHAR, true);         //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testMatch2() throws Exception {
        helpTestMatch("x", "", MatchCriteria.NULL_ESCAPE_CHAR, false);         //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testMatch3() throws Exception {
        helpTestMatch("", "%", MatchCriteria.NULL_ESCAPE_CHAR, true);         //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testMatch4() throws Exception {
        helpTestMatch("x", "%", MatchCriteria.NULL_ESCAPE_CHAR, true);         //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testMatch5() throws Exception {
        helpTestMatch("xx", "%", MatchCriteria.NULL_ESCAPE_CHAR, true);         //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testMatch7() throws Exception {
        helpTestMatch("a", "a%", MatchCriteria.NULL_ESCAPE_CHAR, true);         //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testMatch8() throws Exception {
        helpTestMatch("ab", "a%", MatchCriteria.NULL_ESCAPE_CHAR, true);         //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testMatch9() throws Exception {
        helpTestMatch("a.", "a%", MatchCriteria.NULL_ESCAPE_CHAR, true);         //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testMatch11() throws Exception {
        helpTestMatch("ax.", "a%", MatchCriteria.NULL_ESCAPE_CHAR, true);         //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testMatch12() throws Exception {
        helpTestMatch("a..", "a%", MatchCriteria.NULL_ESCAPE_CHAR, true);         //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testMatch13() throws Exception {
//        helpTestMatch("x.y", "%.", MatchCriteria.NULL_ESCAPE_CHAR, false);
        helpTestMatch("a.b", "a%.", MatchCriteria.NULL_ESCAPE_CHAR, false);         //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testMatch14() throws Exception {
        helpTestMatch("aaa", "%aaa", MatchCriteria.NULL_ESCAPE_CHAR, true);         //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testMatch15() throws Exception {
        helpTestMatch("baaa", "%aaa", MatchCriteria.NULL_ESCAPE_CHAR, true);         //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testMatch16() throws Exception {
        helpTestMatch("aaaa", "%aaa", MatchCriteria.NULL_ESCAPE_CHAR, true);         //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testMatch17() throws Exception {
        helpTestMatch("aaxaa", "%aaa", MatchCriteria.NULL_ESCAPE_CHAR, false);         //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testMatch18() throws Exception {
        helpTestMatch("", "a%b%", MatchCriteria.NULL_ESCAPE_CHAR, false);         //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testMatch19() throws Exception {
        helpTestMatch("a", "a%b%", MatchCriteria.NULL_ESCAPE_CHAR, false);         //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testMatch20() throws Exception {
        helpTestMatch("ab", "a%b%", MatchCriteria.NULL_ESCAPE_CHAR, true);         //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testMatch21() throws Exception {
        helpTestMatch("axb", "a%b%", MatchCriteria.NULL_ESCAPE_CHAR, true);         //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testMatch22() throws Exception {
        helpTestMatch("abx", "a%b%", MatchCriteria.NULL_ESCAPE_CHAR, true);         //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testMatch23() throws Exception {
        helpTestMatch("", "X%", 'X', false);         //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testMatch24() throws Exception {
        helpTestMatch("x", "X%", 'X', false);         //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testMatch25() throws Exception {
        helpTestMatch("xx", "X%", 'X', false);         //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testMatch26() throws Exception {
        helpTestMatch("a%", "aX%", 'X', true);         //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testMatch27() throws Exception {
        helpTestMatch("aX%", "aX%", 'X', false);         //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testMatch28() throws Exception {
        helpTestMatch("a%bb", "aX%b%", 'X', true);         //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testMatch29() throws Exception {
        helpTestMatch("aX%bb", "aX%b%", 'X', false);         //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testMatch30() throws Exception {
        helpTestMatch("", "_", MatchCriteria.NULL_ESCAPE_CHAR, false); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testMatch31() throws Exception {
        helpTestMatch("X", "_", MatchCriteria.NULL_ESCAPE_CHAR, true); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testMatch32() throws Exception {
        helpTestMatch("XX", "_", MatchCriteria.NULL_ESCAPE_CHAR, false); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testMatch33() throws Exception {
        helpTestMatch("", "__", MatchCriteria.NULL_ESCAPE_CHAR, false); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testMatch34() throws Exception {
        helpTestMatch("X", "__", MatchCriteria.NULL_ESCAPE_CHAR, false); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testMatch35() throws Exception {
        helpTestMatch("XX", "__", MatchCriteria.NULL_ESCAPE_CHAR, true); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testMatch36() throws Exception {
        helpTestMatch("XX", "_%_", MatchCriteria.NULL_ESCAPE_CHAR, true); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testMatch37() throws Exception {
        helpTestMatch("XaaY", "_%_", MatchCriteria.NULL_ESCAPE_CHAR, true); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testMatch38() throws Exception {
        helpTestMatch("a.b.c", "a.b.c", MatchCriteria.NULL_ESCAPE_CHAR, true); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testMatch39() throws Exception {
        helpTestMatch("a.b.c", "a%.c", MatchCriteria.NULL_ESCAPE_CHAR, true); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testMatch40() throws Exception {
        helpTestMatch("a.b.", "a.b.", MatchCriteria.NULL_ESCAPE_CHAR, true); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testMatch41() throws Exception {
        helpTestMatch("asjdfajsdf (&). asdfasdf\nkjhkjh", "%&%", MatchCriteria.NULL_ESCAPE_CHAR, true);     //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testMatch42() throws Exception {
        helpTestMatch("x", "", MatchCriteria.NULL_ESCAPE_CHAR, true, true); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testMatch43() throws Exception {
        helpTestMatch("a.b.", "a.b.", MatchCriteria.NULL_ESCAPE_CHAR, true, false); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testMatch44() throws Exception {
        helpTestMatch(null, "a.b.", MatchCriteria.NULL_ESCAPE_CHAR, false); //$NON-NLS-1$
    }

    @Test public void testMatch45() throws Exception {
        helpTestMatch("a.b.", null, MatchCriteria.NULL_ESCAPE_CHAR, false); //$NON-NLS-1$
    }

    @Test public void testMatch46() throws Exception {
        helpTestMatch("ab\r\n", "ab%", MatchCriteria.NULL_ESCAPE_CHAR, true); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testMatch47() throws Exception {
        helpTestMatch("", "", 'a', true); //$NON-NLS-1$ //$NON-NLS-2$
    }

    //should succeed - should be able to escape the escape char
    @Test public void testMatch48() throws Exception {
        helpTestMatch("abc", "aa%", 'a', true); //$NON-NLS-1$ //$NON-NLS-2$
    }

    //should fail - invalid match sequence
    @Test public void testMatch49() throws Exception {
        try {
            helpTestMatch("abc", "a", 'a', true); //$NON-NLS-1$ //$NON-NLS-2$
        } catch (ExpressionEvaluationException cee) {
            assertEquals("TEIID30449 Invalid escape sequence \"a\" with escape character \"a\"", cee.getMessage()); //$NON-NLS-1$
        }
    }

    //should fail - can't escape a non match char
    @Test public void testMatch50() throws Exception {
        try {
            helpTestMatch("abc", "ab", 'a', true); //$NON-NLS-1$ //$NON-NLS-2$
        } catch (ExpressionEvaluationException cee) {
            assertEquals("TEIID30449 Invalid escape sequence \"ab\" with escape character \"a\"", cee.getMessage()); //$NON-NLS-1$
        }
    }

    //should be able to use a regex reserved char as the escape char
    @Test public void testMatch51() throws Exception {
        helpTestMatch("$", "$$", '$', true); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testMatch52() throws Exception {
        helpTestMatch("abc\nde", "a%e", MatchCriteria.NULL_ESCAPE_CHAR, true); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testMatch53() throws Exception {
        helpTestMatch("\\", "\\%", MatchCriteria.NULL_ESCAPE_CHAR, true); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testSetCriteria1() throws Exception {
        helpTestSetCriteria(1000, false, true);
    }

    @Test public void testSetCriteria2() throws Exception {
        helpTestSetCriteria(1, false, false);
    }

    @Test public void testSetCriteria3() throws Exception {
        helpTestSetCriteria(1000, true, false);
    }

    @Test public void testSetCriteria4() throws Exception {
        helpTestSetCriteria(1, true, true);
    }

    @Test public void testSetCriteria5() throws Exception {
        helpTestSetCriteria(null, true, false);
    }

    @Test public void testSetCriteria6() throws Exception {
        helpTestSetCriteria(null, false, false);
    }

    @Test public void testExistsCriteria() throws Exception {
        ExistsCriteria crit = new ExistsCriteria(new Query());
        ArrayList values = new ArrayList();
        values.add("a"); //$NON-NLS-1$
        values.add("b"); //$NON-NLS-1$
        values.add("c"); //$NON-NLS-1$
        helpTestCompareSubqueryCriteria(crit, true, values);
    }

    @Test public void testExistsCriteria2() throws Exception {
        ExistsCriteria crit = new ExistsCriteria(new Query());
        helpTestCompareSubqueryCriteria(crit, false, Collections.emptyList());
    }

    /**
     * If rows are returned but they contain null, the result should
     * still be true.
     */
    @Test public void testExistsCriteria3() throws Exception {
        ExistsCriteria crit = new ExistsCriteria(new Query());
        ArrayList values = new ArrayList();
        values.add(null);
        values.add(null);
        values.add(null);
        helpTestCompareSubqueryCriteria(crit, true, values);
    }

    /**
     * Special case: if ALL is specified and the subquery returns no rows,
     * the result is true.
     */
    @Test public void testCompareSubqueryCriteriaNoRows() throws Exception {
        SubqueryCompareCriteria crit = helpGetCompareSubqueryCriteria(SubqueryCompareCriteria.EQ, SubqueryCompareCriteria.ALL);
        helpTestCompareSubqueryCriteria(crit, true, Collections.emptyList());
    }

    /**
     * The check for empty rows should happen before the check for a null left expression
     * @throws Exception
     */
    @Test public void testCompareSubqueryCriteriaNoRows1() throws Exception {
        SubqueryCompareCriteria crit = helpGetCompareSubqueryCriteria(SubqueryCompareCriteria.EQ, SubqueryCompareCriteria.ANY);
        crit.setLeftExpression(new Constant(null));
        crit.negate();
        helpTestCompareSubqueryCriteria(crit, true, Collections.emptyList());
    }

    /**
     * Special case: if ANY/SOME is specified and the subquery returns no rows,
     * the result is false.
     */
    @Test public void testCompareSubqueryCriteriaNoRows2() throws Exception {
        SubqueryCompareCriteria crit = helpGetCompareSubqueryCriteria(SubqueryCompareCriteria.EQ, SubqueryCompareCriteria.SOME);
        helpTestCompareSubqueryCriteria(crit, false, Collections.emptyList());
    }

    @Test public void testCompareSubqueryCriteria2() throws Exception {
        SubqueryCompareCriteria crit = helpGetCompareSubqueryCriteria(SubqueryCompareCriteria.EQ, SubqueryCompareCriteria.ALL);
        ArrayList values = new ArrayList();
        values.add("a"); //$NON-NLS-1$
        values.add("b"); //$NON-NLS-1$
        values.add("c"); //$NON-NLS-1$
        helpTestCompareSubqueryCriteria(crit, false, values);
    }

    @Test public void testCompareSubqueryCriteria3() throws Exception {
        SubqueryCompareCriteria crit = helpGetCompareSubqueryCriteria(SubqueryCompareCriteria.EQ, SubqueryCompareCriteria.SOME);
        ArrayList values = new ArrayList();
        values.add("a"); //$NON-NLS-1$
        values.add("b"); //$NON-NLS-1$
        values.add("c"); //$NON-NLS-1$
        helpTestCompareSubqueryCriteria(crit, true, values);
    }

    @Test public void testCompareSubqueryCriteria4() throws Exception {
        SubqueryCompareCriteria crit = helpGetCompareSubqueryCriteria(SubqueryCompareCriteria.EQ, SubqueryCompareCriteria.SOME);
        ArrayList values = new ArrayList();
        values.add("b"); //$NON-NLS-1$
        values.add("c"); //$NON-NLS-1$
        helpTestCompareSubqueryCriteria(crit, false, values);
    }

    @Test public void testCompareSubqueryCriteria5() throws Exception {
        SubqueryCompareCriteria crit = helpGetCompareSubqueryCriteria(SubqueryCompareCriteria.EQ, SubqueryCompareCriteria.SOME);
        ArrayList values = new ArrayList();
        values.add("a"); //$NON-NLS-1$
        values.add("b"); //$NON-NLS-1$
        values.add("c"); //$NON-NLS-1$
        helpTestCompareSubqueryCriteria(crit, true, values);
    }

    @Test public void testCompareSubqueryCriteriaNulls3() throws Exception {
        SubqueryCompareCriteria crit = helpGetCompareSubqueryCriteria(SubqueryCompareCriteria.EQ, SubqueryCompareCriteria.ALL);
        ArrayList values = new ArrayList();
        values.add(null);
        values.add(null);
        helpTestCompareSubqueryCriteria(crit, null, values);
    }

    @Test public void testCompareSubqueryCriteriaNulls4() throws Exception {
        SubqueryCompareCriteria crit = helpGetCompareSubqueryCriteria(SubqueryCompareCriteria.EQ, SubqueryCompareCriteria.SOME);
        ArrayList values = new ArrayList();
        values.add(null);
        values.add(null);
        helpTestCompareSubqueryCriteria(crit, null, values);
    }

    @Test public void testCompareSubqueryCriteriaNulls5() throws Exception {
        SubqueryCompareCriteria crit = helpGetCompareSubqueryCriteria(SubqueryCompareCriteria.EQ, SubqueryCompareCriteria.SOME);
        ArrayList values = new ArrayList();
        values.add(null);
        values.add("a"); //$NON-NLS-1$
        values.add(null);
        helpTestCompareSubqueryCriteria(crit, true, values);
    }

    @Test public void testCompareSubqueryCriteriaNulls6() throws Exception {
        SubqueryCompareCriteria crit = helpGetCompareSubqueryCriteria(SubqueryCompareCriteria.EQ, SubqueryCompareCriteria.SOME);
        ArrayList values = new ArrayList();
        values.add("a"); //$NON-NLS-1$
        values.add(null);
        values.add("a"); //$NON-NLS-1$
        helpTestCompareSubqueryCriteria(crit, true, values);
    }

    /**
     * null is unknown
     */
    @Test public void testCompareSubqueryCriteriaNulls7() throws Exception{
        SubqueryCompareCriteria crit = helpGetCompareSubqueryCriteria(SubqueryCompareCriteria.LT, SubqueryCompareCriteria.ALL);
        ArrayList values = new ArrayList();
        values.add(null);
        values.add(null);
        helpTestCompareSubqueryCriteria(crit, null, values);
    }

    /**
     * null is unknown
     */
    @Test public void testCompareSubqueryCriteriaNulls8() throws Exception {
        SubqueryCompareCriteria crit = helpGetCompareSubqueryCriteria(SubqueryCompareCriteria.GT, SubqueryCompareCriteria.ALL);
        ArrayList values = new ArrayList();
        values.add(null);
        values.add(null);
        helpTestCompareSubqueryCriteria(crit, null, values);
    }

    @Test public void testCompareSubqueryCriteriaNull9() throws Exception {
        SubqueryCompareCriteria crit = helpGetCompareSubqueryCriteria(SubqueryCompareCriteria.LT, SubqueryCompareCriteria.ALL);
        ArrayList values = new ArrayList();
        values.add(null);
        values.add("b");
        helpTestCompareSubqueryCriteria(crit, null, values);
    }

    @Test public void testCompareSubqueryCriteriaNull10() throws Exception {
        SubqueryCompareCriteria crit = helpGetCompareSubqueryCriteria(SubqueryCompareCriteria.LT, SubqueryCompareCriteria.ALL);
        ArrayList values = new ArrayList();
        values.add("b");
        values.add(null);
        helpTestCompareSubqueryCriteria(crit, null, values);
    }

    @Test public void testCompareSubqueryCriteriaNull11() throws Exception {
        SubqueryCompareCriteria crit = helpGetCompareSubqueryCriteria(SubqueryCompareCriteria.GT, SubqueryCompareCriteria.SOME);
        ArrayList values = new ArrayList();
        values.add(null);
        values.add("b");
        helpTestCompareSubqueryCriteria(crit, null, values);
    }

    @Test public void testCompareSubqueryCriteriaNull12() throws Exception {
        SubqueryCompareCriteria crit = helpGetCompareSubqueryCriteria(SubqueryCompareCriteria.GT, SubqueryCompareCriteria.SOME);
        ArrayList values = new ArrayList();
        values.add("b");
        values.add(null);
        helpTestCompareSubqueryCriteria(crit, null, values);
    }

    /**
     * Big decimal comparisons should ignore precision.
     */
    @Test public void testBigDecimalEquality() throws Exception {
        CompareCriteria crit = new CompareCriteria(new Constant(new BigDecimal("3.10")), CompareCriteria.EQ, new Constant(new BigDecimal("3.1"))); //$NON-NLS-1$ //$NON-NLS-2$
        assertTrue(Evaluator.evaluate(crit));
    }

}
