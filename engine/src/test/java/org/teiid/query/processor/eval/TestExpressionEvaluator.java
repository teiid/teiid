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
import static org.teiid.query.resolver.TestFunctionResolving.*;

import java.io.Serializable;
import java.math.BigDecimal;
import java.sql.Clob;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TimeZone;

import org.junit.Test;
import org.teiid.api.exception.query.ExpressionEvaluationException;
import org.teiid.common.buffer.BlockedException;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidException;
import org.teiid.core.TeiidProcessingException;
import org.teiid.core.types.DataTypeManager;
import org.teiid.core.util.TimestampWithTimezone;
import org.teiid.query.eval.Evaluator;
import org.teiid.query.function.FunctionDescriptor;
import org.teiid.query.parser.QueryParser;
import org.teiid.query.processor.FakeDataManager;
import org.teiid.query.processor.ProcessorDataManager;
import org.teiid.query.resolver.TestFunctionResolving;
import org.teiid.query.sql.lang.CollectionValueIterator;
import org.teiid.query.sql.lang.CompareCriteria;
import org.teiid.query.sql.lang.IsNullCriteria;
import org.teiid.query.sql.lang.Query;
import org.teiid.query.sql.lang.QueryCommand;
import org.teiid.query.sql.lang.SubqueryContainer;
import org.teiid.query.sql.symbol.*;
import org.teiid.query.sql.util.ValueIterator;
import org.teiid.query.unittest.RealMetadataFactory;
import org.teiid.query.util.CommandContext;

@SuppressWarnings("nls")
public class TestExpressionEvaluator {

    public void helpTestEval(Expression expr, Expression[] elementList, Object[] valueList, ProcessorDataManager dataMgr, CommandContext context, Object expectedValue) {
        try {
            Object actualValue = helpEval(expr, elementList, valueList, dataMgr, context);
            assertEquals("Did not get expected result", expectedValue, actualValue); //$NON-NLS-1$
        } catch(TeiidException e) {
            throw new RuntimeException(e);
        }
    }

    public Object helpEval(Expression expr, Expression[] elementList, Object[] valueList, ProcessorDataManager dataMgr, CommandContext context) throws ExpressionEvaluationException, BlockedException, TeiidComponentException {
        Map<Expression, Integer> elements = new HashMap<Expression, Integer>();
        if (elementList != null) {
            for(int i=0; i<elementList.length; i++) {
                elements.put(elementList[i], i);
            }
        }
        List<Object> tuple = null;
        if (valueList != null) {
            tuple = Arrays.asList(valueList);
        }
        return new Evaluator(elements, dataMgr, context).evaluate(expr, tuple);
    }

    @Test public void testCaseExpression1() {
        CaseExpression expr = TestCaseExpression.example(3);
        expr.setExpression(new Constant("a")); //$NON-NLS-1$
        helpTestEval(expr, null, null, null, null, new Integer(0));
        expr.setExpression(new Constant("b")); //$NON-NLS-1$
        helpTestEval(expr, null, null, null, null, new Integer(1));
        expr.setExpression(new Constant("c")); //$NON-NLS-1$
        helpTestEval(expr, null, null, null, null, new Integer(2));
        expr.setExpression(new Constant("d")); //$NON-NLS-1$
        helpTestEval(expr, null, null, null, null, new Integer(9999));
    }

    @Test public void testSearchedCaseExpression1() {
        SearchedCaseExpression expr = TestSearchedCaseExpression.example(3);
        helpTestEval(expr,
                     new Expression[] {new ElementSymbol("x")}, //$NON-NLS-1$
                     new Object[] {new Integer(0)},
                     null,
                     null,
                     new Integer(0));
        helpTestEval(expr,
                     new Expression[] {new ElementSymbol("x")}, //$NON-NLS-1$
                     new Object[] {new Integer(1)},
                     null,
                     null,
                     new Integer(1));
        helpTestEval(expr,
                     new Expression[] {new ElementSymbol("x")}, //$NON-NLS-1$
                     new Object[] {new Integer(2)},
                     null,
                     null,
                     new Integer(2));
        helpTestEval(expr,
                     new Expression[] {new ElementSymbol("x")}, //$NON-NLS-1$
                     new Object[] {new Integer(3)},
                     null,
                     null,
                     new Integer(9999));
    }

    @Test public void testConstant() {
        helpTestEval(new Constant("xyz", String.class), new Expression[0], new Object[0], null, null, "xyz"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testElement1() {
        ElementSymbol e1 = new ElementSymbol("e1"); //$NON-NLS-1$
        ElementSymbol e2 = new ElementSymbol("e2"); //$NON-NLS-1$

        Expression[] elements = new Expression[] {
            e1, e2
        };

        Object[] values = new Object[] {
            "xyz", "abc" //$NON-NLS-1$ //$NON-NLS-2$
        };

        helpTestEval(e1, elements, values, null, null, "xyz"); //$NON-NLS-1$
    }

    @Test public void testElement2() {
        ElementSymbol e1 = new ElementSymbol("e1"); //$NON-NLS-1$
        ElementSymbol e2 = new ElementSymbol("e2"); //$NON-NLS-1$

        Expression[] elements = new Expression[] {
            e1, e2
        };

        Object[] values = new Object[] {
            "xyz", "abc" //$NON-NLS-1$ //$NON-NLS-2$
        };

        helpTestEval(e2, elements, values, null, null, "abc"); //$NON-NLS-1$
    }

    /**
     * Element Symbols must have values set during evaluation
     */
    @Test public void testElement3() throws Exception {
        ElementSymbol e2 = new ElementSymbol("e2"); //$NON-NLS-1$

        Expression[] elements = new Expression[] {};

        Object[] values = new Object[] {
            "xyz", "abc" //$NON-NLS-1$ //$NON-NLS-2$
        };

        try {
            helpEval(e2, elements, values, null, null);
            fail("Exception expected"); //$NON-NLS-1$
        } catch (TeiidComponentException e){
            //this should be a componentexception, since it is unexpected
            assertEquals("TEIID30328 Unable to evaluate e2: No value was available", e.getMessage()); //$NON-NLS-1$
        }
    }

    @Test public void testFunction1() {
        ElementSymbol e1 = new ElementSymbol("e1"); //$NON-NLS-1$
        e1.setType(String.class);
        ElementSymbol e2 = new ElementSymbol("e2"); //$NON-NLS-1$
        e2.setType(String.class);

        Function func = new Function("concat", new Expression[] { e1, e2 }); //$NON-NLS-1$
        FunctionDescriptor desc = RealMetadataFactory.SFM.getSystemFunctionLibrary().findFunction("concat", new Class[] { String.class, String.class } ); //$NON-NLS-1$
        func.setFunctionDescriptor(desc);

        Expression[] elements = new Expression[] {
            e1, e2
        };

        Object[] values = new Object[] {
            "xyz", "abc" //$NON-NLS-1$ //$NON-NLS-2$
        };


        helpTestEval(func, elements, values, null, null, "xyzabc"); //$NON-NLS-1$
    }

    @Test public void testFunction2() {
        ElementSymbol e1 = new ElementSymbol("e1"); //$NON-NLS-1$
        e1.setType(String.class);
        ElementSymbol e2 = new ElementSymbol("e2"); //$NON-NLS-1$
        e2.setType(String.class);

        Function func = new Function("concat", new Expression[] { e2, e1 }); //$NON-NLS-1$
        FunctionDescriptor desc = RealMetadataFactory.SFM.getSystemFunctionLibrary().findFunction("concat", new Class[] { String.class, String.class } ); //$NON-NLS-1$
        func.setFunctionDescriptor(desc);

        Expression[] elements = new Expression[] {
            e1, e2
        };

        Object[] values = new Object[] {
            "xyz", "abc" //$NON-NLS-1$ //$NON-NLS-2$
        };

        helpTestEval(func, elements, values, null, null, "abcxyz"); //$NON-NLS-1$
    }

    @Test public void testLookupFunction() {
        ElementSymbol e1 = new ElementSymbol("e1"); //$NON-NLS-1$
        e1.setType(String.class);
        ElementSymbol e2 = new ElementSymbol("e2"); //$NON-NLS-1$
        e1.setType(Integer.class);

        Function func = new Function("lookup", new Expression[] { new Constant("pm1.g1"), new Constant("e2"), new Constant("e1"), e1 }); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
        FunctionDescriptor desc = RealMetadataFactory.SFM.getSystemFunctionLibrary().findFunction("lookup", new Class[] { String.class, String.class, String.class, String.class } ); //$NON-NLS-1$
        func.setFunctionDescriptor(desc);

        Expression[] elements = new Expression[] {
            e1, e2
        };

        Object[] values = new Object[] {
            "xyz", new Integer(5) //$NON-NLS-1$
        };

        FakeDataManager dataMgr = new FakeDataManager();
        Map valueMap = new HashMap();
        valueMap.put("xyz", new Integer(5)); //$NON-NLS-1$
        dataMgr.defineCodeTable("pm1.g1", "e1", "e2", valueMap); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$

        helpTestEval(func, elements, values, dataMgr, null, new Integer(5));
    }

    @Test public void testScalarSubquery() throws Exception{
        ScalarSubquery expr = new ScalarSubquery(new Query());
        ArrayList values = new ArrayList(1);
        values.add("a"); //$NON-NLS-1$
        Object expected = "a"; //$NON-NLS-1$
        helpTestWithValueIterator(expr, values, expected);
    }

    private void helpTestWithValueIterator(ScalarSubquery expr,
            List<?> values, Object expected)
            throws BlockedException,
            TeiidComponentException, ExpressionEvaluationException {
        final CollectionValueIterator valueIter = new CollectionValueIterator(values);
        CommandContext cc = new CommandContext();
        assertEquals(expected, new Evaluator(Collections.emptyMap(), null, cc) {
            @Override
            protected ValueIterator evaluateSubquery(
                    SubqueryContainer container, List tuple)
                    throws TeiidProcessingException, BlockedException,
                    TeiidComponentException {
                return valueIter;
            }
        }.evaluate(expr, null) );
    }

    @Test public void testScalarSubquery2() throws Exception{
        ScalarSubquery expr = new ScalarSubquery(new Query());
        ArrayList values = new ArrayList(1);
        values.add(null);
        helpTestWithValueIterator(expr, values, null);
    }

    @Test public void testScalarSubquery3() throws Exception{
        ScalarSubquery expr = new ScalarSubquery(new Query());
        helpTestWithValueIterator(expr, Collections.emptyList(), null);
    }

    @Test public void testScalarSubqueryFails() throws Exception{
        ScalarSubquery expr = new ScalarSubquery((QueryCommand) QueryParser.getQueryParser().parseCommand("select x from y"));
        ArrayList values = new ArrayList(2);
        values.add("a"); //$NON-NLS-1$
        values.add("b"); //$NON-NLS-1$

        try {
            helpTestWithValueIterator(expr, values, null);
            fail("Expected ExpressionEvaluationException but got none"); //$NON-NLS-1$
        } catch (ExpressionEvaluationException e) {
            assertEquals("TEIID30328 Unable to evaluate (SELECT x FROM y): TEIID30345 The command of this scalar subquery returned more than one value: SELECT x FROM y", e.getMessage()); //$NON-NLS-1$
        }
    }

    @Test public void testUser() throws Exception {
        Function func = new Function("user", new Expression[] {}); //$NON-NLS-1$
        FunctionDescriptor desc = RealMetadataFactory.SFM.getSystemFunctionLibrary().findFunction("user", new Class[] {} );         //$NON-NLS-1$
        func.setFunctionDescriptor(desc);

        FakeDataManager dataMgr = new FakeDataManager();
        CommandContext context = new CommandContext(new Long(1), null, null, null, 0);
        context.setUserName("logon");  //$NON-NLS-1$
        assertEquals(context.getUserName(), new Evaluator(Collections.emptyMap(), dataMgr, context).evaluate(func, Collections.emptyList()) );
    }

    public void helpTestCommandPayload(Serializable payload, String property, String expectedValue) throws Exception {
        Function func = new Function("commandpayload", new Expression[] {}); //$NON-NLS-1$

        Class[] parameterSignature = null;
        if(property == null) {
            parameterSignature = new Class[] {};
        } else {
            parameterSignature = new Class[] { String.class };
        }
        FunctionDescriptor desc = RealMetadataFactory.SFM.getSystemFunctionLibrary().findFunction("commandpayload", parameterSignature );         //$NON-NLS-1$
        func.setFunctionDescriptor(desc);

        FakeDataManager dataMgr = new FakeDataManager();
        CommandContext context = new CommandContext(null, "user", payload, "vdb", 1, false); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$

        if(property != null) {
            func.setArgs(new Expression[] {new Constant(property)});
        }
        String actual = (String) new Evaluator(Collections.emptyMap(), dataMgr, context).evaluate(func, Collections.emptyList());
        assertEquals(expectedValue, actual);
    }

    @Test public void testCommandPayloadNoArgsWithPayload() throws Exception {
        helpTestCommandPayload("blah", null, "blah"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testCommandPayloadNoArgsWithoutPayload() throws Exception {
        helpTestCommandPayload(null, null, null);
    }

    @Test public void testCommandPayloadNoArgsWithNonStringPayload() throws Exception {
        helpTestCommandPayload(Boolean.TRUE, null, "true"); //$NON-NLS-1$
    }

    @Test public void testCommandPayloadArgWithPayload() throws Exception {
        Properties props = new Properties();
        props.setProperty("p1", "v1"); //$NON-NLS-1$ //$NON-NLS-2$
        props.setProperty("p2", "v2"); //$NON-NLS-1$ //$NON-NLS-2$
        helpTestCommandPayload(props, "p1", "v1"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testCommandPayloadArgWithPayloadMissingProp() throws Exception {
        Properties props = new Properties();
        props.setProperty("p1", "v1"); //$NON-NLS-1$ //$NON-NLS-2$
        props.setProperty("p2", "v2"); //$NON-NLS-1$ //$NON-NLS-2$
        helpTestCommandPayload(props, "BOGUS", null); //$NON-NLS-1$
    }

    @Test public void testCommandPayloadArgWithoutPayload() throws Exception {
        Properties props = new Properties();
        props.setProperty("p1", "v1"); //$NON-NLS-1$ //$NON-NLS-2$
        props.setProperty("p2", "v2"); //$NON-NLS-1$ //$NON-NLS-2$
        helpTestCommandPayload(null, "BOGUS", null); //$NON-NLS-1$
    }

    @Test(expected=ExpressionEvaluationException.class) public void testCommandPayloadArgWithBadPayload() throws Exception {
        helpTestCommandPayload(Boolean.TRUE, "BOGUS", null); //$NON-NLS-1$
    }

    @Test public void testBigDecimalFromDoubleDivision() throws Exception {
        Expression ex = TestFunctionResolving.getExpression("convert(1.0, bigdecimal)/3");
        assertEquals(new BigDecimal("0.3333333333333333"), Evaluator.evaluate(ex));
    }

    @Test public void testBigDecimalDivision() throws Exception {
        Expression ex = TestFunctionResolving.getExpression("1/convert('3.0', bigdecimal)");
        assertEquals(new BigDecimal("0.3333333333333333"), Evaluator.evaluate(ex));
    }

    @Test public void testIsNull() throws Exception {
        assertEquals(Boolean.TRUE, Evaluator.evaluate(new IsNullCriteria(new Constant(null, DataTypeManager.DefaultDataClasses.BOOLEAN))));
    }

    @Test public void testIsNull1() throws Exception {
        assertEquals(Boolean.FALSE, Evaluator.evaluate(new IsNullCriteria(new Constant(Boolean.TRUE, DataTypeManager.DefaultDataClasses.BOOLEAN))));
    }

    @Test public void testIsNull3() throws Exception {
        IsNullCriteria inc = new IsNullCriteria(new Constant(null, DataTypeManager.DefaultDataClasses.BOOLEAN));
        inc.setNegated(true);
        assertEquals(Boolean.FALSE, Evaluator.evaluate(inc));
    }

    @Test public void testIsNull4() throws Exception {
        IsNullCriteria inc = new IsNullCriteria(new Constant(Boolean.TRUE, DataTypeManager.DefaultDataClasses.BOOLEAN));
        inc.setNegated(true);
        assertEquals(Boolean.TRUE, Evaluator.evaluate(inc));
    }

    @Test public void testSubstring() throws Exception {
        Expression ex = TestFunctionResolving.getExpression("substring('abcd' from 2 for 2)");
        assertEquals("bc", Evaluator.evaluate(ex));
    }

    @Test public void testExtract() throws Exception {
        Expression ex = TestFunctionResolving.getExpression("extract(year from cast('2011-01-01' as date))");
        assertEquals(2011, Evaluator.evaluate(ex));
    }

    @Test public void testExtract1() throws Exception {
        Expression ex = TestFunctionResolving.getExpression("extract(day from cast('2011-01-01' as date))");
        assertEquals(1, Evaluator.evaluate(ex));
    }

    @Test public void testExtract2() throws Exception {
        Expression ex = TestFunctionResolving.getExpression("extract(quarter from cast('2011-04-01' as date))");
        assertEquals(2, Evaluator.evaluate(ex));
    }

    @Test public void testExtract3() throws Exception {
        TimestampWithTimezone.resetCalendar(TimeZone.getTimeZone("America/New_York")); //$NON-NLS-1$
        try {
            Expression ex = TestFunctionResolving.getExpression("extract(epoch from cast('2011-04-01 11:11:11.1234567' as timestamp))");
            assertEquals(DataTypeManager.DefaultDataClasses.DOUBLE, ex.getType());
            assertEquals(1301670671.123456, Evaluator.evaluate(ex));
        } finally {
            TimestampWithTimezone.resetCalendar(null);
        }
    }

    @Test public void testExtractDOW() throws Exception {
        TimestampWithTimezone.resetCalendar(TimeZone.getTimeZone("America/New_York")); //$NON-NLS-1$
        try {
            Expression ex = TestFunctionResolving.getExpression("extract(dow from cast('2011-04-01 11:11:11.1234567' as timestamp))");
            assertEquals(6, Evaluator.evaluate(ex));
        } finally {
            TimestampWithTimezone.resetCalendar(null);
        }
    }

    @Test public void testExtractDOY() throws Exception {
        TimestampWithTimezone.resetCalendar(TimeZone.getTimeZone("America/New_York")); //$NON-NLS-1$
        try {
            Expression ex = TestFunctionResolving.getExpression("extract(doy from cast('2011-04-01 11:11:11.1234567' as timestamp))");
            assertEquals(91, Evaluator.evaluate(ex));
        } finally {
            TimestampWithTimezone.resetCalendar(null);
        }
    }

    @Test public void testSimilarTo() throws Exception {
        Expression ex = TestFunctionResolving.getExpression("'aaaxy' similar to 'a+%'");
        assertEquals(Boolean.TRUE, Evaluator.evaluate(ex));
    }

    @Test public void testSimilarTo1() throws Exception {
        Expression ex = TestFunctionResolving.getExpression("'xaay' similar to 'xa{2,3}y'");
        assertEquals(Boolean.TRUE, Evaluator.evaluate(ex));
    }

    @Test public void testSimilarTo2() throws Exception {
        Expression ex = TestFunctionResolving.getExpression("'xay' similar to 'xa{2,3}y'");
        assertEquals(Boolean.FALSE, Evaluator.evaluate(ex));
    }

    @Test(expected=ExpressionEvaluationException.class) public void testSimilarTo3() throws Exception {
        Expression ex = TestFunctionResolving.getExpression("'xay' similar to '{'");
        assertEquals(Boolean.FALSE, Evaluator.evaluate(ex));
    }

    @Test public void testSimilarTo4() throws Exception {
        Expression ex = TestFunctionResolving.getExpression("'xay' similar to 'xa{2,}y'");
        assertEquals(Boolean.FALSE, Evaluator.evaluate(ex));
    }

    @Test public void testSimilarTo5() throws Exception {
        Expression ex = TestFunctionResolving.getExpression("'x1y' similar to 'x([a-z]+|[0-9])_'");
        assertEquals(Boolean.TRUE, Evaluator.evaluate(ex));
    }

    @Test public void testSimilarTo6() throws Exception {
        Expression ex = TestFunctionResolving.getExpression("'xx' similar to 'x([a-z]+|[0-9])_'");
        assertEquals(Boolean.FALSE, Evaluator.evaluate(ex));
    }

    @Test public void testLikeRegex() throws Exception {
        Expression ex = TestFunctionResolving.getExpression("'aaaxy' like_regex 'a+.*'");
        assertEquals(Boolean.TRUE, Evaluator.evaluate(ex));
    }

    @Test public void testLikeRegex1() throws Exception {
        Expression ex = TestFunctionResolving.getExpression("'xaay' similar to 'xa{2,3}y'");
        assertEquals(Boolean.TRUE, Evaluator.evaluate(ex));
    }

    @Test public void testLikeRegex2() throws Exception {
        Expression ex = TestFunctionResolving.getExpression("'xay' like_regex 'xa{2,3}y'");
        assertEquals(Boolean.FALSE, Evaluator.evaluate(ex));
    }

    @Test(expected=ExpressionEvaluationException.class) public void testLikeRegex3() throws Exception {
        Expression ex = TestFunctionResolving.getExpression("'xay' like_regex '{'");
        assertEquals(Boolean.FALSE, Evaluator.evaluate(ex));
    }

    @Test public void testLikeRegex4() throws Exception {
        Expression ex = TestFunctionResolving.getExpression("'xay' like_regex 'a'");
        assertEquals(Boolean.TRUE, Evaluator.evaluate(ex));
    }

    @Test public void testLikePlus() throws Exception {
        Expression ex = TestFunctionResolving.getExpression("'+' like '+'");
        assertEquals(Boolean.TRUE, Evaluator.evaluate(ex));
    }

    @Test public void testArrayEquality() throws Exception {
        assertEquals(Boolean.TRUE, Evaluator.evaluate(new CompareCriteria(new Array(DataTypeManager.DefaultDataClasses.INTEGER, Arrays.asList((Expression)new Constant(1))), CompareCriteria.EQ, new Array(DataTypeManager.DefaultDataClasses.INTEGER, Arrays.asList((Expression)new Constant(1))))));
        assertNull(new Evaluator(null, null, null).evaluateTVL(new CompareCriteria(new Array(DataTypeManager.DefaultDataClasses.INTEGER, Arrays.asList((Expression)new Constant(1))), CompareCriteria.EQ, new Array(DataTypeManager.DefaultDataClasses.INTEGER, Arrays.asList((Expression)new Constant(null)))), null));
    }

    @Test public void testToCharsBytesWellformed() throws Exception {
        Expression ex = TestFunctionResolving.getExpression("to_chars(to_bytes('abc', 'utf-8', false), 'utf-8', true)");
        assertEquals("abc", ((Clob)Evaluator.evaluate(ex)).getSubString(1, 3));

        try {
            ex = TestFunctionResolving.getExpression("to_bytes('\u00ff', 'ascii', false))");
            Evaluator.evaluate(ex);
            fail("expected exception");
        } catch (ExpressionEvaluationException e) {

        }

        TestFunctionResolving.getExpression("to_bytes('\u00ff', 'ascii', false))");
    }

    @Test public void testRegexpReplaceOkay() throws Exception {
        // Test replace-first vs replace-all.
        assertEval("regexp_replace('foobarbaz', 'b..', 'X')", "fooXbaz");
        assertEval("regexp_replace('foobarbaz', 'b..', 'X', 'g')", "fooXX");
        // Test replace-all with capture group.
        assertEval("regexp_replace('foobarbaz', 'b(..)', 'X$1Y', 'g')", "fooXarYXazY");
        // Test case-insensitive matching.
        assertEval("regexp_replace('fooBARbaz', 'a', 'X', 'g')", "fooBARbXz");
        assertEval("regexp_replace('fooBARbaz', 'a', 'X', 'gi')", "fooBXRbXz");
        // Test multiline.
        assertEval("regexp_replace('foo\nbar\nbaz', '(b[\\d\\w\\s]+?)$', 'X', 'g')", "foo\nX");
        assertEval("regexp_replace('foo\nbar\nbaz', '(b[\\d\\w\\s]+?)$', 'X', 'gm')", "foo\nX\nX");
    }

    @Test public void testTimestampResolving() throws Exception {
        assertEval("TIMESTAMPDIFF(SQL_TSI_YEAR, '2000-01-01', '2002-01-01')", "2");
    }

    @Test public void testEndsWith() throws Exception {
        // Test replace-first vs replace-all.
        assertEval("endsWith('c', 'abc') = 't'", "true");
    }

    @Test public void testIsDistinct() throws Exception {
        assertEval("'a' is distinct from 'b'", "true");
        assertEval("'a' is not distinct from 'b'", "false");
        assertEval("('a', null) is not distinct from ('a', null)", "true");
    }

}
