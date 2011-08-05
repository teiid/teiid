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

package org.teiid.query.processor.eval;

import static org.junit.Assert.*;

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.junit.Test;
import org.teiid.api.exception.query.ExpressionEvaluationException;
import org.teiid.common.buffer.BlockedException;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidException;
import org.teiid.core.TeiidProcessingException;
import org.teiid.core.types.DataTypeManager;
import org.teiid.query.eval.Evaluator;
import org.teiid.query.function.FunctionDescriptor;
import org.teiid.query.processor.FakeDataManager;
import org.teiid.query.processor.ProcessorDataManager;
import org.teiid.query.resolver.TestFunctionResolving;
import org.teiid.query.sql.lang.CollectionValueIterator;
import org.teiid.query.sql.lang.IsNullCriteria;
import org.teiid.query.sql.lang.Query;
import org.teiid.query.sql.lang.SubqueryContainer;
import org.teiid.query.sql.symbol.CaseExpression;
import org.teiid.query.sql.symbol.Constant;
import org.teiid.query.sql.symbol.ElementSymbol;
import org.teiid.query.sql.symbol.Expression;
import org.teiid.query.sql.symbol.Function;
import org.teiid.query.sql.symbol.ScalarSubquery;
import org.teiid.query.sql.symbol.SearchedCaseExpression;
import org.teiid.query.sql.symbol.SingleElementSymbol;
import org.teiid.query.sql.symbol.TestCaseExpression;
import org.teiid.query.sql.symbol.TestSearchedCaseExpression;
import org.teiid.query.sql.util.ValueIterator;
import org.teiid.query.unittest.RealMetadataFactory;
import org.teiid.query.util.CommandContext;

@SuppressWarnings("nls")
public class TestExpressionEvaluator {

    public void helpTestEval(Expression expr, SingleElementSymbol[] elementList, Object[] valueList, ProcessorDataManager dataMgr, CommandContext context, Object expectedValue) {
        try {
            Object actualValue = helpEval(expr, elementList, valueList, dataMgr, context);
            assertEquals("Did not get expected result", expectedValue, actualValue); //$NON-NLS-1$
        } catch(TeiidException e) {
            fail("Received unexpected exception: " + e.getFullMessage()); //$NON-NLS-1$
        }
    }

    public Object helpEval(Expression expr, SingleElementSymbol[] elementList, Object[] valueList, ProcessorDataManager dataMgr, CommandContext context) throws ExpressionEvaluationException, BlockedException, TeiidComponentException {
        Map elements = new HashMap();
        if (elementList != null) {
            for(int i=0; i<elementList.length; i++) {
                elements.put(elementList[i], new Integer(i));
            }
        }
        
        List tuple = new ArrayList();
        if (valueList != null) {
            for(int i=0; i<valueList.length; i++) {
                tuple.add(valueList[i]);
            }
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
                     new SingleElementSymbol[] {new ElementSymbol("x")}, //$NON-NLS-1$
                     new Object[] {new Integer(0)},
                     null,
                     null,
                     new Integer(0));
        helpTestEval(expr,
                     new SingleElementSymbol[] {new ElementSymbol("x")}, //$NON-NLS-1$
                     new Object[] {new Integer(1)},
                     null,
                     null,
                     new Integer(1));
        helpTestEval(expr,
                     new SingleElementSymbol[] {new ElementSymbol("x")}, //$NON-NLS-1$
                     new Object[] {new Integer(2)},
                     null,
                     null,
                     new Integer(2));
        helpTestEval(expr,
                     new SingleElementSymbol[] {new ElementSymbol("x")}, //$NON-NLS-1$
                     new Object[] {new Integer(3)},
                     null,
                     null,
                     new Integer(9999));
    }
    
    @Test public void testConstant() {
        helpTestEval(new Constant("xyz", String.class), new SingleElementSymbol[0], new Object[0], null, null, "xyz"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testElement1() {
        ElementSymbol e1 = new ElementSymbol("e1"); //$NON-NLS-1$
        ElementSymbol e2 = new ElementSymbol("e2"); //$NON-NLS-1$
        
        SingleElementSymbol[] elements = new SingleElementSymbol[] {
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
        
        SingleElementSymbol[] elements = new SingleElementSymbol[] {
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
        
        SingleElementSymbol[] elements = new SingleElementSymbol[] {};
        
        Object[] values = new Object[] { 
            "xyz", "abc" //$NON-NLS-1$ //$NON-NLS-2$
        };

        try {
            helpEval(e2, elements, values, null, null); 
            fail("Exception expected"); //$NON-NLS-1$
        } catch (TeiidComponentException e){
        	//this should be a componentexception, since it is unexpected
            assertEquals(e.getMessage(), "Error Code:ERR.015.006.0033 Message:Unable to evaluate e2: No value was available"); //$NON-NLS-1$
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

        SingleElementSymbol[] elements = new SingleElementSymbol[] {
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

        SingleElementSymbol[] elements = new SingleElementSymbol[] {
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

        SingleElementSymbol[] elements = new SingleElementSymbol[] {
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
        ScalarSubquery expr = new ScalarSubquery(new Query());
        ArrayList values = new ArrayList(2);
        values.add("a"); //$NON-NLS-1$
        values.add("b"); //$NON-NLS-1$
        
        try {
        	helpTestWithValueIterator(expr, values, null);
            fail("Expected ExpressionEvaluationException but got none"); //$NON-NLS-1$
        } catch (ExpressionEvaluationException e) {
            assertEquals("Error Code:ERR.015.006.0058 Message:Unable to evaluate (<undefined>): Error Code:ERR.015.006.0058 Message:The command of this scalar subquery returned more than one value: <undefined>", e.getMessage()); //$NON-NLS-1$
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
    
    /*
     * This is the only test that depends upon the the testsrc/metamatrix.properties
     * and testsrc/config.xml. If the implementation is changed, please update/remove
     * these files.
     * @throws Exception
     */
    @Test public void testEnv() throws Exception {
        Function func = new Function("env", new Expression[] {}); //$NON-NLS-1$
        FunctionDescriptor desc = RealMetadataFactory.SFM.getSystemFunctionLibrary().findFunction("env", new Class[] {String.class} );         //$NON-NLS-1$
        func.setFunctionDescriptor(desc);
        
        FakeDataManager dataMgr = new FakeDataManager();
        
        Properties props = new Properties();
        props.setProperty("http_host", "testHostName"); //$NON-NLS-1$ //$NON-NLS-2$
        props.setProperty("http_port", "8000"); //$NON-NLS-1$ //$NON-NLS-2$
        CommandContext context = new CommandContext(new Long(1), null, null, null, null, 0, props, false);
        
        func.setArgs(new Expression[] {new Constant("http_host")}); //$NON-NLS-1$
        assertEquals("testHostName", new Evaluator(Collections.emptyMap(), dataMgr, context).evaluate(func, Collections.emptyList())); //$NON-NLS-1$
               
        func.setArgs(new Expression[] {new Constant("http_port")}); //$NON-NLS-1$
        assertEquals("8000", new Evaluator(Collections.emptyMap(), dataMgr, context).evaluate(func, Collections.emptyList())); //$NON-NLS-1$
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
        CommandContext context = new CommandContext(new Long(-1), null, "user", payload, "vdb", 1, null, false); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ 

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
    
}
