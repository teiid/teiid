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

package com.metamatrix.query.processor.eval;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import junit.framework.TestCase;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.api.exception.MetaMatrixException;
import com.metamatrix.api.exception.MetaMatrixProcessingException;
import com.metamatrix.api.exception.query.ExpressionEvaluationException;
import com.metamatrix.common.buffer.BlockedException;
import com.metamatrix.query.eval.Evaluator;
import com.metamatrix.query.function.FunctionDescriptor;
import com.metamatrix.query.function.FunctionLibraryManager;
import com.metamatrix.query.processor.FakeDataManager;
import com.metamatrix.query.processor.ProcessorDataManager;
import com.metamatrix.query.sql.ReservedWords;
import com.metamatrix.query.sql.lang.CollectionValueIterator;
import com.metamatrix.query.sql.lang.Query;
import com.metamatrix.query.sql.lang.SubqueryContainer;
import com.metamatrix.query.sql.symbol.AggregateSymbol;
import com.metamatrix.query.sql.symbol.CaseExpression;
import com.metamatrix.query.sql.symbol.Constant;
import com.metamatrix.query.sql.symbol.ContextReference;
import com.metamatrix.query.sql.symbol.ElementSymbol;
import com.metamatrix.query.sql.symbol.Expression;
import com.metamatrix.query.sql.symbol.Function;
import com.metamatrix.query.sql.symbol.ScalarSubquery;
import com.metamatrix.query.sql.symbol.SearchedCaseExpression;
import com.metamatrix.query.sql.symbol.SingleElementSymbol;
import com.metamatrix.query.sql.symbol.TestCaseExpression;
import com.metamatrix.query.sql.symbol.TestSearchedCaseExpression;
import com.metamatrix.query.sql.util.ValueIterator;
import com.metamatrix.query.sql.visitor.EvaluateExpressionVisitor;
import com.metamatrix.query.util.CommandContext;

/**
 */
public class TestExpressionEvaluator extends TestCase {

    /**
     * Constructor for TestExpressionEvaluator.
     * @param name
     */
    public TestExpressionEvaluator(String name) {
        super(name);
    }

    public void helpTestEval(Expression expr, SingleElementSymbol[] elementList, Object[] valueList, ProcessorDataManager dataMgr, CommandContext context, Object expectedValue) {
        try {
            Object actualValue = helpEval(expr, elementList, valueList, dataMgr, context, expectedValue);
            assertEquals("Did not get expected result", expectedValue, actualValue); //$NON-NLS-1$
        } catch(MetaMatrixException e) {
            fail("Received unexpected exception: " + e.getFullMessage()); //$NON-NLS-1$
        }
    }

    public Object helpEval(Expression expr, SingleElementSymbol[] elementList, Object[] valueList, ProcessorDataManager dataMgr, CommandContext context, Object expectedValue) throws ExpressionEvaluationException, BlockedException, MetaMatrixComponentException {
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
    
    public void testCaseExpression1() {
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
    
    public void testSearchedCaseExpression1() {
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
    
    public void testConstant() {
        helpTestEval(new Constant("xyz", String.class), new SingleElementSymbol[0], new Object[0], null, null, "xyz"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testElement1() {
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

    public void testElement2() {
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
    public void testElement3() throws Exception {
        ElementSymbol e2 = new ElementSymbol("e2"); //$NON-NLS-1$
        
        SingleElementSymbol[] elements = new SingleElementSymbol[] {};
        
        Object[] values = new Object[] { 
            "xyz", "abc" //$NON-NLS-1$ //$NON-NLS-2$
        };

        try {
            helpEval(e2, elements, values, null, null, null); 
            fail("Exception expected"); //$NON-NLS-1$
        } catch (MetaMatrixComponentException e){
        	//this should be a componentexception, since it is unexpected
            assertEquals(e.getMessage(), "Error Code:ERR.015.006.0033 Message:Unable to evaluate e2: No value was available"); //$NON-NLS-1$
        }
    }

    public void testFunction1() {
        ElementSymbol e1 = new ElementSymbol("e1"); //$NON-NLS-1$
        e1.setType(String.class);        
        ElementSymbol e2 = new ElementSymbol("e2"); //$NON-NLS-1$
        e2.setType(String.class);
        
        Function func = new Function("concat", new Expression[] { e1, e2 }); //$NON-NLS-1$
        FunctionDescriptor desc = FunctionLibraryManager.getFunctionLibrary().findFunction("concat", new Class[] { String.class, String.class } ); //$NON-NLS-1$
        func.setFunctionDescriptor(desc);

        SingleElementSymbol[] elements = new SingleElementSymbol[] {
            e1, e2
        };
        
        Object[] values = new Object[] { 
            "xyz", "abc" //$NON-NLS-1$ //$NON-NLS-2$
        };


        helpTestEval(func, elements, values, null, null, "xyzabc"); //$NON-NLS-1$
    }

    public void testFunction2() {
        ElementSymbol e1 = new ElementSymbol("e1"); //$NON-NLS-1$
        e1.setType(String.class);        
        ElementSymbol e2 = new ElementSymbol("e2"); //$NON-NLS-1$
        e2.setType(String.class);
        
        Function func = new Function("concat", new Expression[] { e2, e1 }); //$NON-NLS-1$
        FunctionDescriptor desc = FunctionLibraryManager.getFunctionLibrary().findFunction("concat", new Class[] { String.class, String.class } ); //$NON-NLS-1$
        func.setFunctionDescriptor(desc);

        SingleElementSymbol[] elements = new SingleElementSymbol[] {
            e1, e2
        };
        
        Object[] values = new Object[] { 
            "xyz", "abc" //$NON-NLS-1$ //$NON-NLS-2$
        };
        
        helpTestEval(func, elements, values, null, null, "abcxyz"); //$NON-NLS-1$
    }
    
    public void testLookupFunction() {
        ElementSymbol e1 = new ElementSymbol("e1"); //$NON-NLS-1$
        e1.setType(String.class);        
        ElementSymbol e2 = new ElementSymbol("e2"); //$NON-NLS-1$
        e1.setType(Integer.class);        
        
        Function func = new Function("lookup", new Expression[] { new Constant("pm1.g1"), new Constant("e2"), new Constant("e1"), e1 }); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
        FunctionDescriptor desc = FunctionLibraryManager.getFunctionLibrary().findFunction("lookup", new Class[] { String.class, String.class, String.class, String.class } ); //$NON-NLS-1$
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

    public void testScalarSubquery() throws Exception{
        ScalarSubquery expr = new ScalarSubquery(new Query());
        ArrayList values = new ArrayList(1);
        values.add("a"); //$NON-NLS-1$
        Object expected = "a"; //$NON-NLS-1$
        helpTestWithValueIterator(expr, values, expected);
    }

	private void helpTestWithValueIterator(ScalarSubquery expr,
			List<?> values, Object expected)
			throws BlockedException,
			MetaMatrixComponentException, ExpressionEvaluationException {
		final CollectionValueIterator valueIter = new CollectionValueIterator(values);
        CommandContext cc = new CommandContext();
        assertEquals(expected, new Evaluator(Collections.emptyMap(), null, cc) {
        	@Override
        	protected ValueIterator evaluateSubquery(
        			SubqueryContainer container, List tuple)
        			throws MetaMatrixProcessingException, BlockedException,
        			MetaMatrixComponentException {
        		return valueIter;
        	}
        }.evaluate(expr, null) );
	}

    public void testScalarSubquery2() throws Exception{
        ScalarSubquery expr = new ScalarSubquery(new Query());
        ArrayList values = new ArrayList(1);
        values.add(null);
        helpTestWithValueIterator(expr, values, null);
    }

    public void testScalarSubquery3() throws Exception{
        ScalarSubquery expr = new ScalarSubquery(new Query());
        helpTestWithValueIterator(expr, Collections.emptyList(), null);
    }

    public void testScalarSubqueryFails() throws Exception{
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

    public void testUser() throws Exception {
        Function func = new Function("user", new Expression[] {}); //$NON-NLS-1$
        FunctionDescriptor desc = FunctionLibraryManager.getFunctionLibrary().findFunction("user", new Class[] {} );         //$NON-NLS-1$
        func.setFunctionDescriptor(desc);

        FakeDataManager dataMgr = new FakeDataManager();
        CommandContext context = new CommandContext(new Long(1), null, null, null, null);
        context.setUserName("logon");  //$NON-NLS-1$
        assertEquals(context.getUserName(), new Evaluator(Collections.emptyMap(), dataMgr, context).evaluate(func, Collections.emptyList()) );       
    } 
    
    /*
     * This is the only test that depends upon the the testsrc/metamatrix.properties
     * and testsrc/config.xml. If the implementation is changed, please update/remove
     * these files.
     * @throws Exception
     */
    public void testEnv() throws Exception {
        Function func = new Function("env", new Expression[] {}); //$NON-NLS-1$
        FunctionDescriptor desc = FunctionLibraryManager.getFunctionLibrary().findFunction("env", new Class[] {String.class} );         //$NON-NLS-1$
        func.setFunctionDescriptor(desc);
        
        FakeDataManager dataMgr = new FakeDataManager();
        
        Properties props = new Properties();
        props.setProperty("http_host", "testHostName"); //$NON-NLS-1$ //$NON-NLS-2$
        props.setProperty("http_port", "8000"); //$NON-NLS-1$ //$NON-NLS-2$
        CommandContext context = new CommandContext(new Long(1), null, null, null, null, null, props, false, false);
        
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
        FunctionDescriptor desc = FunctionLibraryManager.getFunctionLibrary().findFunction("commandpayload", parameterSignature );         //$NON-NLS-1$
        func.setFunctionDescriptor(desc);
        
        FakeDataManager dataMgr = new FakeDataManager();       
        CommandContext context = new CommandContext(new Long(-1), null, "user", payload, "vdb", "1", null, false, false); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ 

        if(property != null) {
            func.setArgs(new Expression[] {new Constant(property)}); 
        }
        String actual = (String) new Evaluator(Collections.emptyMap(), dataMgr, context).evaluate(func, Collections.emptyList()); 
        assertEquals(expectedValue, actual);
    }
    
    public void testCommandPayloadNoArgsWithPayload() throws Exception {
        helpTestCommandPayload("blah", null, "blah"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testCommandPayloadNoArgsWithoutPayload() throws Exception {
        helpTestCommandPayload(null, null, null);
    }

    public void testCommandPayloadNoArgsWithNonStringPayload() throws Exception {
        helpTestCommandPayload(Boolean.TRUE, null, "true"); //$NON-NLS-1$
    }
    
    public void testCommandPayloadArgWithPayload() throws Exception {
        Properties props = new Properties();
        props.setProperty("p1", "v1"); //$NON-NLS-1$ //$NON-NLS-2$
        props.setProperty("p2", "v2"); //$NON-NLS-1$ //$NON-NLS-2$
        helpTestCommandPayload(props, "p1", "v1"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testCommandPayloadArgWithPayloadMissingProp() throws Exception {
        Properties props = new Properties();
        props.setProperty("p1", "v1"); //$NON-NLS-1$ //$NON-NLS-2$
        props.setProperty("p2", "v2"); //$NON-NLS-1$ //$NON-NLS-2$
        helpTestCommandPayload(props, "BOGUS", null); //$NON-NLS-1$
    }

    public void testCommandPayloadArgWithoutPayload() throws Exception {
        Properties props = new Properties();
        props.setProperty("p1", "v1"); //$NON-NLS-1$ //$NON-NLS-2$
        props.setProperty("p2", "v2"); //$NON-NLS-1$ //$NON-NLS-2$
        helpTestCommandPayload(null, "BOGUS", null); //$NON-NLS-1$
    }

    public void testCommandPayloadArgWithBadPayload() throws Exception {
        try {
            helpTestCommandPayload(Boolean.TRUE, "BOGUS", null); //$NON-NLS-1$
            fail("Expected exception but got none"); //$NON-NLS-1$
        } catch(ExpressionEvaluationException e) {
            // expected
        }
    }    
    
    //tests that the visitor is safe to use against a null expression in the aggregate symbol
    public void testCountStar() throws Exception {
    	ElementSymbol x = new ElementSymbol("x"); //$NON-NLS-1$
        ElementSymbol y = new ElementSymbol("y"); //$NON-NLS-1$
        
        HashMap map = new HashMap();
        map.put(x, y);
        
    	AggregateSymbol countStar = new AggregateSymbol("agg1", ReservedWords.COUNT, false, null); //$NON-NLS-1$ //$NON-NLS-2$
    	AggregateSymbol countStar1 = new AggregateSymbol("agg1", ReservedWords.COUNT, false, null); //$NON-NLS-1$ //$NON-NLS-2$
    	EvaluateExpressionVisitor.replaceExpressions(countStar, true, null, null);
    	
    	assertEquals(countStar1, countStar);
    }
}
