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

package com.metamatrix.dqp.internal.datamgr.language;

import com.metamatrix.query.sql.symbol.CaseExpression;
import com.metamatrix.query.sql.symbol.Constant;
import com.metamatrix.query.sql.symbol.ElementSymbol;
import com.metamatrix.query.sql.symbol.TestCaseExpression;

import junit.framework.TestCase;

public class TestCaseExpressionImpl extends TestCase {

    /**
     * Constructor for TestCaseExpressionImpl.
     * @param name
     */
    public TestCaseExpressionImpl(String name) {
        super(name);
    }

    public static CaseExpression helpExample() {
        ElementSymbol x = TestElementImpl.helpExample("vm1.g1", "e1"); //$NON-NLS-1$ //$NON-NLS-2$
        CaseExpression caseExpr = new CaseExpression(x, TestCaseExpression.getWhenExpressions(3), TestCaseExpression.getThenExpressions(3));
        caseExpr.setElseExpression(new Constant(new Integer(9999)));
        return caseExpr;
    }
    
    public static CaseExpression helpExampleElementElse() {
        ElementSymbol x = TestElementImpl.helpExample("vm1.g1", "e1"); //$NON-NLS-1$ //$NON-NLS-2$
        CaseExpression caseExpr = new CaseExpression(x, TestCaseExpression.getWhenExpressions(3), TestCaseExpression.getThenExpressions(3));
        caseExpr.setElseExpression(x);
        return caseExpr;
    }
    
    public static CaseExpression helpIntExample() {
        ElementSymbol x = TestElementImpl.helpIntExample("vm1.g1", "e1"); //$NON-NLS-1$ //$NON-NLS-2$
        CaseExpression caseExpr = new CaseExpression(x, TestCaseExpression.getWhenExpressions(3), TestCaseExpression.getThenExpressions(3));
        caseExpr.setElseExpression(x);
        return caseExpr;
    }
    
    public static CaseExpression helpExampleNullFirst() {
        ElementSymbol x = TestElementImpl.helpExample("vm1.g1", "e1"); //$NON-NLS-1$ //$NON-NLS-2$
        CaseExpression caseExpr = new CaseExpression(x, TestCaseExpression.getWhenExpressions(3, 0, true), TestCaseExpression.getThenExpressions(3));
        caseExpr.setElseExpression(new Constant(new Integer(9999)));
        return caseExpr;
    }
    
    public static CaseExpression helpExampleNullMiddle() {
        ElementSymbol x = TestElementImpl.helpExample("vm1.g1", "e1"); //$NON-NLS-1$ //$NON-NLS-2$
        CaseExpression caseExpr = new CaseExpression(x, TestCaseExpression.getWhenExpressions(3, 1, true), TestCaseExpression.getThenExpressions(3));
        caseExpr.setElseExpression(new Constant(new Integer(9999)));
        return caseExpr;
    }
    
    public static CaseExpression helpExampleNullLast() {
        ElementSymbol x = TestElementImpl.helpExample("vm1.g1", "e1"); //$NON-NLS-1$ //$NON-NLS-2$
        CaseExpression caseExpr = new CaseExpression(x, TestCaseExpression.getWhenExpressions(3, 2, true), TestCaseExpression.getThenExpressions(3));
        caseExpr.setElseExpression(new Constant(new Integer(9999)));
        return caseExpr;
    }
    
    public static CaseExpressionImpl example() throws Exception {
        return (CaseExpressionImpl)TstLanguageBridgeFactory.factory.translate(helpExample());
    }

    public static CaseExpressionImpl exampleElementElse() throws Exception {
        return (CaseExpressionImpl)TstLanguageBridgeFactory.factory.translate(helpExampleElementElse());
    }
    
    public static CaseExpressionImpl exampleInteger() throws Exception {
        return (CaseExpressionImpl)TstLanguageBridgeFactory.factory.translate(helpIntExample());
    }
    
    public static CaseExpressionImpl exampleNullFirst() throws Exception {
        return (CaseExpressionImpl)TstLanguageBridgeFactory.factory.translate(helpExampleNullFirst());
    }
    
    public static CaseExpressionImpl exampleNullMiddle() throws Exception {
        return (CaseExpressionImpl)TstLanguageBridgeFactory.factory.translate(helpExampleNullMiddle());
    }
    
    public static CaseExpressionImpl exampleNullLast() throws Exception {
        return (CaseExpressionImpl)TstLanguageBridgeFactory.factory.translate(helpExampleNullLast());
    }

    public void testGetElseExpression() throws Exception {
        assertNotNull(example().getElseExpression());
        CaseExpression expr = helpExample();
        expr.setElseExpression(null);
        assertNull(TstLanguageBridgeFactory.factory.translate(expr).getElseExpression());
    }

    public void testGetExpression() throws Exception {
        assertNotNull(example().getExpression());
    }

    public void testGetThenExpression() throws Exception {
        assertNotNull(example().getThenExpression(0));
        assertNotNull(example().getThenExpression(1));
        assertNotNull(example().getThenExpression(2));
    }

    public void testGetWhenCount() throws Exception {
        assertEquals(3, example().getWhenCount());
    }

    public void testGetWhenExpression() throws Exception {
        assertNotNull(example().getWhenExpression(0));
        assertNotNull(example().getWhenExpression(1));
        assertNotNull(example().getWhenExpression(2));
    }

    public void testGetWhenExpressionNullFirst() throws Exception {
        assertNotNull(exampleNullFirst().getWhenExpression(0));
        assertNotNull(exampleNullFirst().getWhenExpression(1));
        assertNotNull(exampleNullFirst().getWhenExpression(2));
}
    public void testGetWhenExpressionNullMiddle() throws Exception {
        assertNotNull(exampleNullMiddle().getWhenExpression(0));
        assertNotNull(exampleNullMiddle().getWhenExpression(1));
        assertNotNull(exampleNullMiddle().getWhenExpression(2));
    }  
    
    public void testGetWhenExpressionNullLast() throws Exception {
        assertNotNull(exampleNullLast().getWhenExpression(0));
        assertNotNull(exampleNullLast().getWhenExpression(1));
        assertNotNull(exampleNullLast().getWhenExpression(2));
    }  

}
