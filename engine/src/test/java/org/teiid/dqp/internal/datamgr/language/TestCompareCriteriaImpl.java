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

package org.teiid.dqp.internal.datamgr.language;

import org.teiid.connector.language.ICompareCriteria.Operator;
import org.teiid.dqp.internal.datamgr.language.CompareCriteriaImpl;
import org.teiid.dqp.internal.datamgr.language.LiteralImpl;

import com.metamatrix.query.sql.lang.CompareCriteria;
import com.metamatrix.query.sql.symbol.Constant;

import junit.framework.TestCase;

public class TestCompareCriteriaImpl extends TestCase {

    /**
     * Constructor for TestCompareCriteriaImpl.
     * @param name
     */
    public TestCompareCriteriaImpl(String name) {
        super(name);
    }

    public static CompareCriteria helpExample(int operator, int leftVal, int rightVal) {
        Constant left = new Constant(new Integer(leftVal));
        Constant right = new Constant(new Integer(rightVal));
        return new CompareCriteria(left, operator, right);
    }
    public static CompareCriteriaImpl example(int operator, int leftVal, int rightVal) throws Exception {
        return (CompareCriteriaImpl)TstLanguageBridgeFactory.factory.translate(helpExample(operator, leftVal, rightVal));
    }

    public void testGetLeftExpression() throws Exception {
        CompareCriteriaImpl impl = example(CompareCriteria.GE, 200, 100);
        assertNotNull(impl.getLeftExpression());
        assertTrue(impl.getLeftExpression() instanceof LiteralImpl);
        assertEquals(new Integer(200), ((LiteralImpl)impl.getLeftExpression()).getValue());
    }

    public void testGetRightExpression() throws Exception {
        CompareCriteriaImpl impl = example(CompareCriteria.GE, 200, 100);
        assertNotNull(impl.getRightExpression());
        assertTrue(impl.getRightExpression() instanceof LiteralImpl);
        assertEquals(new Integer(100), ((LiteralImpl)impl.getRightExpression()).getValue());
    }

    public void testGetOperator() throws Exception {
        assertEquals(Operator.EQ, example(CompareCriteria.EQ, 200, 100).getOperator());
        assertEquals(Operator.GE, example(CompareCriteria.GE, 200, 100).getOperator());
        assertEquals(Operator.GT, example(CompareCriteria.GT, 200, 100).getOperator());
        assertEquals(Operator.LE, example(CompareCriteria.LE, 200, 100).getOperator());
        assertEquals(Operator.LT, example(CompareCriteria.LT, 200, 100).getOperator());
        assertEquals(Operator.NE, example(CompareCriteria.NE, 200, 100).getOperator());
    }

}
