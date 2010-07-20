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

package org.teiid.dqp.internal.datamgr;

import junit.framework.TestCase;

import org.teiid.language.Comparison;
import org.teiid.language.Literal;
import org.teiid.language.Comparison.Operator;
import org.teiid.query.sql.lang.AbstractCompareCriteria;
import org.teiid.query.sql.symbol.Constant;


public class TestCompareCriteriaImpl extends TestCase {

    /**
     * Constructor for TestCompareCriteriaImpl.
     * @param name
     */
    public TestCompareCriteriaImpl(String name) {
        super(name);
    }

    public static org.teiid.query.sql.lang.CompareCriteria helpExample(int operator, int leftVal, int rightVal) {
        Constant left = new Constant(new Integer(leftVal));
        Constant right = new Constant(new Integer(rightVal));
        return new org.teiid.query.sql.lang.CompareCriteria(left, operator, right);
    }
    
    public static Comparison example(int operator, int leftVal, int rightVal) throws Exception {
        return TstLanguageBridgeFactory.factory.translate(helpExample(operator, leftVal, rightVal));
    }

    public void testGetLeftExpression() throws Exception {
        Comparison impl = example(AbstractCompareCriteria.GE, 200, 100);
        assertNotNull(impl.getLeftExpression());
        assertTrue(impl.getLeftExpression() instanceof Literal);
        assertEquals(new Integer(200), ((Literal)impl.getLeftExpression()).getValue());
    }

    public void testGetRightExpression() throws Exception {
        Comparison impl = example(AbstractCompareCriteria.GE, 200, 100);
        assertNotNull(impl.getRightExpression());
        assertTrue(impl.getRightExpression() instanceof Literal);
        assertEquals(new Integer(100), ((Literal)impl.getRightExpression()).getValue());
    }

    public void testGetOperator() throws Exception {
        assertEquals(Operator.EQ, example(AbstractCompareCriteria.EQ, 200, 100).getOperator());
        assertEquals(Operator.GE, example(AbstractCompareCriteria.GE, 200, 100).getOperator());
        assertEquals(Operator.GT, example(AbstractCompareCriteria.GT, 200, 100).getOperator());
        assertEquals(Operator.LE, example(AbstractCompareCriteria.LE, 200, 100).getOperator());
        assertEquals(Operator.LT, example(AbstractCompareCriteria.LT, 200, 100).getOperator());
        assertEquals(Operator.NE, example(AbstractCompareCriteria.NE, 200, 100).getOperator());
    }

}
