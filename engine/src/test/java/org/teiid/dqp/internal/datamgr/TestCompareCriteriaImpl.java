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
