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

import org.teiid.language.AndOr;
import org.teiid.language.Comparison;
import org.teiid.language.AndOr.Operator;
import org.teiid.query.sql.lang.CompareCriteria;


public class TestCompoundCriteriaImpl extends TestCase {

    /**
     * Constructor for TestCompoundCriteriaImpl.
     * @param name
     */
    public TestCompoundCriteriaImpl(String name) {
        super(name);
    }

    public static org.teiid.query.sql.lang.CompoundCriteria helpExample(int operator) {
        CompareCriteria c1 = TestCompareCriteriaImpl.helpExample(CompareCriteria.GE, 100, 200);
        CompareCriteria c2 = TestCompareCriteriaImpl.helpExample(CompareCriteria.LT, 500, 600);
        return new org.teiid.query.sql.lang.CompoundCriteria(operator, c1, c2);
    }
    
    public static AndOr example(int operator) throws Exception {
        return TstLanguageBridgeFactory.factory.translate(helpExample(operator));
    }

    public void testGetOperator() throws Exception {
        assertEquals(Operator.AND, example(org.teiid.query.sql.lang.CompoundCriteria.AND).getOperator());
        assertEquals(Operator.OR, example(org.teiid.query.sql.lang.CompoundCriteria.OR).getOperator());
    }

    public void testGetCriteria() throws Exception {
        AndOr cc = example(org.teiid.query.sql.lang.CompoundCriteria.AND);
        assertTrue(cc.getLeftCondition() instanceof Comparison);
        assertTrue(cc.getRightCondition() instanceof Comparison);
    }

}
