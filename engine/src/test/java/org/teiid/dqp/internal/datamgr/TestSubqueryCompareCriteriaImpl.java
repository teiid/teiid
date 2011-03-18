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
import org.teiid.language.SubqueryComparison;
import org.teiid.language.SubqueryComparison.Quantifier;
import org.teiid.query.sql.lang.AbstractCompareCriteria;
import org.teiid.query.sql.lang.Query;
import org.teiid.query.sql.lang.SubqueryCompareCriteria;
import org.teiid.query.sql.symbol.ElementSymbol;


/**
 */
public class TestSubqueryCompareCriteriaImpl extends TestCase {

    /**
     * Constructor for TestSubqueryCompareCriteriaImpl.
     * @param name
     */
    public TestSubqueryCompareCriteriaImpl(String name) {
        super(name);
    }

    public static SubqueryCompareCriteria helpExample() {
        ElementSymbol element = TestElementImpl.helpExample("g1", "e1"); //$NON-NLS-1$ //$NON-NLS-2$
        Query query = TestQueryImpl.helpExample(true);
        SubqueryCompareCriteria scc = new SubqueryCompareCriteria(element, query, AbstractCompareCriteria.GT, SubqueryCompareCriteria.ANY);
        return scc;
    }
    
    public static SubqueryComparison example() throws Exception {
        return TstLanguageBridgeFactory.factory.translate(helpExample());
    }

    public void testGetExpression() throws Exception {
        assertNotNull(example().getLeftExpression());
    }

    public void testGetQuery() throws Exception {
        assertNotNull(example().getSubquery());
    }
    
    public void testOperator() throws Exception {
        assertEquals("Wrong operator", Comparison.Operator.GT, example().getOperator()); //$NON-NLS-1$
    }

    public void testQuantifier() throws Exception {
        assertEquals("Wrong quantifier", Quantifier.SOME, example().getQuantifier()); //$NON-NLS-1$
    }

}
