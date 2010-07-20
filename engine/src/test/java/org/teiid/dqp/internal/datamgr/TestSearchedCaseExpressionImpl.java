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

import java.util.ArrayList;
import java.util.List;

import org.teiid.language.SearchedCase;
import org.teiid.query.sql.lang.CompareCriteria;
import org.teiid.query.sql.symbol.Constant;
import org.teiid.query.sql.symbol.ElementSymbol;
import org.teiid.query.sql.symbol.SearchedCaseExpression;
import org.teiid.query.sql.symbol.TestCaseExpression;



import junit.framework.TestCase;

public class TestSearchedCaseExpressionImpl extends TestCase {

    /**
     * Constructor for TestSearchedCaseExpressionImpl.
     * @param name
     */
    public TestSearchedCaseExpressionImpl(String name) {
        super(name);
    }

    public static List getWhenCriteria(int criteria) {
        ArrayList list = new ArrayList();
        ElementSymbol x = TestElementImpl.helpExample("vm1.g1", "e1"); //$NON-NLS-1$ //$NON-NLS-2$
        for (int i = 0; i < criteria; i++) {
            list.add(new CompareCriteria(x, CompareCriteria.EQ, new Constant(new Integer(i))));
        }
        return list;
    }
    
    public static SearchedCaseExpression helpExample() {
        SearchedCaseExpression caseExpr = new SearchedCaseExpression(getWhenCriteria(3), TestCaseExpression.getThenExpressions(3));
        caseExpr.setElseExpression(new Constant(new Integer(9999)));
        return caseExpr;
    }
    
    public static SearchedCase example() throws Exception {
        return TstLanguageBridgeFactory.factory.translate(helpExample());
    }

    public void testGetElseExpression() throws Exception {
        assertNotNull(example().getElseExpression());
        SearchedCaseExpression expr = helpExample();
        expr.setElseExpression(null);
        assertNull(TstLanguageBridgeFactory.factory.translate(expr).getElseExpression());
    }

    public void testGetThenExpression() throws Exception {
        assertNotNull(example().getCases().get(0));
        assertNotNull(example().getCases().get(1));
        assertNotNull(example().getCases().get(2));
    }

    public void testGetWhenCount() throws Exception {
        assertEquals(3, example().getCases().size());
    }

    public void testGetWhenCriteria() throws Exception {
        assertNotNull(example().getCases().get(0));
        assertNotNull(example().getCases().get(1));
        assertNotNull(example().getCases().get(2));
    }

}
