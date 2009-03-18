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

import java.util.ArrayList;
import java.util.List;

import org.teiid.dqp.internal.datamgr.language.SearchedCaseExpressionImpl;

import com.metamatrix.query.sql.lang.CompareCriteria;
import com.metamatrix.query.sql.symbol.Constant;
import com.metamatrix.query.sql.symbol.ElementSymbol;
import com.metamatrix.query.sql.symbol.SearchedCaseExpression;
import com.metamatrix.query.sql.symbol.TestCaseExpression;

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
    
    public static SearchedCaseExpressionImpl example() throws Exception {
        return (SearchedCaseExpressionImpl)TstLanguageBridgeFactory.factory.translate(helpExample());
    }

    public void testGetElseExpression() throws Exception {
        assertNotNull(example().getElseExpression());
        SearchedCaseExpression expr = helpExample();
        expr.setElseExpression(null);
        assertNull(TstLanguageBridgeFactory.factory.translate(expr).getElseExpression());
    }

    public void testGetThenExpression() throws Exception {
        assertNotNull(example().getThenExpression(0));
        assertNotNull(example().getThenExpression(1));
        assertNotNull(example().getThenExpression(2));
    }

    public void testGetWhenCount() throws Exception {
        assertEquals(3, example().getWhenCount());
    }

    public void testGetWhenCriteria() throws Exception {
        assertNotNull(example().getWhenCriteria(0));
        assertNotNull(example().getWhenCriteria(1));
        assertNotNull(example().getWhenCriteria(2));
    }

}
