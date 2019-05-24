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
