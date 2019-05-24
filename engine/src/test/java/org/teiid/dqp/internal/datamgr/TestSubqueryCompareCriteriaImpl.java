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
