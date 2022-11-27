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
import java.util.Iterator;
import java.util.List;

import junit.framework.TestCase;

import org.teiid.core.util.EquivalenceUtil;
import org.teiid.language.DerivedColumn;
import org.teiid.language.Select;
import org.teiid.query.sql.lang.CompoundCriteria;
import org.teiid.query.sql.lang.Query;
import org.teiid.query.sql.lang.UnaryFromClause;
import org.teiid.query.sql.symbol.ElementSymbol;


public class TestQueryImpl extends TestCase {

    /**
     * Constructor for TestQueryImpl.
     * @param name
     */
    public TestQueryImpl(String name) {
        super(name);
    }

    public static org.teiid.query.sql.lang.Select helpExampleSelect(boolean distinct) {
        ArrayList<ElementSymbol> symbols = new ArrayList<ElementSymbol>();
        symbols.add(TestElementImpl.helpExample("vm1.g1", "e1")); //$NON-NLS-1$ //$NON-NLS-2$
        symbols.add(TestElementImpl.helpExample("vm1.g1", "e2")); //$NON-NLS-1$ //$NON-NLS-2$
        symbols.add(TestElementImpl.helpExample("vm1.g1", "e3")); //$NON-NLS-1$ //$NON-NLS-2$
        symbols.add(TestElementImpl.helpExample("vm1.g1", "e4")); //$NON-NLS-1$ //$NON-NLS-2$
        org.teiid.query.sql.lang.Select sel = new org.teiid.query.sql.lang.Select(symbols);
        sel.setDistinct(distinct);
        return sel;
    }

    public static Query helpExample(boolean distinct) {
        return new Query(helpExampleSelect(distinct),
                         TestQueryImpl.helpExampleFrom(),
                         TestCompoundCriteriaImpl.helpExample(CompoundCriteria.AND),
                         TestGroupByImpl.helpExample(),
                         TestCompoundCriteriaImpl.helpExample(CompoundCriteria.AND),
                         TestOrderByImpl.helpExample(),
                         null);
    }

    public static Select example(boolean distinct) throws Exception {
        return TstLanguageBridgeFactory.factory.translate(helpExample(distinct));
    }

    public void testGetSelect() throws Exception {
        assertNotNull(example(true).getDerivedColumns());
    }

    public void testGetFrom() throws Exception {
        assertNotNull(example(true).getFrom());
    }

    public void testGetWhere() throws Exception {
        assertNotNull(example(true).getWhere());
    }

    public void testGetGroupBy() throws Exception {
        assertNotNull(example(true).getGroupBy());
    }

    public void testGetHaving() throws Exception {
        assertNotNull(example(true).getHaving());
    }

    public void testGetOrderBy() throws Exception {
        assertNotNull(example(true).getOrderBy());
    }

    public void testGetColumnNames() throws Exception {
        String[] expected = new String[4];
        String[] names = example(true).getColumnNames();
        assertTrue(EquivalenceUtil.areEquivalent(expected, names));
    }

    public void testGetColumnTypes() throws Exception {
        Class[] expected = {String.class, String.class, String.class, String.class};
        Class[] types = example(true).getColumnTypes();
        assertTrue(EquivalenceUtil.areEquivalent(expected, types));
    }

    public static org.teiid.query.sql.lang.From helpExampleFrom() {
        List<UnaryFromClause> clauses = new ArrayList<UnaryFromClause>();
        clauses.add(new UnaryFromClause(TestGroupImpl.helpExample("vm1.g1"))); //$NON-NLS-1$
        clauses.add(new UnaryFromClause(TestGroupImpl.helpExample("myAlias", "vm1.g2"))); //$NON-NLS-1$ //$NON-NLS-2$
        clauses.add(new UnaryFromClause(TestGroupImpl.helpExample("vm1.g3"))); //$NON-NLS-1$
        clauses.add(new UnaryFromClause(TestGroupImpl.helpExample("vm1.g4"))); //$NON-NLS-1$
        return new org.teiid.query.sql.lang.From(clauses);
    }

    public void testGetSelectSymbols() throws Exception {
        List symbols = example(false).getDerivedColumns();
        assertNotNull(symbols);
        assertEquals(4, symbols.size());
        for (Iterator i = symbols.iterator(); i.hasNext();) {
            assertTrue(i.next() instanceof DerivedColumn);
        }
    }

    public void testIsDistinct() throws Exception {
        assertTrue(example(true).isDistinct());
        assertFalse(example(false).isDistinct());
    }

}
