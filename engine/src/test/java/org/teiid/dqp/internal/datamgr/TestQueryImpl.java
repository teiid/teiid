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
