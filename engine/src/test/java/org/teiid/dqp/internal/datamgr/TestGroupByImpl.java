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

import org.teiid.language.ColumnReference;
import org.teiid.language.GroupBy;
import org.teiid.query.sql.symbol.ElementSymbol;
import org.teiid.query.sql.symbol.Expression;
import org.teiid.query.sql.symbol.Function;


public class TestGroupByImpl extends TestCase {

    /**
     * Constructor for TestGroupByImpl.
     * @param name
     */
    public TestGroupByImpl(String name) {
        super(name);
    }

    public static org.teiid.query.sql.lang.GroupBy helpExample() {
        List<ElementSymbol> symbols = new ArrayList<ElementSymbol>();
        symbols.add(TestElementImpl.helpExample("vm1.g1", "e1")); //$NON-NLS-1$ //$NON-NLS-2$
        symbols.add(TestElementImpl.helpExample("vm1.g1", "e2")); //$NON-NLS-1$ //$NON-NLS-2$
        symbols.add(TestElementImpl.helpExample("vm1.g1", "e3")); //$NON-NLS-1$ //$NON-NLS-2$
        symbols.add(TestElementImpl.helpExample("vm1.g1", "e4")); //$NON-NLS-1$ //$NON-NLS-2$
        return new org.teiid.query.sql.lang.GroupBy(symbols);
    }

    public static org.teiid.query.sql.lang.GroupBy helpExampleWithFunctions() {
        List<Expression> symbols = new ArrayList<Expression>();
        
        ElementSymbol e1 = TestElementImpl.helpExample("vm1.g1", "e1");//$NON-NLS-1$ //$NON-NLS-2$
        Function f = new Function("length", new Expression[] { e1 } );//$NON-NLS-1$ 
        
        symbols.add(e1); 
        symbols.add(f);
        return new org.teiid.query.sql.lang.GroupBy(symbols);
    }
    

    public static GroupBy example() throws Exception {
        return TstLanguageBridgeFactory.factory.translate(helpExample());
    }

    public void testGetElements() throws Exception {
        GroupBy gb = example();
        assertNotNull(gb.getElements());
        assertEquals(4, gb.getElements().size());
        for (Iterator i = gb.getElements().iterator(); i.hasNext();) {
            assertTrue(i.next() instanceof ColumnReference);
        }
    }
    
    public void testTranslateWithFunction() throws Exception {
        TstLanguageBridgeFactory.factory.translate(helpExampleWithFunctions());
    }

}
