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
import java.util.Iterator;
import java.util.List;

import org.teiid.connector.language.IElement;
import org.teiid.dqp.internal.datamgr.language.GroupByImpl;

import com.metamatrix.query.sql.lang.GroupBy;
import com.metamatrix.query.sql.symbol.ElementSymbol;
import com.metamatrix.query.sql.symbol.Expression;
import com.metamatrix.query.sql.symbol.Function;

import junit.framework.TestCase;

public class TestGroupByImpl extends TestCase {

    /**
     * Constructor for TestGroupByImpl.
     * @param name
     */
    public TestGroupByImpl(String name) {
        super(name);
    }

    public static GroupBy helpExample() {
        List symbols = new ArrayList();
        symbols.add(TestElementImpl.helpExample("vm1.g1", "e1")); //$NON-NLS-1$ //$NON-NLS-2$
        symbols.add(TestElementImpl.helpExample("vm1.g1", "e2")); //$NON-NLS-1$ //$NON-NLS-2$
        symbols.add(TestElementImpl.helpExample("vm1.g1", "e3")); //$NON-NLS-1$ //$NON-NLS-2$
        symbols.add(TestElementImpl.helpExample("vm1.g1", "e4")); //$NON-NLS-1$ //$NON-NLS-2$
        return new GroupBy(symbols);
    }

    public static GroupBy helpExampleWithFunctions() {
        List symbols = new ArrayList();
        
        ElementSymbol e1 = TestElementImpl.helpExample("vm1.g1", "e1");//$NON-NLS-1$ //$NON-NLS-2$
        Function f = new Function("length", new Expression[] { e1 } );//$NON-NLS-1$ 
        
        symbols.add(e1); 
        symbols.add(f);
        return new GroupBy(symbols);
    }
    

    public static GroupByImpl example() throws Exception {
        return (GroupByImpl)TstLanguageBridgeFactory.factory.translate(helpExample());
    }

    public void testGetElements() throws Exception {
        GroupByImpl gb = example();
        assertNotNull(gb.getElements());
        assertEquals(4, gb.getElements().size());
        for (Iterator i = gb.getElements().iterator(); i.hasNext();) {
            assertTrue(i.next() instanceof IElement);
        }
    }
    
    public void testTranslateWithFunction() throws Exception {
        TstLanguageBridgeFactory.factory.translate(helpExampleWithFunctions());
    }

}
