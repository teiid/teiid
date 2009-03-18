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

import org.teiid.connector.language.ISelectSymbol;
import org.teiid.dqp.internal.datamgr.language.SelectImpl;

import com.metamatrix.query.sql.lang.Select;

import junit.framework.TestCase;

public class TestSelectImpl extends TestCase {

    /**
     * Constructor for TestSelectImpl.
     * @param name
     */
    public TestSelectImpl(String name) {
        super(name);
    }

    public static Select helpExample(boolean distinct) {
        ArrayList symbols = new ArrayList();
        symbols.add(TestElementImpl.helpExample("vm1.g1", "e1")); //$NON-NLS-1$ //$NON-NLS-2$
        symbols.add(TestElementImpl.helpExample("vm1.g1", "e2")); //$NON-NLS-1$ //$NON-NLS-2$
        symbols.add(TestElementImpl.helpExample("vm1.g1", "e3")); //$NON-NLS-1$ //$NON-NLS-2$
        symbols.add(TestElementImpl.helpExample("vm1.g1", "e4")); //$NON-NLS-1$ //$NON-NLS-2$
        Select sel = new Select(symbols);
        sel.setDistinct(distinct);
        return sel;
    }
    
    public static SelectImpl example(boolean distinct) throws Exception {
        return (SelectImpl)TstLanguageBridgeFactory.factory.translate(helpExample(distinct));
    }

    public void testGetSelectSymbols() throws Exception {
        List symbols = example(false).getSelectSymbols();
        assertNotNull(symbols);
        assertEquals(4, symbols.size());
        for (Iterator i = symbols.iterator(); i.hasNext();) {
            assertTrue(i.next() instanceof ISelectSymbol);
        }
    }

    public void testIsDistinct() throws Exception {
        assertTrue(example(true).isDistinct());
        assertFalse(example(false).isDistinct());
    }

}
