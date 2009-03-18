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

import org.teiid.connector.language.IExpression;
import org.teiid.dqp.internal.datamgr.language.SelectSymbolImpl;

import com.metamatrix.query.sql.symbol.*;

import junit.framework.TestCase;

public class TestSelectSymbolImpl extends TestCase {

    /**
     * Constructor for TestSelectSymbolImpl.
     * @param name
     */
    public TestSelectSymbolImpl(String name) {
        super(name);
    }

    public static Expression helpExample(String name, String alias) {
        SingleElementSymbol symbol = TestElementImpl.helpExample("vm1.g1", name); //$NON-NLS-1$

        if (alias != null) {
            return new AliasSymbol(alias, symbol);
        }
        return symbol;
    }
    
    public static SelectSymbolImpl example(String symbolName, String alias) throws Exception {
        Expression expr = helpExample(symbolName, alias);
        IExpression iExp = TstLanguageBridgeFactory.factory.translate(expr);
        String name = null;
        if (expr instanceof Function) {
            name = ((Function)expr).getName();
        }else if (expr instanceof SingleElementSymbol) {
            name = ((SingleElementSymbol)expr).getName();
        }
        SelectSymbolImpl selectSymbol = new SelectSymbolImpl(name, iExp);
        if(expr instanceof AliasSymbol){
            selectSymbol.setAlias(true);
        }
        return selectSymbol;
    }

    public void testHasAlias() throws Exception {
        assertTrue(example("testName", "testAlias").hasAlias()); //$NON-NLS-1$ //$NON-NLS-2$
        assertFalse(example("testName", null).hasAlias()); //$NON-NLS-1$
    }

    public void testGetOutputName() throws Exception {
        assertEquals("testName", example("testName", null).getOutputName()); //$NON-NLS-1$ //$NON-NLS-2$
        assertEquals("testAlias", example("testName", "testAlias").getOutputName()); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }

    public void testGetExpression() throws Exception {
        assertNotNull(example("testName", null).getExpression()); //$NON-NLS-1$
    }

}
