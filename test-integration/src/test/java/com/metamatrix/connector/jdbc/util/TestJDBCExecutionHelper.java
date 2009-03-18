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

/*
 */
package com.metamatrix.connector.jdbc.util;

import java.util.ArrayList;
import java.util.List;

import junit.framework.TestCase;

import org.teiid.connector.jdbc.JDBCQueryExecution;
import org.teiid.connector.language.IQuery;
import org.teiid.connector.language.ISelect;
import org.teiid.dqp.internal.datamgr.language.LiteralImpl;
import org.teiid.dqp.internal.datamgr.language.QueryImpl;
import org.teiid.dqp.internal.datamgr.language.SelectImpl;
import org.teiid.dqp.internal.datamgr.language.SelectSymbolImpl;

import com.metamatrix.common.types.DataTypeManager;

public class TestJDBCExecutionHelper extends TestCase{
    
    public TestJDBCExecutionHelper(String name) {
        super(name);
    }

    //tests
    public void testGetColumnDataTypes(){
        Class[] expectedResults = new Class[2];
        List symbols = new ArrayList();
        symbols.add(new SelectSymbolImpl("c1", new LiteralImpl("3", DataTypeManager.DefaultDataClasses.STRING)));  //$NON-NLS-1$//$NON-NLS-2$
        expectedResults[0] = DataTypeManager.DefaultDataClasses.STRING;
        symbols.add(new SelectSymbolImpl("c2", new LiteralImpl(new Integer(5), DataTypeManager.DefaultDataClasses.INTEGER)));  //$NON-NLS-1$//$NON-NLS-2$
        expectedResults[1] = DataTypeManager.DefaultDataClasses.INTEGER;
        ISelect select = new SelectImpl(symbols, false);        
        IQuery query = new QueryImpl(select, null, null, null, null, null);
        Class[] results = query.getColumnTypes();  
        assertEquals( results[0], expectedResults[0]);
        assertEquals( results[1], expectedResults[1]);     
    }
    
    public void helpTestTrimString(String value, String expected) {
        String actual = JDBCQueryExecution.trimString(value);
        assertEquals("Did not get a match, expected=[" + expected + "', actual=[" + actual + "]", expected, actual); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }
    
    public void testTrimEmpty() {
        helpTestTrimString("", ""); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testTrimNoWhitespace() {
        helpTestTrimString("abc", "abc"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testTrimSome() {
        helpTestTrimString("abc  ", "abc"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testDontTrimLeft() {
        helpTestTrimString("   abc  ", "   abc"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testDontTrimInternal() {
        helpTestTrimString("a b c  ", "a b c"); //$NON-NLS-1$ //$NON-NLS-2$
    }

}
