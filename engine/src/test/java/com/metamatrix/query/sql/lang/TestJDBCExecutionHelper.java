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
package com.metamatrix.query.sql.lang;

import java.util.ArrayList;
import java.util.List;

import junit.framework.TestCase;

import org.teiid.connector.language.DerivedColumn;
import org.teiid.connector.language.Literal;
import org.teiid.connector.language.Select;

import com.metamatrix.common.types.DataTypeManager;

public class TestJDBCExecutionHelper extends TestCase{
    
    public TestJDBCExecutionHelper(String name) {
        super(name);
    }

    //tests
    public void testGetColumnDataTypes(){
        Class[] expectedResults = new Class[2];
        List symbols = new ArrayList();
        symbols.add(new DerivedColumn("c1", new Literal("3", DataTypeManager.DefaultDataClasses.STRING)));  //$NON-NLS-1$//$NON-NLS-2$
        expectedResults[0] = DataTypeManager.DefaultDataClasses.STRING;
        symbols.add(new DerivedColumn("c2", new Literal(new Integer(5), DataTypeManager.DefaultDataClasses.INTEGER)));  //$NON-NLS-1$//$NON-NLS-2$
        expectedResults[1] = DataTypeManager.DefaultDataClasses.INTEGER;
        Select query = new Select(symbols, false, null, null, null, null, null);
        Class[] results = query.getColumnTypes();  
        assertEquals( results[0], expectedResults[0]);
        assertEquals( results[1], expectedResults[1]);     
    }

}
