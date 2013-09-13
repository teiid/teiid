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

package org.teiid.query.parser;

import static org.junit.Assert.*;

import org.junit.Test;
import org.teiid.api.exception.query.QueryParserException;
import org.teiid.query.sql.lang.StoredProcedure;

@SuppressWarnings("nls")
public class TestCallableStatementParsing {
    
    private void helpTestIllegalCall(String call) {
        try {
            QueryParser.getQueryParser().parseCommand(call);
            fail("expected exception"); //$NON-NLS-1$
        } catch (QueryParserException qpe) {
            
        }
    }
    
    private void helpTestGetExec(String call, boolean returnValue) throws QueryParserException {
        StoredProcedure sp = helpTest(call, returnValue);
        assertEquals((returnValue ? "? = ":"") +"EXEC procedure_name(?, ?, ?)", sp.toString()); //$NON-NLS-1$
    }

	private StoredProcedure helpTest(String call, boolean returnValue)
			throws QueryParserException {
		StoredProcedure sp = (StoredProcedure)QueryParser.getQueryParser().parseCommand(call);
        assertTrue(sp.isCallableStatement());
        assertEquals(returnValue, sp.returnsScalarValue());
        assertEquals("procedure_name", sp.getProcedureName()); //$NON-NLS-1$
		return sp;
	}
            
    @Test public void testCallNoParams() throws QueryParserException {
        StoredProcedure sp = (StoredProcedure)QueryParser.getQueryParser().parseCommand("{call procedure_name}"); //$NON-NLS-1$
        assertFalse(sp.returnsScalarValue());
        assertEquals("procedure_name", sp.getProcedureName()); //$NON-NLS-1$
        assertEquals(0, sp.getParameters().size());
    }
    
    @Test public void testCallWithReturnParam() throws QueryParserException {
        helpTestGetExec("{?=call procedure_name(?, ?, ?)}", true); //$NON-NLS-1$
        helpTestGetExec(" {?=call procedure_name(?, ?, ?)}", true); //$NON-NLS-1$
        helpTestGetExec("{ ?=call procedure_name(?, ?, ?)}", true); //$NON-NLS-1$
        helpTestGetExec("{? =call procedure_name(?, ?, ?)}", true); //$NON-NLS-1$
        helpTestGetExec("{?= call procedure_name(?, ?, ?)}", true); //$NON-NLS-1$
        helpTestGetExec("{?=\ncall procedure_name(?, ?, ?)}", true); //$NON-NLS-1$
    }
    
    @Test public void testIllegalCalls() {
        helpTestIllegalCall("{call procedure_name"); //$NON-NLS-1$
        helpTestIllegalCall("call procedure_name}"); //$NON-NLS-1$
        helpTestIllegalCall("{call procedure_name(}"); //$NON-NLS-1$
        helpTestIllegalCall("{callprocedure_name()}"); //$NON-NLS-1$
        helpTestIllegalCall("{call procedure_name)}"); //$NON-NLS-1$
        helpTestIllegalCall("{call procedure name}"); //$NON-NLS-1$
        helpTestIllegalCall("{call procedure name()}"); //$NON-NLS-1$
        helpTestIllegalCall("{?call procedure_name}"); //$NON-NLS-1$
        helpTestIllegalCall("{=call procedure_name}"); //$NON-NLS-1$
        helpTestIllegalCall("{?=cal procedure_name}"); //$NON-NLS-1$
    }
    
    @Test public void testGetExec() throws QueryParserException {
        helpTestGetExec("{call procedure_name(?, ?, ?)}", false); //$NON-NLS-1$
        helpTestGetExec(" {call procedure_name(?, ?, ?)}", false); //$NON-NLS-1$
        helpTestGetExec("{ call procedure_name(?, ?, ?)}", false); //$NON-NLS-1$
        helpTestGetExec("{call\tprocedure_name(?, ?, ?)}", false); //$NON-NLS-1$
        helpTestGetExec("{call procedure_name (?, ?, ?)}", false); //$NON-NLS-1$
        helpTestGetExec("{call procedure_name(?, ?, ?) }", false); //$NON-NLS-1$
        helpTestGetExec("{CALL procedure_name(?, ?, ?)} ", false); //$NON-NLS-1$
    }
    
    @Test public void testNamedParams() throws QueryParserException {
    	assertEquals("? = EXEC procedure_name(a => ?)", helpTest("{?=call procedure_name(a=>?)}", true).toString());
    }
    
    @Test(expected=QueryParserException.class) public void testBadCallKeyword() throws Exception {
        QueryParser.getQueryParser().parseCommand("{calli procedure_name}"); //$NON-NLS-1$
    }

}
