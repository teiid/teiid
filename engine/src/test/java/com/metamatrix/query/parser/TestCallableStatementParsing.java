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

package com.metamatrix.query.parser;

import com.metamatrix.api.exception.query.QueryParserException;
import com.metamatrix.query.sql.lang.StoredProcedure;

public class TestCallableStatementParsing extends junit.framework.TestCase {
    
    private void helpTestIllegalCall(String call) {
        try {
            QueryParser.getQueryParser().parseCommand(call);
            fail("expected exception"); //$NON-NLS-1$
        } catch (QueryParserException qpe) {
            
        }
    }
    
    private void helpTestGetExec(String call, boolean returnValue) throws QueryParserException {
        StoredProcedure sp = (StoredProcedure)QueryParser.getQueryParser().parseCommand(call);
        assertTrue(sp.isCallableStatement());
        assertEquals(returnValue, sp.returnsScalarValue());
        assertEquals("procedure_name", sp.getProcedureName()); //$NON-NLS-1$
        assertEquals("EXEC procedure_name(?, ?, ?)", sp.toString()); //$NON-NLS-1$
    }
            
    public void testCallNoParams() throws QueryParserException {
        StoredProcedure sp = (StoredProcedure)QueryParser.getQueryParser().parseCommand("{call procedure_name}"); //$NON-NLS-1$
        assertFalse(sp.returnsScalarValue());
        assertEquals("procedure_name", sp.getProcedureName()); //$NON-NLS-1$
        assertEquals(0, sp.getParameters().size());
    }
    
    public void testCallWithReturnParam() throws QueryParserException {
        helpTestGetExec("{?=call procedure_name(?, ?, ?)}", true); //$NON-NLS-1$
        helpTestGetExec(" {?=call procedure_name(?, ?, ?)}", true); //$NON-NLS-1$
        helpTestGetExec("{ ?=call procedure_name(?, ?, ?)}", true); //$NON-NLS-1$
        helpTestGetExec("{? =call procedure_name(?, ?, ?)}", true); //$NON-NLS-1$
        helpTestGetExec("{?= call procedure_name(?, ?, ?)}", true); //$NON-NLS-1$
        helpTestGetExec("{?=\ncall procedure_name(?, ?, ?)}", true); //$NON-NLS-1$
    }
    
    public void testIllegalCalls() {
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
    
    public void testGetExec() throws QueryParserException {
        helpTestGetExec("{call procedure_name(?, ?, ?)}", false); //$NON-NLS-1$
        helpTestGetExec(" {call procedure_name(?, ?, ?)}", false); //$NON-NLS-1$
        helpTestGetExec("{ call procedure_name(?, ?, ?)}", false); //$NON-NLS-1$
        helpTestGetExec("{call\tprocedure_name(?, ?, ?)}", false); //$NON-NLS-1$
        helpTestGetExec("{call procedure_name (?, ?, ?)}", false); //$NON-NLS-1$
        helpTestGetExec("{call procedure_name(?, ?, ?) }", false); //$NON-NLS-1$
        helpTestGetExec("{CALL procedure_name(?, ?, ?)} ", false); //$NON-NLS-1$
    }
    
    public void testBadCallKeyword() {
        try {
            QueryParser.getQueryParser().parseCommand("{calli procedure_name}"); //$NON-NLS-1$
            fail("expected exception"); //$NON-NLS-1$
        } catch (QueryParserException qpe) {
            assertEquals("Parsing error: Call keyword expected in callable statement", qpe.getMessage()); //$NON-NLS-1$
        }
    }

}
