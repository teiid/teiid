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
