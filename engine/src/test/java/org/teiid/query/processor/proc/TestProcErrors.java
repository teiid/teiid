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

package org.teiid.query.processor.proc;

import static org.junit.Assert.*;
import static org.teiid.query.processor.proc.TestProcedureProcessor.*;

import org.junit.Test;
import org.teiid.core.TeiidProcessingException;
import org.teiid.jdbc.TeiidSQLException;
import org.teiid.jdbc.TeiidSQLWarning;
import org.teiid.query.metadata.TransformationMetadata;
import org.teiid.query.processor.HardcodedDataManager;
import org.teiid.query.processor.ProcessorPlan;
import org.teiid.query.resolver.TestProcedureResolving;

@SuppressWarnings("nls")
public class TestProcErrors {
	
    @Test public void testInvalidException() throws Exception {
    	String ddl = 
    			"create virtual procedure vproc (x integer) returns integer as begin declare object e = sqlwarning 'hello'; raise e; raise sqlexception 'hello world' sqlstate 'abc', 1 exception e; end;";
    	try {
    		TestProcedureResolving.createMetadata(ddl);
    		fail();
    	} catch (RuntimeException e) {
    		assertEquals("TEIID31080 View test.vproc validation error: QueryResolverException-TEIID31120 An exception may only be chained to another exception. e is not valid.", e.getMessage());
    	}
    }

    @Test public void testExceptionAndWarning() throws Exception {
    	String ddl = 
    			"create virtual procedure vproc (x integer) returns integer as begin declare exception e = sqlwarning 'hello'; raise e; raise sqlexception 'hello world' sqlstate 'abc', 1 exception e; end;";
    	TransformationMetadata tm = TestProcedureResolving.createMetadata(ddl);    	

    	String sql = "call vproc(1)"; //$NON-NLS-1$

        ProcessorPlan plan = getProcedurePlan(sql, tm);

        HardcodedDataManager dataManager = new HardcodedDataManager(tm);
        
        try {
        	helpTestProcess(plan, null, dataManager, tm);
        	fail();
        } catch (TeiidProcessingException e) {
            TeiidSQLWarning tsw = (TeiidSQLWarning) plan.getContext().getAndClearWarnings().get(0);
        	assertEquals("hello", tsw.getMessage());
        	
        	assertEquals(e.getCause().getCause(), tsw);
        	TeiidSQLException tse = (TeiidSQLException)e.getCause();
        	assertEquals("hello world", tse.getMessage());
        	assertEquals("abc", tse.getSQLState());
        	assertEquals(1, tse.getErrorCode());
        }
        
    }
	
}
