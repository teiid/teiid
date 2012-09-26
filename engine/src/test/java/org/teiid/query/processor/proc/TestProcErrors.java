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

import java.util.Arrays;
import java.util.List;

import org.junit.Test;
import org.teiid.core.TeiidProcessingException;
import org.teiid.jdbc.TeiidSQLException;
import org.teiid.query.metadata.TransformationMetadata;
import org.teiid.query.processor.HardcodedDataManager;
import org.teiid.query.processor.ProcessorPlan;
import org.teiid.query.resolver.TestProcedureResolving;

@SuppressWarnings("nls")
public class TestProcErrors {
	
    @Test public void testInvalidException() throws Exception {
    	String ddl = 
    			"create virtual procedure vproc (x integer) returns integer as begin declare object e = sqlexception 'hello'; raise e; raise sqlexception 'hello world' sqlstate 'abc', 1 chain e; end;";
    	try {
    		TestProcedureResolving.createMetadata(ddl);
    		fail();
    	} catch (RuntimeException e) {
    		assertEquals("TEIID31080 View test.vproc validation error: QueryResolverException-TEIID31120 An exception may only be chained to another exception. e is not valid.", e.getMessage());
    	}
    }

    @Test public void testExceptionAndWarning() throws Exception {
    	String ddl = 
    			"create virtual procedure vproc (x integer) returns integer as begin declare exception e = sqlexception 'hello'; raise sqlwarning e; raise sqlexception 'hello world' sqlstate 'abc', 1 chain e; end;";
    	TransformationMetadata tm = TestProcedureResolving.createMetadata(ddl);    	

    	String sql = "call vproc(1)"; //$NON-NLS-1$

        ProcessorPlan plan = getProcedurePlan(sql, tm);

        HardcodedDataManager dataManager = new HardcodedDataManager(tm);
        
        try {
        	helpTestProcess(plan, null, dataManager, tm);
        	fail();
        } catch (TeiidProcessingException e) {
            TeiidSQLException tsw = (TeiidSQLException) plan.getContext().getAndClearWarnings().get(0);
        	assertEquals("hello", tsw.getMessage());
        	
        	assertEquals(e.getCause().getCause(), tsw);
        	TeiidSQLException tse = (TeiidSQLException)e.getCause();
        	assertEquals("hello world", tse.getMessage());
        	assertEquals("abc", tse.getSQLState());
        	assertEquals(1, tse.getErrorCode());
        }
    }
    
    @Test public void testExceptionGroup() throws Exception {
    	String ddl = 
    			"create virtual procedure vproc () returns string as begin select 1/0; exception e \"return\" = e.state || ' ' || e.errorcode || ' ' || e.teiidcode || ' ' || cast(e.exception as string) || ' ' || cast(e.chain as string); end;";
    	TransformationMetadata tm = TestProcedureResolving.createMetadata(ddl);    	

    	String sql = "call vproc()"; //$NON-NLS-1$

        ProcessorPlan plan = getProcedurePlan(sql, tm);

        HardcodedDataManager dataManager = new HardcodedDataManager(tm);
        
    	helpTestProcess(plan, new List[] {Arrays.asList("50000 30328 TEIID30328 org.teiid.jdbc.TeiidSQLException: TEIID30328 Unable to evaluate (1 / 0): TEIID30384 Error while evaluating function / org.teiid.api.exception.query.ExpressionEvaluationException: TEIID30328 Unable to evaluate (1 / 0): TEIID30384 Error while evaluating function /")}, dataManager, tm);
    }
    
    @Test public void testExceptionHandling() throws Exception {
    	String ddl = 
    			"create virtual procedure vproc (x integer) returns integer as begin " +
    			"raise sqlexception 'hello world' sqlstate 'abc', 1;" +
    			"exception e " +
    			"raise sqlwarning sqlexception 'caught' chain e.exception; " +
    			"\"return\" = 1;"+
    			"end;";
    	TransformationMetadata tm = TestProcedureResolving.createMetadata(ddl);    	

    	String sql = "call vproc(1)"; //$NON-NLS-1$

        ProcessorPlan plan = getProcedurePlan(sql, tm);

        HardcodedDataManager dataManager = new HardcodedDataManager(tm);
        
    	helpTestProcess(plan, new List[] {Arrays.asList(1)}, dataManager, tm);
    	
    	TeiidSQLException tse = (TeiidSQLException)plan.getContext().getAndClearWarnings().get(0);
    	assertEquals("caught", tse.getMessage());
    	assertEquals("hello world", tse.getCause().getMessage());
    }
    
    /**
     * ensures that a processing error is trappable 
     */
    @Test public void testExceptionHandlingWithResultSet() throws Exception {
    	String ddl = 
    			"create virtual procedure proc2 (x integer) as begin atomic select 1; begin select 1/x; end exception e end;";
    	TransformationMetadata tm = TestProcedureResolving.createMetadata(ddl);    	

    	String sql = "call proc2(0)"; //$NON-NLS-1$

        ProcessorPlan plan = getProcedurePlan(sql, tm);

        HardcodedDataManager dataManager = new HardcodedDataManager(tm);
        
    	helpTestProcess(plan, new List[] {Arrays.asList(1)}, dataManager, tm);
    }
    
    /**
     * ensures that the whole result is formed so that the error does not escape the handler
     */
    @Test public void testExceptionHandlingWithResultSet1() throws Exception {
    	String ddl = 
    			"create virtual procedure proc2 (x integer) as begin create local temporary table t (i integer); insert into t (i) values (1); declare integer y = 0; while (y < 16) begin insert into t (i) select 1 from t; y = y+1; end insert into t (i) values (0); select cast(1/i as string) from t; exception e end;";
    	TransformationMetadata tm = TestProcedureResolving.createMetadata(ddl);    	

    	String sql = "call proc2(0)"; //$NON-NLS-1$

        ProcessorPlan plan = getProcedurePlan(sql, tm);

        HardcodedDataManager dataManager = new HardcodedDataManager(tm);
        
    	helpTestProcess(plan, new List[0], dataManager, tm);
    }
	
}
