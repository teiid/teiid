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
package com.metamatrix.server.integration;

import java.sql.SQLXML;
import java.util.List;
import java.util.Properties;

import org.teiid.metadata.index.VDBMetadataFactory;

import junit.framework.TestCase;

import com.metamatrix.common.buffer.BufferManager;
import com.metamatrix.common.buffer.BufferManagerFactory;
import com.metamatrix.common.buffer.TupleSource;
import com.metamatrix.common.buffer.TupleSourceID;
import com.metamatrix.query.analysis.AnalysisRecord;
import com.metamatrix.query.metadata.QueryMetadataInterface;
import com.metamatrix.query.optimizer.QueryOptimizer;
import com.metamatrix.query.optimizer.TestOptimizer;
import com.metamatrix.query.optimizer.capabilities.CapabilitiesFinder;
import com.metamatrix.query.processor.ProcessorDataManager;
import com.metamatrix.query.processor.ProcessorPlan;
import com.metamatrix.query.processor.QueryProcessor;
import com.metamatrix.query.sql.lang.Command;
import com.metamatrix.query.util.CommandContext;


/** 
 * @since 4.2
 */
public abstract class BaseQueryTest extends TestCase {

    public BaseQueryTest(String name) {
        super(name);
    }
    
    public static QueryMetadataInterface createMetadata(String vdbFile) {
        return VDBMetadataFactory.getVDBMetadata(vdbFile);
    }
        
    public static QueryMetadataInterface createMetadata(String vdbFile, String systemVDBFile) {        
    	return VDBMetadataFactory.getVDBMetadata(new String[] {vdbFile, systemVDBFile});
    }
        
    public ProcessorPlan createPlan(QueryMetadataInterface metadata, String sql, CapabilitiesFinder capFinder, boolean debug) throws Exception {
        
        Command command = TestOptimizer.helpGetCommand(sql, metadata, null);

        // plan
        AnalysisRecord analysisRecord = new AnalysisRecord(false, debug, debug);
        ProcessorPlan plan = null;
        try {
            plan = QueryOptimizer.optimizePlan(command, metadata, null, capFinder, analysisRecord, createCommandContext());
        } finally {
            if(debug) {
                System.out.println(analysisRecord.getDebugLog());
            }
        }
        
        return plan;
    }
    
    protected void doProcess(ProcessorPlan plan, ProcessorDataManager dataManager, List[] expectedResults, boolean debug) throws Exception {
        BufferManager bufferMgr = BufferManagerFactory.getStandaloneBufferManager();
        CommandContext context = createCommandContext();
        context.setProcessDebug(debug);
        QueryProcessor processor = new QueryProcessor(plan, context, bufferMgr, dataManager);
        TupleSourceID tsID = processor.getResultsID();
        processor.process();

        // Create QueryResults from TupleSource
        TupleSource ts = bufferMgr.getTupleSource(tsID);
        int count = bufferMgr.getFinalRowCount(tsID);   

        if(debug) {
            System.out.println("\nResults:\n" + bufferMgr.getTupleSchema(tsID)); //$NON-NLS-1$
            TupleSource ts2 = bufferMgr.getTupleSource(tsID);
            for(int j=0; j<count; j++) {
                System.out.println("" + j + ": " + ts2.nextTuple());     //$NON-NLS-1$ //$NON-NLS-2$
            }    
            ts2.closeSource();
        }
        
        // Compare actual to expected row count
        assertEquals("Did not get expected row count: ", expectedResults.length, count); //$NON-NLS-1$
     
        // Walk results and compare
        for(int i=0; i<count; i++) { 
            List record = ts.nextTuple();
            
            Object value = record.get(0);
            if(value instanceof SQLXML) {
                record.set(0, ((SQLXML)value).getString());
            }            
            assertEquals(expectedResults[i].get(0), record.get(0));                
        }
        ts.closeSource();        
        
        bufferMgr.removeTupleSource(tsID);
    }

    protected void doProcessNoResultsCheck(ProcessorPlan plan, ProcessorDataManager dataManager, int expectedRowCount, boolean debug) throws Exception {
        BufferManager bufferMgr = BufferManagerFactory.getStandaloneBufferManager();
        CommandContext context = createCommandContext();
        QueryProcessor processor = new QueryProcessor(plan, context, bufferMgr, dataManager);
        TupleSourceID tsID = processor.getResultsID();
        processor.process();

        // Create QueryResults from TupleSource
        TupleSource ts = bufferMgr.getTupleSource(tsID);
        int count = bufferMgr.getFinalRowCount(tsID);   

        if(debug) {
            System.out.println("\nResults:\n" + bufferMgr.getTupleSchema(tsID)); //$NON-NLS-1$
            TupleSource ts2 = bufferMgr.getTupleSource(tsID);
            for(int j=0; j<count; j++) {
                System.out.println("" + j + ": " + ts2.nextTuple());     //$NON-NLS-1$ //$NON-NLS-2$
            }    
            ts2.closeSource();
        }
        
        // Compare actual to expected row count
        assertEquals("Did not get expected row count: ", expectedRowCount, count); //$NON-NLS-1$
     
        // Walk results 
        for(int i=0; i<count; i++) { 
            ts.nextTuple();
        }
        ts.closeSource();        
        bufferMgr.removeTupleSource(tsID);
    }
    
    protected CommandContext createCommandContext() {
        Properties props = new Properties();
        //props.setProperty(ContextProperties.SOAP_HOST, "my.host.com"); //$NON-NLS-1$
        CommandContext context = new CommandContext("0", "test", "user", null, "myvdb", "1", props, false, false); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$ //$NON-NLS-5$
        
        return context;
    }       
    
    public void verifyQueryPlan(String[] expectedAtomic, ProcessorPlan plan, QueryMetadataInterface md, CapabilitiesFinder capFinder) throws Exception {
        TestOptimizer.checkAtomicQueries(expectedAtomic, plan, md, capFinder);
    }
    
}
