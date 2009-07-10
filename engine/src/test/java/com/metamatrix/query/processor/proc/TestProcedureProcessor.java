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

package com.metamatrix.query.processor.proc;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import junit.framework.TestCase;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.api.exception.MetaMatrixException;
import com.metamatrix.api.exception.query.QueryMetadataException;
import com.metamatrix.api.exception.query.QueryParserException;
import com.metamatrix.api.exception.query.QueryPlannerException;
import com.metamatrix.api.exception.query.QueryResolverException;
import com.metamatrix.api.exception.query.QueryValidatorException;
import com.metamatrix.common.buffer.BufferManager;
import com.metamatrix.common.buffer.BufferManagerFactory;
import com.metamatrix.common.buffer.TupleSource;
import com.metamatrix.common.buffer.TupleSourceID;
import com.metamatrix.common.types.DataTypeManager;
import com.metamatrix.common.types.XMLType;
import com.metamatrix.core.MetaMatrixCoreException;
import com.metamatrix.core.util.StringUtilities;
import com.metamatrix.dqp.message.ParameterInfo;
import com.metamatrix.query.analysis.AnalysisRecord;
import com.metamatrix.query.mapping.relational.QueryNode;
import com.metamatrix.query.metadata.TempMetadataID;
import com.metamatrix.query.optimizer.QueryOptimizer;
import com.metamatrix.query.optimizer.TestOptimizer;
import com.metamatrix.query.optimizer.capabilities.AllCapabilities;
import com.metamatrix.query.optimizer.capabilities.CapabilitiesFinder;
import com.metamatrix.query.optimizer.capabilities.DefaultCapabilitiesFinder;
import com.metamatrix.query.optimizer.capabilities.FakeCapabilitiesFinder;
import com.metamatrix.query.optimizer.capabilities.SourceCapabilities;
import com.metamatrix.query.parser.QueryParser;
import com.metamatrix.query.processor.FakeDataManager;
import com.metamatrix.query.processor.ProcessorDataManager;
import com.metamatrix.query.processor.ProcessorPlan;
import com.metamatrix.query.processor.QueryProcessor;
import com.metamatrix.query.processor.TestProcessor;
import com.metamatrix.query.processor.program.Program;
import com.metamatrix.query.processor.xml.TestXMLPlanningEnhancements;
import com.metamatrix.query.processor.xml.TestXMLProcessor;
import com.metamatrix.query.resolver.QueryResolver;
import com.metamatrix.query.rewriter.QueryRewriter;
import com.metamatrix.query.sql.lang.Command;
import com.metamatrix.query.sql.symbol.ElementSymbol;
import com.metamatrix.query.unittest.FakeMetadataFacade;
import com.metamatrix.query.unittest.FakeMetadataFactory;
import com.metamatrix.query.unittest.FakeMetadataObject;
import com.metamatrix.query.unittest.FakeMetadataStore;
import com.metamatrix.query.util.CommandContext;
import com.metamatrix.query.validator.Validator;
import com.metamatrix.query.validator.ValidatorFailure;
import com.metamatrix.query.validator.ValidatorReport;

public class TestProcedureProcessor extends TestCase {

    public TestProcedureProcessor(String name) {
        super(name);
    }
	
    public static ProcessorPlan getProcedurePlan(String userQuery, FakeMetadataFacade metadata) throws Exception {
    	return getProcedurePlan(userQuery, metadata, /*capabilitiesFinder*/null);
    }
    
    public static ProcessorPlan getProcedurePlan(String userQuery, FakeMetadataFacade metadata, CapabilitiesFinder capabilitiesFinder) throws Exception {
        Command userCommand = QueryParser.getQueryParser().parseCommand(userQuery);
        QueryResolver.resolveCommand(userCommand, metadata);
        ValidatorReport report = Validator.validate(userCommand, metadata);
        
        if (report.hasItems()) {
            ValidatorFailure firstFailure = (ValidatorFailure) report.getItems().iterator().next();
            throw new QueryValidatorException(firstFailure.getMessage());
        }
        QueryRewriter.rewrite(userCommand, null, metadata, new CommandContext());

        AnalysisRecord analysisRecord = new AnalysisRecord(false, false, DEBUG);
        try {
        	if ( capabilitiesFinder == null ) capabilitiesFinder = new DefaultCapabilitiesFinder();
        	ProcessorPlan plan = QueryOptimizer.optimizePlan(userCommand, metadata, null, capabilitiesFinder, analysisRecord, null);

            // verify we can get child plans for any plan with no problem
            plan.getChildPlans();
            
            return plan;
            
        } finally {
            if(DEBUG) {
                System.out.println(analysisRecord.getDebugLog());  	
            }
        }
    }
    
    static void helpTestProcess(boolean optimistic, ProcessorPlan procPlan, int rowsUpdated, List[] expectedResults, boolean shouldFail, 
    		ProcessorDataManager dataMgr) throws SQLException, MetaMatrixCoreException {
        // Process twice, testing reset and clone method of Processor plan
        for (int i=1; i<=2; i++) {
	        BufferManager bufferMgr = BufferManagerFactory.getStandaloneBufferManager();
            CommandContext context = new CommandContext("pID", null, null, null, null); //$NON-NLS-1$
            context.getNextRand(0);
            context.setOptimisticTransaction(optimistic);
            context.setProcessDebug(DEBUG);
            QueryProcessor processor = new QueryProcessor(procPlan, context, bufferMgr, dataMgr);
            TupleSourceID tsID = processor.getResultsID();  
	        processor.process();
	
	        // Create QueryResults from TupleSource
	        TupleSource ts = bufferMgr.getTupleSource(tsID);
	
            if(DEBUG) {
                int count = bufferMgr.getFinalRowCount(tsID);   
                System.out.println("\nResults:\n" + bufferMgr.getTupleSchema(tsID)); //$NON-NLS-1$
                TupleSource ts2 = bufferMgr.getTupleSource(tsID);
                for(int j=0; j<count; j++) {
                    System.out.println("" + j + ": " + ts2.nextTuple());     //$NON-NLS-1$ //$NON-NLS-2$
                }    
                ts2.closeSource();
            }
            
            if (shouldFail) {
                fail("Expected processing to fail"); //$NON-NLS-1$
            }
            
            int count = bufferMgr.getFinalRowCount(tsID);
                   
            if (expectedResults == null) {
                assertEquals("Incorrect number of rows: ", 1, count);   //$NON-NLS-1$
                List tuple = ts.nextTuple();   	
    	        assertEquals("Incorrect number of columns: ", 1, tuple.size());	 //$NON-NLS-1$
                
                Integer rowCount = (Integer) tuple.get(0);                
                assertEquals("Rows updated mismatch: ", rowsUpdated, rowCount.intValue()); //$NON-NLS-1$
            }
	        
            if (expectedResults != null) {
    	        //Walk results and compare
                ts = bufferMgr.getTupleSource(tsID);
                assertEquals("Incorrect number of rows: ", expectedResults.length, count);   //$NON-NLS-1$
                for(int j=0; j<count; j++) { 
                    List record = ts.nextTuple();  
                    
                    //handle xml
                    if(record.size() == 1 && expectedResults[j].size() == 1){
                        Object cellValue = record.get(0);
                        if(cellValue instanceof XMLType){
                            XMLType id =  (XMLType)cellValue; 
                            String actualDoc = id.getString(); 
                            TestXMLProcessor.compareDocuments((String)expectedResults[j].get(0), actualDoc);
                            continue;
                        }
                    }
                    assertEquals("Row " + j + " does not match expected: ", expectedResults[j], record);                 //$NON-NLS-1$ //$NON-NLS-2$
                }
                ts.closeSource();
            }
            
            //reset and clone after 1st run
            if (i==1) {
                procPlan.reset();
                procPlan = (ProcessorPlan)procPlan.clone();
            }
        }
    }

    private void helpTestProcessFailure(boolean optimistic, ProcessorPlan procPlan, 
                                 FakeDataManager dataMgr, String failMessage) throws SQLException, MetaMatrixCoreException {
        try {
            helpTestProcess(optimistic, procPlan, new List[] {}, dataMgr, true);
        } catch(MetaMatrixException ex) {
            assertEquals(failMessage, ex.getMessage());
        }
    }
    
    public static void helpTestProcess(ProcessorPlan procPlan, List[] expectedResults, ProcessorDataManager dataMgr) throws SQLException, MetaMatrixCoreException {
        helpTestProcess(false, procPlan, expectedResults, dataMgr, false);
    }
    
    private void helpTestProcess(ProcessorPlan procPlan, int expectedRows, FakeDataManager dataMgr) throws SQLException, MetaMatrixCoreException {
        helpTestProcess(false, procPlan, expectedRows, null, false, dataMgr);
    }
    
    static void helpTestProcess(boolean optimistic, ProcessorPlan procPlan, List[] expectedResults, 
    		ProcessorDataManager dataMgr, boolean shouldFail) throws SQLException, MetaMatrixCoreException {
        helpTestProcess(optimistic, procPlan, 0, expectedResults, shouldFail, dataMgr);
    }

    // Helper to create a list of elements - used in creating sample data
    private static List createElements(List elementIDs) { 
        List elements = new ArrayList();
        for(int i=0; i<elementIDs.size(); i++) {
            FakeMetadataObject elementID = (FakeMetadataObject) elementIDs.get(i);            
            ElementSymbol element = new ElementSymbol(elementID.getName());
            elements.add(element);
        }        
        
        return elements;
    }    
    
    private FakeDataManager exampleDataManager(FakeMetadataFacade metadata) throws QueryMetadataException, MetaMatrixComponentException {
        FakeDataManager dataMgr = new FakeDataManager();
    
        FakeMetadataObject groupID = (FakeMetadataObject) metadata.getGroupID("pm1.g1"); //$NON-NLS-1$
        List elementIDs = metadata.getElementIDsInGroupID(groupID);
        List elementSymbols = createElements(elementIDs);
    
        dataMgr.registerTuples(
            groupID,
            elementSymbols,
            
            new List[] { 
                Arrays.asList( new Object[] { "First", new Integer(5), new Boolean(true), new Double(1.003)} ), //$NON-NLS-1$
                Arrays.asList( new Object[] { "Second", new Integer(15), new Boolean(true), new Double(2.003)} ), //$NON-NLS-1$
                Arrays.asList( new Object[] { "Third", new Integer(51), new Boolean(true), new Double(3.003)} ) //$NON-NLS-1$
                } );

        groupID = (FakeMetadataObject) metadata.getGroupID("pm1.g2"); //$NON-NLS-1$
        elementIDs = metadata.getElementIDsInGroupID(groupID);
        elementSymbols = createElements(elementIDs);
    
        dataMgr.registerTuples(
            groupID,
            elementSymbols,
            
            new List[] { 
                Arrays.asList( new Object[] { "First", new Integer(5), new Boolean(true), new Double(1.003)} ), //$NON-NLS-1$
                Arrays.asList( new Object[] { "Second", new Integer(15), new Boolean(true), new Double(2.003)} ), //$NON-NLS-1$
                Arrays.asList( new Object[] { "Third", new Integer(51), new Boolean(true), new Double(3.003)} ) //$NON-NLS-1$
                } );
        
        return dataMgr;
    }    
    
    private FakeDataManager exampleDataManager2(FakeMetadataFacade metadata) throws QueryMetadataException, MetaMatrixComponentException {
        FakeDataManager dataMgr = new FakeDataManager();
    
        FakeMetadataObject groupID = (FakeMetadataObject) metadata.getGroupID("pm1.g1"); //$NON-NLS-1$
        List elementIDs = metadata.getElementIDsInGroupID(groupID);
        List elementSymbols = createElements(elementIDs);
    
        dataMgr.registerTuples(
            groupID,
            elementSymbols,
            
            new List[] { 
                Arrays.asList( new Object[] { "First", new Integer(5), new Boolean(true), new Double(1.003)} ), //$NON-NLS-1$
                Arrays.asList( new Object[] { "Second", new Integer(15), new Boolean(true), new Double(2.003)} ), //$NON-NLS-1$
                Arrays.asList( new Object[] { "Third", new Integer(51), new Boolean(true), new Double(3.003)} ) //$NON-NLS-1$
                } );

        groupID = (FakeMetadataObject) metadata.getGroupID("pm1.g2"); //$NON-NLS-1$
        elementIDs = metadata.getElementIDsInGroupID(groupID);
        elementSymbols = createElements(elementIDs);
    
        dataMgr.registerTuples(
            groupID,
            elementSymbols,
            
            new List[] { 
                Arrays.asList( new Object[] { "First", new Integer(5), new Boolean(true), new Double(1.003)} ), //$NON-NLS-1$
                Arrays.asList( new Object[] { "Second", new Integer(15), new Boolean(true), new Double(2.003)} ), //$NON-NLS-1$
                Arrays.asList( new Object[] { "Third", new Integer(51), new Boolean(true), new Double(3.003)} ) //$NON-NLS-1$
                } );
        
        groupID = (FakeMetadataObject) metadata.getGroupID("pm2.g1"); //$NON-NLS-1$
        elementIDs = metadata.getElementIDsInGroupID(groupID);
        elementSymbols = createElements(elementIDs);
    
        dataMgr.registerTuples(
            groupID,
            elementSymbols,
            
            new List[] { 
                Arrays.asList( new Object[] { "First", new Integer(5), new Boolean(true), new Double(1.003)} ), //$NON-NLS-1$
                Arrays.asList( new Object[] { "Second", new Integer(15), new Boolean(true), new Double(2.003)} ), //$NON-NLS-1$
                Arrays.asList( new Object[] { "Third", new Integer(51), new Boolean(true), new Double(3.003)} ) //$NON-NLS-1$
                } );
        
        groupID = (FakeMetadataObject) metadata.getGroupID("pm2.g2"); //$NON-NLS-1$
        elementIDs = metadata.getElementIDsInGroupID(groupID);
        elementSymbols = createElements(elementIDs);
    
        dataMgr.registerTuples(
            groupID,
            elementSymbols,
            
            new List[] { 
                Arrays.asList( new Object[] { "First", new Integer(5), new Boolean(true), new Double(1.003)} ), //$NON-NLS-1$
                Arrays.asList( new Object[] { "Second", new Integer(15), new Boolean(true), new Double(2.003)} ), //$NON-NLS-1$
                Arrays.asList( new Object[] { "Third", new Integer(51), new Boolean(true), new Double(3.003)} ) //$NON-NLS-1$
                } );
        
        return dataMgr;
    }    
    
    private FakeDataManager exampleDataManagerPm5(FakeMetadataFacade metadata) throws QueryMetadataException, MetaMatrixComponentException {
        FakeDataManager dataMgr = new FakeDataManager();
    
        // Group stock.items
        FakeMetadataObject groupID = (FakeMetadataObject) metadata.getGroupID("pm5.g3"); //$NON-NLS-1$
        List elementIDs = metadata.getElementIDsInGroupID(groupID);
        List elementSymbols = createElements(elementIDs);
    
        dataMgr.registerTuples(
            groupID,
            elementSymbols,
            
            new List[] { 
                Arrays.asList( new Object[] { "First", new Short((short)5), new Boolean(true), new Double(1.003)} ), //$NON-NLS-1$
                Arrays.asList( new Object[] { "Second", new Short((short)15), new Boolean(true), new Double(2.003)} ), //$NON-NLS-1$
                Arrays.asList( new Object[] { "Third", new Short((short)51), new Boolean(true), new Double(3.003)} ) //$NON-NLS-1$
                } );
        
        return dataMgr;
    }    
    
	// procedure does nothing returns zero update count	
    public void testProcedureProcessor1() throws Exception {
        String procedure = "CREATE PROCEDURE  "; //$NON-NLS-1$
        procedure = procedure + "BEGIN\n"; //$NON-NLS-1$
        procedure = procedure + "DECLARE integer var1 = 0;\n"; //$NON-NLS-1$
		procedure = procedure + "ROWS_UPDATED = ROWS_UPDATED + var1;\n"; //$NON-NLS-1$
        procedure = procedure + "END\n"; //$NON-NLS-1$

        String userUpdateStr = "UPDATE vm1.g1 SET e1='x'"; //$NON-NLS-1$
        
        FakeMetadataFacade metadata = FakeMetadataFactory.exampleUpdateProc(FakeMetadataObject.Props.UPDATE_PROCEDURE, procedure);
        
        FakeDataManager dataMgr = exampleDataManager(metadata);

		ProcessorPlan plan = getProcedurePlan(userUpdateStr, metadata);
									 
		helpTestProcess(plan, 0, dataMgr);									 
    }

	// testing if statement    
    public void testProcedureProcessor2() throws Exception {
        String procedure = "CREATE PROCEDURE  "; //$NON-NLS-1$
        procedure = procedure + "BEGIN\n"; //$NON-NLS-1$
        procedure = procedure + "DECLARE integer var1;\n"; //$NON-NLS-1$
        procedure = procedure + "var1 = Select pm1.g1.e2 from pm1.g1 where e2=5;\n"; //$NON-NLS-1$
		procedure = procedure + "if(var1 = 5)\n"; //$NON-NLS-1$
        procedure = procedure + "BEGIN\n";		 //$NON-NLS-1$
		procedure = procedure + "ROWS_UPDATED = ROWS_UPDATED + var1;\n"; //$NON-NLS-1$
        procedure = procedure + "END\n";		 //$NON-NLS-1$
        procedure = procedure + "END"; //$NON-NLS-1$

        String userUpdateStr = "UPDATE vm1.g1 SET e1='x'"; //$NON-NLS-1$
        
        FakeMetadataFacade metadata = FakeMetadataFactory.exampleUpdateProc(FakeMetadataObject.Props.UPDATE_PROCEDURE, procedure);
        
        FakeDataManager dataMgr = exampleDataManager(metadata);

		ProcessorPlan plan = getProcedurePlan(userUpdateStr, metadata);
									 
		helpTestProcess(plan, 5, dataMgr);
    }
    
    // testing if statement    
    public void testProcedureProcessor2WithBlockedException() throws Exception  {
        String procedure = "CREATE PROCEDURE  "; //$NON-NLS-1$
        procedure = procedure + "BEGIN\n"; //$NON-NLS-1$
        procedure = procedure + "DECLARE integer var1;\n"; //$NON-NLS-1$
        procedure = procedure + "var1 = Select pm1.g1.e2 from pm1.g1 where e2=5;\n"; //$NON-NLS-1$
        procedure = procedure + "if(var1 = 5)\n"; //$NON-NLS-1$
        procedure = procedure + "BEGIN\n";       //$NON-NLS-1$
        procedure = procedure + "ROWS_UPDATED = ROWS_UPDATED + var1;\n"; //$NON-NLS-1$
        procedure = procedure + "END\n";         //$NON-NLS-1$
        procedure = procedure + "END"; //$NON-NLS-1$

        String userUpdateStr = "UPDATE vm1.g1 SET e1='x'"; //$NON-NLS-1$
        
        FakeMetadataFacade metadata = FakeMetadataFactory.exampleUpdateProc(FakeMetadataObject.Props.UPDATE_PROCEDURE, procedure);
        
        FakeDataManager dataMgr = exampleDataManager(metadata);
        dataMgr.setBlockOnce();

        ProcessorPlan plan = getProcedurePlan(userUpdateStr, metadata);
                                     
        helpTestProcess(plan, 5, dataMgr);
    }

	// testing rows updated incremented, Input and assignment statements
    public void testProcedureProcessor3() throws Exception {
        String procedure = "CREATE PROCEDURE  "; //$NON-NLS-1$
        procedure = procedure + "BEGIN\n"; //$NON-NLS-1$
        procedure = procedure + "DECLARE integer var1;\n"; //$NON-NLS-1$
        procedure = procedure + "ROWS_UPDATED = Select pm1.g1.e2 from pm1.g1 where e2=5;\n"; //$NON-NLS-1$
		procedure = procedure + "var1 = INPUT.e2;\n"; //$NON-NLS-1$
		procedure = procedure + "ROWS_UPDATED = ROWS_UPDATED + var1;\n"; //$NON-NLS-1$
        procedure = procedure + "END\n"; //$NON-NLS-1$

        String userUpdateStr = "UPDATE vm1.g1 SET e2=40"; //$NON-NLS-1$
        
        FakeMetadataFacade metadata = FakeMetadataFactory.exampleUpdateProc(FakeMetadataObject.Props.UPDATE_PROCEDURE, procedure);
        
        FakeDataManager dataMgr = exampleDataManager(metadata);

		ProcessorPlan plan = getProcedurePlan(userUpdateStr, metadata);
									 
		helpTestProcess(plan, 45, dataMgr);									 
    }
    
    // if/else test
    public void testProcedureProcessor4() throws Exception {
        String procedure = "CREATE PROCEDURE  "; //$NON-NLS-1$
        procedure = procedure + "BEGIN\n"; //$NON-NLS-1$
        procedure = procedure + "DECLARE integer var1;\n"; //$NON-NLS-1$
        procedure = procedure + "var1 = Select pm1.g1.e2 from pm1.g1 where e2=5;\n"; //$NON-NLS-1$
		procedure = procedure + "if(var1 = 5)\n"; //$NON-NLS-1$
        procedure = procedure + "BEGIN\n";		 //$NON-NLS-1$
		procedure = procedure + "ROWS_UPDATED = ROWS_UPDATED + var1;\n"; //$NON-NLS-1$
        procedure = procedure + "END\n"; //$NON-NLS-1$
        procedure = procedure + "ELSE\n"; //$NON-NLS-1$
        procedure = procedure + "BEGIN\n";         //$NON-NLS-1$
        procedure = procedure + "ROWS_UPDATED = Select pm1.g1.e2 from pm1.g1 where e2=5;\n"; //$NON-NLS-1$
        procedure = procedure + "UPDATE pm1.g1 SET pm1.g1.e1 = INPUT.e1, pm1.g1.e2 = var1;\n"; //$NON-NLS-1$
		procedure = procedure + "var1 = INPUT.e2;\n"; //$NON-NLS-1$
		procedure = procedure + "ROWS_UPDATED = ROWS_UPDATED + var1;\n";         //$NON-NLS-1$
        procedure = procedure + "END\n"; //$NON-NLS-1$
        procedure = procedure + "END";         //$NON-NLS-1$

        String userUpdateStr = "UPDATE vm1.g1 SET e2=45"; //$NON-NLS-1$
        
        FakeMetadataFacade metadata = FakeMetadataFactory.exampleUpdateProc(FakeMetadataObject.Props.UPDATE_PROCEDURE, procedure);
        
        FakeDataManager dataMgr = exampleDataManager(metadata);

		ProcessorPlan plan = getProcedurePlan(userUpdateStr, metadata);
									 
		helpTestProcess(plan, 5, dataMgr);									 
    }
    
    public void testProcedureProcessor4WithBlockedException() throws Exception {
        String procedure = "CREATE PROCEDURE  "; //$NON-NLS-1$
        procedure = procedure + "BEGIN\n"; //$NON-NLS-1$
        procedure = procedure + "DECLARE integer var1;\n"; //$NON-NLS-1$
        procedure = procedure + "var1 = Select pm1.g1.e2 from pm1.g1 where e2=5;\n"; //$NON-NLS-1$
        procedure = procedure + "if(var1 = 5)\n"; //$NON-NLS-1$
        procedure = procedure + "BEGIN\n";       //$NON-NLS-1$
        procedure = procedure + "ROWS_UPDATED = ROWS_UPDATED + var1;\n"; //$NON-NLS-1$
        procedure = procedure + "END\n"; //$NON-NLS-1$
        procedure = procedure + "ELSE\n"; //$NON-NLS-1$
        procedure = procedure + "BEGIN\n";         //$NON-NLS-1$
        procedure = procedure + "ROWS_UPDATED = Select pm1.g1.e2 from pm1.g1 where e2=5;\n"; //$NON-NLS-1$
        procedure = procedure + "UPDATE pm1.g1 SET pm1.g1.e1 = INPUT.e1, pm1.g1.e2 = var1;\n"; //$NON-NLS-1$
        procedure = procedure + "var1 = INPUT.e2;\n"; //$NON-NLS-1$
        procedure = procedure + "ROWS_UPDATED = ROWS_UPDATED + var1;\n";         //$NON-NLS-1$
        procedure = procedure + "END\n"; //$NON-NLS-1$
        procedure = procedure + "END";         //$NON-NLS-1$

        String userUpdateStr = "UPDATE vm1.g1 SET e2=45"; //$NON-NLS-1$
        
        FakeMetadataFacade metadata = FakeMetadataFactory.exampleUpdateProc(FakeMetadataObject.Props.UPDATE_PROCEDURE, procedure);
        
        FakeDataManager dataMgr = exampleDataManager(metadata);
        dataMgr.setBlockOnce();

        ProcessorPlan plan = getProcedurePlan(userUpdateStr, metadata);
                                     
        helpTestProcess(plan, 5, dataMgr);                                     
    }

    // if/else test    
    public void testProcedureProcessor5() throws Exception {
        String procedure = "CREATE PROCEDURE  "; //$NON-NLS-1$
        procedure = procedure + "BEGIN\n"; //$NON-NLS-1$
        procedure = procedure + "DECLARE integer var1;\n"; //$NON-NLS-1$
        procedure = procedure + "var1 = Select pm1.g1.e2 from pm1.g1 where e2=15;\n"; //$NON-NLS-1$
		procedure = procedure + "if(var1 = 5)\n"; //$NON-NLS-1$
        procedure = procedure + "BEGIN\n";		 //$NON-NLS-1$
		procedure = procedure + "ROWS_UPDATED = ROWS_UPDATED + var1;\n"; //$NON-NLS-1$
        procedure = procedure + "END\n"; //$NON-NLS-1$
        procedure = procedure + "ELSE\n"; //$NON-NLS-1$
        procedure = procedure + "ROWS_UPDATED = Select pm1.g1.e2 from pm1.g1 where e2=5;\n"; //$NON-NLS-1$
		procedure = procedure + "var1 = INPUT.e2;\n"; //$NON-NLS-1$
		procedure = procedure + "ROWS_UPDATED = ROWS_UPDATED + var1;\n";         //$NON-NLS-1$
        procedure = procedure + "END"; //$NON-NLS-1$

        String userUpdateStr = "UPDATE vm1.g1 SET e2=45"; //$NON-NLS-1$
        
        FakeMetadataFacade metadata = FakeMetadataFactory.exampleUpdateProc(FakeMetadataObject.Props.UPDATE_PROCEDURE, procedure);
        
        FakeDataManager dataMgr = exampleDataManager(metadata);

		ProcessorPlan plan = getProcedurePlan(userUpdateStr, metadata);
									 
		helpTestProcess(plan, 50, dataMgr);									 
    }
    
    // more rows than expected
    public void testProcedureProcessor6() throws Exception {
        String procedure = "CREATE PROCEDURE  "; //$NON-NLS-1$
        procedure = procedure + "BEGIN\n"; //$NON-NLS-1$
        procedure = procedure + "ROWS_UPDATED = Select pm1.g1.e2 from pm1.g1;\n"; //$NON-NLS-1$
        procedure = procedure + "END\n"; //$NON-NLS-1$

        String userUpdateStr = "UPDATE vm1.g1 SET e1='x'"; //$NON-NLS-1$
        
        FakeMetadataFacade metadata = FakeMetadataFactory.exampleUpdateProc(FakeMetadataObject.Props.UPDATE_PROCEDURE, procedure);
        
        FakeDataManager dataMgr = exampleDataManager(metadata);

		ProcessorPlan plan = getProcedurePlan(userUpdateStr, metadata);
									 
        helpTestProcessFailure(false, plan, dataMgr, "Error Code:ERR.015.006.0019 Message:Error processing the AssignmentStatement in stored procedure language, expected to get a single row of data to be assigned to the variable ROWS_UPDATED but got more."); //$NON-NLS-1$ 
    }

    // error statement
    public void testProcedureProcessor7() throws Exception {
        String errorValue = "\"MY ERROR\""; //$NON-NLS-1$
        helpTestErrorStatment(errorValue, "MY ERROR"); //$NON-NLS-1$
    }
    
    public void testProcedureProcessor8() throws Exception {
        String errorValue = "var1"; //$NON-NLS-1$
        helpTestErrorStatment(errorValue, "5"); //$NON-NLS-1$
    }
    
    public void testProcedureProcessor9() throws Exception {
        String errorValue = "var1||\"MY ERROR\""; //$NON-NLS-1$
        helpTestErrorStatment(errorValue, "5MY ERROR"); //$NON-NLS-1$
    }
        
    public void testProcedureProcessor10() throws Exception {
        String procedure = "CREATE PROCEDURE  "; //$NON-NLS-1$
        procedure = procedure + "BEGIN\n"; //$NON-NLS-1$
        procedure = procedure + "loop on (Select pm1.g1.e2 from pm1.g1 where e2 = 5) as mycursor\n"; //$NON-NLS-1$
        procedure = procedure + "BEGIN\n"; //$NON-NLS-1$ 
        procedure = procedure + "ERROR (mycursor.e2||\"MY ERROR\");\n"; //$NON-NLS-1$
        procedure = procedure + "ROWS_UPDATED = 0;\n"; //$NON-NLS-1$
        procedure = procedure + "END\n"; //$NON-NLS-1$
        procedure = procedure + "END\n"; //$NON-NLS-1$

        String userUpdateStr = "UPDATE vm1.g1 SET e1='x'"; //$NON-NLS-1$
        
        FakeMetadataFacade metadata = FakeMetadataFactory.exampleUpdateProc(FakeMetadataObject.Props.UPDATE_PROCEDURE, procedure);
        
        FakeDataManager dataMgr = exampleDataManager(metadata);

        ProcessorPlan plan = getProcedurePlan(userUpdateStr, metadata);
                                     
        helpTestProcessFailure(false, plan, dataMgr, ErrorInstruction.ERROR_PREFIX + "5MY ERROR"); //$NON-NLS-1$ 
    }

    private void helpTestErrorStatment(String errorValue, String expected) throws Exception {
        String procedure = "CREATE PROCEDURE  "; //$NON-NLS-1$
        procedure = procedure + "BEGIN\n"; //$NON-NLS-1$
        procedure = procedure + "DECLARE integer var1;\n"; //$NON-NLS-1$
        procedure = procedure + "var1 = 5;\n"; //$NON-NLS-1$
        procedure = procedure + "ERROR "+errorValue+";\n"; //$NON-NLS-1$ //$NON-NLS-2$
        procedure = procedure + "ROWS_UPDATED = 0;\n"; //$NON-NLS-1$
        procedure = procedure + "END\n"; //$NON-NLS-1$

        String userUpdateStr = "UPDATE vm1.g1 SET e1='x'"; //$NON-NLS-1$
        
        FakeMetadataFacade metadata = FakeMetadataFactory.exampleUpdateProc(FakeMetadataObject.Props.UPDATE_PROCEDURE, procedure);
        
        FakeDataManager dataMgr = exampleDataManager(metadata);

		ProcessorPlan plan = getProcedurePlan(userUpdateStr, metadata);
									 
        helpTestProcessFailure(false, plan, dataMgr, ErrorInstruction.ERROR_PREFIX + expected); 
    }
    
	/** test if statement's if block with lookup in if condition */
	public void testLookupFunction1() throws Exception {     
		String procedure = "CREATE PROCEDURE "; //$NON-NLS-1$
		procedure = procedure + "BEGIN\n"; //$NON-NLS-1$
		procedure = procedure + "DECLARE integer var2;\n"; //$NON-NLS-1$
		procedure = procedure + "if('xyz' = lookup('pm1.g1','e1', 'e2', 5))\n"; //$NON-NLS-1$
		procedure = procedure + "BEGIN\n";         //$NON-NLS-1$
		procedure = procedure + "var2 = INPUT.e2;\n"; //$NON-NLS-1$
		procedure = procedure + "ROWS_UPDATED = ROWS_UPDATED +1;\n"; //$NON-NLS-1$
		procedure = procedure + "END\n"; //$NON-NLS-1$
		procedure = procedure + "ELSE\n"; //$NON-NLS-1$
		procedure = procedure + "BEGIN\n"; //$NON-NLS-1$
		procedure = procedure + "var2 = 100;\n"; //$NON-NLS-1$
		procedure = procedure + "ROWS_UPDATED = ROWS_UPDATED +13;\n"; //$NON-NLS-1$
		procedure = procedure + "END\n";		 //$NON-NLS-1$
		procedure = procedure + "END\n"; //$NON-NLS-1$
		
		String userUpdateStr = "UPDATE vm1.g1 SET e2=30";    //$NON-NLS-1$
		 
		FakeMetadataFacade metadata = FakeMetadataFactory.exampleUpdateProc(FakeMetadataObject.Props.UPDATE_PROCEDURE, procedure);
		FakeDataManager dataMgr = new FakeDataManager();
		
		Map valueMap = new HashMap();
		valueMap.put( new Integer(5), "xyz"); //$NON-NLS-1$
		dataMgr.defineCodeTable("pm1.g1", "e2", "e1", valueMap); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$

		ProcessorPlan plan = getProcedurePlan(userUpdateStr, metadata);                         
		helpTestProcess(plan, 1, dataMgr);        

	}
	
	/** test if statement's else block with lookup in if condition */
	public void testLookupFunction2() throws Exception {     
		String procedure = "CREATE PROCEDURE "; //$NON-NLS-1$
		procedure = procedure + "BEGIN\n"; //$NON-NLS-1$
		procedure = procedure + "DECLARE integer var2;\n"; //$NON-NLS-1$
		procedure = procedure + "if('xy' = lookup('pm1.g1','e1', 'e2', 5))\n"; //$NON-NLS-1$
		procedure = procedure + "BEGIN\n";         //$NON-NLS-1$
		procedure = procedure + "var2 = INPUT.e2;\n"; //$NON-NLS-1$
		procedure = procedure + "ROWS_UPDATED = ROWS_UPDATED +1;\n"; //$NON-NLS-1$
		procedure = procedure + "END\n"; //$NON-NLS-1$
		procedure = procedure + "ELSE\n"; //$NON-NLS-1$
		procedure = procedure + "BEGIN\n"; //$NON-NLS-1$
		procedure = procedure + "var2 = 100;\n"; //$NON-NLS-1$
		procedure = procedure + "ROWS_UPDATED = ROWS_UPDATED +12;\n"; //$NON-NLS-1$
		procedure = procedure + "END\n";		 //$NON-NLS-1$
		procedure = procedure + "END\n"; //$NON-NLS-1$
		
		String userUpdateStr = "UPDATE vm1.g1 SET e2=30";    //$NON-NLS-1$
		 
		FakeMetadataFacade metadata = FakeMetadataFactory.exampleUpdateProc(FakeMetadataObject.Props.UPDATE_PROCEDURE, procedure);
		FakeDataManager dataMgr = new FakeDataManager();
		
		Map valueMap = new HashMap();
		valueMap.put( new Integer(5), "xyz"); //$NON-NLS-1$
		dataMgr.defineCodeTable("pm1.g1", "e2", "e1", valueMap); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
		
		ProcessorPlan plan = getProcedurePlan(userUpdateStr, metadata);                         
		helpTestProcess(plan, 12, dataMgr);        
	}

    public void testVirtualProcedure() throws Exception {
        String userUpdateStr = "EXEC pm1.vsp2()"; //$NON-NLS-1$
    
        FakeMetadataFacade metadata = FakeMetadataFactory.example1Cached();

        FakeDataManager dataMgr = exampleDataManager(metadata);

        ProcessorPlan plan = getProcedurePlan(userUpdateStr, metadata);
                
        // Create expected results
        List[] expected = new List[] {
            Arrays.asList(new Object[] { "First"}),  //$NON-NLS-1$
            Arrays.asList(new Object[] { "Second"}),  //$NON-NLS-1$
            Arrays.asList(new Object[] { "Third"})};         //$NON-NLS-1$
        helpTestProcess(plan, expected, dataMgr);
    }
    
    public void testVirtualProcedureWithBlockedException() throws Exception {
        String userUpdateStr = "EXEC pm1.vsp2()"; //$NON-NLS-1$
    
        FakeMetadataFacade metadata = FakeMetadataFactory.example1Cached();

        FakeDataManager dataMgr = exampleDataManager(metadata);
        dataMgr.setBlockOnce();
        
        ProcessorPlan plan = getProcedurePlan(userUpdateStr, metadata);
                
        // Create expected results
        List[] expected = new List[] {
            Arrays.asList(new Object[] { "First"}),  //$NON-NLS-1$
            Arrays.asList(new Object[] { "Second"}),  //$NON-NLS-1$
            Arrays.asList(new Object[] { "Third"})};         //$NON-NLS-1$
        helpTestProcess(plan, expected, dataMgr);
    }
    
    public void testVirtualProcedure2() throws Exception {
        String userUpdateStr = "EXEC pm1.vsp3()"; //$NON-NLS-1$
    
        FakeMetadataFacade metadata = FakeMetadataFactory.example1Cached();

        FakeDataManager dataMgr = exampleDataManager(metadata);

        ProcessorPlan plan = getProcedurePlan(userUpdateStr, metadata);
                
        // Create expected results
        List[] expected = new List[] {
            Arrays.asList(new Object[] { "Third"})};         //$NON-NLS-1$
        helpTestProcess(plan, expected, dataMgr);
    }
    
    public void testVirtualProcedure3() throws Exception {
        String userUpdateStr = "EXEC pm1.vsp4()"; //$NON-NLS-1$
    
        FakeMetadataFacade metadata = FakeMetadataFactory.example1Cached();

        FakeDataManager dataMgr = exampleDataManager(metadata);

        ProcessorPlan plan = getProcedurePlan(userUpdateStr, metadata);
                
        // Create expected results
        List[] expected = new List[] {
            Arrays.asList(new Object[] { "First"})};         //$NON-NLS-1$
        helpTestProcess(plan, expected, dataMgr);
    }
        
    public void testVirtualProcedure4() throws Exception {
        String userUpdateStr = "EXEC pm1.vsp5()"; //$NON-NLS-1$
    
        FakeMetadataFacade metadata = FakeMetadataFactory.example1Cached();

        FakeDataManager dataMgr = exampleDataManager(metadata);

        ProcessorPlan plan = getProcedurePlan(userUpdateStr, metadata);
                
        // Create expected results
        List[] expected = new List[] {
            Arrays.asList(new Object[] { "First"})};         //$NON-NLS-1$
        helpTestProcess(plan, expected, dataMgr);
    }
    
    public void testVirtualProcedure5() throws Exception {
        String userUpdateStr = "EXEC pm1.vsp6()"; //$NON-NLS-1$
    
        FakeMetadataFacade metadata = FakeMetadataFactory.example1Cached();

        FakeDataManager dataMgr = exampleDataManager(metadata);

        ProcessorPlan plan = getProcedurePlan(userUpdateStr, metadata);
                
        // Create expected results
        List[] expected = new List[] {
            Arrays.asList(new Object[] { "Second"})};         //$NON-NLS-1$
        helpTestProcess(plan, expected, dataMgr);
    }
       
    public void testVirtualProcedure6() throws Exception {
        String userUpdateStr = "EXEC pm1.vsp7(5)"; //$NON-NLS-1$
    
        FakeMetadataFacade metadata = FakeMetadataFactory.example1Cached();

        FakeDataManager dataMgr = exampleDataManager(metadata);

        ProcessorPlan plan = getProcedurePlan(userUpdateStr, metadata);
                
        // Create expected results
        List[] expected = new List[] {
            Arrays.asList(new Object[] { "Second"})};         //$NON-NLS-1$
        helpTestProcess(plan, expected, dataMgr);
    }
          
    public void testVirtualProcedure7() throws Exception {
        String userUpdateStr = "EXEC pm1.vsp8(51)"; //$NON-NLS-1$
    
        FakeMetadataFacade metadata = FakeMetadataFactory.example1Cached();

        FakeDataManager dataMgr = exampleDataManager(metadata);

        ProcessorPlan plan = getProcedurePlan(userUpdateStr, metadata);
                
        // Create expected results
        List[] expected = new List[] {
            Arrays.asList(new Object[] { "Third"})};         //$NON-NLS-1$
        helpTestProcess(plan, expected, dataMgr);
    }
              
    public void testVirtualProcedure8() throws Exception {
        String userUpdateStr = "EXEC pm1.vsp9(51)"; //$NON-NLS-1$
    
        FakeMetadataFacade metadata = FakeMetadataFactory.example1Cached();

        FakeDataManager dataMgr = exampleDataManager(metadata);

        ProcessorPlan plan = getProcedurePlan(userUpdateStr, metadata);
                
        // Create expected results
        List[] expected = new List[] {
            Arrays.asList(new Object[] { "Third"})};         //$NON-NLS-1$
        helpTestProcess(plan, expected, dataMgr);
    }
             
    public void testVirtualProcedure9() throws Exception {
        String userUpdateStr = "EXEC pm1.vsp10(51)"; //$NON-NLS-1$
    
        FakeMetadataFacade metadata = FakeMetadataFactory.example1Cached();

        FakeDataManager dataMgr = exampleDataManager(metadata);

        ProcessorPlan plan = getProcedurePlan(userUpdateStr, metadata);
                
        // Create expected results
        List[] expected = new List[] {};        
        helpTestProcess(plan, expected, dataMgr);
    }

              
    public void testVirtualProcedure10() throws Exception {
        String userUpdateStr = "EXEC pm1.vsp13()"; //$NON-NLS-1$
    
        FakeMetadataFacade metadata = FakeMetadataFactory.example1Cached();

        FakeDataManager dataMgr = exampleDataManager(metadata);

        ProcessorPlan plan = getProcedurePlan(userUpdateStr, metadata);
                
        // Create expected results
        List[] expected = new List[] {
            Arrays.asList(new Object[] { "Third", new Integer(5)})};         //$NON-NLS-1$
        helpTestProcess(plan, expected, dataMgr);
    }
    
    public void testVirtualProcedure11() throws Exception {
        String userUpdateStr = "EXEC pm1.vsp14()";     //$NON-NLS-1$
        FakeMetadataFacade metadata = FakeMetadataFactory.example1Cached();
        // Set up data
        FakeDataManager dataMgr = exampleDataManager(metadata);

        ProcessorPlan plan = helpTestTempTable(userUpdateStr, metadata, dataMgr, new List[] {
            Arrays.asList( new Object[] { new Integer(1) } ) 
        } );
        
        // Create expected results
        List[] expected = new List[] {
            Arrays.asList(new Object[] { "First"}),  //$NON-NLS-1$
            Arrays.asList(new Object[] { "Second"}),  //$NON-NLS-1$
            Arrays.asList(new Object[] { "Third"})};           //$NON-NLS-1$
        helpTestProcess(plan, expected, dataMgr);
    }
    
    public void testVirtualProcedure12() throws Exception {
        String userUpdateStr = "EXEC pm1.vsp15()";     //$NON-NLS-1$
        FakeMetadataFacade metadata = FakeMetadataFactory.example1Cached();
        // Set up data
        FakeDataManager dataMgr = exampleDataManager(metadata);

        ProcessorPlan plan = helpTestTempTable(userUpdateStr, metadata, dataMgr, new List[] {
            Arrays.asList( new Object[] { new Integer(1) } ) 
        } );
                
        FakeMetadataObject groupID = (FakeMetadataObject) metadata.getGroupID("pm1.g2"); //$NON-NLS-1$
        List elementIDs = metadata.getElementIDsInGroupID(groupID);
        List elementSymbols = createElements(elementIDs);
    
        dataMgr.registerTuples(
            groupID,
            elementSymbols,
            
            new List[] { 
                Arrays.asList( new Object[] { "First", new Integer(5), new Boolean(true), new Double(1.003)} ), //$NON-NLS-1$
                Arrays.asList( new Object[] { "Third", new Integer(51), new Boolean(true), new Double(3.003)} ) //$NON-NLS-1$
                } );
        
        // Create expected results
        List[] expected = new List[] {
            Arrays.asList(new Object[] { "First"}),  //$NON-NLS-1$
            Arrays.asList(new Object[] { "Third"})};           //$NON-NLS-1$
        helpTestProcess(plan, expected, dataMgr);
    }
    
    //Defect17447_testVirtualProcedure13
    public void testVirtualProcedure13() throws Exception {
        String userUpdateStr = "EXEC pm1.vsp16()";     //$NON-NLS-1$
        FakeMetadataFacade metadata = FakeMetadataFactory.example1Cached();

        // Set up data
        FakeDataManager dataMgr = exampleDataManager(metadata);

        ProcessorPlan plan = helpTestTempTable(userUpdateStr, metadata, dataMgr, new List[] {
            Arrays.asList( new Object[] { new Integer(1) } ) 
        } );
                
        FakeMetadataObject groupID = (FakeMetadataObject) metadata.getGroupID("pm1.g2"); //$NON-NLS-1$
        List elementIDs = metadata.getElementIDsInGroupID(groupID);
        List elementSymbols = createElements(elementIDs);
    
        dataMgr.registerTuples(
            groupID,
            elementSymbols,
            
            new List[] { 
                Arrays.asList( new Object[] { "First", new Integer(5), new Boolean(true), new Double(1.003)} ), //$NON-NLS-1$
                Arrays.asList( new Object[] { "Third", new Integer(51), new Boolean(true), new Double(3.003)} ) //$NON-NLS-1$
                } );
        
        // Create expected results
        List[] expected = new List[] {
            Arrays.asList(new Object[] { "First"}),  //$NON-NLS-1$
            Arrays.asList(new Object[] { "Third"})};           //$NON-NLS-1$
        helpTestProcess(plan, expected, dataMgr);
    }
    		
    public void testVirtualProcedure14() throws Exception {
        String userUpdateStr = "EXEC pm1.vsp17()";     //$NON-NLS-1$
        FakeMetadataFacade metadata = FakeMetadataFactory.example1Cached();
        // Set up data
        FakeDataManager dataMgr = exampleDataManager(metadata);

        ProcessorPlan plan = helpTestTempTable(userUpdateStr, metadata, dataMgr, new List[] {
            Arrays.asList( new Object[] { new Integer(1) } ) 
        } );
        
        // Create expected results
        List[] expected = new List[] {
            Arrays.asList(new Object[] { "Third"})};           //$NON-NLS-1$
        helpTestProcess(plan, expected, dataMgr);
    }

    private ProcessorPlan helpTestTempTable(String userUpdateStr,
                                            FakeMetadataFacade metadata,
                                            FakeDataManager dataMgr, List[] tempdata) throws QueryParserException,
                                                                    QueryResolverException,
                                                                    MetaMatrixComponentException,
                                                                    QueryValidatorException,
                                                                    QueryPlannerException,
                                                                    QueryMetadataException {
        QueryParser parser = new QueryParser();
        Command userCommand = parser.parseCommand(userUpdateStr);
        QueryResolver.resolveCommand(userCommand, metadata);
        ValidatorReport report = Validator.validate(userCommand, metadata);
        // Get invalid objects from report
        Collection actualObjs = new ArrayList();
        report.collectInvalidObjects(actualObjs);
        if(actualObjs.size() > 0) {
            fail("Expected no failures but got some: " + report.getFailureMessage());                //$NON-NLS-1$
        }
        QueryRewriter.rewrite(userCommand, null, metadata, null);

        ProcessorPlan plan = null;        
        AnalysisRecord analysisRecord = new AnalysisRecord(false, false, DEBUG);
        try {
            plan = QueryOptimizer.optimizePlan(userCommand, metadata, null, new DefaultCapabilitiesFinder(), analysisRecord, null);
        } finally {
            if(DEBUG) {
                System.out.println(analysisRecord.getDebugLog());     
            }
        }       
        
        Object tempGroup = new TempMetadataID("#temptable", Collections.EMPTY_LIST); //$NON-NLS-1$
        List tempSymbols = new ArrayList(1);
        ElementSymbol element = new ElementSymbol("Count"); //$NON-NLS-1$
        tempSymbols.add(element);            
                    
        dataMgr.registerTuples(
            tempGroup,
            tempSymbols, tempdata);
        return plan;
    }
    
    public void testVirtualProcedure15() throws Exception {
        String userUpdateStr = "EXEC pm1.vsp19()";     //$NON-NLS-1$
        FakeMetadataFacade metadata = FakeMetadataFactory.example1Cached();

        // Set up data
        FakeDataManager dataMgr = exampleDataManager(metadata);

        ProcessorPlan plan = helpTestTempTable(userUpdateStr, metadata, dataMgr, new List[] {
            Arrays.asList( new Object[] { new Integer(1) } ) 
        } );   
        
        // Create expected results
        List[] expected = new List[] {
            Arrays.asList(new Object[] { "First"}),  //$NON-NLS-1$
            Arrays.asList(new Object[] { "Second"}),  //$NON-NLS-1$
            Arrays.asList(new Object[] { "Third"}), //$NON-NLS-1$
            Arrays.asList(new Object[] { "First"}),  //$NON-NLS-1$
            Arrays.asList(new Object[] { "Second"}),  //$NON-NLS-1$
            Arrays.asList(new Object[] { "Third"})};           //$NON-NLS-1$
        helpTestProcess(plan, expected, dataMgr);
    }
    
    public void testVirtualProcedure16() throws Exception {
        String userUpdateStr = "EXEC pm1.vsp20()";     //$NON-NLS-1$
        FakeMetadataFacade metadata = FakeMetadataFactory.example1Cached();
        // Set up data
        FakeDataManager dataMgr = exampleDataManager(metadata);

        ProcessorPlan plan = helpTestTempTable(userUpdateStr, metadata, dataMgr, new List[] {
            Arrays.asList( new Object[] { new Integer(1) } ) 
        } );  
        // Create expected results
        List[] expected = new List[] {
            Arrays.asList(new Object[] { "First"}),  //$NON-NLS-1$
            Arrays.asList(new Object[] { "Second"}),  //$NON-NLS-1$
            Arrays.asList(new Object[] { "Third"}),  //$NON-NLS-1$
            Arrays.asList(new Object[] { "Fourth"})};           //$NON-NLS-1$
        helpTestProcess(plan, expected, dataMgr);
    }
    
    public void testVirtualProcedure17() throws Exception {
        String userUpdateStr = "EXEC pm1.vsp21(7)";     //$NON-NLS-1$
        FakeMetadataFacade metadata = FakeMetadataFactory.example1Cached();
        //Set up data
        FakeDataManager dataMgr = exampleDataManager(metadata);

        ProcessorPlan plan = helpTestTempTable(userUpdateStr, metadata, dataMgr, new List[] {
            Arrays.asList( new Object[] { new Integer(1) } ) 
        } );
  
        // Create expected results
        List[] expected = new List[] {
            Arrays.asList(new Object[] { "First", new Integer(5)}),  //$NON-NLS-1$
            Arrays.asList(new Object[] { "Second", new Integer(15)}),  //$NON-NLS-1$
            Arrays.asList(new Object[] { "Third", new Integer(51)}),  //$NON-NLS-1$
            Arrays.asList(new Object[] { "Fourth", new Integer(7)})};           //$NON-NLS-1$
        helpTestProcess(plan, expected, dataMgr);
    }
    
    public void testVirtualProcedure18() throws Exception {
        String userUpdateStr = "EXEC pm1.vsp22(7)";     //$NON-NLS-1$
        FakeMetadataFacade metadata = FakeMetadataFactory.example1Cached();
        //Set up data
        FakeDataManager dataMgr = exampleDataManager(metadata);

        ProcessorPlan plan = helpTestTempTable(userUpdateStr, metadata, dataMgr, new List[] {
            Arrays.asList( new Object[] { new Integer(1) } ) 
        } );   
  
        // Create expected results
        List[] expected = new List[] {
            Arrays.asList(new Object[] { "Second", new Integer(15)}),  //$NON-NLS-1$
            Arrays.asList(new Object[] { "Third", new Integer(51)}) }; //$NON-NLS-1$
        helpTestProcess(plan, expected, dataMgr);
    }
    
    public void testVirtualProcedure19() throws Exception {
        String userUpdateStr = "EXEC pm1.vsp23(7)";     //$NON-NLS-1$
        FakeMetadataFacade metadata = FakeMetadataFactory.example1Cached();
        //Set up data
        FakeDataManager dataMgr = exampleDataManager(metadata);

        ProcessorPlan plan = helpTestTempTable(userUpdateStr, metadata, dataMgr, new List[] {
            Arrays.asList( new Object[] { new Integer(1) } ) 
        } );   
  
        // Create expected results
        List[] expected = new List[] {
            Arrays.asList(new Object[] { "Second", new Integer(15)})}; //$NON-NLS-1$
        helpTestProcess(plan, expected, dataMgr);
    }
    
    public void testVirtualProcedure19WithBlockedException() throws Exception {
        String userUpdateStr = "EXEC pm1.vsp23(7)";     //$NON-NLS-1$
        FakeMetadataFacade metadata = FakeMetadataFactory.example1Cached();
        //Set up data
        FakeDataManager dataMgr = exampleDataManager(metadata);

        ProcessorPlan plan = helpTestTempTable(userUpdateStr, metadata, dataMgr, new List[] {
            Arrays.asList( new Object[] { new Integer(1) } ) 
        } );  
        // Create expected results
        List[] expected = new List[] {
            Arrays.asList(new Object[] { "Second", new Integer(15)})}; //$NON-NLS-1$
        helpTestProcess(plan, expected, dataMgr);
    }

    public void testVirtualProcedureNoDataInTempTable() throws Exception {
        String userUpdateStr = "EXEC pm1.vsp25()";     //$NON-NLS-1$
        FakeMetadataFacade metadata = FakeMetadataFactory.example1Cached();

        //Set up data
        FakeDataManager dataMgr = exampleDataManager(metadata);

        ProcessorPlan plan = helpTestTempTable(userUpdateStr, metadata, dataMgr, new List[] {
            Arrays.asList( new Object[] { new Integer(1) } ) 
        } );   
        
        // Create expected results
        List[] expected = new List[] {};           
        helpTestProcess(plan, expected, dataMgr);
    }
    
    //procedure with Has Criteria and Translate Criteria 
    public void testDefect13625() throws Exception {
        String procedure = "CREATE PROCEDURE  "; //$NON-NLS-1$
        procedure = procedure + "BEGIN\n"; //$NON-NLS-1$
        procedure = procedure + "DECLARE integer var1;\n"; //$NON-NLS-1$
        procedure = procedure + "IF(HAS CRITERIA ON (vm1.g4.e2))\n"; //$NON-NLS-1$
        procedure = procedure + "BEGIN\n"; //$NON-NLS-1$
        procedure = procedure + "var1 = Select pm1.g1.e2 from pm1.g1 where TRANSLATE = CRITERIA ON (vm1.g4.e2);\n"; //$NON-NLS-1$
        procedure = procedure + "END\n";		 //$NON-NLS-1$
        procedure = procedure + "if(var1 = 5)\n"; //$NON-NLS-1$
        procedure = procedure + "BEGIN\n";		 //$NON-NLS-1$
		procedure = procedure + "ROWS_UPDATED = ROWS_UPDATED + var1;\n"; //$NON-NLS-1$
        procedure = procedure + "END\n";		 //$NON-NLS-1$
        procedure = procedure + "END"; //$NON-NLS-1$

        String userUpdateStr = "UPDATE g4 SET e1='x' where e2=5"; //$NON-NLS-1$
        
        FakeMetadataFacade metadata = FakeMetadataFactory.exampleUpdateProc(FakeMetadataObject.Props.UPDATE_PROCEDURE, procedure);
        
        FakeDataManager dataMgr = exampleDataManager(metadata);

		ProcessorPlan plan = getProcedurePlan(userUpdateStr, metadata);
									 
		helpTestProcess(plan, 5, dataMgr);
    }
    
    public void testVirtualProcedure30() throws Exception {
        String userUpdateStr = "EXEC pm1.vsp30()";     //$NON-NLS-1$
        FakeMetadataFacade metadata = FakeMetadataFactory.example1Cached();
    
        ProcessorPlan plan = getProcedurePlan(userUpdateStr, metadata);

        // Set up data
        FakeDataManager dataMgr = exampleDataManager(metadata);
  
        // Create expected results
        List[] expected = new List[] {
            Arrays.asList(new Object[] { "First" }),  //$NON-NLS-1$
            Arrays.asList(new Object[] { "Second"}),  //$NON-NLS-1$
            Arrays.asList(new Object[] { "Third"}) }; //$NON-NLS-1$
        helpTestProcess(plan, expected, dataMgr);
    }

    public void testVirtualProcedure31() throws Exception {
        String userUpdateStr = "EXEC pm1.vsp31(51)";     //$NON-NLS-1$
        FakeMetadataFacade metadata = FakeMetadataFactory.example1Cached();
        
        ProcessorPlan plan = getProcedurePlan(userUpdateStr, metadata);
        // Set up data
        FakeDataManager dataMgr = exampleDataManager(metadata);
  
        // Create expected results
        List[] expected = new List[] {
            Arrays.asList(new Object[] { "Third"}) }; //$NON-NLS-1$
        helpTestProcess(plan, expected, dataMgr);
    }

    public void testVirtualProcedureDefect14282() throws Exception {
        String userUpdateStr = "EXEC pm1.vsp24()"; //$NON-NLS-1$
    
        FakeMetadataFacade metadata = FakeMetadataFactory.example1Cached();

        FakeDataManager dataMgr = exampleDataManager(metadata);

        ProcessorPlan plan = getProcedurePlan(userUpdateStr, metadata);
                
        // Create expected results
        List[] expected = new List[] {
            Arrays.asList(new Object[] { "Second"})};         //$NON-NLS-1$
        helpTestProcess(plan, expected, dataMgr);
    }    
    
    public void testDefect16193() throws Exception {
        String userUpdateStr = "EXEC pm1.vsp35(51)";     //$NON-NLS-1$
        FakeMetadataFacade metadata = FakeMetadataFactory.example1Cached();
        ProcessorPlan plan = getProcedurePlan(userUpdateStr, metadata);

        // Set up data
        FakeDataManager dataMgr = exampleDataManager(metadata);
  
        // Create expected results
        List[] expected = new List[] {
            Arrays.asList(new Object[] { "Third"}) }; //$NON-NLS-1$
        helpTestProcess(plan, expected, dataMgr);
    }
    
    public void testVirtualProcedure16602() throws Exception {
        String userUpdateStr = "EXEC pm1.vsp37()";     //$NON-NLS-1$
        FakeMetadataFacade metadata = FakeMetadataFactory.example1Cached();
        ProcessorPlan plan = getProcedurePlan(userUpdateStr, metadata);
      

        // Set up data
        FakeDataManager dataMgr = exampleDataManager(metadata);
        FakeMetadataObject groupID = (FakeMetadataObject) metadata.getGroupID("pm1.g1"); //$NON-NLS-1$
        List elementSymbols = new ArrayList(); 
        ElementSymbol element = new ElementSymbol("Count"); //$NON-NLS-1$
        elementSymbols.add(element);            
                    
        dataMgr.registerTuples(
        		groupID,
        		elementSymbols,
            new List[] {
                 Arrays.asList( new Object[] { new Integer(1) } ) 
                } );
        
        // Create expected results
        List[] expected = new List[] {
            Arrays.asList(new Object[] { new Integer(1)})};           
        helpTestProcess(plan, expected, dataMgr);
    }
    
    public void testDefect16649_1() throws Exception {
        String userUpdateStr = "EXEC pm1.vsp38()";     //$NON-NLS-1$
        FakeMetadataFacade metadata = FakeMetadataFactory.example1Cached();
        ProcessorPlan plan = getProcedurePlan(userUpdateStr, metadata);

        // Set up data
        FakeDataManager dataMgr = exampleDataManager(metadata);
  
        // Create expected results
        List[] expected = new List[] {
            Arrays.asList(new Object[] { "Second"}) }; //$NON-NLS-1$
        helpTestProcess(plan, expected, dataMgr);
    }
    
    public void testDefect16649_2() throws Exception {
        String userUpdateStr = "EXEC pm1.vsp39()";     //$NON-NLS-1$
        FakeMetadataFacade metadata = FakeMetadataFactory.example1Cached();
        ProcessorPlan plan = getProcedurePlan(userUpdateStr, metadata);

        // Set up data
        FakeDataManager dataMgr = exampleDataManager(metadata);
  
        // Create expected results
        List[] expected = new List[] {
            Arrays.asList(new Object[] { "Second"}) }; //$NON-NLS-1$
        helpTestProcess(plan, expected, dataMgr);
    }
    
    public void testDefect16694() throws Exception {
        String userUpdateStr = "EXEC pm1.vsp40()";     //$NON-NLS-1$
        FakeMetadataFacade metadata = FakeMetadataFactory.example1Cached();
        ProcessorPlan plan = getProcedurePlan(userUpdateStr, metadata);

        // Set up data
        FakeDataManager dataMgr = exampleDataManager(metadata);
  
        // Create expected results
        List[] expected = new List[] {
            Arrays.asList(new Object[] { "Second"}) }; //$NON-NLS-1$
        helpTestProcess(plan, expected, dataMgr);
    }
    
    public void testDefect16707() throws Exception {
        String userUpdateStr = "EXEC pm1.vsp44(2)";     //$NON-NLS-1$
        FakeMetadataFacade metadata = FakeMetadataFactory.example1Cached();
        ProcessorPlan plan = getProcedurePlan(userUpdateStr, metadata);

        // Set up data
        FakeDataManager dataMgr = exampleDataManager(metadata);
  
        // Create expected results
        List[] expected = new List[] {
                Arrays.asList(new Object[] { "First"}),  //$NON-NLS-1$
	            Arrays.asList(new Object[] { "Second"}),  //$NON-NLS-1$
	            Arrays.asList(new Object[] { "Third"})}; //$NON-NLS-1$
        helpTestProcess(plan, expected, dataMgr);
    }
    
    public void testDefect16707_1() throws Exception {
        String userUpdateStr = "EXEC pm1.vsp43(2)";     //$NON-NLS-1$
        FakeMetadataFacade metadata = FakeMetadataFactory.example1Cached();
        ProcessorPlan plan = getProcedurePlan(userUpdateStr, metadata);

        // Set up data
        FakeDataManager dataMgr = exampleDataManager(metadata);
  
        // Create expected results
        List[] expected = new List[] {
                Arrays.asList(new Object[] { "First"}),  //$NON-NLS-1$
	            Arrays.asList(new Object[] { "Second"}),  //$NON-NLS-1$
	            Arrays.asList(new Object[] { "Third"})}; //$NON-NLS-1$
        helpTestProcess(plan, expected, dataMgr);
    }
    
    public void testDefect17451() throws Exception {
        String userUpdateStr = "EXEC pm1.vsp45()";     //$NON-NLS-1$
        FakeMetadataFacade metadata = FakeMetadataFactory.example1Cached();

        //Set up data
        FakeDataManager dataMgr = exampleDataManager(metadata);

        ProcessorPlan plan = helpTestTempTable(userUpdateStr, metadata, dataMgr, new List[] {
            Arrays.asList( new Object[] { new Integer(1) } ) 
        } );
                    
        FakeMetadataObject groupID = (FakeMetadataObject) metadata.getGroupID("pm1.g2"); //$NON-NLS-1$
        List elementIDs = metadata.getElementIDsInGroupID(groupID);
        List elementSymbols = createElements(elementIDs);
    
        dataMgr.registerTuples(
            groupID,
            elementSymbols,
            
            new List[] { 
                Arrays.asList( new Object[] { "First", new Integer(5), new Boolean(true), new Double(1.003)} ), //$NON-NLS-1$
                Arrays.asList( new Object[] { "Third", new Integer(51), new Boolean(true), new Double(3.003)} ) //$NON-NLS-1$
                } );
        
        // Create expected results
        List[] expected = new List[] {
            Arrays.asList(new Object[] { "First"}),  //$NON-NLS-1$
            Arrays.asList(new Object[] { "Third"})};           //$NON-NLS-1$
        helpTestProcess(plan, expected, dataMgr);
    }
    
    //Defect 17447
    public void testVirtualProcedure46() throws Exception {
        String userUpdateStr = "EXEC pm1.vsp46()";     //$NON-NLS-1$
        FakeMetadataFacade metadata = FakeMetadataFactory.example1Cached();

        Command userCommand = null;       
        QueryParser parser = new QueryParser();
        userCommand = parser.parseCommand(userUpdateStr);
        QueryResolver.resolveCommand(userCommand, metadata);
        QueryRewriter.rewrite(userCommand, null, metadata, null);

        ProcessorPlan plan = null;        
        AnalysisRecord analysisRecord = new AnalysisRecord(false, false, DEBUG);
        try {
            plan = QueryOptimizer.optimizePlan(userCommand, metadata, null, new DefaultCapabilitiesFinder(), analysisRecord, null);
        } finally {
            if(DEBUG) {
                System.out.println(analysisRecord.getDebugLog());     
            }
        }       

        // Set up data
        FakeDataManager dataMgr = exampleDataManager(metadata);
        
        // Create expected results
        List[] expected = new List[] {
            Arrays.asList(new Object[] { "First"}),  //$NON-NLS-1$
            Arrays.asList(new Object[] { "Second"}), //$NON-NLS-1$
            Arrays.asList(new Object[] { "Third"})};           //$NON-NLS-1$
        helpTestProcess(plan, expected, dataMgr);
    }
    
    public void testDefect17650() throws Exception {
        String procedure1 = "CREATE PROCEDURE  "; //$NON-NLS-1$
        procedure1 = procedure1 + "BEGIN\n"; //$NON-NLS-1$
        procedure1 = procedure1 + "DECLARE integer var1;\n"; //$NON-NLS-1$
        procedure1 = procedure1 + "IF(HAS CRITERIA ON (vm1.g1.e2))\n"; //$NON-NLS-1$
        procedure1 = procedure1 + "BEGIN\n"; //$NON-NLS-1$
        procedure1 = procedure1 + "ROWS_UPDATED = UPDATE vm1.g2 SET e1='x' where TRANSLATE = CRITERIA ON (vm1.g1.e2);\n"; //$NON-NLS-1$
        procedure1 = procedure1 + "END\n";         //$NON-NLS-1$
        procedure1 = procedure1 + "END"; //$NON-NLS-1$
        
        String procedure2 = "CREATE PROCEDURE  "; //$NON-NLS-1$
        procedure2 = procedure2 + "BEGIN\n"; //$NON-NLS-1$
        procedure2 = procedure2 + "DECLARE integer var1;\n"; //$NON-NLS-1$
        procedure2 = procedure2 + "Select pm1.g2.e2 from pm1.g2 where TRANSLATE CRITERIA;\n";//$NON-NLS-1$
        procedure2 = procedure2 + "ROWS_UPDATED = 5;\n"; //$NON-NLS-1$
        procedure2 = procedure2 + "END"; //$NON-NLS-1$

        String userUpdateStr = "UPDATE vm1.g1 SET e1='x' where e2=5"; //$NON-NLS-1$
        
        FakeMetadataFacade metadata = FakeMetadataFactory.exampleUpdateProc(FakeMetadataObject.Props.UPDATE_PROCEDURE, procedure1, procedure2);
        
        FakeDataManager dataMgr = exampleDataManager(metadata);

        ProcessorPlan plan = getProcedurePlan(userUpdateStr, metadata);
                                     
        helpTestProcess(plan, 5, dataMgr);
    }
    
    public void testDefect19982() throws Exception {
        String userUpdateStr = "EXEC pm1.vsp55(5)";     //$NON-NLS-1$
        FakeMetadataFacade metadata = FakeMetadataFactory.example1Cached();
        ProcessorPlan plan = getProcedurePlan(userUpdateStr, metadata);

        // Set up data
        FakeDataManager dataMgr = exampleDataManager(metadata);
  
        // Create expected results
        List[] expected = new List[] {
            Arrays.asList(new Object[] { "First", new Integer(5)}),  //$NON-NLS-1$
            Arrays.asList(new Object[] { "Second", new Integer(5)}), //$NON-NLS-1$
            Arrays.asList(new Object[] { "Third", new Integer(5)})};           //$NON-NLS-1$
        helpTestProcess(plan, expected, dataMgr);
    } 
    
    public void testCase3521() throws Exception {
        String userUpdateStr = "EXEC pm1.vsp1()"; //$NON-NLS-1$
    
        FakeMetadataFacade metadata = FakeMetadataFactory.example1Cached();

        FakeDataManager dataMgr = exampleDataManager(metadata);

        ProcessorPlan plan = getProcedurePlan(userUpdateStr, metadata);
                
        // Create expected results
        List[] expected = new List[] {
//            Arrays.asList(new Object[] { "First"}),  //$NON-NLS-1$
            Arrays.asList(new Object[] { "Second"}),  //$NON-NLS-1$
//            Arrays.asList(new Object[] { "Third"}),   //$NON-NLS-1$
        };        
        helpTestProcess(plan, expected, dataMgr);
    }

    //procedure with Has Criteria and Translate Criteria and changing
    public void testDynamicCommandWithTranslate() throws Exception {
        String procedure = "CREATE PROCEDURE  "; //$NON-NLS-1$
        procedure = procedure + "BEGIN\n"; //$NON-NLS-1$
        procedure = procedure + "DECLARE integer var1;\n"; //$NON-NLS-1$
        procedure = procedure + "IF(HAS CRITERIA ON (vm1.g4.e2))\n"; //$NON-NLS-1$
        procedure = procedure + "BEGIN\n"; //$NON-NLS-1$
        procedure = procedure + "execute string 'Select pm1.g1.e2 x, changing.e1 y from pm1.g1 where TRANSLATE = CRITERIA ON (vm1.g4.e2)' as x integer, y boolean into #temp;\n"; //$NON-NLS-1$
        procedure = procedure + "var1 = select x from #temp;\n"; //$NON-NLS-1$
        procedure = procedure + "END\n";		 //$NON-NLS-1$
        procedure = procedure + "if(var1 = 5)\n"; //$NON-NLS-1$
        procedure = procedure + "BEGIN\n";		 //$NON-NLS-1$
		procedure = procedure + "ROWS_UPDATED = ROWS_UPDATED + var1;\n"; //$NON-NLS-1$
        procedure = procedure + "END\n";		 //$NON-NLS-1$
        procedure = procedure + "END"; //$NON-NLS-1$

        String userUpdateStr = "UPDATE g4 SET e1='x' where e2=5"; //$NON-NLS-1$
        
        FakeMetadataFacade metadata = FakeMetadataFactory.exampleUpdateProc(FakeMetadataObject.Props.UPDATE_PROCEDURE, procedure);
        
        FakeDataManager dataMgr = exampleDataManager(metadata);

		ProcessorPlan plan = getProcedurePlan(userUpdateStr, metadata);
									 
		helpTestProcess(plan, 5, dataMgr);
    }
    
    public void testDynamicCommandWithIntoExpression() throws Exception {
    	
    	//Test INTO clause with expression
    	FakeMetadataFacade metadata = FakeMetadataFactory.example1();
        
        FakeMetadataObject pm1 = metadata.getStore().findObject("pm1",FakeMetadataObject.MODEL); //$NON-NLS-1$
        
        FakeMetadataObject rs2 = FakeMetadataFactory.createResultSet("pm1.rs1", pm1, new String[] { "e1" }, new String[] { DataTypeManager.DefaultDataTypes.STRING }); //$NON-NLS-1$ //$NON-NLS-2$
        FakeMetadataObject rs2p1 = FakeMetadataFactory.createParameter("ret", 1, ParameterInfo.RESULT_SET, DataTypeManager.DefaultDataTypes.OBJECT, rs2);  //$NON-NLS-1$
        QueryNode sq2n1 = new QueryNode("pm1.sq1", "CREATE VIRTUAL PROCEDURE BEGIN\n" //$NON-NLS-1$ //$NON-NLS-2$
                                        + "execute string 'SELECT e1 FROM pm1.g1 WHERE e1 = ''First''' as x string into #temp; declare string VARIABLES.RESULT = select x from #temp;select VARIABLES.RESULT; END"); //$NON-NLS-1$ 
        FakeMetadataObject sq1 = FakeMetadataFactory.createVirtualProcedure("pm1.sq1", pm1, Arrays.asList(new FakeMetadataObject[] { rs2p1 }), sq2n1);  //$NON-NLS-1$

        metadata.getStore().addObject(rs2);
        metadata.getStore().addObject(sq1);
        
        String userUpdateStr = "EXEC pm1.sq1()"; //$NON-NLS-1$
        
        FakeDataManager dataMgr = exampleDataManager(metadata);

        ProcessorPlan plan = getProcedurePlan(userUpdateStr, metadata);
    	
        //Create expected results
        List[] expected = new List[] {
            Arrays.asList(new Object[] { "First" }),  //$NON-NLS-1$
            };           
        helpTestProcess(plan, expected, dataMgr);
      }
    
    public void testDynamicCommandWithIntoAndLoop() throws Exception {
    	
    	//Test INTO clause with loop
    	FakeMetadataFacade metadata = FakeMetadataFactory.example1();
        
        FakeMetadataObject pm1 = metadata.getStore().findObject("pm1",FakeMetadataObject.MODEL); //$NON-NLS-1$
        
        FakeMetadataObject rs2 = FakeMetadataFactory.createResultSet("pm1.rs1", pm1, new String[] { "e1" }, new String[] { DataTypeManager.DefaultDataTypes.STRING }); //$NON-NLS-1$ //$NON-NLS-2$
        FakeMetadataObject rs2p1 = FakeMetadataFactory.createParameter("ret", 1, ParameterInfo.RESULT_SET, DataTypeManager.DefaultDataTypes.OBJECT, rs2);  //$NON-NLS-1$
        StringBuffer procedure = new StringBuffer("CREATE VIRTUAL PROCEDURE \n"); //$NON-NLS-1$
        procedure.append("BEGIN\n"); //$NON-NLS-1$
        procedure.append("declare integer VARIABLES.e2_total=0;\n"); //$NON-NLS-1$
        procedure.append("execute string 'SELECT e1, e2 FROM pm1.g1' as e1 string, e2 integer into #temp;\n"); //$NON-NLS-1$
        procedure.append("loop on (Select e2 from #temp where e2 > 2) as mycursor\n"); //$NON-NLS-1$
        procedure.append("BEGIN\n"); //$NON-NLS-1$
        procedure.append("IF (mycursor.e2>5) \n"); //$NON-NLS-1$
        procedure.append("VARIABLES.e2_total=VARIABLES.e2_total+mycursor.e2;\n"); //$NON-NLS-1$
        procedure.append("END\n"); //$NON-NLS-1$
        procedure.append("SELECT VARIABLES.e2_total;\n"); //$NON-NLS-1$
        procedure.append("END"); //$NON-NLS-1$
        
        
        
        QueryNode sq2n1 = new QueryNode("pm1.sq1", procedure.toString()); //$NON-NLS-1$ 
        FakeMetadataObject sq1 = FakeMetadataFactory.createVirtualProcedure("pm1.sq1", pm1, Arrays.asList(new FakeMetadataObject[] { rs2p1 }), sq2n1);  //$NON-NLS-1$

        metadata.getStore().addObject(rs2);
        metadata.getStore().addObject(sq1);
        
        String userUpdateStr = "EXEC pm1.sq1()"; //$NON-NLS-1$
        
        FakeDataManager dataMgr = exampleDataManager(metadata);

        ProcessorPlan plan = getProcedurePlan(userUpdateStr, metadata);
    	
        //Create expected results
        List[] expected = new List[] {
            Arrays.asList(new Object[] { new Integer(66)}),  
            };           
        helpTestProcess(plan, expected, dataMgr);
      }
    
    public void testDynamicCommandWithParameter() throws Exception {
        FakeMetadataFacade metadata = FakeMetadataFactory.example1();
        
        FakeMetadataObject pm1 = metadata.getStore().findObject("pm1",FakeMetadataObject.MODEL); //$NON-NLS-1$
        
        FakeMetadataObject rs2 = FakeMetadataFactory.createResultSet("pm1.rs2", pm1, new String[] { "e1", "e2" }, new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.INTEGER }); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        FakeMetadataObject rs2p1 = FakeMetadataFactory.createParameter("ret", 1, ParameterInfo.RESULT_SET, DataTypeManager.DefaultDataTypes.OBJECT, rs2);  //$NON-NLS-1$
        FakeMetadataObject rs2p2 = FakeMetadataFactory.createParameter("in", 2, ParameterInfo.IN, DataTypeManager.DefaultDataTypes.STRING, null);  //$NON-NLS-1$
        QueryNode sq2n1 = new QueryNode("pm1.sq2", "CREATE VIRTUAL PROCEDURE BEGIN\n" //$NON-NLS-1$ //$NON-NLS-2$
                                        + "execute string 'SELECT e1, e2 FROM pm1.g1 WHERE e1=pm1.sq2.in' as e1 string, e2 integer; END"); //$NON-NLS-1$ //
        FakeMetadataObject sq2 = FakeMetadataFactory.createVirtualProcedure("pm1.sq2", pm1, Arrays.asList(new FakeMetadataObject[] { rs2p1, rs2p2 }), sq2n1);  //$NON-NLS-1$

        metadata.getStore().addObject(rs2);
        metadata.getStore().addObject(sq2);
        
        String userUpdateStr = "EXEC pm1.sq2('First')"; //$NON-NLS-1$
        
        FakeDataManager dataMgr = exampleDataManager(metadata);

        ProcessorPlan plan = getProcedurePlan(userUpdateStr, metadata);
                
        // Create expected results
        List[] expected = new List[] {
                Arrays.asList(new Object[] { "First", new Integer(5) }),  //$NON-NLS-1$
        };        
        helpTestProcess(plan, expected, dataMgr);
    }
    
    public void testDynamicCommandWithUsing() throws Exception {
        FakeMetadataFacade metadata = FakeMetadataFactory.example1();
        
        FakeMetadataObject pm1 = metadata.getStore().findObject("pm1",FakeMetadataObject.MODEL); //$NON-NLS-1$
        
        FakeMetadataObject rs2 = FakeMetadataFactory.createResultSet("pm1.rs2", pm1, new String[] { "e1", "e2" }, new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.INTEGER }); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        FakeMetadataObject rs2p1 = FakeMetadataFactory.createParameter("ret", 1, ParameterInfo.RESULT_SET, DataTypeManager.DefaultDataTypes.OBJECT, rs2);  //$NON-NLS-1$
        FakeMetadataObject rs2p2 = FakeMetadataFactory.createParameter("in", 2, ParameterInfo.IN, DataTypeManager.DefaultDataTypes.STRING, null);  //$NON-NLS-1$
        QueryNode sq2n1 = new QueryNode("pm1.sq2", "CREATE VIRTUAL PROCEDURE BEGIN\n" //$NON-NLS-1$ //$NON-NLS-2$
                                        + "execute string 'SELECT e1, e2 FROM pm1.g1 WHERE e1=using.id' using id=pm1.sq2.in; END"); //$NON-NLS-1$ 
        FakeMetadataObject sq2 = FakeMetadataFactory.createVirtualProcedure("pm1.sq2", pm1, Arrays.asList(new FakeMetadataObject[] { rs2p1, rs2p2 }), sq2n1);  //$NON-NLS-1$

        metadata.getStore().addObject(rs2);
        metadata.getStore().addObject(sq2);
        
        String userUpdateStr = "EXEC pm1.sq2('First')"; //$NON-NLS-1$
        
        FakeDataManager dataMgr = exampleDataManager(metadata);

        ProcessorPlan plan = getProcedurePlan(userUpdateStr, metadata);
                
        // Create expected results
        List[] expected = new List[] {
                Arrays.asList(new Object[] { "First", new Integer(5) }),  //$NON-NLS-1$
        };        
        helpTestProcess(plan, expected, dataMgr);
    }
    
    public void testDynamicCommandWithVariable() throws Exception {
        FakeMetadataFacade metadata = FakeMetadataFactory.example1();
        
        FakeMetadataObject pm1 = metadata.getStore().findObject("pm1",FakeMetadataObject.MODEL); //$NON-NLS-1$
        
        FakeMetadataObject rs2 = FakeMetadataFactory.createResultSet("pm1.rs2", pm1, new String[] { "e1", "e2" }, new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.INTEGER }); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        FakeMetadataObject rs2p1 = FakeMetadataFactory.createParameter("ret", 1, ParameterInfo.RESULT_SET, DataTypeManager.DefaultDataTypes.OBJECT, rs2);  //$NON-NLS-1$
        FakeMetadataObject rs2p2 = FakeMetadataFactory.createParameter("in", 2, ParameterInfo.IN, DataTypeManager.DefaultDataTypes.STRING, null);  //$NON-NLS-1$
        QueryNode sq2n1 = new QueryNode("pm1.sq2", "CREATE VIRTUAL PROCEDURE BEGIN\n" //$NON-NLS-1$ //$NON-NLS-2$
                                        + "declare string VARIABLES.x; VARIABLES.x = pm1.sq2.in; execute string 'SELECT e1, e2 FROM pm1.g1 WHERE e1=VARIABLES.x'; END"); //$NON-NLS-1$ 
        FakeMetadataObject sq2 = FakeMetadataFactory.createVirtualProcedure("pm1.sq2", pm1, Arrays.asList(new FakeMetadataObject[] { rs2p1, rs2p2 }), sq2n1);  //$NON-NLS-1$

        metadata.getStore().addObject(rs2);
        metadata.getStore().addObject(sq2);
        
        String userUpdateStr = "EXEC pm1.sq2('First')"; //$NON-NLS-1$
        
        FakeDataManager dataMgr = exampleDataManager(metadata);

        ProcessorPlan plan = getProcedurePlan(userUpdateStr, metadata);
                
        // Create expected results
        List[] expected = new List[] {
                Arrays.asList(new Object[] { "First", new Integer(5) }),  //$NON-NLS-1$
        };        
        helpTestProcess(plan, expected, dataMgr);
    }

    public void testDynamicCommandWithSingleSelect() throws Exception {
    	//Test select of a single value in a DynamicCommand
        FakeMetadataFacade metadata = FakeMetadataFactory.example1();
        
        FakeMetadataObject pm1 = metadata.getStore().findObject("pm1",FakeMetadataObject.MODEL); //$NON-NLS-1$
        FakeMetadataObject rs1 = FakeMetadataFactory.createResultSet("pm1.rs2", pm1, new String[] { "e1" }, new String[] { DataTypeManager.DefaultDataTypes.STRING }); //$NON-NLS-1$ //$NON-NLS-2$ 
        FakeMetadataObject rs2p1 = FakeMetadataFactory.createParameter("ret", 1, ParameterInfo.RESULT_SET, DataTypeManager.DefaultDataTypes.OBJECT, rs1);  //$NON-NLS-1$
        QueryNode sq2n1 = new QueryNode("pm1.sq1", "CREATE VIRTUAL PROCEDURE BEGIN\n" //$NON-NLS-1$ //$NON-NLS-2$
                                        + "execute string 'SELECT 26'; END"); //$NON-NLS-1$ 
        FakeMetadataObject sq1 = FakeMetadataFactory.createVirtualProcedure("pm1.sq1", pm1, Arrays.asList(new FakeMetadataObject[] { rs2p1 }), sq2n1);  //$NON-NLS-1$

        metadata.getStore().addObject(rs1);
        metadata.getStore().addObject(sq1);
        
        String userUpdateStr = "EXEC pm1.sq1()"; //$NON-NLS-1$
        
        FakeDataManager dataMgr = exampleDataManager(metadata);

        ProcessorPlan plan = getProcedurePlan(userUpdateStr, metadata);
                
        // Create expected results
        List[] expected = new List[] {
                Arrays.asList(new Object[] { "26" }),  //$NON-NLS-1$
        };        
        helpTestProcess(plan, expected, dataMgr);
    }

    
    //converts e1 from integer to string, with a different name
    public void testDynamicCommandTypeConversion() throws Exception {
    	 FakeMetadataFacade metadata = FakeMetadataFactory.example1();
         
         FakeMetadataObject pm1 = metadata.getStore().findObject("pm1",FakeMetadataObject.MODEL); //$NON-NLS-1$
         
         FakeMetadataObject rs2 = FakeMetadataFactory.createResultSet("pm1.rs2", pm1, new String[] { "e1" }, new String[] { DataTypeManager.DefaultDataTypes.STRING }); //$NON-NLS-1$ //$NON-NLS-2$ 
         FakeMetadataObject rs2p1 = FakeMetadataFactory.createParameter("ret", 1, ParameterInfo.RESULT_SET, DataTypeManager.DefaultDataTypes.OBJECT, rs2);  //$NON-NLS-1$
         FakeMetadataObject rs2p2 = FakeMetadataFactory.createParameter("in", 2, ParameterInfo.IN, DataTypeManager.DefaultDataTypes.STRING, null);  //$NON-NLS-1$
         QueryNode sq2n1 = new QueryNode("pm1.sq2", "CREATE VIRTUAL PROCEDURE BEGIN\n" //$NON-NLS-1$ //$NON-NLS-2$
                                         + "declare string VARIABLES.x; VARIABLES.x = 'a'; execute string 'SELECT e2 ' || ' FROM pm1.g1 ' || ' where e1=pm1.sq2.in'; END"); //$NON-NLS-1$ //
         FakeMetadataObject sq2 = FakeMetadataFactory.createVirtualProcedure("pm1.sq2", pm1, Arrays.asList(new FakeMetadataObject[] { rs2p1, rs2p2 }), sq2n1);  //$NON-NLS-1$

         metadata.getStore().addObject(rs2);
         metadata.getStore().addObject(sq2);
         
         String userUpdateStr = "EXEC pm1.sq2('First')"; //$NON-NLS-1$
         
         FakeDataManager dataMgr = exampleDataManager(metadata);

         ProcessorPlan plan = getProcedurePlan(userUpdateStr, metadata);
                 
         // Create expected results
         List[] expected = new List[] {
                 Arrays.asList(new Object[] { "5" }),  //$NON-NLS-1$
         };        
         helpTestProcess(plan, expected, dataMgr);
    }
    
    public void testDynamicCommandRecursion() throws Exception {
        FakeMetadataFacade metadata = FakeMetadataFactory.example1();

        FakeMetadataObject pm1 = metadata.getStore().findObject("pm1", FakeMetadataObject.MODEL); //$NON-NLS-1$

        FakeMetadataObject rs2 = FakeMetadataFactory
                                                    .createResultSet("pm1.rs2", pm1, new String[] {"e1", "e2"}, new String[] {DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.INTEGER}); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        FakeMetadataObject rs2p1 = FakeMetadataFactory
                                                      .createParameter("ret", 1, ParameterInfo.RESULT_SET, DataTypeManager.DefaultDataTypes.OBJECT, rs2); //$NON-NLS-1$
        FakeMetadataObject rs2p2 = FakeMetadataFactory
                                                      .createParameter("in", 2, ParameterInfo.IN, DataTypeManager.DefaultDataTypes.STRING, null); //$NON-NLS-1$
        QueryNode sq2n1 = new QueryNode("pm1.sq2", "CREATE VIRTUAL PROCEDURE BEGIN\n" //$NON-NLS-1$ //$NON-NLS-2$
                                                   + "execute string 'EXEC pm1.sq2(''First'')' as e1 string, e2 integer; END"); //$NON-NLS-1$ //
        FakeMetadataObject sq2 = FakeMetadataFactory
                                                    .createVirtualProcedure("pm1.sq2", pm1, Arrays.asList(new FakeMetadataObject[] {rs2p1, rs2p2}), sq2n1); //$NON-NLS-1$

        metadata.getStore().addObject(rs2);
        metadata.getStore().addObject(sq2);

        String userUpdateStr = "EXEC pm1.sq2('First')"; //$NON-NLS-1$

        FakeDataManager dataMgr = exampleDataManager(metadata);

        ProcessorPlan plan = getProcedurePlan(userUpdateStr, metadata);

        helpTestProcessFailure(false,
                               plan,
                               dataMgr,
                               "Couldn't execute the dynamic SQL command \"EXECUTE STRING 'EXEC pm1.sq2(''First'')' AS e1 string, e2 integer\" with the SQL statement \"'EXEC pm1.sq2(''First'')'\" due to: There is a recursive invocation of group 'PM1.SQ2'. Please correct the SQL."); //$NON-NLS-1$
    }
    
    public void testDynamicCommandIncorrectProjectSymbolCount() throws Exception {
    	//Tests dynamic query with incorrect number of elements   
        FakeMetadataFacade metadata = FakeMetadataFactory.example1();
        
        FakeMetadataObject pm1 = metadata.getStore().findObject("pm1",FakeMetadataObject.MODEL); //$NON-NLS-1$
        
        FakeMetadataObject rs2 = FakeMetadataFactory.createResultSet("pm1.rs1", pm1, new String[] { "e1" }, new String[] { DataTypeManager.DefaultDataTypes.STRING }); //$NON-NLS-1$ //$NON-NLS-2$
        FakeMetadataObject rs2p1 = FakeMetadataFactory.createParameter("ret", 1, ParameterInfo.RESULT_SET, DataTypeManager.DefaultDataTypes.OBJECT, rs2);  //$NON-NLS-1$
        FakeMetadataObject rs2p2 = FakeMetadataFactory.createParameter("in", 2, ParameterInfo.IN, DataTypeManager.DefaultDataTypes.STRING, null);  //$NON-NLS-1$
        QueryNode sq2n1 = new QueryNode("pm1.sq1", "CREATE VIRTUAL PROCEDURE BEGIN\n" //$NON-NLS-1$ //$NON-NLS-2$
                                        + "SELECT pm1.g1.e1 FROM pm1.g1; END"); //$NON-NLS-1$ 
        FakeMetadataObject sq1 = FakeMetadataFactory.createVirtualProcedure("pm1.sq1", pm1, Arrays.asList(new FakeMetadataObject[] { rs2p1, rs2p2 }), sq2n1);  //$NON-NLS-1$
        
        QueryNode sq2n2 = new QueryNode("pm1.sq2", "CREATE VIRTUAL PROCEDURE BEGIN\n" //$NON-NLS-1$ //$NON-NLS-2$
                + "execute string 'EXEC pm1.sq1(''First'')' as e1 string, e2 integer; END"); //$NON-NLS-1$ 
        FakeMetadataObject sq2 = FakeMetadataFactory.createVirtualProcedure("pm1.sq2", pm1, Arrays.asList(new FakeMetadataObject[] { rs2p1, rs2p2 }), sq2n2);  //$NON-NLS-1$

        metadata.getStore().addObject(rs2);
        metadata.getStore().addObject(sq1);
        metadata.getStore().addObject(sq2);
        
        String userUpdateStr = "EXEC pm1.sq2('test')"; //$NON-NLS-1$
        
        FakeDataManager dataMgr = exampleDataManager(metadata);

        ProcessorPlan plan = getProcedurePlan(userUpdateStr, metadata);
    	
        helpTestProcessFailure(false, plan, dataMgr, "Couldn't execute the dynamic SQL command \"EXECUTE STRING 'EXEC pm1.sq1(''First'')' AS e1 string, e2 integer\" with the SQL statement \"'EXEC pm1.sq1(''First'')'\" due to: The dynamic sql string contains an incorrect number of elements."); //$NON-NLS-1$
     }
    
    public void testDynamicCommandIncorrectProjectSymbolNames() throws Exception {
    	//Tests dynamic query with incorrect number of elements   
        FakeMetadataFacade metadata = FakeMetadataFactory.example1();
        
        FakeMetadataObject pm1 = metadata.getStore().findObject("pm1",FakeMetadataObject.MODEL); //$NON-NLS-1$
        
        FakeMetadataObject rs2 = FakeMetadataFactory.createResultSet("pm1.rs1", pm1, new String[] { "e1", "e2" }, new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.STRING}); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        FakeMetadataObject rs2p1 = FakeMetadataFactory.createParameter("ret", 1, ParameterInfo.RESULT_SET, DataTypeManager.DefaultDataTypes.OBJECT, rs2);  //$NON-NLS-1$
        FakeMetadataObject rs2p2 = FakeMetadataFactory.createParameter("in", 2, ParameterInfo.IN, DataTypeManager.DefaultDataTypes.STRING, null);  //$NON-NLS-1$
        QueryNode sq2n1 = new QueryNode("pm1.sq1", "CREATE VIRTUAL PROCEDURE BEGIN\n" //$NON-NLS-1$ //$NON-NLS-2$
                                        + "execute string 'select e1 as x, e2 from pm1.g1'; END"); //$NON-NLS-1$ 
        FakeMetadataObject sq1 = FakeMetadataFactory.createVirtualProcedure("pm1.sq1", pm1, Arrays.asList(new FakeMetadataObject[] { rs2p1, rs2p2 }), sq2n1);  //$NON-NLS-1$
        
        metadata.getStore().addObject(rs2);
        metadata.getStore().addObject(sq1);
        
        String userUpdateStr = "EXEC pm1.sq1('test')"; //$NON-NLS-1$
        
        FakeDataManager dataMgr = exampleDataManager(metadata);

        ProcessorPlan plan = getProcedurePlan(userUpdateStr, metadata);
    	
        helpTestProcessFailure(false, plan, dataMgr, "Couldn't execute the dynamic SQL command \"EXECUTE STRING 'select e1 as x, e2 from pm1.g1'\" with the SQL statement \"'select e1 as x, e2 from pm1.g1'\" due to: No match found for expected symbol 'E1' in the dynamic SQL."); //$NON-NLS-1$
     }
    
    public void testDynamicCommandIncorrectProjectSymbolDatatypes() throws Exception {
    	//Tests dynamic query with a different datatype definition for an element in the AS clause that
    	//has no implicit conversion. 
        FakeMetadataFacade metadata = FakeMetadataFactory.example1();
        
        FakeMetadataObject pm1 = metadata.getStore().findObject("pm1",FakeMetadataObject.MODEL); //$NON-NLS-1$
        
        FakeMetadataObject rs2 = FakeMetadataFactory.createResultSet("pm1.rs1", pm1, new String[] { "e1" }, new String[] { DataTypeManager.DefaultDataTypes.INTEGER }); //$NON-NLS-1$ //$NON-NLS-2$
        FakeMetadataObject rs2p1 = FakeMetadataFactory.createParameter("ret", 1, ParameterInfo.RESULT_SET, DataTypeManager.DefaultDataTypes.OBJECT, rs2);  //$NON-NLS-1$
        QueryNode sq2n1 = new QueryNode("pm1.sq1", "CREATE VIRTUAL PROCEDURE BEGIN\n" //$NON-NLS-1$ //$NON-NLS-2$
                                        + "execute string 'select e1 from pm1.g1'; END"); //$NON-NLS-1$ 
        FakeMetadataObject sq1 = FakeMetadataFactory.createVirtualProcedure("pm1.sq1", pm1, Arrays.asList(new FakeMetadataObject[] { rs2p1 }), sq2n1);  //$NON-NLS-1$
        
        metadata.getStore().addObject(rs2);
        metadata.getStore().addObject(sq1);
        
        String userUpdateStr = "EXEC pm1.sq1()"; //$NON-NLS-1$
        
        FakeDataManager dataMgr = exampleDataManager(metadata);

        ProcessorPlan plan = getProcedurePlan(userUpdateStr, metadata);
    	
        helpTestProcessFailure(false, plan, dataMgr, "Couldn't execute the dynamic SQL command \"EXECUTE STRING 'select e1 from pm1.g1'\" with the SQL statement \"'select e1 from pm1.g1'\" due to: The datatype 'string' for element 'E1' in the dynamic SQL cannot be implicitly converted to 'integer'."); //$NON-NLS-1$
     }
 
    public void testDynamicCommandInvalidModelUpdateCountEqualOne() throws Exception {
    	//Test invalid update model count
    	//Set update model count to 1 while actual is 2   
        FakeMetadataFacade metadata = FakeMetadataFactory.example1();
        
        FakeMetadataObject pm1 = metadata.getStore().findObject("pm1",FakeMetadataObject.MODEL); //$NON-NLS-1$
        
        FakeMetadataObject rs2 = FakeMetadataFactory.createResultSet("pm1.rs1", pm1, new String[] { "e1" }, new String[] { DataTypeManager.DefaultDataTypes.STRING }); //$NON-NLS-1$ //$NON-NLS-2$
        FakeMetadataObject rs2p1 = FakeMetadataFactory.createParameter("ret", 1, ParameterInfo.RESULT_SET, DataTypeManager.DefaultDataTypes.OBJECT, rs2);  //$NON-NLS-1$
        FakeMetadataObject rs2p2 = FakeMetadataFactory.createParameter("in", 2, ParameterInfo.IN, DataTypeManager.DefaultDataTypes.STRING, null);  //$NON-NLS-1$
        QueryNode sq2n1 = new QueryNode("pm1.sq1", "CREATE VIRTUAL PROCEDURE BEGIN\n" //$NON-NLS-1$ //$NON-NLS-2$
                                        + "INSERT INTO pm1.g1 (e1) VALUES (pm1.sq1.in);UPDATE pm1.g2 SET e1 = 'Next'; END"); //$NON-NLS-1$ 
        FakeMetadataObject sq1 = FakeMetadataFactory.createVirtualProcedure("pm1.sq1", pm1, Arrays.asList(new FakeMetadataObject[] { rs2p1, rs2p2 }), sq2n1);  //$NON-NLS-1$
        
        QueryNode sq2n2 = new QueryNode("pm1.sq2", "CREATE VIRTUAL PROCEDURE BEGIN\n" //$NON-NLS-1$ //$NON-NLS-2$
                + "execute string 'EXEC pm1.sq1(''First'')' as e1 string  UPDATE 1; END"); //$NON-NLS-1$ 
        FakeMetadataObject sq2 = FakeMetadataFactory.createVirtualProcedure("pm1.sq2", pm1, Arrays.asList(new FakeMetadataObject[] { rs2p1, rs2p2 }), sq2n2);  //$NON-NLS-1$

        metadata.getStore().addObject(rs2);
        metadata.getStore().addObject(sq1);
        metadata.getStore().addObject(sq2);
        
        String userUpdateStr = "EXEC pm1.sq2('test')"; //$NON-NLS-1$
        
        FakeDataManager dataMgr = exampleDataManager(metadata);

        ProcessorPlan plan = getProcedurePlan(userUpdateStr, metadata);
    	
        helpTestProcessFailure(true, plan, dataMgr, "Couldn't execute the dynamic SQL command \"EXECUTE STRING 'EXEC pm1.sq1(''First'')' AS e1 string UPDATE 1\" with the SQL statement \"'EXEC pm1.sq1(''First'')'\" due to: The actual model update count '2' is greater than the expected value of '1'.  This is potentially unsafe in OPTIMISTIC transaction mode.  Please adjust the UPDATE clause of the dynamic SQL statement."); //$NON-NLS-1$ 
      }
    
    public void testDynamicCommandWithTwoDynamicStatements() throws Exception {
    	//Tests dynamic query with two consecutive DynamicCommands. The first without an AS clause and returning different results. 
        FakeMetadataFacade metadata = FakeMetadataFactory.example1();
        
        FakeMetadataObject pm1 = metadata.getStore().findObject("pm1",FakeMetadataObject.MODEL); //$NON-NLS-1$
        
        FakeMetadataObject rs2 = FakeMetadataFactory.createResultSet("pm1.rs1", pm1, new String[] { "e1", "e2" }, new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.STRING}); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        FakeMetadataObject rs2p1 = FakeMetadataFactory.createParameter("ret", 1, ParameterInfo.RESULT_SET, DataTypeManager.DefaultDataTypes.OBJECT, rs2);  //$NON-NLS-1$
        QueryNode sq2n1 = new QueryNode("pm1.sq1", "CREATE VIRTUAL PROCEDURE BEGIN\n" //$NON-NLS-1$ //$NON-NLS-2$
                                        + "execute string 'select e1 from pm1.g1'; execute string 'select e1, e2 from pm1.g1' as e1 string, e2 integer; END"); //$NON-NLS-1$ 
        FakeMetadataObject sq1 = FakeMetadataFactory.createVirtualProcedure("pm1.sq1", pm1, Arrays.asList(new FakeMetadataObject[] { rs2p1 }), sq2n1);  //$NON-NLS-1$
        
        metadata.getStore().addObject(rs2);
        metadata.getStore().addObject(sq1);
        
        String userUpdateStr = "EXEC pm1.sq1()"; //$NON-NLS-1$
        
        FakeDataManager dataMgr = exampleDataManager(metadata);

        ProcessorPlan plan = getProcedurePlan(userUpdateStr, metadata);
    	
        // Create expected results
        List[] expected = new List[] {
            Arrays.asList(new Object[] { "First", new Integer(5)}),  //$NON-NLS-1$
            Arrays.asList(new Object[] { "Second", new Integer(15)}), //$NON-NLS-1$
            Arrays.asList(new Object[] { "Third", new Integer(51)})};           //$NON-NLS-1$      
       
        helpTestProcess(plan, expected, dataMgr);
     }
    
    public void testAssignmentWithCase() throws Exception {
        FakeMetadataFacade metadata = FakeMetadataFactory.example1();
        
        String sql = new StringBuffer("declare integer caseValue = ") //$NON-NLS-1$
        .append("CASE") //$NON-NLS-1$
        .append(" WHEN pm1.sq1.param='a' THEN 0") //$NON-NLS-1$
        .append(" WHEN pm1.sq1.param='b' THEN 1") //$NON-NLS-1$
        .append(" WHEN pm1.sq1.param='c' THEN 2") //$NON-NLS-1$
        .append(" WHEN pm1.sq1.param='d' THEN 3") //$NON-NLS-1$
        .append(" ELSE 9999") //$NON-NLS-1$
        .append(" END").toString(); //$NON-NLS-1$

        
        FakeMetadataObject pm1 = metadata.getStore().findObject("pm1",FakeMetadataObject.MODEL); //$NON-NLS-1$
        FakeMetadataObject rs2p2 = FakeMetadataFactory.createParameter("param", 2, ParameterInfo.IN, DataTypeManager.DefaultDataTypes.STRING, null);  //$NON-NLS-1$        
        FakeMetadataObject rs1 = FakeMetadataFactory.createResultSet("pm1.rs2", pm1, new String[] { "e1" }, new String[] { DataTypeManager.DefaultDataTypes.STRING }); //$NON-NLS-1$ //$NON-NLS-2$ 
        FakeMetadataObject rs2p1 = FakeMetadataFactory.createParameter("ret", 1, ParameterInfo.RESULT_SET, DataTypeManager.DefaultDataTypes.OBJECT, rs1);  //$NON-NLS-1$
        QueryNode sq2n1 = new QueryNode("pm1.sq1", "CREATE VIRTUAL PROCEDURE BEGIN\n" //$NON-NLS-1$ //$NON-NLS-2$
                                        + sql + "; SELECT caseValue; END"); //$NON-NLS-1$ 
        FakeMetadataObject sq1 = FakeMetadataFactory.createVirtualProcedure("pm1.sq1", pm1, Arrays.asList(new FakeMetadataObject[] { rs2p1, rs2p2 }), sq2n1);  //$NON-NLS-1$

        metadata.getStore().addObject(rs1);
        metadata.getStore().addObject(sq1);
        
        String userUpdateStr = "EXEC pm1.sq1('d')"; //$NON-NLS-1$
        
        FakeDataManager dataMgr = exampleDataManager(metadata);

        ProcessorPlan plan = getProcedurePlan(userUpdateStr, metadata);
                
        // Create expected results
        List[] expected = new List[] {
                Arrays.asList(new Object[] { new Integer(3) }),  
        };        
        helpTestProcess(plan, expected, dataMgr);
    }
    
    public void testDynamicCommandInsertIntoTempTableWithDifferentDatatypeFromSource() throws Exception {
    	//Tests dynamic query with insert into a temp table using data returned from a physical table.
    	//See defect 23394  
        FakeMetadataFacade metadata = FakeMetadataFactory.example1();
        
        FakeMetadataObject pm5 = metadata.getStore().findObject("pm5",FakeMetadataObject.MODEL); //$NON-NLS-1$
        FakeMetadataObject rs1 = FakeMetadataFactory.createResultSet("pm5.rs1", pm5, new String[] { "e1", "e2" }, new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.SHORT}); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        FakeMetadataObject rs2p1 = FakeMetadataFactory.createParameter("ret", 1, ParameterInfo.RESULT_SET, DataTypeManager.DefaultDataTypes.OBJECT, rs1);  //$NON-NLS-1$
        QueryNode sq2n1 = new QueryNode("pm5.sq1", "CREATE VIRTUAL PROCEDURE BEGIN\n" //$NON-NLS-1$ //$NON-NLS-2$
                                        + "execute string 'select e1,e2 from pm5.g3' as e1 string, e2 integer INTO #temp; select * from #temp; END"); //$NON-NLS-1$ 
        FakeMetadataObject sq1 = FakeMetadataFactory.createVirtualProcedure("pm5.sq1", pm5, Arrays.asList(new FakeMetadataObject[] { rs2p1 }), sq2n1 );  //$NON-NLS-1$
        
        metadata.getStore().addObject(rs1);
        metadata.getStore().addObject(sq1);
        
        String userUpdateStr = "EXEC pm5.sq1()"; //$NON-NLS-1$
        
        FakeDataManager dataMgr = exampleDataManagerPm5(metadata);

        ProcessorPlan plan = getProcedurePlan(userUpdateStr, metadata);
    	
        // Create expected results
        List[] expected = new List[] {
            Arrays.asList(new Object[] { "First", new Integer(5)}),  //$NON-NLS-1$
            Arrays.asList(new Object[] { "Second", new Integer(15)}), //$NON-NLS-1$
            Arrays.asList(new Object[] { "Third", new Integer(51)})};           //$NON-NLS-1$      
       
        helpTestProcess(plan, expected, dataMgr);
     }
    
    public void testDynamicCommandWithVariableOnly() throws Exception {
    	//Tests dynamic query with only a variable that represents thte entire dynamic query.
        FakeMetadataFacade metadata = FakeMetadataFactory.example1();
        
        FakeMetadataObject pm5 = metadata.getStore().findObject("pm5",FakeMetadataObject.MODEL); //$NON-NLS-1$
        FakeMetadataObject rs1 = FakeMetadataFactory.createResultSet("pm5.rs1", pm5, new String[] { "e1", "e2" }, new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.SHORT}); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        FakeMetadataObject rs2p1 = FakeMetadataFactory.createParameter("param", 1, ParameterInfo.IN, DataTypeManager.DefaultDataTypes.SHORT, rs1);  //$NON-NLS-1$
        FakeMetadataObject rs2p2 = FakeMetadataFactory.createParameter("ret", 2, ParameterInfo.RESULT_SET, DataTypeManager.DefaultDataTypes.OBJECT, rs1);  //$NON-NLS-1$
        QueryNode sq2n1 = new QueryNode("pm5.sq1", "CREATE VIRTUAL PROCEDURE BEGIN\n" //$NON-NLS-1$ //$NON-NLS-2$
                                        + "DECLARE string VARIABLES.CRIT = 'select e1, e2 from pm5.g3 where e2=using.id'; execute string VARIABLES.CRIT USING ID = pm5.sq1.param; END"); //$NON-NLS-1$ 
        FakeMetadataObject sq1 = FakeMetadataFactory.createVirtualProcedure("pm5.sq1", pm5, Arrays.asList(new FakeMetadataObject[] { rs2p1, rs2p2 }), sq2n1 );  //$NON-NLS-1$
        
        metadata.getStore().addObject(rs1);
        metadata.getStore().addObject(sq1);
        
        String userUpdateStr = "EXEC pm5.sq1(convert(5,short))"; //$NON-NLS-1$
        
        FakeDataManager dataMgr = exampleDataManagerPm5(metadata);

        ProcessorPlan plan = getProcedurePlan(userUpdateStr, metadata);
    	
        // Create expected results
        List[] expected = new List[] { Arrays.asList(new Object[] { "First", new Short((short)5)})};           //$NON-NLS-1$      
        
        helpTestProcess(plan, expected, dataMgr);
     }
    
    public void testVirtualProcedureWithCreate() throws Exception{
        String userUpdateStr = "EXEC pm1.vsp60()"; //$NON-NLS-1$
        
        FakeMetadataFacade metadata = FakeMetadataFactory.example1Cached();

        FakeDataManager dataMgr = exampleDataManager(metadata);

        ProcessorPlan plan = getProcedurePlan(userUpdateStr, metadata);
                
        // Create expected results
        List[] expected = new List[] {
            Arrays.asList(new Object[] { "First"}),  //$NON-NLS-1$
            Arrays.asList(new Object[] { "Second"}),  //$NON-NLS-1$
            Arrays.asList(new Object[] { "Third"})};         //$NON-NLS-1$
        helpTestProcess(plan, expected, dataMgr);
    }
    
    public void testVirtualProcedureWithCreateAndDrop() throws Exception{
        String userUpdateStr = "EXEC pm1.vsp61()"; //$NON-NLS-1$
        
        FakeMetadataFacade metadata = FakeMetadataFactory.example1Cached();

        FakeDataManager dataMgr = exampleDataManager(metadata);

        ProcessorPlan plan = getProcedurePlan(userUpdateStr, metadata);
                
        // Create expected results
        List[] expected = new List[] {
            Arrays.asList(new Object[] { "First"}),  //$NON-NLS-1$
            Arrays.asList(new Object[] { "Second"}),  //$NON-NLS-1$
            Arrays.asList(new Object[] { "Third"})};         //$NON-NLS-1$
        helpTestProcess(plan, expected, dataMgr);
    }
    
    public void testVirtualProcedureWithCreateAndSelectInto() throws Exception{
        String userUpdateStr = "EXEC pm1.vsp62()"; //$NON-NLS-1$
        
        FakeMetadataFacade metadata = FakeMetadataFactory.example1Cached();

        FakeDataManager dataMgr = exampleDataManager(metadata);

        ProcessorPlan plan = getProcedurePlan(userUpdateStr, metadata);
                
        // Create expected results
        List[] expected = new List[] {
            Arrays.asList(new Object[] { "First"}),  //$NON-NLS-1$
            Arrays.asList(new Object[] { "Second"}),  //$NON-NLS-1$
            Arrays.asList(new Object[] { "Third"})};         //$NON-NLS-1$
        helpTestProcess(plan, expected, dataMgr);
    }
    
    public void testDifferentlyScopedTempTables() throws Exception {
        
        FakeMetadataFacade metadata = FakeMetadataFactory.example1();
        
        FakeMetadataObject pm1 = metadata.getStore().findObject("pm1",FakeMetadataObject.MODEL); //$NON-NLS-1$
        
        FakeMetadataObject rs2 = FakeMetadataFactory.createResultSet("pm1.rs1", pm1, new String[] { "e1" }, new String[] { DataTypeManager.DefaultDataTypes.STRING }); //$NON-NLS-1$ //$NON-NLS-2$
        FakeMetadataObject rs2p1 = FakeMetadataFactory.createParameter("ret", 1, ParameterInfo.RESULT_SET, DataTypeManager.DefaultDataTypes.OBJECT, rs2);  //$NON-NLS-1$
        StringBuffer procedure = new StringBuffer("CREATE VIRTUAL PROCEDURE \n"); //$NON-NLS-1$
        procedure.append("BEGIN\n"); //$NON-NLS-1$
        procedure.append("declare integer VARIABLES.e2_total=0;\n"); //$NON-NLS-1$
        procedure.append("if (e2_total = 0)"); //$NON-NLS-1$
        procedure.append("BEGIN\n"); //$NON-NLS-1$
        procedure.append("select e1 into #temp from pm1.g1;\n"); //$NON-NLS-1$
        procedure.append("VARIABLES.e2_total=select count(*) from #temp;\n"); //$NON-NLS-1$
        procedure.append("END\n"); //$NON-NLS-1$
        procedure.append("if (e2_total = 3)"); //$NON-NLS-1$
        procedure.append("BEGIN\n"); //$NON-NLS-1$
        procedure.append("select e1 into #temp from pm1.g1;\n"); //$NON-NLS-1$
        procedure.append("VARIABLES.e2_total=select count(*) from #temp;\n"); //$NON-NLS-1$
        procedure.append("END\n"); //$NON-NLS-1$
        procedure.append("SELECT VARIABLES.e2_total;\n"); //$NON-NLS-1$
        procedure.append("END"); //$NON-NLS-1$
        
        QueryNode sq2n1 = new QueryNode("pm1.sq1", procedure.toString()); //$NON-NLS-1$ 
        FakeMetadataObject sq1 = FakeMetadataFactory.createVirtualProcedure("pm1.sq1", pm1, Arrays.asList(new FakeMetadataObject[] { rs2p1 }), sq2n1);  //$NON-NLS-1$

        metadata.getStore().addObject(rs2);
        metadata.getStore().addObject(sq1);
        
        String userUpdateStr = "EXEC pm1.sq1()"; //$NON-NLS-1$
        
        FakeDataManager dataMgr = exampleDataManager(metadata);

        ProcessorPlan plan = getProcedurePlan(userUpdateStr, metadata);
        
        //Create expected results
        List[] expected = new List[] {
            Arrays.asList(new Object[] { new Integer(3)}),  
            };           
        helpTestProcess(plan, expected, dataMgr);
    }
    
    public void testLoopsWithBreak() throws Exception {
        
        FakeMetadataFacade metadata = FakeMetadataFactory.example1();
        
        FakeMetadataObject pm1 = metadata.getStore().findObject("pm1",FakeMetadataObject.MODEL); //$NON-NLS-1$
        
        FakeMetadataObject rs2 = FakeMetadataFactory.createResultSet("pm1.rs1", pm1, new String[] { "e1" }, new String[] { DataTypeManager.DefaultDataTypes.STRING }); //$NON-NLS-1$ //$NON-NLS-2$
        FakeMetadataObject rs2p1 = FakeMetadataFactory.createParameter("ret", 1, ParameterInfo.RESULT_SET, DataTypeManager.DefaultDataTypes.OBJECT, rs2);  //$NON-NLS-1$
        StringBuffer procedure = new StringBuffer("CREATE VIRTUAL PROCEDURE \n"); //$NON-NLS-1$
        procedure.append("BEGIN\n"); //$NON-NLS-1$
        procedure.append("declare integer VARIABLES.e2_total=0;\n"); //$NON-NLS-1$
        procedure.append("loop on (select e2 as x from pm1.g1) as mycursor\n"); //$NON-NLS-1$
        procedure.append("BEGIN\n"); //$NON-NLS-1$
        procedure.append("e2_total=e2_total+mycursor.x;\n"); //$NON-NLS-1$
        procedure.append("break;\n"); //$NON-NLS-1$
        procedure.append("END\n"); //$NON-NLS-1$
        procedure.append("loop on (select e2 as x from pm1.g1) as mycursor\n"); //$NON-NLS-1$
        procedure.append("BEGIN\n"); //$NON-NLS-1$
        procedure.append("e2_total=e2_total+mycursor.x;"); //$NON-NLS-1$
        procedure.append("END\n"); //$NON-NLS-1$
        procedure.append("SELECT VARIABLES.e2_total;\n"); //$NON-NLS-1$
        procedure.append("END"); //$NON-NLS-1$
        
        QueryNode sq2n1 = new QueryNode("pm1.sq1", procedure.toString()); //$NON-NLS-1$ 
        FakeMetadataObject sq1 = FakeMetadataFactory.createVirtualProcedure("pm1.sq1", pm1, Arrays.asList(new FakeMetadataObject[] { rs2p1 }), sq2n1);  //$NON-NLS-1$

        metadata.getStore().addObject(rs2);
        metadata.getStore().addObject(sq1);
        
        String userUpdateStr = "EXEC pm1.sq1()"; //$NON-NLS-1$
        
        FakeDataManager dataMgr = exampleDataManager(metadata);

        ProcessorPlan plan = getProcedurePlan(userUpdateStr, metadata);
        
        //Create expected results
        List[] expected = new List[] {
            Arrays.asList(new Object[] { new Integer(76)}),  
            };           
        helpTestProcess(plan, expected, dataMgr);
    }
    
    public void testCreateWithoutDrop() throws Exception {
        
        FakeMetadataFacade metadata = FakeMetadataFactory.example1();
        
        FakeMetadataObject pm1 = metadata.getStore().findObject("pm1",FakeMetadataObject.MODEL); //$NON-NLS-1$
        
        FakeMetadataObject rs2 = FakeMetadataFactory.createResultSet("pm1.rs1", pm1, new String[] { "e1" }, new String[] { DataTypeManager.DefaultDataTypes.STRING }); //$NON-NLS-1$ //$NON-NLS-2$
        FakeMetadataObject rs2p1 = FakeMetadataFactory.createParameter("ret", 1, ParameterInfo.RESULT_SET, DataTypeManager.DefaultDataTypes.OBJECT, rs2);  //$NON-NLS-1$
        StringBuffer procedure = new StringBuffer("CREATE VIRTUAL PROCEDURE \n"); //$NON-NLS-1$
        procedure.append("BEGIN\n"); //$NON-NLS-1$
        procedure.append("create local temporary table t1 (e1 integer);\n"); //$NON-NLS-1$
        procedure.append("create local temporary table t1 (e1 integer);\n"); //$NON-NLS-1$
        procedure.append("SELECT e1 from t1;\n"); //$NON-NLS-1$
        procedure.append("END"); //$NON-NLS-1$
        
        QueryNode sq2n1 = new QueryNode("pm1.sq1", procedure.toString()); //$NON-NLS-1$ 
        FakeMetadataObject sq1 = FakeMetadataFactory.createVirtualProcedure("pm1.sq1", pm1, Arrays.asList(new FakeMetadataObject[] { rs2p1 }), sq2n1);  //$NON-NLS-1$

        metadata.getStore().addObject(rs2);
        metadata.getStore().addObject(sq1);
        
        String userUpdateStr = "EXEC pm1.sq1()"; //$NON-NLS-1$
        
        FakeDataManager dataMgr = exampleDataManager(metadata);

        ProcessorPlan plan = getProcedurePlan(userUpdateStr, metadata);
        
        helpTestProcessFailure(false, plan, dataMgr, "Temporary table \"T1\" already exists."); //$NON-NLS-1$
    }
    
    /**
     *  We allow drops to silently fail
     */
    public void testDoubleDrop() throws Exception {
        
        FakeMetadataFacade metadata = FakeMetadataFactory.example1();
        
        FakeMetadataObject pm1 = metadata.getStore().findObject("pm1",FakeMetadataObject.MODEL); //$NON-NLS-1$
        
        FakeMetadataObject rs2 = FakeMetadataFactory.createResultSet("pm1.rs1", pm1, new String[] { "e1" }, new String[] { DataTypeManager.DefaultDataTypes.STRING }); //$NON-NLS-1$ //$NON-NLS-2$
        FakeMetadataObject rs2p1 = FakeMetadataFactory.createParameter("ret", 1, ParameterInfo.RESULT_SET, DataTypeManager.DefaultDataTypes.OBJECT, rs2);  //$NON-NLS-1$
        StringBuffer procedure = new StringBuffer("CREATE VIRTUAL PROCEDURE \n"); //$NON-NLS-1$
        procedure.append("BEGIN\n"); //$NON-NLS-1$
        procedure.append("create local temporary table t1 (e1 string);\n"); //$NON-NLS-1$
        procedure.append("select e1 into t1 from pm1.g1;\n"); //$NON-NLS-1$
        procedure.append("drop table t1;\n"); //$NON-NLS-1$
        procedure.append("drop table t1;\n"); //$NON-NLS-1$
        procedure.append("SELECT 1;\n"); //$NON-NLS-1$
        procedure.append("END"); //$NON-NLS-1$
        
        QueryNode sq2n1 = new QueryNode("pm1.sq1", procedure.toString()); //$NON-NLS-1$ 
        FakeMetadataObject sq1 = FakeMetadataFactory.createVirtualProcedure("pm1.sq1", pm1, Arrays.asList(new FakeMetadataObject[] { rs2p1 }), sq2n1);  //$NON-NLS-1$

        metadata.getStore().addObject(rs2);
        metadata.getStore().addObject(sq1);
        
        String userUpdateStr = "EXEC pm1.sq1()"; //$NON-NLS-1$
        
        FakeDataManager dataMgr = exampleDataManager(metadata);

        ProcessorPlan plan = getProcedurePlan(userUpdateStr, metadata);
        
        helpTestProcess(plan, new List[] {Arrays.asList(1)}, dataMgr); 
    }
    
    /**
     * defect 23975 
     */
    public void testFunctionInput() throws Exception {
        FakeMetadataObject v1 = FakeMetadataFactory.createVirtualModel("v1"); //$NON-NLS-1$

        FakeMetadataObject p1 = FakeMetadataFactory.createParameter("v1.vp1.in", 2, ParameterInfo.IN, DataTypeManager.DefaultDataTypes.STRING, null);  //$NON-NLS-1$
        FakeMetadataObject rs1 = FakeMetadataFactory.createResultSet("v1.rs1", v1, new String[] {"e1"}, new String[] { DataTypeManager.DefaultDataTypes.STRING }); //$NON-NLS-1$ //$NON-NLS-2$ 
        FakeMetadataObject rs1p1 = FakeMetadataFactory.createParameter("ret", 1, ParameterInfo.RESULT_SET, DataTypeManager.DefaultDataTypes.OBJECT, rs1);  //$NON-NLS-1$

        QueryNode n1 = new QueryNode("v1.vp1", "CREATE VIRTUAL PROCEDURE BEGIN declare string VARIABLES.x = '1'; exec v1.vp2(concat(x, v1.vp1.in)); END"); //$NON-NLS-1$ //$NON-NLS-2$
        FakeMetadataObject vt1 = FakeMetadataFactory.createVirtualProcedure("v1.vp1", v1, Arrays.asList(new FakeMetadataObject[] { rs1p1, p1 }), n1); //$NON-NLS-1$
        
        FakeMetadataObject p2 = FakeMetadataFactory.createParameter("v1.vp2.in", 2, ParameterInfo.IN, DataTypeManager.DefaultDataTypes.STRING, null);  //$NON-NLS-1$
        QueryNode n2 = new QueryNode("v1.vp2", "CREATE VIRTUAL PROCEDURE BEGIN select v1.vp2.in; end"); //$NON-NLS-1$ //$NON-NLS-2$
        FakeMetadataObject vt2 = FakeMetadataFactory.createVirtualProcedure("v1.vp2", v1, Arrays.asList(new FakeMetadataObject[] { rs1p1, p2 }), n2); //$NON-NLS-1$
                
        FakeMetadataStore store = new FakeMetadataStore();
        store.addObject(v1);
        store.addObject(rs1);
        store.addObject(vt1);
        store.addObject(vt2);
        store.addObject(vt2);
        
        String sql = "exec v1.vp1('1')"; //$NON-NLS-1$
        
        List[] expected = new List[] {  
            Arrays.asList(new Object[] { "11" }), //$NON-NLS-1$ 
        };        
        
        FakeMetadataFacade metadata = new FakeMetadataFacade(store);
        
        // Construct data manager with data 
        // Plan query 
        ProcessorPlan plan = getProcedurePlan(sql, metadata);        
        // Run query 
        helpTestProcess(plan, expected, new FakeDataManager());
    }
    
    /**
     * defect 23976
     * Also, even after the bug for passing procedure inputs to non-execs was fixed, the special case of
     * if (below) and while statements popped up.  
     */
    public void testIfEvaluation() throws Exception {
        String procedure1 = "CREATE PROCEDURE  "; //$NON-NLS-1$
        procedure1 += "BEGIN\n"; //$NON-NLS-1$
        procedure1 += "DECLARE string var1 = INPUT.e1;\n"; //$NON-NLS-1$
        procedure1 += "ROWS_UPDATED = UPDATE vm1.g2 SET e1=var1;\n"; //$NON-NLS-1$
        procedure1 += "END"; //$NON-NLS-1$
        
        String procedure2 = "CREATE PROCEDURE  "; //$NON-NLS-1$
        procedure2 += "BEGIN\n"; //$NON-NLS-1$
        procedure2 += "DECLARE integer var1;\n"; //$NON-NLS-1$
        procedure2 += "IF (INPUT.e1 = 1)\n"; //$NON-NLS-1$
        procedure2 += "ROWS_UPDATED = 5;\n"; //$NON-NLS-1$
        procedure2 += "ELSE\n"; //$NON-NLS-1$
        procedure2 += "ROWS_UPDATED = 4;\n"; //$NON-NLS-1$
        procedure2 += "END"; //$NON-NLS-1$

        String userUpdateStr = "UPDATE vm1.g1 SET e1='x' where e2=5"; //$NON-NLS-1$
        
        FakeMetadataFacade metadata = FakeMetadataFactory.exampleUpdateProc(FakeMetadataObject.Props.UPDATE_PROCEDURE, procedure1, procedure2);
        
        ProcessorPlan plan = getProcedurePlan(userUpdateStr, metadata);
                                     
        helpTestProcess(plan, 4, new FakeDataManager());
    }
    
    /**
     *  This is a slight variation of TestProcessor.testVariableInExecParam, where the proc wrapper can be 
     *  removed after rewrite
     */
    public void testReferenceForwarding() throws Exception { 
        // Create query 
        String sql = "EXEC pm1.vsp49()"; //$NON-NLS-1$
        
        FakeMetadataFacade metadata = FakeMetadataFactory.example1();
        
        FakeMetadataStore store = metadata.getStore();
        
        FakeMetadataObject pm1 = store.findObject("pm1", FakeMetadataObject.MODEL); //$NON-NLS-1$
        
        FakeMetadataObject rs2 = FakeMetadataFactory.createResultSet("pm1.rs2", pm1, new String[] { "e1", "e2" }, new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.INTEGER }); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        FakeMetadataObject rs2p1 = FakeMetadataFactory.createParameter("ret", 1, ParameterInfo.RESULT_SET, DataTypeManager.DefaultDataTypes.OBJECT, rs2);  //$NON-NLS-1$
        FakeMetadataObject rs2p2 = FakeMetadataFactory.createParameter("in", 2, ParameterInfo.IN, DataTypeManager.DefaultDataTypes.STRING, null);  //$NON-NLS-1$
        QueryNode sq2n1 = new QueryNode("pm1.sq2", "CREATE VIRTUAL PROCEDURE BEGIN if (1 = 2) begin declare integer x = 1; end SELECT e1, e2 FROM pm1.g1 WHERE e1=pm1.sq2.in; END"); //$NON-NLS-1$ //$NON-NLS-2$
        FakeMetadataObject sq2 = FakeMetadataFactory.createVirtualProcedure("pm1.sq2", pm1, Arrays.asList(new FakeMetadataObject[] { rs2p1, rs2p2 }), sq2n1);  //$NON-NLS-1$
        
        store.addObject(sq2);
        
        // Create expected results
        List[] expected = new List[] { 
            Arrays.asList(new Object[] { "b", new Integer(2) }), //$NON-NLS-1$
        };    
    
        // Construct data manager with data
        FakeDataManager dataManager = new FakeDataManager();
        TestProcessor.sampleData1(dataManager);
        
        // Plan query
        ProcessorPlan plan = getProcedurePlan(sql, metadata);

        // Run query
        helpTestProcess(plan, expected, dataManager);
    }
    
    public void testInsertAfterCreate() throws Exception {
        FakeMetadataFacade metadata = FakeMetadataFactory.example1();
        
        FakeMetadataObject pm1 = metadata.getStore().findObject("pm1",FakeMetadataObject.MODEL); //$NON-NLS-1$
        
        FakeMetadataObject rs2 = FakeMetadataFactory.createResultSet("pm1.rs1", pm1, new String[] { "e1" }, new String[] { DataTypeManager.DefaultDataTypes.STRING }); //$NON-NLS-1$ //$NON-NLS-2$
        FakeMetadataObject rs2p1 = FakeMetadataFactory.createParameter("ret", 1, ParameterInfo.RESULT_SET, DataTypeManager.DefaultDataTypes.OBJECT, rs2);  //$NON-NLS-1$
        StringBuffer procedure = new StringBuffer("CREATE VIRTUAL PROCEDURE \n") //$NON-NLS-1$
        .append("BEGIN\n") //$NON-NLS-1$
        .append("\n  create local temporary table #temp (e1 string, e2 string);") //$NON-NLS-1$
        .append("\n  insert into #temp (e1) values ('a');") //$NON-NLS-1$
        .append("\n  insert into #temp (e2) values ('b');") //$NON-NLS-1$
        .append("SELECT e2 as e1 from #temp;\n") //$NON-NLS-1$
        .append("END"); //$NON-NLS-1$
        
        QueryNode sq2n1 = new QueryNode("pm1.sq1", procedure.toString()); //$NON-NLS-1$ 
        FakeMetadataObject sq1 = FakeMetadataFactory.createVirtualProcedure("pm1.sq1", pm1, Arrays.asList(new FakeMetadataObject[] { rs2p1 }), sq2n1);  //$NON-NLS-1$

        metadata.getStore().addObject(rs2);
        metadata.getStore().addObject(sq1);
        
        String userUpdateStr = "EXEC pm1.sq1()"; //$NON-NLS-1$
        
        FakeDataManager dataMgr = exampleDataManager(metadata);

        ProcessorPlan plan = getProcedurePlan(userUpdateStr, metadata);
        
        helpTestProcess(plan, new List[] {
            Arrays.asList(new Object[] {null}),  
            Arrays.asList(new Object[] {"b"})}, dataMgr); //$NON-NLS-1$
        
    }
    
    /**
     * the update will not be executed, but the assignment value should still be 0
     */
    public void testUpdateAssignmentNotExecuted() throws Exception {
        String procedure = "CREATE PROCEDURE  "; //$NON-NLS-1$
        procedure = procedure + "BEGIN\n"; //$NON-NLS-1$
        procedure = procedure + "DECLARE integer var1 = UPDATE pm1.g1 SET pm1.g1.e1 = INPUT.e2;"; //$NON-NLS-1$
        procedure = procedure + "ROWS_UPDATED = var1 + 1;\n"; //$NON-NLS-1$
        procedure = procedure + "END"; //$NON-NLS-1$

        String userUpdateStr = "UPDATE g4 SET e1='x' where e2=5"; //$NON-NLS-1$
        
        FakeMetadataFacade metadata = FakeMetadataFactory.exampleUpdateProc(FakeMetadataObject.Props.UPDATE_PROCEDURE, procedure);
        
        FakeDataManager dataMgr = exampleDataManager(metadata);

        ProcessorPlan plan = getProcedurePlan(userUpdateStr, metadata);
                                     
        helpTestProcess(plan, 1, dataMgr);
    }
    
    public void testUpdateAssignmentNotExecutedVirtual() throws Exception {
        String procedure = "CREATE PROCEDURE  "; //$NON-NLS-1$
        procedure = procedure + "BEGIN\n"; //$NON-NLS-1$
        procedure = procedure + "DECLARE integer var1 = UPDATE vm1.g2 SET e1 = INPUT.e2;"; //$NON-NLS-1$
        procedure = procedure + "ROWS_UPDATED = var1 + 1;\n"; //$NON-NLS-1$
        procedure = procedure + "END"; //$NON-NLS-1$
        
        String procedure2 = "CREATE PROCEDURE  "; //$NON-NLS-1$
        procedure2 += "BEGIN\n"; //$NON-NLS-1$
        procedure2 += "DECLARE integer var1;\n"; //$NON-NLS-1$
        procedure2 += "IF (INPUT.e1 = 1)\n"; //$NON-NLS-1$
        procedure2 += "ROWS_UPDATED = 5;\n"; //$NON-NLS-1$
        procedure2 += "ELSE\n"; //$NON-NLS-1$
        procedure2 += "ROWS_UPDATED = 4;\n"; //$NON-NLS-1$
        procedure2 += "END"; //$NON-NLS-1$

        String userUpdateStr = "UPDATE vm1.g1 SET e1='x' where e2=5"; //$NON-NLS-1$
        
        FakeMetadataFacade metadata = FakeMetadataFactory.exampleUpdateProc(FakeMetadataObject.Props.UPDATE_PROCEDURE, procedure, procedure2);
        
        FakeDataManager dataMgr = exampleDataManager(metadata);

        ProcessorPlan plan = getProcedurePlan(userUpdateStr, metadata);
                                     
        helpTestProcess(plan, 1, dataMgr);
    }
    
    public void testEvaluatableSelectWithOrderBy() throws Exception {
        FakeMetadataFacade metadata = FakeMetadataFactory.example1();
        
        FakeMetadataObject pm1 = metadata.getStore().findObject("pm1",FakeMetadataObject.MODEL); //$NON-NLS-1$
        
        FakeMetadataObject rs2 = FakeMetadataFactory.createResultSet("pm1.rs1", pm1, new String[] { "e1" }, new String[] { DataTypeManager.DefaultDataTypes.STRING }); //$NON-NLS-1$ //$NON-NLS-2$
        FakeMetadataObject rs2p1 = FakeMetadataFactory.createParameter("ret", 1, ParameterInfo.RESULT_SET, DataTypeManager.DefaultDataTypes.OBJECT, rs2);  //$NON-NLS-1$
        FakeMetadataObject rs2p2 = FakeMetadataFactory.createParameter("param", 2, ParameterInfo.IN, DataTypeManager.DefaultDataTypes.STRING, null);  //$NON-NLS-1$
        StringBuffer procedure = new StringBuffer("CREATE VIRTUAL PROCEDURE \n") //$NON-NLS-1$
        .append("BEGIN\n") //$NON-NLS-1$
        .append("SELECT param from pm1.g1 order by param limit 1;\n") //$NON-NLS-1$
        .append("END"); //$NON-NLS-1$
        
        QueryNode sq2n1 = new QueryNode("pm1.sq1", procedure.toString()); //$NON-NLS-1$ 
        FakeMetadataObject sq1 = FakeMetadataFactory.createVirtualProcedure("pm1.sq1", pm1, Arrays.asList(new FakeMetadataObject[] { rs2p1, rs2p2 }), sq2n1);  //$NON-NLS-1$

        metadata.getStore().addObject(rs2);
        metadata.getStore().addObject(sq1);
        
        String userUpdateStr = "EXEC pm1.sq1(1)"; //$NON-NLS-1$
        
        FakeDataManager dataMgr = exampleDataManager(metadata);

        ProcessorPlan plan = getProcedurePlan(userUpdateStr, metadata);
        
        helpTestProcess(plan, new List[] {
            Arrays.asList(new Object[] {"1"})}, dataMgr); //$NON-NLS-1$
        
    }
    
    public void testEvaluatableSelectWithOrderBy1() throws Exception {
        FakeMetadataFacade metadata = FakeMetadataFactory.example1();
        
        FakeMetadataObject pm1 = metadata.getStore().findObject("pm1",FakeMetadataObject.MODEL); //$NON-NLS-1$
        
        FakeMetadataObject rs2 = FakeMetadataFactory.createResultSet("pm1.rs1", pm1, new String[] { "e1" }, new String[] { DataTypeManager.DefaultDataTypes.STRING }); //$NON-NLS-1$ //$NON-NLS-2$
        FakeMetadataObject rs2p1 = FakeMetadataFactory.createParameter("ret", 1, ParameterInfo.RESULT_SET, DataTypeManager.DefaultDataTypes.OBJECT, rs2);  //$NON-NLS-1$
        FakeMetadataObject rs2p2 = FakeMetadataFactory.createParameter("param", 2, ParameterInfo.IN, DataTypeManager.DefaultDataTypes.STRING, null);  //$NON-NLS-1$
        StringBuffer procedure = new StringBuffer("CREATE VIRTUAL PROCEDURE \n") //$NON-NLS-1$
        .append("BEGIN\n") //$NON-NLS-1$
        .append("SELECT param from pm1.g1 union select e1 from pm1.g1 order by param limit 2;\n") //$NON-NLS-1$
        .append("END"); //$NON-NLS-1$
        
        QueryNode sq2n1 = new QueryNode("pm1.sq1", procedure.toString()); //$NON-NLS-1$ 
        FakeMetadataObject sq1 = FakeMetadataFactory.createVirtualProcedure("pm1.sq1", pm1, Arrays.asList(new FakeMetadataObject[] { rs2p1, rs2p2 }), sq2n1);  //$NON-NLS-1$

        metadata.getStore().addObject(rs2);
        metadata.getStore().addObject(sq1);
        
        String userUpdateStr = "EXEC pm1.sq1(1)"; //$NON-NLS-1$
        
        FakeDataManager dataMgr = exampleDataManager(metadata);

        ProcessorPlan plan = getProcedurePlan(userUpdateStr, metadata);
        
        helpTestProcess(plan, new List[] {
            Arrays.asList(new Object[] {"1"}), //$NON-NLS-1$
            Arrays.asList(new Object[] {"First"}), //$NON-NLS-1$
            }, dataMgr); 
        
    }
    
    /**
     * Tests non-deterministic evaluation of the rand function.  There are two important things happening
     * 1. is that the evaluation of the rand function is delayed until processing time (which actually has predictable
     * values since the test initializes the command context with the same seed)
     * 2. The values are different, meaning that we got individual evaluations
     * 
     * If this function were deterministic, it would be evaluated during rewrite to a single value.
     */
    public void testNonDeterministicEvaluation() throws Exception {
        StringBuffer procedure = new StringBuffer("CREATE VIRTUAL PROCEDURE \n") //$NON-NLS-1$
        .append("BEGIN\n") //$NON-NLS-1$
        .append("DECLARE integer x = 0;\n") //$NON-NLS-1$
        .append("CREATE LOCAL TEMPORARY TABLE #TEMP (e1 integer);\n") //$NON-NLS-1$
        .append("while (x < 2)\n") //$NON-NLS-1$
        .append("BEGIN\n") //$NON-NLS-1$
        .append("insert into #temp (e1) values (convert(rand() * 1000, integer));\n") //$NON-NLS-1$
        .append("x = x + 1;\n") //$NON-NLS-1$
        .append("END\n") //$NON-NLS-1$
        .append("SELECT * FROM #TEMP;\n") //$NON-NLS-1$
        .append("END"); //$NON-NLS-1$
        
        FakeMetadataFacade metadata = createProcedureMetadata(procedure.toString());
        
        String userUpdateStr = "EXEC pm1.sq1()"; //$NON-NLS-1$
        
        FakeDataManager dataMgr = exampleDataManager(metadata);

        ProcessorPlan plan = getProcedurePlan(userUpdateStr, metadata);
        
        helpTestProcess(plan, new List[] {
            Arrays.asList(new Object[] {new Integer(240)}),
            Arrays.asList(new Object[] {new Integer(637)})}, dataMgr);
    }

    private FakeMetadataFacade createProcedureMetadata(String procedure) {
        FakeMetadataFacade metadata = FakeMetadataFactory.example1();
        
        FakeMetadataObject pm1 = metadata.getStore().findObject("pm1",FakeMetadataObject.MODEL); //$NON-NLS-1$
        
        FakeMetadataObject rs2 = FakeMetadataFactory.createResultSet("pm1.rs1", pm1, new String[] { "e1" }, new String[] { DataTypeManager.DefaultDataTypes.INTEGER }); //$NON-NLS-1$ //$NON-NLS-2$
        FakeMetadataObject rs2p1 = FakeMetadataFactory.createParameter("ret", 1, ParameterInfo.RESULT_SET, DataTypeManager.DefaultDataTypes.OBJECT, rs2);  //$NON-NLS-1$
        
        
        QueryNode sq2n1 = new QueryNode("pm1.sq1", procedure.toString()); //$NON-NLS-1$ 
        FakeMetadataObject sq1 = FakeMetadataFactory.createVirtualProcedure("pm1.sq1", pm1, Arrays.asList(new FakeMetadataObject[] { rs2p1}), sq2n1);  //$NON-NLS-1$

        metadata.getStore().addObject(rs2);
        metadata.getStore().addObject(sq1);
        return metadata;
    }
    
    public void testTempTableTypeConversion() throws Exception {
        
        String procedure = "CREATE VIRTUAL PROCEDURE\n"; //$NON-NLS-1$
        procedure += "BEGIN\n";       //$NON-NLS-1$
        procedure += "CREATE local temporary table temp (x string, y integer);\n";       //$NON-NLS-1$
        procedure += "Select pm1.g1.e2 as e1, pm1.g1.e2 into temp from pm1.g1 order by pm1.g1.e2 limit 1;\n"; //$NON-NLS-1$
        procedure += "Select x from temp;\n"; //$NON-NLS-1$
        procedure += "END\n"; //$NON-NLS-1$
                
        FakeMetadataFacade metadata = createProcedureMetadata(procedure);
        
        String userUpdateStr = "EXEC pm1.sq1()"; //$NON-NLS-1$
        
        FakeDataManager dataMgr = exampleDataManager(metadata);

        ProcessorPlan plan = getProcedurePlan(userUpdateStr, metadata);
        
        helpTestProcess(plan, new List[] {
            Arrays.asList(new Object[] {"5"}), //$NON-NLS-1$
            }, dataMgr);
    }
    
    /**
     * previously the following would break with an npe
     * 
     * Now it will not and the rewriter should remove empty loops, where this was happening
     */
    public void testGetChildPlans() {
        Program program = new Program();
        assertEquals(Collections.EMPTY_LIST, program.getChildPlans());
    }
    
    /**
     * wraps {@link TestXMLPlanningEnhancements.testNested2WithContextCriteria5d1} in a procedure
     */
    public void testXMLWithExternalCriteria() throws Exception {
        FakeMetadataFacade metadata = TestXMLProcessor.exampleMetadataCached();
        FakeDataManager dataMgr = TestXMLProcessor.exampleDataManagerNested(metadata);
        String resultFile = "TestXMLProcessor-testNested2WithContextCriteria5d.xml"; //$NON-NLS-1$
        String expectedDoc = TestXMLProcessor.readFile(resultFile);
                
        FakeMetadataObject pm1 = metadata.getStore().findObject("xqttest",FakeMetadataObject.MODEL); //$NON-NLS-1$
        
        FakeMetadataObject rs2 = FakeMetadataFactory.createResultSet("pm1.rs2", pm1, new String[] { "e1" }, new String[] { DataTypeManager.DefaultDataTypes.XML }); //$NON-NLS-1$ //$NON-NLS-2$
        FakeMetadataObject rs2p1 = FakeMetadataFactory.createParameter("ret", 1, ParameterInfo.RESULT_SET, DataTypeManager.DefaultDataTypes.OBJECT, rs2);  //$NON-NLS-1$
        FakeMetadataObject rs2p2 = FakeMetadataFactory.createParameter("input", 2, ParameterInfo.IN, DataTypeManager.DefaultDataTypes.INTEGER, null);  //$NON-NLS-1$
        QueryNode sq2n1 = new QueryNode("xqttest.proc", "CREATE VIRTUAL PROCEDURE BEGIN\n" //$NON-NLS-1$ //$NON-NLS-2$
                                        + "declare integer VARIABLES.x = input; SELECT * FROM xmltest.doc9 WHERE context(SupplierID, OrderID)=x OR OrderID='2'; END"); //$NON-NLS-1$ 
        FakeMetadataObject sq2 = FakeMetadataFactory.createVirtualProcedure("xqttest.proc", pm1, Arrays.asList(new FakeMetadataObject[] { rs2p1, rs2p2 }), sq2n1);  //$NON-NLS-1$

        metadata.getStore().addObject(rs2);
        metadata.getStore().addObject(sq2);
        
        String userUpdateStr = "EXEC xqttest.proc(5)"; //$NON-NLS-1$
        
        ProcessorPlan plan = getProcedurePlan(userUpdateStr, metadata);
                        
        // Create expected results
        List[] expected = new List[] {
                Arrays.asList(new Object[] { expectedDoc }),
        };        
        helpTestProcess(plan, expected, dataMgr);
    }
    
    public void testXMLWithExternalCriteria_InXMLVar() throws Exception {
        FakeMetadataFacade metadata = TestXMLProcessor.exampleMetadataCached();
        FakeDataManager dataMgr = TestXMLProcessor.exampleDataManagerNested(metadata);
        String resultFile = "TestXMLProcessor-testNested2WithContextCriteria5d.xml"; //$NON-NLS-1$
        String expectedDoc = TestXMLProcessor.readFile(resultFile);
        expectedDoc = StringUtilities.removeChars(expectedDoc, new char[] {'\r'});        
        FakeMetadataObject pm1 = metadata.getStore().findObject("xqttest",FakeMetadataObject.MODEL); //$NON-NLS-1$
        
        FakeMetadataObject rs2 = FakeMetadataFactory.createResultSet("pm1.rs2", pm1, new String[] { "e1" }, new String[] { DataTypeManager.DefaultDataTypes.XML }); //$NON-NLS-1$ //$NON-NLS-2$
        FakeMetadataObject rs2p1 = FakeMetadataFactory.createParameter("ret", 1, ParameterInfo.RESULT_SET, DataTypeManager.DefaultDataTypes.OBJECT, rs2);  //$NON-NLS-1$
        FakeMetadataObject rs2p2 = FakeMetadataFactory.createParameter("input", 2, ParameterInfo.IN, DataTypeManager.DefaultDataTypes.INTEGER, null);  //$NON-NLS-1$
        QueryNode sq2n1 = new QueryNode("xqttest.proc", "CREATE VIRTUAL PROCEDURE BEGIN\n" //$NON-NLS-1$ //$NON-NLS-2$
                                        + "declare integer VARIABLES.x = input; declare xml y = SELECT * FROM xmltest.doc9 WHERE context(SupplierID, OrderID)=x OR OrderID='2'; select convert(y, string); END"); //$NON-NLS-1$ 
        FakeMetadataObject sq2 = FakeMetadataFactory.createVirtualProcedure("xqttest.proc", pm1, Arrays.asList(new FakeMetadataObject[] { rs2p1, rs2p2 }), sq2n1);  //$NON-NLS-1$

        metadata.getStore().addObject(rs2);
        metadata.getStore().addObject(sq2);
        
        String userUpdateStr = "EXEC xqttest.proc(5)"; //$NON-NLS-1$
        
        ProcessorPlan plan = getProcedurePlan(userUpdateStr, metadata);
                        
        // Create expected results
        List[] expected = new List[] {
                Arrays.asList(new Object[] { expectedDoc }),
        };        
        helpTestProcess(plan, expected, dataMgr);
    }
    
    /**
     * wraps {@link TestXMLProcessor.testNested2WithCriteria2} in a procedure
     * 
     * This one will successfully auto-stage
     */
    public void testXMLWithExternalCriteria1() throws Exception {
        FakeMetadataFacade metadata = TestXMLProcessor.exampleMetadataCached();
        FakeDataManager dataMgr = TestXMLProcessor.exampleDataManagerNested(metadata);
        String expectedDoc = 
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\r\n" +  //$NON-NLS-1$
            "<Catalogs>\r\n" + //$NON-NLS-1$
            "    <Catalog>\r\n" +  //$NON-NLS-1$
            "        <Items>\r\n" +  //$NON-NLS-1$
            "            <Item ItemID=\"001\">\r\n" +  //$NON-NLS-1$
            "                <Name>Lamp</Name>\r\n" +  //$NON-NLS-1$
            "                <Quantity>5</Quantity>\r\n" + //$NON-NLS-1$
            "                <Suppliers>\r\n" + //$NON-NLS-1$
            "                    <Supplier SupplierID=\"52\">\r\n" + //$NON-NLS-1$
            "                        <Name>Biff's Stuff</Name>\r\n" + //$NON-NLS-1$
            "                        <Zip>22222</Zip>\r\n" + //$NON-NLS-1$
            "                        <Orders>\r\n" + //$NON-NLS-1$
            "                            <Order OrderID=\"2\">\r\n" + //$NON-NLS-1$
            "                                <OrderDate>12/31/01</OrderDate>\r\n" + //$NON-NLS-1$
            "                                <OrderQuantity>87</OrderQuantity>\r\n" + //$NON-NLS-1$
            "                                <OrderStatus>complete</OrderStatus>\r\n" + //$NON-NLS-1$
            "                            </Order>\r\n" + //$NON-NLS-1$
            "                        </Orders>\r\n" + //$NON-NLS-1$
            "                    </Supplier>\r\n" + //$NON-NLS-1$
            "                </Suppliers>\r\n" + //$NON-NLS-1$
            "            </Item>\r\n" +  //$NON-NLS-1$
            "            <Item ItemID=\"002\">\r\n" +  //$NON-NLS-1$
            "                <Name>Screwdriver</Name>\r\n" +  //$NON-NLS-1$
            "                <Quantity>100</Quantity>\r\n" +  //$NON-NLS-1$
            "                <Suppliers/>\r\n" + //$NON-NLS-1$
            "            </Item>\r\n" +  //$NON-NLS-1$
            "            <Item ItemID=\"003\">\r\n" +  //$NON-NLS-1$
            "                <Name>Goat</Name>\r\n" +  //$NON-NLS-1$
            "                <Quantity>4</Quantity>\r\n" +  //$NON-NLS-1$
            "                <Suppliers/>\r\n" + //$NON-NLS-1$
            "            </Item>\r\n" +  //$NON-NLS-1$
            "        </Items>\r\n" +  //$NON-NLS-1$
            "    </Catalog>\r\n" +  //$NON-NLS-1$
            "</Catalogs>\r\n\r\n"; //$NON-NLS-1$

        FakeMetadataObject pm1 = metadata.getStore().findObject("xmltest",FakeMetadataObject.MODEL); //$NON-NLS-1$
        
        FakeMetadataObject rs2 = FakeMetadataFactory.createResultSet("pm1.rs2", pm1, new String[] { "e1" }, new String[] { DataTypeManager.DefaultDataTypes.XML }); //$NON-NLS-1$ //$NON-NLS-2$
        FakeMetadataObject rs2p1 = FakeMetadataFactory.createParameter("ret", 1, ParameterInfo.RESULT_SET, DataTypeManager.DefaultDataTypes.OBJECT, rs2);  //$NON-NLS-1$
        FakeMetadataObject rs2p2 = FakeMetadataFactory.createParameter("input", 2, ParameterInfo.IN, DataTypeManager.DefaultDataTypes.INTEGER, null);  //$NON-NLS-1$
        QueryNode sq2n1 = new QueryNode("xmltest.proc", "CREATE VIRTUAL PROCEDURE BEGIN\n" //$NON-NLS-1$ //$NON-NLS-2$
                                        + "declare integer VARIABLES.x = input; SELECT * FROM xmltest.doc9 WHERE context(SupplierID, SupplierID)=x; END"); //$NON-NLS-1$ 
        FakeMetadataObject sq2 = FakeMetadataFactory.createVirtualProcedure("xmltest.proc", pm1, Arrays.asList(new FakeMetadataObject[] { rs2p1, rs2p2 }), sq2n1);  //$NON-NLS-1$

        metadata.getStore().addObject(rs2);
        metadata.getStore().addObject(sq2);
        
        String userUpdateStr = "EXEC xmltest.proc(52)"; //$NON-NLS-1$
        
        ProcessorPlan plan = getProcedurePlan(userUpdateStr, metadata);
                        
        // Create expected results
        List[] expected = new List[] {
                Arrays.asList(new Object[] { expectedDoc }),
        };        
        helpTestProcess(plan, expected, dataMgr);
    }
    
    public void testCase174806() throws Exception{
        String userUpdateStr = "EXEC pm1.vsp63()"; //$NON-NLS-1$
        
        FakeMetadataFacade metadata = FakeMetadataFactory.example1Cached();

        FakeDataManager dataMgr = exampleDataManager(metadata);

        ProcessorPlan plan = getProcedurePlan(userUpdateStr, metadata);
                
        // Create expected results
        List[] expected = new List[] {
            Arrays.asList(new Object[] { "c"})};         //$NON-NLS-1$
        helpTestProcess(plan, expected, dataMgr);
    }
    
    public void testJoinProcAndPhysicalModel() throws Exception {
        String userUpdateStr = "select a.e1 from (EXEC pm1.vsp46()) as a, pm1.g1 where a.e1=pm1.g1.e1";     //$NON-NLS-1$
        FakeMetadataFacade metadata = FakeMetadataFactory.example1Cached();

        Command userCommand = null;       
        QueryParser parser = new QueryParser();
        userCommand = parser.parseCommand(userUpdateStr);
        QueryResolver.resolveCommand(userCommand, metadata);
        QueryRewriter.rewrite(userCommand, null, metadata, null);

        ProcessorPlan plan = null;        
        AnalysisRecord analysisRecord = new AnalysisRecord(false, false, DEBUG);
        try {
            plan = QueryOptimizer.optimizePlan(userCommand, metadata, null, new DefaultCapabilitiesFinder(), analysisRecord, null);
        } finally {
            if(DEBUG) {
                System.out.println(analysisRecord.getDebugLog());     
            }
        }       

        // Set up data
        FakeDataManager dataMgr = exampleDataManager(metadata);
        
        // Create expected results
        List[] expected = new List[] {
            Arrays.asList(new Object[] { "First"}),  //$NON-NLS-1$
            Arrays.asList(new Object[] { "Second"}), //$NON-NLS-1$
            Arrays.asList(new Object[] { "Third"})};           //$NON-NLS-1$
        helpTestProcess(plan, expected, dataMgr);
    }

    /**
     * Test the procedure <code>DECLARE</code> statement using a query as the assignment value 
     * 
     * <p>The use of a query as the assignment value to a <code>DECLARE</code> 
     * statement should execute without error as long as the query 
     * is valid and fully resolvable.</p> 
     * 
     * <p>This test is related to JBEDSP-818 in which the query in the 
     * <code>LOOP</code> statement would fail due to a query being used as the 
     * assigned value in the <code>DECLARE</code> statement.</p> 
     * @throws Exception
     */
    public void testDeclareWithQueryAssignment() throws Exception {
    	// procedure comes from test case IT236455 / JBEDSP-818
        String procedure = "CREATE VIRTUAL PROCEDURE \n"; //$NON-NLS-1$
        procedure += "BEGIN\n"; //$NON-NLS-1$
        procedure += "   DECLARE integer VARIABLES.var1 = 0;\n"; //$NON-NLS-1$
        procedure += "   /* the following DECLARE with ASSIGNMENT to a query should work "; //$NON-NLS-1$
        procedure += "      but in IT236455 it results in the assignment inside the LOOP "; //$NON-NLS-1$
        procedure += "      to fail */ "; //$NON-NLS-1$
        procedure += "   DECLARE integer VARIABLES.NLEVELS = SELECT COUNT(*) FROM (\n"; //$NON-NLS-1$
        procedure += "                                          SELECT 'Col1' AS ACol1, 'Col2' AS ACol2, convert(3, integer) AS ACol3\n"; //$NON-NLS-1$
        procedure += "                                       ) AS Src;\n"; //$NON-NLS-1$
        procedure += "   LOOP ON (\n"; //$NON-NLS-1$
        procedure += "      SELECT StaticTable.BCol1, StaticTable.BCol2, StaticTable.BCol3 FROM (\n"; //$NON-NLS-1$
        procedure += "         SELECT 'Col 1' AS BCol1, 'Col 2' AS BCol2, convert(3, integer) AS BCol3\n"; //$NON-NLS-1$
        procedure += "      ) AS StaticTable\n"; //$NON-NLS-1$
        procedure += "   ) AS L1\n"; //$NON-NLS-1$
        procedure += "   BEGIN\n"; //$NON-NLS-1$
        procedure += "      /* In IT236455 the following would fail as the results from "; //$NON-NLS-1$
        procedure += "         the LOOP (L1) are not in scope when the assignment is being "; //$NON-NLS-1$
        procedure += "         performed due to the query earlier being part of a DECLARE  "; //$NON-NLS-1$
        procedure += "         statement. */  "; //$NON-NLS-1$
        procedure += "      VARIABLES.var1 = L1.BCol3;\n"; //$NON-NLS-1$
        procedure += "   END\n"; //$NON-NLS-1$
        procedure += "   SELECT VARIABLES.Var1 AS e1;\n"; //$NON-NLS-1$
        procedure += "END\n"; //$NON-NLS-1$

        FakeMetadataFacade metadata = createProcedureMetadata(procedure);
        String userQuery = "SELECT e1 FROM (EXEC pm1.sq1()) as proc"; //$NON-NLS-1$
        FakeDataManager dataMgr = exampleDataManager(metadata);
        ProcessorPlan plan = getProcedurePlan(userQuery, metadata);

        List[] expected = new List[] {Arrays.asList(new Object[] {new Integer(3)})};
        helpTestProcess(plan, expected, dataMgr);
    }
    
    public void testDefect8693() throws Exception {
        String procedure = "CREATE PROCEDURE  "; //$NON-NLS-1$
        procedure = procedure + "BEGIN\n"; //$NON-NLS-1$
        procedure = procedure + "DECLARE integer var1;\n"; //$NON-NLS-1$
        procedure = procedure + "var1 = Select pm1.g1.e2 from pm1.g1 where e2 = 5;\n"; //$NON-NLS-1$
        procedure = procedure + "if (5 in (select 5 from pm1.g1))\n"; //$NON-NLS-1$
        procedure = procedure + "BEGIN\n";       //$NON-NLS-1$
        procedure = procedure + "ROWS_UPDATED = ROWS_UPDATED + var1;\n"; //$NON-NLS-1$
        procedure = procedure + "END\n";         //$NON-NLS-1$
        procedure = procedure + "END"; //$NON-NLS-1$

        String userUpdateStr = "UPDATE vm1.g1 SET e1='x'"; //$NON-NLS-1$
        
        FakeMetadataFacade metadata = FakeMetadataFactory.exampleUpdateProc(FakeMetadataObject.Props.UPDATE_PROCEDURE, procedure);
        FakeDataManager dataMgr = exampleDataManager(metadata);
		ProcessorPlan plan = getProcedurePlan(userUpdateStr, metadata);
		helpTestProcess(plan, 5, dataMgr);									 
    }
    
    public void testWhileWithSubquery() throws Exception {
        String procedure = "CREATE PROCEDURE  "; //$NON-NLS-1$
        procedure = procedure + "BEGIN\n"; //$NON-NLS-1$
        procedure = procedure + "DECLARE integer var1 = 2;\n"; //$NON-NLS-1$
        procedure = procedure + "WHILE (5 in (select var1 from pm1.g1))\n"; //$NON-NLS-1$
        procedure = procedure + "BEGIN\n";       //$NON-NLS-1$
        procedure = procedure + "ROWS_UPDATED = ROWS_UPDATED + var1;\n"; //$NON-NLS-1$
        procedure = procedure + "END\n";         //$NON-NLS-1$
        procedure = procedure + "END"; //$NON-NLS-1$

        String userUpdateStr = "UPDATE vm1.g1 SET e1='x'"; //$NON-NLS-1$
                                     
        FakeMetadataFacade metadata = FakeMetadataFactory.exampleUpdateProc(FakeMetadataObject.Props.UPDATE_PROCEDURE, procedure);
        FakeDataManager dataMgr = exampleDataManager(metadata);
		ProcessorPlan plan = getProcedurePlan(userUpdateStr, metadata);
		helpTestProcess(plan, 0, dataMgr);									 
    }
    
    public void testDefect18404() throws Exception {
        String procedure = "CREATE PROCEDURE  "; //$NON-NLS-1$
        procedure = procedure + "BEGIN\n"; //$NON-NLS-1$
        procedure = procedure + "DECLARE integer var1 = 5 + (select count(e2) from pm1.g1);\n"; //$NON-NLS-1$
        procedure = procedure + "ROWS_UPDATED = ROWS_UPDATED + var1;\n"; //$NON-NLS-1$
        procedure = procedure + "END"; //$NON-NLS-1$
    
        String userUpdateStr = "UPDATE vm1.g1 SET e1='x'"; //$NON-NLS-1$
    
        FakeMetadataFacade metadata = FakeMetadataFactory.exampleUpdateProc(FakeMetadataObject.Props.UPDATE_PROCEDURE, procedure);
        FakeDataManager dataMgr = exampleDataManager(metadata);
		ProcessorPlan plan = getProcedurePlan(userUpdateStr, metadata);
		helpTestProcess(plan, 8, dataMgr);									 
    }

    /**
     * Test the use of a procedure variable in the criteria of a LEFT OUTER
     * JOIN which will be optimized out as non-JOIN criteria.
     * <p>
     * This test case verifies that the procedure variable will not be pushed 
     * to the data manager when a federated source JOIN is performed. 
     *  
     * @throws Exception
     */
    public void testRemovalOfNonJoinCritWithReference() throws Exception {
    	String proc = ""; //$NON-NLS-1$
    	
        String sql = ""; //$NON-NLS-1$
        sql += "SELECT " +  //$NON-NLS-1$
        		"	pm1.g1.e1 AS pm1g1e1, " +  //$NON-NLS-1$
        		"	pm2.g2.e1 AS pm2g2e1, " +  //$NON-NLS-1$
        		"	pm1.g1.e2 AS pm1g1e2, " +  //$NON-NLS-1$
        		"	pm2.g2.e2 AS pm2g2e2 " +  //$NON-NLS-1$
        		"FROM " +  //$NON-NLS-1$
        		"	pm1.g1	" +  //$NON-NLS-1$
        		"LEFT OUTER JOIN pm2.g2 " +  //$NON-NLS-1$
        		"	ON pm1.g1.e1 = pm2.g2.e1 " +  //$NON-NLS-1$ 
        		"	AND pm2.g2.e2 = VARIABLES.myVar ";  //$NON-NLS-1$
        
        proc += "CREATE VIRTUAL PROCEDURE " + //$NON-NLS-1$
        		"BEGIN " + //$NON-NLS-1$
        		"   declare integer myVar = 5;" + //$NON-NLS-1$
        		"   " + sql + ";" + //$NON-NLS-1$ //$NON-NLS-2$
        		"END"; //$NON-NLS-1$
        	
        FakeMetadataFacade metadata = createProcedureMetadata(proc);
        String userQuery = "SELECT * FROM (EXEC pm1.sq1()) as proc"; //$NON-NLS-1$
        FakeDataManager dataMgr = exampleDataManager2(metadata);
        ProcessorPlan plan = getProcedurePlan(userQuery, metadata);

        List[] expected = new List[] {
                Arrays.asList( new Object[] { "First", "First", new Integer(5), new Integer(5)} ), //$NON-NLS-1$ //$NON-NLS-2$
                Arrays.asList( new Object[] { "Second", null, new Integer(15), null} ), //$NON-NLS-1$
                Arrays.asList( new Object[] { "Third", null, new Integer(51), null} ) //$NON-NLS-1$
        };
        helpTestProcess(plan, expected, dataMgr);
    }
    
    /**
     * Test the use of a procedure variable in the criteria of a LEFT OUTER
     * JOIN which will be optimized out as non-JOIN criteria.
     * <p>
     * This test case verifies that the procedure variable will not be pushed 
     * to the data manager when a federated source JOIN is performed and the 
     * physical source supports all capabilities. 
     *  
     * @throws Exception
     */
    public void testRemovalOfNonJoinCritWithReference2() throws Exception {
    	String proc = ""; //$NON-NLS-1$
    	
        String sql = ""; //$NON-NLS-1$
        sql += "SELECT " +  //$NON-NLS-1$
        		"	pm1.g1.e1 AS pm1g1e1, " +  //$NON-NLS-1$
        		"	pm2.g2.e1 AS pm2g2e1, " +  //$NON-NLS-1$
        		"	pm1.g1.e2 AS pm1g1e2, " +  //$NON-NLS-1$
        		"	pm2.g2.e2 AS pm2g2e2 " +  //$NON-NLS-1$
        		"FROM " +  //$NON-NLS-1$
        		"	pm1.g1	" +  //$NON-NLS-1$
        		"LEFT OUTER JOIN pm2.g2 " +  //$NON-NLS-1$
        		"	ON pm1.g1.e1 = pm2.g2.e1 " +  //$NON-NLS-1$ 
        		"	AND pm2.g2.e2 = VARIABLES.myVar ";  //$NON-NLS-1$
        
        proc += "CREATE VIRTUAL PROCEDURE " + //$NON-NLS-1$
        		"BEGIN " + //$NON-NLS-1$
        		"   declare integer myVar = 5;" + //$NON-NLS-1$
        		"   " + sql + ";" + //$NON-NLS-1$ //$NON-NLS-2$
        		"END"; //$NON-NLS-1$

        FakeMetadataFacade metadata = createProcedureMetadata(proc);
        String userQuery = "SELECT * FROM (EXEC pm1.sq1()) as proc"; //$NON-NLS-1$
        FakeDataManager dataMgr = exampleDataManager2(metadata);
        ProcessorPlan plan = getProcedurePlan(userQuery, metadata, TestOptimizer.getGenericFinder());

        List[] expected = new List[] {
                Arrays.asList( new Object[] { "First", "First", new Integer(5), new Integer(5)} ), //$NON-NLS-1$ //$NON-NLS-2$
                Arrays.asList( new Object[] { "Second", null, new Integer(15), null} ), //$NON-NLS-1$
                Arrays.asList( new Object[] { "Third", null, new Integer(51), null} ) //$NON-NLS-1$
        };
        helpTestProcess(plan, expected, dataMgr);
    }
    
    private static final boolean DEBUG = false;
}
