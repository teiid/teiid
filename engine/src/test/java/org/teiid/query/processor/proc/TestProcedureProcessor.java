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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.Test;
import org.teiid.api.exception.query.QueryMetadataException;
import org.teiid.api.exception.query.QueryProcessingException;
import org.teiid.api.exception.query.QueryValidatorException;
import org.teiid.client.metadata.ParameterInfo;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidException;
import org.teiid.core.types.DataTypeManager;
import org.teiid.query.analysis.AnalysisRecord;
import org.teiid.query.mapping.relational.QueryNode;
import org.teiid.query.metadata.QueryMetadataInterface;
import org.teiid.query.metadata.TempMetadataAdapter;
import org.teiid.query.metadata.TempMetadataStore;
import org.teiid.query.metadata.TransformationMetadata;
import org.teiid.query.optimizer.QueryOptimizer;
import org.teiid.query.optimizer.TestOptimizer;
import org.teiid.query.optimizer.capabilities.CapabilitiesFinder;
import org.teiid.query.optimizer.capabilities.DefaultCapabilitiesFinder;
import org.teiid.query.parser.QueryParser;
import org.teiid.query.processor.FakeDataManager;
import org.teiid.query.processor.FakeDataStore;
import org.teiid.query.processor.HardcodedDataManager;
import org.teiid.query.processor.ProcessorDataManager;
import org.teiid.query.processor.ProcessorPlan;
import org.teiid.query.processor.TestProcessor;
import org.teiid.query.processor.xml.TestXMLPlanningEnhancements;
import org.teiid.query.processor.xml.TestXMLProcessor;
import org.teiid.query.resolver.QueryResolver;
import org.teiid.query.rewriter.QueryRewriter;
import org.teiid.query.sql.lang.Command;
import org.teiid.query.sql.lang.SPParameter;
import org.teiid.query.sql.symbol.ElementSymbol;
import org.teiid.query.unittest.FakeMetadataFacade;
import org.teiid.query.unittest.FakeMetadataFactory;
import org.teiid.query.unittest.FakeMetadataObject;
import org.teiid.query.unittest.FakeMetadataStore;
import org.teiid.query.unittest.RealMetadataFactory;
import org.teiid.query.util.CommandContext;
import org.teiid.query.validator.Validator;
import org.teiid.query.validator.ValidatorFailure;
import org.teiid.query.validator.ValidatorReport;

@SuppressWarnings({"unchecked", "nls"})
public class TestProcedureProcessor {
	
    public static ProcessorPlan getProcedurePlan(String userQuery, QueryMetadataInterface metadata) throws Exception {
    	return getProcedurePlan(userQuery, metadata, /*capabilitiesFinder*/null);
    }
    
    public static ProcessorPlan getProcedurePlan(String userQuery, QueryMetadataInterface metadata, CapabilitiesFinder capabilitiesFinder) throws Exception {
        Command userCommand = QueryParser.getQueryParser().parseCommand(userQuery);
        QueryResolver.resolveCommand(userCommand, metadata);
        ValidatorReport report = Validator.validate(userCommand, metadata);
        
        if (report.hasItems()) {
            ValidatorFailure firstFailure = report.getItems().iterator().next();
            throw new QueryValidatorException(firstFailure.getMessage());
        }
        QueryRewriter.rewrite(userCommand, metadata, new CommandContext());

        AnalysisRecord analysisRecord = new AnalysisRecord(false, DEBUG);
        try {
        	if ( capabilitiesFinder == null ) capabilitiesFinder = new DefaultCapabilitiesFinder();
        	ProcessorPlan plan = QueryOptimizer.optimizePlan(userCommand, metadata, null, capabilitiesFinder, analysisRecord, null);

            return plan;
        } finally {
            if(DEBUG) {
                System.out.println(analysisRecord.getDebugLog());  	
            }
        }
    }
    
    public static void helpTestProcess(ProcessorPlan procPlan, List[] expectedResults, ProcessorDataManager dataMgr, QueryMetadataInterface metadata) throws Exception {
        CommandContext context = new CommandContext("pID", null, null, null, 1); //$NON-NLS-1$
        if (!(metadata instanceof TempMetadataAdapter)) {
        	metadata = new TempMetadataAdapter(metadata, new TempMetadataStore());
        }
        context.setMetadata(metadata);        	
        
    	TestProcessor.helpProcess(procPlan, context, dataMgr, expectedResults);
    	assertNotNull("Expected processing to fail", expectedResults);
    }

    private void helpTestProcessFailure(ProcessorPlan procPlan, FakeDataManager dataMgr, 
                                 String failMessage, QueryMetadataInterface metadata) throws Exception {
        try {
            helpTestProcess(procPlan, null, dataMgr, metadata);
        } catch(TeiidException ex) {
            assertEquals(failMessage, ex.getMessage());
        }
    }
    
    private void helpTestProcess(ProcessorPlan procPlan, int expectedRows, FakeDataManager dataMgr, QueryMetadataInterface metadata) throws Exception {
    	helpTestProcess(procPlan, new List[] {Arrays.asList(expectedRows)}, dataMgr, metadata);
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
    
    private FakeDataManager exampleDataManager(FakeMetadataFacade metadata) throws QueryMetadataException, TeiidComponentException {
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
    
    private FakeDataManager exampleDataManager2(FakeMetadataFacade metadata) throws QueryMetadataException, TeiidComponentException {
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
    
    private FakeDataManager exampleDataManagerPm5(FakeMetadataFacade metadata) throws QueryMetadataException, TeiidComponentException {
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
    @Test public void testProcedureProcessor1() throws Exception {
        String procedure = "CREATE PROCEDURE  "; //$NON-NLS-1$
        procedure = procedure + "BEGIN\n"; //$NON-NLS-1$
        procedure = procedure + "DECLARE integer var1 = 0;\n"; //$NON-NLS-1$
		procedure = procedure + "ROWS_UPDATED = ROWS_UPDATED + var1;\n"; //$NON-NLS-1$
        procedure = procedure + "END\n"; //$NON-NLS-1$

        String userUpdateStr = "UPDATE vm1.g1 SET e1='x'"; //$NON-NLS-1$
        
        FakeMetadataFacade metadata = FakeMetadataFactory.exampleUpdateProc(FakeMetadataObject.Props.UPDATE_PROCEDURE, procedure);
        
        FakeDataManager dataMgr = exampleDataManager(metadata);

		ProcessorPlan plan = getProcedurePlan(userUpdateStr, metadata);
									 
		helpTestProcess(plan, 0, dataMgr, metadata);									 
    }

	// testing if statement    
    @Test public void testProcedureProcessor2() throws Exception {
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
									 
		helpTestProcess(plan, 5, dataMgr, metadata);
    }
    
    // testing if statement    
    @Test public void testProcedureProcessor2WithBlockedException() throws Exception  {
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
                                     
        helpTestProcess(plan, 5, dataMgr, metadata);
    }

	// testing rows updated incremented, Input and assignment statements
    @Test public void testProcedureProcessor3() throws Exception {
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
									 
		helpTestProcess(plan, 45, dataMgr, metadata);									 
    }
    
    // if/else test
    @Test public void testProcedureProcessor4() throws Exception {
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
									 
		helpTestProcess(plan, 5, dataMgr, metadata);									 
    }
    
    @Test public void testProcedureProcessor4WithBlockedException() throws Exception {
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
                                     
        helpTestProcess(plan, 5, dataMgr, metadata);                                     
    }

    // if/else test    
    @Test public void testProcedureProcessor5() throws Exception {
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
									 
		helpTestProcess(plan, 50, dataMgr, metadata);									 
    }
    
    // more rows than expected
    @Test public void testProcedureProcessor6() throws Exception {
        String procedure = "CREATE PROCEDURE  "; //$NON-NLS-1$
        procedure = procedure + "BEGIN\n"; //$NON-NLS-1$
        procedure = procedure + "ROWS_UPDATED = Select pm1.g1.e2 from pm1.g1;\n"; //$NON-NLS-1$
        procedure = procedure + "END\n"; //$NON-NLS-1$

        String userUpdateStr = "UPDATE vm1.g1 SET e1='x'"; //$NON-NLS-1$
        
        FakeMetadataFacade metadata = FakeMetadataFactory.exampleUpdateProc(FakeMetadataObject.Props.UPDATE_PROCEDURE, procedure);
        
        FakeDataManager dataMgr = exampleDataManager(metadata);

		ProcessorPlan plan = getProcedurePlan(userUpdateStr, metadata);
									 
        helpTestProcessFailure(plan, dataMgr, "Error Code:ERR.015.006.0058 Message:Unable to evaluate (SELECT pm1.g1.e2 FROM pm1.g1 LIMIT 2): Error Code:ERR.015.006.0058 Message:The command of this scalar subquery returned more than one value: SELECT pm1.g1.e2 FROM pm1.g1 LIMIT 2", metadata); //$NON-NLS-1$ 
    }

    // error statement
    @Test public void testProcedureProcessor7() throws Exception {
        String errorValue = "'MY ERROR'"; //$NON-NLS-1$
        helpTestErrorStatment(errorValue, "MY ERROR"); //$NON-NLS-1$
    }
    
    @Test public void testProcedureProcessor8() throws Exception {
        String errorValue = "var1"; //$NON-NLS-1$
        helpTestErrorStatment(errorValue, "5"); //$NON-NLS-1$
    }
    
    @Test public void testProcedureProcessor9() throws Exception {
        String errorValue = "var1||'MY ERROR'"; //$NON-NLS-1$
        helpTestErrorStatment(errorValue, "5MY ERROR"); //$NON-NLS-1$
    }
        
    @Test public void testProcedureProcessor10() throws Exception {
        String procedure = "CREATE PROCEDURE  "; //$NON-NLS-1$
        procedure = procedure + "BEGIN\n"; //$NON-NLS-1$
        procedure = procedure + "loop on (Select pm1.g1.e2 from pm1.g1 where e2 = 5) as mycursor\n"; //$NON-NLS-1$
        procedure = procedure + "BEGIN\n"; //$NON-NLS-1$ 
        procedure = procedure + "ERROR (mycursor.e2||'MY ERROR');\n"; //$NON-NLS-1$
        procedure = procedure + "ROWS_UPDATED = 0;\n"; //$NON-NLS-1$
        procedure = procedure + "END\n"; //$NON-NLS-1$
        procedure = procedure + "END\n"; //$NON-NLS-1$

        String userUpdateStr = "UPDATE vm1.g1 SET e1='x'"; //$NON-NLS-1$
        
        FakeMetadataFacade metadata = FakeMetadataFactory.exampleUpdateProc(FakeMetadataObject.Props.UPDATE_PROCEDURE, procedure);
        
        FakeDataManager dataMgr = exampleDataManager(metadata);

        ProcessorPlan plan = getProcedurePlan(userUpdateStr, metadata);
                                     
        helpTestProcessFailure(plan, dataMgr, ErrorInstruction.ERROR_PREFIX + "5MY ERROR", metadata); //$NON-NLS-1$ 
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
									 
        helpTestProcessFailure(plan, dataMgr, ErrorInstruction.ERROR_PREFIX + expected, metadata); 
    }
    
	/** test if statement's if block with lookup in if condition */
	@Test public void testLookupFunction1() throws Exception {     
		String procedure = "CREATE PROCEDURE "; //$NON-NLS-1$
		procedure = procedure + "BEGIN\n"; //$NON-NLS-1$
		procedure = procedure + "DECLARE integer var2;\n"; //$NON-NLS-1$
		procedure = procedure + "if('a' = lookup('pm1.g1','e1', 'e2', 0))\n"; //$NON-NLS-1$
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
		FakeDataStore.sampleData2(dataMgr);

		ProcessorPlan plan = getProcedurePlan(userUpdateStr, metadata);                         
		helpTestProcess(plan, 1, dataMgr, metadata);        

	}
	
	/** test if statement's else block with lookup in if condition */
	@Test public void testLookupFunction2() throws Exception {     
		String procedure = "CREATE PROCEDURE "; //$NON-NLS-1$
		procedure = procedure + "BEGIN\n"; //$NON-NLS-1$
		procedure = procedure + "DECLARE integer var2;\n"; //$NON-NLS-1$
		procedure = procedure + "if('a' = lookup('pm1.g1','e1', 'e2', 5))\n"; //$NON-NLS-1$
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
		FakeDataStore.sampleData2(dataMgr);
		
		ProcessorPlan plan = getProcedurePlan(userUpdateStr, metadata);                         
		helpTestProcess(plan, 12, dataMgr, metadata);        
	}

    @Test public void testVirtualProcedure() throws Exception {
        String userUpdateStr = "EXEC pm1.vsp2()"; //$NON-NLS-1$
    
        FakeMetadataFacade metadata = FakeMetadataFactory.example1Cached();

        FakeDataManager dataMgr = exampleDataManager(metadata);

        ProcessorPlan plan = getProcedurePlan(userUpdateStr, metadata);
                
        // Create expected results
        List[] expected = new List[] {
            Arrays.asList(new Object[] { "First"}),  //$NON-NLS-1$
            Arrays.asList(new Object[] { "Second"}),  //$NON-NLS-1$
            Arrays.asList(new Object[] { "Third"})};         //$NON-NLS-1$
        helpTestProcess(plan, expected, dataMgr, metadata);
    }
    
    @Test public void testVirtualProcedureWithBlockedException() throws Exception {
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
        helpTestProcess(plan, expected, dataMgr, metadata);
    }
    
    @Test public void testVirtualProcedure2() throws Exception {
        String userUpdateStr = "EXEC pm1.vsp3()"; //$NON-NLS-1$
    
        FakeMetadataFacade metadata = FakeMetadataFactory.example1Cached();

        FakeDataManager dataMgr = exampleDataManager(metadata);

        ProcessorPlan plan = getProcedurePlan(userUpdateStr, metadata);
                
        // Create expected results
        List[] expected = new List[] {
            Arrays.asList(new Object[] { "Third"})};         //$NON-NLS-1$
        helpTestProcess(plan, expected, dataMgr, metadata);
    }
    
    @Test public void testVirtualProcedure3() throws Exception {
        String userUpdateStr = "EXEC pm1.vsp4()"; //$NON-NLS-1$
    
        FakeMetadataFacade metadata = FakeMetadataFactory.example1Cached();

        FakeDataManager dataMgr = exampleDataManager(metadata);

        ProcessorPlan plan = getProcedurePlan(userUpdateStr, metadata);
                
        // Create expected results
        List[] expected = new List[] {
            Arrays.asList(new Object[] { "First"})};         //$NON-NLS-1$
        helpTestProcess(plan, expected, dataMgr, metadata);
    }
        
    @Test public void testVirtualProcedure4() throws Exception {
        String userUpdateStr = "EXEC pm1.vsp5()"; //$NON-NLS-1$
    
        FakeMetadataFacade metadata = FakeMetadataFactory.example1Cached();

        FakeDataManager dataMgr = exampleDataManager(metadata);

        ProcessorPlan plan = getProcedurePlan(userUpdateStr, metadata);
                
        // Create expected results
        List[] expected = new List[] {
            Arrays.asList(new Object[] { "First"})};         //$NON-NLS-1$
        helpTestProcess(plan, expected, dataMgr, metadata);
    }
    
    @Test public void testVirtualProcedure5() throws Exception {
        String userUpdateStr = "EXEC pm1.vsp6()"; //$NON-NLS-1$
    
        FakeMetadataFacade metadata = FakeMetadataFactory.example1Cached();

        FakeDataManager dataMgr = exampleDataManager(metadata);

        ProcessorPlan plan = getProcedurePlan(userUpdateStr, metadata);
                
        // Create expected results
        List[] expected = new List[] {
            Arrays.asList(new Object[] { "Second"})};         //$NON-NLS-1$
        helpTestProcess(plan, expected, dataMgr, metadata);
    }
       
    @Test public void testVirtualProcedure6() throws Exception {
        String userUpdateStr = "EXEC pm1.vsp7(5)"; //$NON-NLS-1$
    
        FakeMetadataFacade metadata = FakeMetadataFactory.example1Cached();

        FakeDataManager dataMgr = exampleDataManager(metadata);

        ProcessorPlan plan = getProcedurePlan(userUpdateStr, metadata);
                
        // Create expected results
        List[] expected = new List[] {
            Arrays.asList(new Object[] { "Second"})};         //$NON-NLS-1$
        helpTestProcess(plan, expected, dataMgr, metadata);
    }
          
    @Test public void testVirtualProcedure7() throws Exception {
        String userUpdateStr = "EXEC pm1.vsp8(51)"; //$NON-NLS-1$
    
        FakeMetadataFacade metadata = FakeMetadataFactory.example1Cached();

        FakeDataManager dataMgr = exampleDataManager(metadata);

        ProcessorPlan plan = getProcedurePlan(userUpdateStr, metadata);
                
        // Create expected results
        List[] expected = new List[] {
            Arrays.asList(new Object[] { "Third"})};         //$NON-NLS-1$
        helpTestProcess(plan, expected, dataMgr, metadata);
    }
              
    @Test public void testVirtualProcedure8() throws Exception {
        String userUpdateStr = "EXEC pm1.vsp9(51)"; //$NON-NLS-1$
    
        FakeMetadataFacade metadata = FakeMetadataFactory.example1Cached();

        FakeDataManager dataMgr = exampleDataManager(metadata);

        ProcessorPlan plan = getProcedurePlan(userUpdateStr, metadata);
                
        // Create expected results
        List[] expected = new List[] {
            Arrays.asList(new Object[] { "Third"})};         //$NON-NLS-1$
        helpTestProcess(plan, expected, dataMgr, metadata);
    }
             
    @Test public void testVirtualProcedure9() throws Exception {
        String userUpdateStr = "EXEC pm1.vsp10(51)"; //$NON-NLS-1$
    
        FakeMetadataFacade metadata = FakeMetadataFactory.example1Cached();

        FakeDataManager dataMgr = exampleDataManager(metadata);

        ProcessorPlan plan = getProcedurePlan(userUpdateStr, metadata);
                
        // Create expected results
        List[] expected = new List[] {};        
        helpTestProcess(plan, expected, dataMgr, metadata);
    }

              
    @Test public void testVirtualProcedure10() throws Exception {
        String userUpdateStr = "EXEC pm1.vsp13()"; //$NON-NLS-1$
    
        FakeMetadataFacade metadata = FakeMetadataFactory.example1Cached();

        FakeDataManager dataMgr = exampleDataManager(metadata);

        ProcessorPlan plan = getProcedurePlan(userUpdateStr, metadata);
                
        // Create expected results
        List[] expected = new List[] {
            Arrays.asList(new Object[] { "Third", new Integer(5)})};         //$NON-NLS-1$
        helpTestProcess(plan, expected, dataMgr, metadata);
    }
    
    @Test public void testVirtualProcedure11() throws Exception {
        String userUpdateStr = "EXEC pm1.vsp14()";     //$NON-NLS-1$
        FakeMetadataFacade metadata = FakeMetadataFactory.example1Cached();
        // Set up data
        FakeDataManager dataMgr = exampleDataManager(metadata);

        ProcessorPlan plan = getProcedurePlan(userUpdateStr, metadata);
        
        // Create expected results
        List[] expected = new List[] {
            Arrays.asList(new Object[] { "First"}),  //$NON-NLS-1$
            Arrays.asList(new Object[] { "Second"}),  //$NON-NLS-1$
            Arrays.asList(new Object[] { "Third"})};           //$NON-NLS-1$
        helpTestProcess(plan, expected, dataMgr, metadata);
    }
    
    @Test public void testVirtualProcedure12() throws Exception {
        String userUpdateStr = "EXEC pm1.vsp15()";     //$NON-NLS-1$
        FakeMetadataFacade metadata = FakeMetadataFactory.example1Cached();
        // Set up data
        FakeDataManager dataMgr = exampleDataManager(metadata);

        ProcessorPlan plan = getProcedurePlan(userUpdateStr, metadata);
                
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
        helpTestProcess(plan, expected, dataMgr, metadata);
    }
    
    //Defect17447_testVirtualProcedure13
    @Test public void testVirtualProcedure13() throws Exception {
        String userUpdateStr = "EXEC pm1.vsp16()";     //$NON-NLS-1$
        FakeMetadataFacade metadata = FakeMetadataFactory.example1Cached();

        // Set up data
        FakeDataManager dataMgr = exampleDataManager(metadata);

        ProcessorPlan plan = getProcedurePlan(userUpdateStr, metadata);
                
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
        helpTestProcess(plan, expected, dataMgr, metadata);
    }
    		
    @Test public void testVirtualProcedure14() throws Exception {
        String userUpdateStr = "EXEC pm1.vsp17()";     //$NON-NLS-1$
        FakeMetadataFacade metadata = FakeMetadataFactory.example1Cached();
        // Set up data
        FakeDataManager dataMgr = exampleDataManager(metadata);

        ProcessorPlan plan = getProcedurePlan(userUpdateStr, metadata);
        
        // Create expected results
        List[] expected = new List[] {
            Arrays.asList(new Object[] { "Third"})};           //$NON-NLS-1$
        helpTestProcess(plan, expected, dataMgr, metadata);
    }

    @Test public void testVirtualProcedure15() throws Exception {
        String userUpdateStr = "EXEC pm1.vsp19()";     //$NON-NLS-1$
        FakeMetadataFacade metadata = FakeMetadataFactory.example1Cached();

        // Set up data
        FakeDataManager dataMgr = exampleDataManager(metadata);

        ProcessorPlan plan = getProcedurePlan(userUpdateStr, metadata);  
        
        // Create expected results
        List[] expected = new List[] {
            Arrays.asList(new Object[] { "First"}),  //$NON-NLS-1$
            Arrays.asList(new Object[] { "Second"}),  //$NON-NLS-1$
            Arrays.asList(new Object[] { "Third"}), //$NON-NLS-1$
            Arrays.asList(new Object[] { "First"}),  //$NON-NLS-1$
            Arrays.asList(new Object[] { "Second"}),  //$NON-NLS-1$
            Arrays.asList(new Object[] { "Third"})};           //$NON-NLS-1$
        helpTestProcess(plan, expected, dataMgr, metadata);
    }
    
    @Test public void testVirtualProcedure16() throws Exception {
        String userUpdateStr = "EXEC pm1.vsp20()";     //$NON-NLS-1$
        FakeMetadataFacade metadata = FakeMetadataFactory.example1Cached();
        // Set up data
        FakeDataManager dataMgr = exampleDataManager(metadata);

        ProcessorPlan plan = getProcedurePlan(userUpdateStr, metadata); 
        // Create expected results
        List[] expected = new List[] {
            Arrays.asList(new Object[] { "First"}),  //$NON-NLS-1$
            Arrays.asList(new Object[] { "Second"}),  //$NON-NLS-1$
            Arrays.asList(new Object[] { "Third"}),  //$NON-NLS-1$
            Arrays.asList(new Object[] { "Fourth"})};           //$NON-NLS-1$
        CommandContext context = new CommandContext("pID", null, null, null, 1); //$NON-NLS-1$
        context.setMetadata(metadata);
        context.setProcessorBatchSize(1); //ensure that the final temp result set will not be deleted prematurely 

    	TestProcessor.helpProcess(plan, context, dataMgr, expected);
    }
    
    @Test public void testVirtualProcedure17() throws Exception {
        String userUpdateStr = "EXEC pm1.vsp21(7)";     //$NON-NLS-1$
        FakeMetadataFacade metadata = FakeMetadataFactory.example1Cached();
        //Set up data
        FakeDataManager dataMgr = exampleDataManager(metadata);

        ProcessorPlan plan = getProcedurePlan(userUpdateStr, metadata);
  
        // Create expected results
        List[] expected = new List[] {
            Arrays.asList(new Object[] { "First", new Integer(5)}),  //$NON-NLS-1$
            Arrays.asList(new Object[] { "Second", new Integer(15)}),  //$NON-NLS-1$
            Arrays.asList(new Object[] { "Third", new Integer(51)}),  //$NON-NLS-1$
            Arrays.asList(new Object[] { "Fourth", new Integer(7)})};           //$NON-NLS-1$
        helpTestProcess(plan, expected, dataMgr, metadata);
    }
    
    @Test public void testVirtualProcedure18() throws Exception {
        String userUpdateStr = "EXEC pm1.vsp22(7)";     //$NON-NLS-1$
        FakeMetadataFacade metadata = FakeMetadataFactory.example1Cached();
        //Set up data
        FakeDataManager dataMgr = exampleDataManager(metadata);

        ProcessorPlan plan = getProcedurePlan(userUpdateStr, metadata);  
  
        // Create expected results
        List[] expected = new List[] {
            Arrays.asList(new Object[] { "Second", new Integer(15)}),  //$NON-NLS-1$
            Arrays.asList(new Object[] { "Third", new Integer(51)}) }; //$NON-NLS-1$
        helpTestProcess(plan, expected, dataMgr, metadata);
    }
    
    @Test public void testVirtualProcedure19() throws Exception {
        String userUpdateStr = "EXEC pm1.vsp23(7)";     //$NON-NLS-1$
        FakeMetadataFacade metadata = FakeMetadataFactory.example1Cached();
        //Set up data
        FakeDataManager dataMgr = exampleDataManager(metadata);

        ProcessorPlan plan = getProcedurePlan(userUpdateStr, metadata);  
  
        // Create expected results
        List[] expected = new List[] {
            Arrays.asList(new Object[] { "Second", new Integer(15)})}; //$NON-NLS-1$
        helpTestProcess(plan, expected, dataMgr, metadata);
    }
    
    @Test public void testVirtualProcedure19WithBlockedException() throws Exception {
        String userUpdateStr = "EXEC pm1.vsp23(7)";     //$NON-NLS-1$
        FakeMetadataFacade metadata = FakeMetadataFactory.example1Cached();
        //Set up data
        FakeDataManager dataMgr = exampleDataManager(metadata);

        ProcessorPlan plan = getProcedurePlan(userUpdateStr, metadata); 
        // Create expected results
        List[] expected = new List[] {
            Arrays.asList(new Object[] { "Second", new Integer(15)})}; //$NON-NLS-1$
        helpTestProcess(plan, expected, dataMgr, metadata);
    }

    @Test public void testVirtualProcedureNoDataInTempTable() throws Exception {
        String userUpdateStr = "EXEC pm1.vsp25()";     //$NON-NLS-1$
        FakeMetadataFacade metadata = FakeMetadataFactory.example1Cached();

        //Set up data
        FakeDataManager dataMgr = exampleDataManager(metadata);

        ProcessorPlan plan = getProcedurePlan(userUpdateStr, metadata);  
        
        // Create expected results
        List[] expected = new List[] {};           
        helpTestProcess(plan, expected, dataMgr, metadata);
    }
    
    //procedure with Has Criteria and Translate Criteria 
    @Test public void testDefect13625() throws Exception {
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
									 
		helpTestProcess(plan, 5, dataMgr, metadata);
    }
    
    @Test public void testVirtualProcedure30() throws Exception {
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
        helpTestProcess(plan, expected, dataMgr, metadata);
    }

    @Test public void testVirtualProcedure31() throws Exception {
        String userUpdateStr = "EXEC pm1.vsp31(51)";     //$NON-NLS-1$
        FakeMetadataFacade metadata = FakeMetadataFactory.example1Cached();
        
        ProcessorPlan plan = getProcedurePlan(userUpdateStr, metadata);
        // Set up data
        FakeDataManager dataMgr = exampleDataManager(metadata);
  
        // Create expected results
        List[] expected = new List[] {
            Arrays.asList(new Object[] { "Third"}) }; //$NON-NLS-1$
        helpTestProcess(plan, expected, dataMgr, metadata);
    }

    @Test public void testVirtualProcedureDefect14282() throws Exception {
        String userUpdateStr = "EXEC pm1.vsp24()"; //$NON-NLS-1$
    
        FakeMetadataFacade metadata = FakeMetadataFactory.example1Cached();

        FakeDataManager dataMgr = exampleDataManager(metadata);

        ProcessorPlan plan = getProcedurePlan(userUpdateStr, metadata);
                
        // Create expected results
        List[] expected = new List[] {
            Arrays.asList(new Object[] { "Second"})};         //$NON-NLS-1$
        helpTestProcess(plan, expected, dataMgr, metadata);
    }    
    
    @Test public void testDefect16193() throws Exception {
        String userUpdateStr = "EXEC pm1.vsp35(51)";     //$NON-NLS-1$
        FakeMetadataFacade metadata = FakeMetadataFactory.example1Cached();
        ProcessorPlan plan = getProcedurePlan(userUpdateStr, metadata);

        // Set up data
        FakeDataManager dataMgr = exampleDataManager(metadata);
  
        // Create expected results
        List[] expected = new List[] {
            Arrays.asList(new Object[] { "Third"}) }; //$NON-NLS-1$
        helpTestProcess(plan, expected, dataMgr, metadata);
    }
    
    @Test public void testVirtualProcedure16602() throws Exception {
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
        helpTestProcess(plan, expected, dataMgr, metadata);
    }
    
    @Test public void testDefect16649_1() throws Exception {
        String userUpdateStr = "EXEC pm1.vsp38()";     //$NON-NLS-1$
        FakeMetadataFacade metadata = FakeMetadataFactory.example1Cached();
        ProcessorPlan plan = getProcedurePlan(userUpdateStr, metadata);

        // Set up data
        FakeDataManager dataMgr = exampleDataManager(metadata);
  
        // Create expected results
        List[] expected = new List[] {
            Arrays.asList(new Object[] { "Second"}) }; //$NON-NLS-1$
        helpTestProcess(plan, expected, dataMgr, metadata);
    }
    
    @Test public void testDefect16649_2() throws Exception {
        String userUpdateStr = "EXEC pm1.vsp39()";     //$NON-NLS-1$
        FakeMetadataFacade metadata = FakeMetadataFactory.example1Cached();
        ProcessorPlan plan = getProcedurePlan(userUpdateStr, metadata);

        // Set up data
        FakeDataManager dataMgr = exampleDataManager(metadata);
  
        // Create expected results
        List[] expected = new List[] {
            Arrays.asList(new Object[] { "Second"}) }; //$NON-NLS-1$
        helpTestProcess(plan, expected, dataMgr, metadata);
    }
    
    @Test public void testDefect16694() throws Exception {
        String userUpdateStr = "EXEC pm1.vsp40()";     //$NON-NLS-1$
        FakeMetadataFacade metadata = FakeMetadataFactory.example1Cached();
        ProcessorPlan plan = getProcedurePlan(userUpdateStr, metadata);

        // Set up data
        FakeDataManager dataMgr = exampleDataManager(metadata);
  
        // Create expected results
        List[] expected = new List[] {
            Arrays.asList(new Object[] { "Second"}) }; //$NON-NLS-1$
        helpTestProcess(plan, expected, dataMgr, metadata);
    }
    
    @Test public void testDefect16707() throws Exception {
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
        helpTestProcess(plan, expected, dataMgr, metadata);
    }
    
    @Test public void testDefect16707_1() throws Exception {
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
        helpTestProcess(plan, expected, dataMgr, metadata);
    }
    
    @Test public void testDefect17451() throws Exception {
        String userUpdateStr = "EXEC pm1.vsp45()";     //$NON-NLS-1$
        FakeMetadataFacade metadata = FakeMetadataFactory.example1Cached();

        //Set up data
        FakeDataManager dataMgr = exampleDataManager(metadata);

        ProcessorPlan plan = getProcedurePlan(userUpdateStr, metadata);
                    
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
        helpTestProcess(plan, expected, dataMgr, metadata);
    }
    
    //Defect 17447
    @Test public void testVirtualProcedure46() throws Exception {
        String userUpdateStr = "EXEC pm1.vsp46()";     //$NON-NLS-1$
        FakeMetadataFacade metadata = FakeMetadataFactory.example1Cached();

        ProcessorPlan plan = getProcedurePlan(userUpdateStr, metadata);       

        // Set up data
        FakeDataManager dataMgr = exampleDataManager(metadata);
        
        // Create expected results
        List[] expected = new List[] {
            Arrays.asList(new Object[] { "First"}),  //$NON-NLS-1$
            Arrays.asList(new Object[] { "Second"}), //$NON-NLS-1$
            Arrays.asList(new Object[] { "Third"})};           //$NON-NLS-1$
        helpTestProcess(plan, expected, dataMgr, metadata);
    }
    
    @Test public void testDefect17650() throws Exception {
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
                                     
        helpTestProcess(plan, 5, dataMgr, metadata);
    }
    
    @Test public void testDefect19982() throws Exception {
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
        helpTestProcess(plan, expected, dataMgr, metadata);
    } 
    
    @Test public void testCase3521() throws Exception {
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
        helpTestProcess(plan, expected, dataMgr, metadata);
    }

    //procedure with Has Criteria and Translate Criteria and changing
    @Test public void testDynamicCommandWithTranslate() throws Exception {
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
									 
		helpTestProcess(plan, 5, dataMgr, metadata);
    }
    
    @Test public void testDynamicCommandWithIntoExpression() throws Exception {
    	
    	//Test INTO clause with expression
    	FakeMetadataFacade metadata = FakeMetadataFactory.example1();
        
        FakeMetadataObject pm1 = metadata.getStore().findObject("pm1",FakeMetadataObject.MODEL); //$NON-NLS-1$
        
        FakeMetadataObject rs2 = FakeMetadataFactory.createResultSet("pm1.rs1", pm1, new String[] { "e1" }, new String[] { DataTypeManager.DefaultDataTypes.STRING }); //$NON-NLS-1$ //$NON-NLS-2$
        FakeMetadataObject rs2p1 = FakeMetadataFactory.createParameter("ret", 1, ParameterInfo.RESULT_SET, DataTypeManager.DefaultDataTypes.OBJECT, rs2);  //$NON-NLS-1$
        QueryNode sq2n1 = new QueryNode("CREATE VIRTUAL PROCEDURE BEGIN\n" //$NON-NLS-1$ //$NON-NLS-2$
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
        helpTestProcess(plan, expected, dataMgr, metadata);
      }
    
    @Test public void testDynamicCommandWithIntoAndLoop() throws Exception {
    	
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
        
        QueryNode sq2n1 = new QueryNode(procedure.toString()); //$NON-NLS-1$ 
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
        helpTestProcess(plan, expected, dataMgr, metadata);
      }
    
    @Test public void testDynamicCommandWithParameter() throws Exception {
        FakeMetadataFacade metadata = FakeMetadataFactory.example1();
        
        FakeMetadataObject pm1 = metadata.getStore().findObject("pm1",FakeMetadataObject.MODEL); //$NON-NLS-1$
        
        FakeMetadataObject rs2 = FakeMetadataFactory.createResultSet("pm1.rs2", pm1, new String[] { "e1", "e2" }, new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.INTEGER }); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        FakeMetadataObject rs2p1 = FakeMetadataFactory.createParameter("ret", 1, ParameterInfo.RESULT_SET, DataTypeManager.DefaultDataTypes.OBJECT, rs2);  //$NON-NLS-1$
        FakeMetadataObject rs2p2 = FakeMetadataFactory.createParameter("in", 2, ParameterInfo.IN, DataTypeManager.DefaultDataTypes.STRING, null);  //$NON-NLS-1$
        QueryNode sq2n1 = new QueryNode("CREATE VIRTUAL PROCEDURE BEGIN\n" //$NON-NLS-1$ //$NON-NLS-2$
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
        helpTestProcess(plan, expected, dataMgr, metadata);
    }
    
    @Test public void testDynamicCommandWithUsing() throws Exception {
        FakeMetadataFacade metadata = FakeMetadataFactory.example1();
        
        FakeMetadataObject pm1 = metadata.getStore().findObject("pm1",FakeMetadataObject.MODEL); //$NON-NLS-1$
        
        FakeMetadataObject rs2 = FakeMetadataFactory.createResultSet("pm1.rs2", pm1, new String[] { "e1", "e2" }, new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.INTEGER }); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        FakeMetadataObject rs2p1 = FakeMetadataFactory.createParameter("ret", 1, ParameterInfo.RESULT_SET, DataTypeManager.DefaultDataTypes.OBJECT, rs2);  //$NON-NLS-1$
        FakeMetadataObject rs2p2 = FakeMetadataFactory.createParameter("in", 2, ParameterInfo.IN, DataTypeManager.DefaultDataTypes.STRING, null);  //$NON-NLS-1$
        QueryNode sq2n1 = new QueryNode("CREATE VIRTUAL PROCEDURE BEGIN\n" //$NON-NLS-1$ //$NON-NLS-2$
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
        helpTestProcess(plan, expected, dataMgr, metadata);
    }
    
    @Test public void testDynamicCommandWithVariable() throws Exception {
        FakeMetadataFacade metadata = FakeMetadataFactory.example1();
        
        FakeMetadataObject pm1 = metadata.getStore().findObject("pm1",FakeMetadataObject.MODEL); //$NON-NLS-1$
        
        FakeMetadataObject rs2 = FakeMetadataFactory.createResultSet("pm1.rs2", pm1, new String[] { "e1", "e2" }, new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.INTEGER }); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        FakeMetadataObject rs2p1 = FakeMetadataFactory.createParameter("ret", 1, ParameterInfo.RESULT_SET, DataTypeManager.DefaultDataTypes.OBJECT, rs2);  //$NON-NLS-1$
        FakeMetadataObject rs2p2 = FakeMetadataFactory.createParameter("in", 2, ParameterInfo.IN, DataTypeManager.DefaultDataTypes.STRING, null);  //$NON-NLS-1$
        QueryNode sq2n1 = new QueryNode("CREATE VIRTUAL PROCEDURE BEGIN\n" //$NON-NLS-1$ //$NON-NLS-2$
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
        helpTestProcess(plan, expected, dataMgr, metadata);
    }
    
    @Test public void testDynamicCommandValidationFails() throws Exception {
        FakeMetadataFacade metadata = FakeMetadataFactory.example1();
        
        FakeMetadataObject pm1 = metadata.getStore().findObject("pm1",FakeMetadataObject.MODEL); //$NON-NLS-1$
        
        FakeMetadataObject rs2 = FakeMetadataFactory.createResultSet("pm1.rs2", pm1, new String[] { "e1", "e2" }, new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.INTEGER }); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        FakeMetadataObject rs2p1 = FakeMetadataFactory.createParameter("ret", 1, ParameterInfo.RESULT_SET, DataTypeManager.DefaultDataTypes.OBJECT, rs2);  //$NON-NLS-1$
        FakeMetadataObject rs2p2 = FakeMetadataFactory.createParameter("in", 2, ParameterInfo.IN, DataTypeManager.DefaultDataTypes.STRING, null);  //$NON-NLS-1$
        QueryNode sq2n1 = new QueryNode("CREATE VIRTUAL PROCEDURE BEGIN\n" //$NON-NLS-1$ //$NON-NLS-2$
                                        + "declare object VARIABLES.x; execute string 'SELECT xmlelement(name elem, x)'; select 1; END"); //$NON-NLS-1$ 
        FakeMetadataObject sq2 = FakeMetadataFactory.createVirtualProcedure("pm1.sq2", pm1, Arrays.asList(new FakeMetadataObject[] { rs2p1, rs2p2 }), sq2n1);  //$NON-NLS-1$

        metadata.getStore().addObject(rs2);
        metadata.getStore().addObject(sq2);
        
        String userUpdateStr = "EXEC pm1.sq2('First')"; //$NON-NLS-1$
        
        FakeDataManager dataMgr = exampleDataManager(metadata);

        ProcessorPlan plan = getProcedurePlan(userUpdateStr, metadata);
         
        try {
        	helpTestProcess(plan, null, dataMgr, metadata);
        	fail("exception expected");
        } catch (QueryProcessingException e) {
        	assertTrue(e.getCause() instanceof QueryValidatorException);
        }
    }

    @Test public void testDynamicCommandWithSingleSelect() throws Exception {
    	//Test select of a single value in a DynamicCommand
        FakeMetadataFacade metadata = FakeMetadataFactory.example1();
        
        FakeMetadataObject pm1 = metadata.getStore().findObject("pm1",FakeMetadataObject.MODEL); //$NON-NLS-1$
        FakeMetadataObject rs1 = FakeMetadataFactory.createResultSet("pm1.rs2", pm1, new String[] { "e1" }, new String[] { DataTypeManager.DefaultDataTypes.STRING }); //$NON-NLS-1$ //$NON-NLS-2$ 
        FakeMetadataObject rs2p1 = FakeMetadataFactory.createParameter("ret", 1, ParameterInfo.RESULT_SET, DataTypeManager.DefaultDataTypes.OBJECT, rs1);  //$NON-NLS-1$
        QueryNode sq2n1 = new QueryNode("CREATE VIRTUAL PROCEDURE BEGIN\n" //$NON-NLS-1$ //$NON-NLS-2$
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
        helpTestProcess(plan, expected, dataMgr, metadata);
    }

    
    //converts e1 from integer to string, with a different name
    @Test public void testDynamicCommandTypeConversion() throws Exception {
    	 FakeMetadataFacade metadata = FakeMetadataFactory.example1();
         
         FakeMetadataObject pm1 = metadata.getStore().findObject("pm1",FakeMetadataObject.MODEL); //$NON-NLS-1$
         
         FakeMetadataObject rs2 = FakeMetadataFactory.createResultSet("pm1.rs2", pm1, new String[] { "e1" }, new String[] { DataTypeManager.DefaultDataTypes.STRING }); //$NON-NLS-1$ //$NON-NLS-2$ 
         FakeMetadataObject rs2p1 = FakeMetadataFactory.createParameter("ret", 1, ParameterInfo.RESULT_SET, DataTypeManager.DefaultDataTypes.OBJECT, rs2);  //$NON-NLS-1$
         FakeMetadataObject rs2p2 = FakeMetadataFactory.createParameter("in", 2, ParameterInfo.IN, DataTypeManager.DefaultDataTypes.STRING, null);  //$NON-NLS-1$
         QueryNode sq2n1 = new QueryNode("CREATE VIRTUAL PROCEDURE BEGIN\n" //$NON-NLS-1$ //$NON-NLS-2$
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
         helpTestProcess(plan, expected, dataMgr, metadata);
    }
    
    @Test public void testDynamicCommandRecursion() throws Exception {
        FakeMetadataFacade metadata = FakeMetadataFactory.example1();

        FakeMetadataObject pm1 = metadata.getStore().findObject("pm1", FakeMetadataObject.MODEL); //$NON-NLS-1$

        FakeMetadataObject rs2 = FakeMetadataFactory
                                                    .createResultSet("pm1.rs2", pm1, new String[] {"e1", "e2"}, new String[] {DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.INTEGER}); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        FakeMetadataObject rs2p1 = FakeMetadataFactory
                                                      .createParameter("ret", 1, ParameterInfo.RESULT_SET, DataTypeManager.DefaultDataTypes.OBJECT, rs2); //$NON-NLS-1$
        FakeMetadataObject rs2p2 = FakeMetadataFactory
                                                      .createParameter("in", 2, ParameterInfo.IN, DataTypeManager.DefaultDataTypes.STRING, null); //$NON-NLS-1$
        QueryNode sq2n1 = new QueryNode("CREATE VIRTUAL PROCEDURE BEGIN\n" //$NON-NLS-1$ //$NON-NLS-2$
                                                   + "execute string 'EXEC pm1.sq2(''First'')' as e1 string, e2 integer; END"); //$NON-NLS-1$ //
        FakeMetadataObject sq2 = FakeMetadataFactory
                                                    .createVirtualProcedure("pm1.sq2", pm1, Arrays.asList(new FakeMetadataObject[] {rs2p1, rs2p2}), sq2n1); //$NON-NLS-1$

        metadata.getStore().addObject(rs2);
        metadata.getStore().addObject(sq2);

        String userUpdateStr = "EXEC pm1.sq2('First')"; //$NON-NLS-1$

        FakeDataManager dataMgr = exampleDataManager(metadata);

        ProcessorPlan plan = getProcedurePlan(userUpdateStr, metadata);

        helpTestProcessFailure(plan,
                               dataMgr,
                               "Couldn't execute the dynamic SQL command \"EXECUTE 'EXEC pm1.sq2(''First'')' AS e1 string, e2 integer\" with the SQL statement \"'EXEC pm1.sq2(''First'')'\" due to: There is a recursive invocation of group 'PM1.SQ2'. Please correct the SQL.", metadata); //$NON-NLS-1$
    }
    
    @Test public void testDynamicCommandIncorrectProjectSymbolCount() throws Exception {
    	//Tests dynamic query with incorrect number of elements   
        FakeMetadataFacade metadata = FakeMetadataFactory.example1();
        
        FakeMetadataObject pm1 = metadata.getStore().findObject("pm1",FakeMetadataObject.MODEL); //$NON-NLS-1$
        
        FakeMetadataObject rs2 = FakeMetadataFactory.createResultSet("pm1.rs1", pm1, new String[] { "e1" }, new String[] { DataTypeManager.DefaultDataTypes.STRING }); //$NON-NLS-1$ //$NON-NLS-2$
        FakeMetadataObject rs2p1 = FakeMetadataFactory.createParameter("ret", 1, ParameterInfo.RESULT_SET, DataTypeManager.DefaultDataTypes.OBJECT, rs2);  //$NON-NLS-1$
        FakeMetadataObject rs2p2 = FakeMetadataFactory.createParameter("in", 2, ParameterInfo.IN, DataTypeManager.DefaultDataTypes.STRING, null);  //$NON-NLS-1$
        QueryNode sq2n1 = new QueryNode("CREATE VIRTUAL PROCEDURE BEGIN\n" //$NON-NLS-1$ //$NON-NLS-2$
                                        + "SELECT pm1.g1.e1 FROM pm1.g1; END"); //$NON-NLS-1$ 
        FakeMetadataObject sq1 = FakeMetadataFactory.createVirtualProcedure("pm1.sq1", pm1, Arrays.asList(new FakeMetadataObject[] { rs2p1, rs2p2 }), sq2n1);  //$NON-NLS-1$
        
        QueryNode sq2n2 = new QueryNode("CREATE VIRTUAL PROCEDURE BEGIN\n" //$NON-NLS-1$ //$NON-NLS-2$
                + "execute string 'EXEC pm1.sq1(''First'')' as e1 string, e2 integer; END"); //$NON-NLS-1$ 
        FakeMetadataObject sq2 = FakeMetadataFactory.createVirtualProcedure("pm1.sq2", pm1, Arrays.asList(new FakeMetadataObject[] { rs2p1, rs2p2 }), sq2n2);  //$NON-NLS-1$

        metadata.getStore().addObject(rs2);
        metadata.getStore().addObject(sq1);
        metadata.getStore().addObject(sq2);
        
        String userUpdateStr = "EXEC pm1.sq2('test')"; //$NON-NLS-1$
        
        FakeDataManager dataMgr = exampleDataManager(metadata);

        ProcessorPlan plan = getProcedurePlan(userUpdateStr, metadata);
    	
        helpTestProcessFailure(plan, dataMgr, "Couldn't execute the dynamic SQL command \"EXECUTE 'EXEC pm1.sq1(''First'')' AS e1 string, e2 integer\" with the SQL statement \"'EXEC pm1.sq1(''First'')'\" due to: The dynamic sql string contains an incorrect number of elements.", metadata); //$NON-NLS-1$
     }
    
    @Test public void testDynamicCommandPositional() throws Exception {
    	//Tests dynamic query with incorrect number of elements   
        FakeMetadataFacade metadata = FakeMetadataFactory.example1();
        
        FakeMetadataObject pm1 = metadata.getStore().findObject("pm1",FakeMetadataObject.MODEL); //$NON-NLS-1$
        
        FakeMetadataObject rs2 = FakeMetadataFactory.createResultSet("pm1.rs1", pm1, new String[] { "e1", "e2" }, new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.STRING}); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        FakeMetadataObject rs2p1 = FakeMetadataFactory.createParameter("ret", 1, ParameterInfo.RESULT_SET, DataTypeManager.DefaultDataTypes.OBJECT, rs2);  //$NON-NLS-1$
        FakeMetadataObject rs2p2 = FakeMetadataFactory.createParameter("in", 2, ParameterInfo.IN, DataTypeManager.DefaultDataTypes.STRING, null);  //$NON-NLS-1$
        QueryNode sq2n1 = new QueryNode("CREATE VIRTUAL PROCEDURE BEGIN\n" //$NON-NLS-1$ //$NON-NLS-2$
                                        + "execute string 'select e1 as x, e2 from pm1.g1'; END"); //$NON-NLS-1$ 
        FakeMetadataObject sq1 = FakeMetadataFactory.createVirtualProcedure("pm1.sq1", pm1, Arrays.asList(new FakeMetadataObject[] { rs2p1, rs2p2 }), sq2n1);  //$NON-NLS-1$
        
        metadata.getStore().addObject(rs2);
        metadata.getStore().addObject(sq1);
        
        String userUpdateStr = "EXEC pm1.sq1('test')"; //$NON-NLS-1$
        
        FakeDataManager dataMgr = exampleDataManager(metadata);

        ProcessorPlan plan = getProcedurePlan(userUpdateStr, metadata);
    	
        helpTestProcess(plan, new List[] {Arrays.asList("First", "5"), //$NON-NLS-1$ //$NON-NLS-2$
        		Arrays.asList("Second", "15"), //$NON-NLS-1$ //$NON-NLS-2$
        		Arrays.asList("Third", "51")}, dataMgr, metadata); //$NON-NLS-1$ //$NON-NLS-2$
     }
    
    @Test public void testDynamicCommandIncorrectProjectSymbolDatatypes() throws Exception {
    	//Tests dynamic query with a different datatype definition for an element in the AS clause that
    	//has no implicit conversion. 
        FakeMetadataFacade metadata = FakeMetadataFactory.example1();
        
        FakeMetadataObject pm1 = metadata.getStore().findObject("pm1",FakeMetadataObject.MODEL); //$NON-NLS-1$
        
        FakeMetadataObject rs2 = FakeMetadataFactory.createResultSet("pm1.rs1", pm1, new String[] { "e1" }, new String[] { DataTypeManager.DefaultDataTypes.INTEGER }); //$NON-NLS-1$ //$NON-NLS-2$
        FakeMetadataObject rs2p1 = FakeMetadataFactory.createParameter("ret", 1, ParameterInfo.RESULT_SET, DataTypeManager.DefaultDataTypes.OBJECT, rs2);  //$NON-NLS-1$
        QueryNode sq2n1 = new QueryNode("CREATE VIRTUAL PROCEDURE BEGIN\n" //$NON-NLS-1$ //$NON-NLS-2$
                                        + "execute string 'select e1 from pm1.g1'; END"); //$NON-NLS-1$ 
        FakeMetadataObject sq1 = FakeMetadataFactory.createVirtualProcedure("pm1.sq1", pm1, Arrays.asList(new FakeMetadataObject[] { rs2p1 }), sq2n1);  //$NON-NLS-1$
        
        metadata.getStore().addObject(rs2);
        metadata.getStore().addObject(sq1);
        
        String userUpdateStr = "EXEC pm1.sq1()"; //$NON-NLS-1$
        
        FakeDataManager dataMgr = exampleDataManager(metadata);

        ProcessorPlan plan = getProcedurePlan(userUpdateStr, metadata);
    	
        helpTestProcessFailure(plan, dataMgr, "Couldn't execute the dynamic SQL command \"EXECUTE 'select e1 from pm1.g1'\" with the SQL statement \"'select e1 from pm1.g1'\" due to: The datatype 'string' for element 'e1' in the dynamic SQL cannot be implicitly converted to 'integer'.", metadata); //$NON-NLS-1$
     }
     
    @Test public void testDynamicCommandWithTwoDynamicStatements() throws Exception {
    	//Tests dynamic query with two consecutive DynamicCommands. The first without an AS clause and returning different results. 
        FakeMetadataFacade metadata = FakeMetadataFactory.example1();
        
        FakeMetadataObject pm1 = metadata.getStore().findObject("pm1",FakeMetadataObject.MODEL); //$NON-NLS-1$
        
        FakeMetadataObject rs2 = FakeMetadataFactory.createResultSet("pm1.rs1", pm1, new String[] { "e1", "e2" }, new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.STRING}); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        FakeMetadataObject rs2p1 = FakeMetadataFactory.createParameter("ret", 1, ParameterInfo.RESULT_SET, DataTypeManager.DefaultDataTypes.OBJECT, rs2);  //$NON-NLS-1$
        QueryNode sq2n1 = new QueryNode("CREATE VIRTUAL PROCEDURE BEGIN\n" //$NON-NLS-1$ //$NON-NLS-2$
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
       
        helpTestProcess(plan, expected, dataMgr, metadata);
     }
    
    @Test public void testAssignmentWithCase() throws Exception {
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
        QueryNode sq2n1 = new QueryNode("CREATE VIRTUAL PROCEDURE BEGIN\n" //$NON-NLS-1$ //$NON-NLS-2$
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
        helpTestProcess(plan, expected, dataMgr, metadata);
    }
    
    @Test public void testDynamicCommandInsertIntoTempTableWithDifferentDatatypeFromSource() throws Exception {
    	//Tests dynamic query with insert into a temp table using data returned from a physical table.
    	//See defect 23394  
        FakeMetadataFacade metadata = FakeMetadataFactory.example1();
        
        FakeMetadataObject pm5 = metadata.getStore().findObject("pm5",FakeMetadataObject.MODEL); //$NON-NLS-1$
        FakeMetadataObject rs1 = FakeMetadataFactory.createResultSet("pm5.rs1", pm5, new String[] { "e1", "e2" }, new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.SHORT}); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        FakeMetadataObject rs2p1 = FakeMetadataFactory.createParameter("ret", 1, ParameterInfo.RESULT_SET, DataTypeManager.DefaultDataTypes.OBJECT, rs1);  //$NON-NLS-1$
        QueryNode sq2n1 = new QueryNode("CREATE VIRTUAL PROCEDURE BEGIN\n" //$NON-NLS-1$ //$NON-NLS-2$
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
       
        helpTestProcess(plan, expected, dataMgr, metadata);
     }
    
    @Test public void testDynamicCommandWithVariableOnly() throws Exception {
    	//Tests dynamic query with only a variable that represents thte entire dynamic query.
        FakeMetadataFacade metadata = FakeMetadataFactory.example1();
        
        FakeMetadataObject pm5 = metadata.getStore().findObject("pm5",FakeMetadataObject.MODEL); //$NON-NLS-1$
        FakeMetadataObject rs1 = FakeMetadataFactory.createResultSet("pm5.rs1", pm5, new String[] { "e1", "e2" }, new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.SHORT}); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        FakeMetadataObject rs2p1 = FakeMetadataFactory.createParameter("param", 1, ParameterInfo.IN, DataTypeManager.DefaultDataTypes.SHORT, rs1);  //$NON-NLS-1$
        FakeMetadataObject rs2p2 = FakeMetadataFactory.createParameter("ret", 2, ParameterInfo.RESULT_SET, DataTypeManager.DefaultDataTypes.OBJECT, rs1);  //$NON-NLS-1$
        QueryNode sq2n1 = new QueryNode("CREATE VIRTUAL PROCEDURE BEGIN\n" //$NON-NLS-1$ //$NON-NLS-2$
                                        + "DECLARE string VARIABLES.CRIT = 'select e1, e2 from pm5.g3 where e2=using.id'; execute string VARIABLES.CRIT USING ID = pm5.sq1.param; END"); //$NON-NLS-1$ 
        FakeMetadataObject sq1 = FakeMetadataFactory.createVirtualProcedure("pm5.sq1", pm5, Arrays.asList(new FakeMetadataObject[] { rs2p1, rs2p2 }), sq2n1 );  //$NON-NLS-1$
        
        metadata.getStore().addObject(rs1);
        metadata.getStore().addObject(sq1);
        
        String userUpdateStr = "EXEC pm5.sq1(convert(5,short))"; //$NON-NLS-1$
        
        FakeDataManager dataMgr = exampleDataManagerPm5(metadata);

        ProcessorPlan plan = getProcedurePlan(userUpdateStr, metadata);
    	
        // Create expected results
        List[] expected = new List[] { Arrays.asList(new Object[] { "First", new Short((short)5)})};           //$NON-NLS-1$      
        
        helpTestProcess(plan, expected, dataMgr, metadata);
     }
    
    @Test public void testVirtualProcedureWithCreate() throws Exception{
        String userUpdateStr = "EXEC pm1.vsp60()"; //$NON-NLS-1$
        
        FakeMetadataFacade metadata = FakeMetadataFactory.example1Cached();

        FakeDataManager dataMgr = exampleDataManager(metadata);

        ProcessorPlan plan = getProcedurePlan(userUpdateStr, metadata);
                
        // Create expected results
        List[] expected = new List[] {
            Arrays.asList(new Object[] { "First"}),  //$NON-NLS-1$
            Arrays.asList(new Object[] { "Second"}),  //$NON-NLS-1$
            Arrays.asList(new Object[] { "Third"})};         //$NON-NLS-1$
        helpTestProcess(plan, expected, dataMgr, metadata);
    }
    
    @Test public void testVirtualProcedureWithCreateAndDrop() throws Exception{
        String userUpdateStr = "EXEC pm1.vsp61()"; //$NON-NLS-1$
        
        FakeMetadataFacade metadata = FakeMetadataFactory.example1Cached();

        FakeDataManager dataMgr = exampleDataManager(metadata);

        ProcessorPlan plan = getProcedurePlan(userUpdateStr, metadata);
                
        // Create expected results
        List[] expected = new List[] {
            Arrays.asList(new Object[] { "First"}),  //$NON-NLS-1$
            Arrays.asList(new Object[] { "Second"}),  //$NON-NLS-1$
            Arrays.asList(new Object[] { "Third"})};         //$NON-NLS-1$
        helpTestProcess(plan, expected, dataMgr, metadata);
    }
    
    @Test public void testVirtualProcedureWithCreateAndSelectInto() throws Exception{
        String userUpdateStr = "EXEC pm1.vsp62()"; //$NON-NLS-1$
        
        FakeMetadataFacade metadata = FakeMetadataFactory.example1Cached();

        FakeDataManager dataMgr = exampleDataManager(metadata);

        ProcessorPlan plan = getProcedurePlan(userUpdateStr, metadata);
                
        // Create expected results
        List[] expected = new List[] {
            Arrays.asList(new Object[] { "First"}),  //$NON-NLS-1$
            Arrays.asList(new Object[] { "Second"}),  //$NON-NLS-1$
            Arrays.asList(new Object[] { "Third"})};         //$NON-NLS-1$
        helpTestProcess(plan, expected, dataMgr, metadata);
    }
    
    @Test public void testDifferentlyScopedTempTables() throws Exception {
        
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
        
        QueryNode sq2n1 = new QueryNode(procedure.toString()); //$NON-NLS-1$ 
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
        helpTestProcess(plan, expected, dataMgr, metadata);
    }
    
    @Test public void testLoopsWithBreak() throws Exception {
        
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
        
        QueryNode sq2n1 = new QueryNode(procedure.toString()); //$NON-NLS-1$ 
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
        helpTestProcess(plan, expected, dataMgr, metadata);
    }
    
    @Test public void testCreateWithoutDrop() throws Exception {
        
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
        
        QueryNode sq2n1 = new QueryNode(procedure.toString()); //$NON-NLS-1$ 
        FakeMetadataObject sq1 = FakeMetadataFactory.createVirtualProcedure("pm1.sq1", pm1, Arrays.asList(new FakeMetadataObject[] { rs2p1 }), sq2n1);  //$NON-NLS-1$

        metadata.getStore().addObject(rs2);
        metadata.getStore().addObject(sq1);
        
        String userUpdateStr = "EXEC pm1.sq1()"; //$NON-NLS-1$
        
        FakeDataManager dataMgr = exampleDataManager(metadata);

        ProcessorPlan plan = getProcedurePlan(userUpdateStr, metadata);
        
        helpTestProcessFailure(plan, dataMgr, "Temporary table \"T1\" already exists.", metadata); //$NON-NLS-1$
    }
    
    /**
     *  We allow drops to silently fail
     */
    @Test public void testDoubleDrop() throws Exception {
        
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
        
        QueryNode sq2n1 = new QueryNode(procedure.toString()); //$NON-NLS-1$ 
        FakeMetadataObject sq1 = FakeMetadataFactory.createVirtualProcedure("pm1.sq1", pm1, Arrays.asList(new FakeMetadataObject[] { rs2p1 }), sq2n1);  //$NON-NLS-1$

        metadata.getStore().addObject(rs2);
        metadata.getStore().addObject(sq1);
        
        String userUpdateStr = "EXEC pm1.sq1()"; //$NON-NLS-1$
        
        FakeDataManager dataMgr = exampleDataManager(metadata);

        ProcessorPlan plan = getProcedurePlan(userUpdateStr, metadata);
        
        helpTestProcess(plan, new List[] {Arrays.asList(1)}, dataMgr, metadata); 
    }
    
    /**
     * defect 23975 
     */
    @Test public void testFunctionInput() throws Exception {
        FakeMetadataObject v1 = FakeMetadataFactory.createVirtualModel("v1"); //$NON-NLS-1$

        FakeMetadataObject p1 = FakeMetadataFactory.createParameter("v1.vp1.in", 2, ParameterInfo.IN, DataTypeManager.DefaultDataTypes.STRING, null);  //$NON-NLS-1$
        FakeMetadataObject rs1 = FakeMetadataFactory.createResultSet("v1.rs1", v1, new String[] {"e1"}, new String[] { DataTypeManager.DefaultDataTypes.STRING }); //$NON-NLS-1$ //$NON-NLS-2$ 
        FakeMetadataObject rs1p1 = FakeMetadataFactory.createParameter("ret", 1, ParameterInfo.RESULT_SET, DataTypeManager.DefaultDataTypes.OBJECT, rs1);  //$NON-NLS-1$

        QueryNode n1 = new QueryNode("CREATE VIRTUAL PROCEDURE BEGIN declare string VARIABLES.x = '1'; exec v1.vp2(concat(x, v1.vp1.in)); END"); //$NON-NLS-1$ //$NON-NLS-2$
        FakeMetadataObject vt1 = FakeMetadataFactory.createVirtualProcedure("v1.vp1", v1, Arrays.asList(new FakeMetadataObject[] { rs1p1, p1 }), n1); //$NON-NLS-1$
        
        FakeMetadataObject p2 = FakeMetadataFactory.createParameter("v1.vp2.in", 2, ParameterInfo.IN, DataTypeManager.DefaultDataTypes.STRING, null);  //$NON-NLS-1$
        QueryNode n2 = new QueryNode("CREATE VIRTUAL PROCEDURE BEGIN select v1.vp2.in; end"); //$NON-NLS-1$ //$NON-NLS-2$
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
        helpTestProcess(plan, expected, new FakeDataManager(), metadata);
    }
    
    /**
     * defect 23976
     * Also, even after the bug for passing procedure inputs to non-execs was fixed, the special case of
     * if (below) and while statements popped up.  
     */
    @Test public void testIfEvaluation() throws Exception {
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
                                     
        helpTestProcess(plan, 4, new FakeDataManager(), metadata);
    }
    
    /**
     *  This is a slight variation of TestProcessor.testVariableInExecParam, where the proc wrapper can be 
     *  removed after rewrite
     */
    @Test public void testReferenceForwarding() throws Exception { 
        // Create query 
        String sql = "EXEC pm1.vsp49()"; //$NON-NLS-1$
        
        FakeMetadataFacade metadata = FakeMetadataFactory.example1();
        
        FakeMetadataStore store = metadata.getStore();
        
        FakeMetadataObject pm1 = store.findObject("pm1", FakeMetadataObject.MODEL); //$NON-NLS-1$
        
        FakeMetadataObject rs2 = FakeMetadataFactory.createResultSet("pm1.rs2", pm1, new String[] { "e1", "e2" }, new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.INTEGER }); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        FakeMetadataObject rs2p1 = FakeMetadataFactory.createParameter("ret", 1, ParameterInfo.RESULT_SET, DataTypeManager.DefaultDataTypes.OBJECT, rs2);  //$NON-NLS-1$
        FakeMetadataObject rs2p2 = FakeMetadataFactory.createParameter("in", 2, ParameterInfo.IN, DataTypeManager.DefaultDataTypes.STRING, null);  //$NON-NLS-1$
        QueryNode sq2n1 = new QueryNode("CREATE VIRTUAL PROCEDURE BEGIN if (1 = 2) begin declare integer x = 1; end SELECT e1, e2 FROM pm1.g1 WHERE e1=pm1.sq2.in; END"); //$NON-NLS-1$ //$NON-NLS-2$
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
        helpTestProcess(plan, expected, dataManager, metadata);
    }
    
    @Test public void testInsertAfterCreate() throws Exception {
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
        
        QueryNode sq2n1 = new QueryNode(procedure.toString()); //$NON-NLS-1$ 
        FakeMetadataObject sq1 = FakeMetadataFactory.createVirtualProcedure("pm1.sq1", pm1, Arrays.asList(new FakeMetadataObject[] { rs2p1 }), sq2n1);  //$NON-NLS-1$

        metadata.getStore().addObject(rs2);
        metadata.getStore().addObject(sq1);
        
        String userUpdateStr = "EXEC pm1.sq1()"; //$NON-NLS-1$
        
        FakeDataManager dataMgr = exampleDataManager(metadata);

        ProcessorPlan plan = getProcedurePlan(userUpdateStr, metadata);
        
        helpTestProcess(plan, new List[] {
            Arrays.asList(new Object[] {null}),  
            Arrays.asList(new Object[] {"b"})}, dataMgr, metadata); //$NON-NLS-1$
        
    }
    
    /**
     * the update will not be executed, but the assignment value should still be 0
     */
    @Test public void testUpdateAssignmentNotExecuted() throws Exception {
        String procedure = "CREATE PROCEDURE  "; //$NON-NLS-1$
        procedure = procedure + "BEGIN\n"; //$NON-NLS-1$
        procedure = procedure + "DECLARE integer var1 = UPDATE pm1.g1 SET pm1.g1.e1 = INPUT.e2;"; //$NON-NLS-1$
        procedure = procedure + "ROWS_UPDATED = var1 + 1;\n"; //$NON-NLS-1$
        procedure = procedure + "END"; //$NON-NLS-1$

        String userUpdateStr = "UPDATE g4 SET e1='x' where e2=5"; //$NON-NLS-1$
        
        FakeMetadataFacade metadata = FakeMetadataFactory.exampleUpdateProc(FakeMetadataObject.Props.UPDATE_PROCEDURE, procedure);
        
        FakeDataManager dataMgr = exampleDataManager(metadata);

        ProcessorPlan plan = getProcedurePlan(userUpdateStr, metadata);
                                     
        helpTestProcess(plan, 1, dataMgr, metadata);
        
        assertTrue(plan.requiresTransaction(false));
    }
    
    @Test public void testUpdateAssignmentNotExecutedVirtual() throws Exception {
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
                                     
        helpTestProcess(plan, 1, dataMgr, metadata);
    }
    
    @Test public void testEvaluatableSelectWithOrderBy() throws Exception {
        FakeMetadataFacade metadata = FakeMetadataFactory.example1();
        
        FakeMetadataObject pm1 = metadata.getStore().findObject("pm1",FakeMetadataObject.MODEL); //$NON-NLS-1$
        
        FakeMetadataObject rs2 = FakeMetadataFactory.createResultSet("pm1.rs1", pm1, new String[] { "e1" }, new String[] { DataTypeManager.DefaultDataTypes.STRING }); //$NON-NLS-1$ //$NON-NLS-2$
        FakeMetadataObject rs2p1 = FakeMetadataFactory.createParameter("ret", 1, ParameterInfo.RESULT_SET, DataTypeManager.DefaultDataTypes.OBJECT, rs2);  //$NON-NLS-1$
        FakeMetadataObject rs2p2 = FakeMetadataFactory.createParameter("param", 2, ParameterInfo.IN, DataTypeManager.DefaultDataTypes.STRING, null);  //$NON-NLS-1$
        StringBuffer procedure = new StringBuffer("CREATE VIRTUAL PROCEDURE \n") //$NON-NLS-1$
        .append("BEGIN\n") //$NON-NLS-1$
        .append("SELECT param from pm1.g1 order by param limit 1;\n") //$NON-NLS-1$
        .append("END"); //$NON-NLS-1$
        
        QueryNode sq2n1 = new QueryNode(procedure.toString()); //$NON-NLS-1$ 
        FakeMetadataObject sq1 = FakeMetadataFactory.createVirtualProcedure("pm1.sq1", pm1, Arrays.asList(new FakeMetadataObject[] { rs2p1, rs2p2 }), sq2n1);  //$NON-NLS-1$

        metadata.getStore().addObject(rs2);
        metadata.getStore().addObject(sq1);
        
        String userUpdateStr = "EXEC pm1.sq1(1)"; //$NON-NLS-1$
        
        FakeDataManager dataMgr = exampleDataManager(metadata);

        ProcessorPlan plan = getProcedurePlan(userUpdateStr, metadata);
        
        helpTestProcess(plan, new List[] {
            Arrays.asList(new Object[] {"1"})}, dataMgr, metadata); //$NON-NLS-1$
        
    }
    
    @Test public void testEvaluatableSelectWithOrderBy1() throws Exception {
        FakeMetadataFacade metadata = FakeMetadataFactory.example1();
        
        FakeMetadataObject pm1 = metadata.getStore().findObject("pm1",FakeMetadataObject.MODEL); //$NON-NLS-1$
        
        FakeMetadataObject rs2 = FakeMetadataFactory.createResultSet("pm1.rs1", pm1, new String[] { "e1" }, new String[] { DataTypeManager.DefaultDataTypes.STRING }); //$NON-NLS-1$ //$NON-NLS-2$
        FakeMetadataObject rs2p1 = FakeMetadataFactory.createParameter("ret", 1, ParameterInfo.RESULT_SET, DataTypeManager.DefaultDataTypes.OBJECT, rs2);  //$NON-NLS-1$
        FakeMetadataObject rs2p2 = FakeMetadataFactory.createParameter("param", 2, ParameterInfo.IN, DataTypeManager.DefaultDataTypes.STRING, null);  //$NON-NLS-1$
        StringBuffer procedure = new StringBuffer("CREATE VIRTUAL PROCEDURE \n") //$NON-NLS-1$
        .append("BEGIN\n") //$NON-NLS-1$
        .append("SELECT param from pm1.g1 union select e1 from pm1.g1 order by param limit 2;\n") //$NON-NLS-1$
        .append("END"); //$NON-NLS-1$
        
        QueryNode sq2n1 = new QueryNode(procedure.toString()); //$NON-NLS-1$ 
        FakeMetadataObject sq1 = FakeMetadataFactory.createVirtualProcedure("pm1.sq1", pm1, Arrays.asList(new FakeMetadataObject[] { rs2p1, rs2p2 }), sq2n1);  //$NON-NLS-1$

        metadata.getStore().addObject(rs2);
        metadata.getStore().addObject(sq1);
        
        String userUpdateStr = "EXEC pm1.sq1(1)"; //$NON-NLS-1$
        
        FakeDataManager dataMgr = exampleDataManager(metadata);

        ProcessorPlan plan = getProcedurePlan(userUpdateStr, metadata);
        
        helpTestProcess(plan, new List[] {
            Arrays.asList(new Object[] {"1"}), //$NON-NLS-1$
            Arrays.asList(new Object[] {"First"}), //$NON-NLS-1$
            }, dataMgr, metadata); 
        
    }
    
    /**
     * Tests non-deterministic evaluation of the rand function.  There are two important things happening
     * 1. is that the evaluation of the rand function is delayed until processing time (which actually has predictable
     * values since the test initializes the command context with the same seed)
     * 2. The values are different, meaning that we got individual evaluations
     * 
     * If this function were deterministic, it would be evaluated during rewrite to a single value.
     */
    @Test public void testNonDeterministicEvaluation() throws Exception {
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
            Arrays.asList(new Object[] {new Integer(637)})}, dataMgr, metadata);
    }

    private FakeMetadataFacade createProcedureMetadata(String procedure) {
        FakeMetadataFacade metadata = FakeMetadataFactory.example1();
        
        FakeMetadataObject pm1 = metadata.getStore().findObject("pm1",FakeMetadataObject.MODEL); //$NON-NLS-1$
        
        FakeMetadataObject rs2 = FakeMetadataFactory.createResultSet("pm1.rs1", pm1, new String[] { "e1" }, new String[] { DataTypeManager.DefaultDataTypes.INTEGER }); //$NON-NLS-1$ //$NON-NLS-2$
        FakeMetadataObject rs2p1 = FakeMetadataFactory.createParameter("ret", 1, ParameterInfo.RESULT_SET, DataTypeManager.DefaultDataTypes.OBJECT, rs2);  //$NON-NLS-1$
        
        
        QueryNode sq2n1 = new QueryNode(procedure.toString()); //$NON-NLS-1$ 
        FakeMetadataObject sq1 = FakeMetadataFactory.createVirtualProcedure("pm1.sq1", pm1, Arrays.asList(new FakeMetadataObject[] { rs2p1}), sq2n1);  //$NON-NLS-1$

        metadata.getStore().addObject(rs2);
        metadata.getStore().addObject(sq1);
        return metadata;
    }
    
    @Test public void testTempTableTypeConversion() throws Exception {
        
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
            }, dataMgr, metadata);
    }
    
    /**
     * wraps {@link TestXMLPlanningEnhancements.testNested2WithContextCriteria5d1} in a procedure
     */
    @Test public void testXMLWithExternalCriteria() throws Exception {
        FakeMetadataFacade metadata = TestXMLProcessor.exampleMetadataCached();
        FakeDataManager dataMgr = TestXMLProcessor.exampleDataManagerNested(metadata);
        String resultFile = "TestXMLProcessor-testNested2WithContextCriteria5d.xml"; //$NON-NLS-1$
        String expectedDoc = TestXMLProcessor.readFile(resultFile);
                
        FakeMetadataObject pm1 = metadata.getStore().findObject("xqttest",FakeMetadataObject.MODEL); //$NON-NLS-1$
        
        FakeMetadataObject rs2 = FakeMetadataFactory.createResultSet("pm1.rs2", pm1, new String[] { "e1" }, new String[] { DataTypeManager.DefaultDataTypes.XML }); //$NON-NLS-1$ //$NON-NLS-2$
        FakeMetadataObject rs2p1 = FakeMetadataFactory.createParameter("ret", 1, ParameterInfo.RESULT_SET, DataTypeManager.DefaultDataTypes.OBJECT, rs2);  //$NON-NLS-1$
        FakeMetadataObject rs2p2 = FakeMetadataFactory.createParameter("input", 2, ParameterInfo.IN, DataTypeManager.DefaultDataTypes.INTEGER, null);  //$NON-NLS-1$
        QueryNode sq2n1 = new QueryNode("CREATE VIRTUAL PROCEDURE BEGIN\n" //$NON-NLS-1$ //$NON-NLS-2$
                                        + "declare integer VARIABLES.x = xqttest.proc.input; SELECT * FROM xmltest.doc9 WHERE context(SupplierID, OrderID)=x OR OrderID='2'; END"); //$NON-NLS-1$ 
        FakeMetadataObject sq2 = FakeMetadataFactory.createVirtualProcedure("xqttest.proc", pm1, Arrays.asList(new FakeMetadataObject[] { rs2p1, rs2p2 }), sq2n1);  //$NON-NLS-1$

        metadata.getStore().addObject(rs2);
        metadata.getStore().addObject(sq2);
        
        String userUpdateStr = "EXEC xqttest.proc(5)"; //$NON-NLS-1$
        
        ProcessorPlan plan = getProcedurePlan(userUpdateStr, metadata);
                        
        // Create expected results
        List[] expected = new List[] {
                Arrays.asList(new Object[] { expectedDoc }),
        };        
        helpTestProcess(plan, expected, dataMgr, metadata);
    }
    
    @Test public void testXMLWithExternalCriteria_InXMLVar() throws Exception {
        FakeMetadataFacade metadata = TestXMLProcessor.exampleMetadataCached();
        FakeDataManager dataMgr = TestXMLProcessor.exampleDataManagerNested(metadata);
        String resultFile = "TestXMLProcessor-testNested2WithContextCriteria5d.xml"; //$NON-NLS-1$
        String expectedDoc = TestXMLProcessor.readFile(resultFile);
        expectedDoc = expectedDoc.replaceAll("\\r", ""); //$NON-NLS-1$ //$NON-NLS-2$        
        FakeMetadataObject pm1 = metadata.getStore().findObject("xqttest",FakeMetadataObject.MODEL); //$NON-NLS-1$
        
        FakeMetadataObject rs2 = FakeMetadataFactory.createResultSet("pm1.rs2", pm1, new String[] { "e1" }, new String[] { DataTypeManager.DefaultDataTypes.XML }); //$NON-NLS-1$ //$NON-NLS-2$
        FakeMetadataObject rs2p1 = FakeMetadataFactory.createParameter("ret", 1, ParameterInfo.RESULT_SET, DataTypeManager.DefaultDataTypes.OBJECT, rs2);  //$NON-NLS-1$
        FakeMetadataObject rs2p2 = FakeMetadataFactory.createParameter("input", 2, ParameterInfo.IN, DataTypeManager.DefaultDataTypes.INTEGER, null);  //$NON-NLS-1$
        QueryNode sq2n1 = new QueryNode("CREATE VIRTUAL PROCEDURE BEGIN\n" //$NON-NLS-1$ //$NON-NLS-2$
                                        + "declare integer VARIABLES.x = xqttest.proc.input; declare xml y = SELECT * FROM xmltest.doc9 WHERE context(SupplierID, OrderID)=x OR OrderID='2'; select convert(y, string); END"); //$NON-NLS-1$ 
        FakeMetadataObject sq2 = FakeMetadataFactory.createVirtualProcedure("xqttest.proc", pm1, Arrays.asList(new FakeMetadataObject[] { rs2p1, rs2p2 }), sq2n1);  //$NON-NLS-1$

        metadata.getStore().addObject(rs2);
        metadata.getStore().addObject(sq2);
        
        String userUpdateStr = "EXEC xqttest.proc(5)"; //$NON-NLS-1$
        
        ProcessorPlan plan = getProcedurePlan(userUpdateStr, metadata);
                        
        // Create expected results
        List[] expected = new List[] {
                Arrays.asList(new Object[] { expectedDoc }),
        };        
        helpTestProcess(plan, expected, dataMgr, metadata);
    }
    
    /**
     * wraps {@link TestXMLProcessor.testNested2WithCriteria2} in a procedure
     * 
     * This one will successfully auto-stage
     */
    @Test public void testXMLWithExternalCriteria1() throws Exception {
        FakeMetadataFacade metadata = TestXMLProcessor.exampleMetadataCached();
        FakeDataManager dataMgr = TestXMLProcessor.exampleDataManagerNested(metadata);
        String expectedDoc = 
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +  //$NON-NLS-1$
            "<Catalogs>\n" + //$NON-NLS-1$
            "   <Catalog>\n" +  //$NON-NLS-1$
            "      <Items>\n" +  //$NON-NLS-1$
            "         <Item ItemID=\"001\">\n" +  //$NON-NLS-1$
            "            <Name>Lamp</Name>\n" +  //$NON-NLS-1$
            "            <Quantity>5</Quantity>\n" + //$NON-NLS-1$
            "            <Suppliers>\n" + //$NON-NLS-1$
            "               <Supplier SupplierID=\"52\">\n" + //$NON-NLS-1$
            "                  <Name>Biff's Stuff</Name>\n" + //$NON-NLS-1$
            "                  <Zip>22222</Zip>\n" + //$NON-NLS-1$
            "                  <Orders>\n" + //$NON-NLS-1$
            "                     <Order OrderID=\"2\">\n" + //$NON-NLS-1$
            "                        <OrderDate>12/31/01</OrderDate>\n" + //$NON-NLS-1$
            "                        <OrderQuantity>87</OrderQuantity>\n" + //$NON-NLS-1$
            "                        <OrderStatus>complete</OrderStatus>\n" + //$NON-NLS-1$
            "                     </Order>\n" + //$NON-NLS-1$
            "                  </Orders>\n" + //$NON-NLS-1$
            "               </Supplier>\n" + //$NON-NLS-1$
            "            </Suppliers>\n" + //$NON-NLS-1$
            "         </Item>\n" +  //$NON-NLS-1$
            "         <Item ItemID=\"002\">\n" +  //$NON-NLS-1$
            "            <Name>Screwdriver</Name>\n" +  //$NON-NLS-1$
            "            <Quantity>100</Quantity>\n" +  //$NON-NLS-1$
            "            <Suppliers/>\n" + //$NON-NLS-1$
            "         </Item>\n" +  //$NON-NLS-1$
            "         <Item ItemID=\"003\">\n" +  //$NON-NLS-1$
            "            <Name>Goat</Name>\n" +  //$NON-NLS-1$
            "            <Quantity>4</Quantity>\n" +  //$NON-NLS-1$
            "            <Suppliers/>\n" + //$NON-NLS-1$
            "         </Item>\n" +  //$NON-NLS-1$
            "      </Items>\n" +  //$NON-NLS-1$
            "   </Catalog>\n" +  //$NON-NLS-1$
            "</Catalogs>"; //$NON-NLS-1$

        FakeMetadataObject pm1 = metadata.getStore().findObject("xmltest",FakeMetadataObject.MODEL); //$NON-NLS-1$
        
        FakeMetadataObject rs2 = FakeMetadataFactory.createResultSet("pm1.rs2", pm1, new String[] { "e1" }, new String[] { DataTypeManager.DefaultDataTypes.XML }); //$NON-NLS-1$ //$NON-NLS-2$
        FakeMetadataObject rs2p1 = FakeMetadataFactory.createParameter("ret", 1, ParameterInfo.RESULT_SET, DataTypeManager.DefaultDataTypes.OBJECT, rs2);  //$NON-NLS-1$
        FakeMetadataObject rs2p2 = FakeMetadataFactory.createParameter("input", 2, ParameterInfo.IN, DataTypeManager.DefaultDataTypes.INTEGER, null);  //$NON-NLS-1$
        QueryNode sq2n1 = new QueryNode("CREATE VIRTUAL PROCEDURE BEGIN\n" //$NON-NLS-1$ //$NON-NLS-2$
                                        + "declare integer VARIABLES.x = xmltest.proc.input; SELECT * FROM xmltest.doc9 WHERE context(SupplierID, SupplierID)=x; END"); //$NON-NLS-1$ 
        FakeMetadataObject sq2 = FakeMetadataFactory.createVirtualProcedure("xmltest.proc", pm1, Arrays.asList(new FakeMetadataObject[] { rs2p1, rs2p2 }), sq2n1);  //$NON-NLS-1$

        metadata.getStore().addObject(rs2);
        metadata.getStore().addObject(sq2);
        
        String userUpdateStr = "EXEC xmltest.proc(52)"; //$NON-NLS-1$
        
        ProcessorPlan plan = getProcedurePlan(userUpdateStr, metadata);
                        
        // Create expected results
        List[] expected = new List[] {
                Arrays.asList(new Object[] { expectedDoc }),
        };        
        helpTestProcess(plan, expected, dataMgr, metadata);
    }
    
    @Test public void testCase174806() throws Exception{
        String userUpdateStr = "EXEC pm1.vsp63()"; //$NON-NLS-1$
        
        FakeMetadataFacade metadata = FakeMetadataFactory.example1Cached();

        FakeDataManager dataMgr = exampleDataManager(metadata);

        ProcessorPlan plan = getProcedurePlan(userUpdateStr, metadata);
                
        // Create expected results
        List[] expected = new List[] {
            Arrays.asList(new Object[] { "c"})};         //$NON-NLS-1$
        helpTestProcess(plan, expected, dataMgr, metadata);
    }
    
    @Test public void testJoinProcAndPhysicalModel() throws Exception {
        String userUpdateStr = "select a.e1 from (EXEC pm1.vsp46()) as a, pm1.g1 where a.e1=pm1.g1.e1";     //$NON-NLS-1$
        FakeMetadataFacade metadata = FakeMetadataFactory.example1Cached();

        ProcessorPlan plan = getProcedurePlan(userUpdateStr, metadata);   

        // Set up data
        FakeDataManager dataMgr = exampleDataManager(metadata);
        
        // Create expected results
        List[] expected = new List[] {
            Arrays.asList(new Object[] { "First"}),  //$NON-NLS-1$
            Arrays.asList(new Object[] { "Second"}), //$NON-NLS-1$
            Arrays.asList(new Object[] { "Third"})};           //$NON-NLS-1$
        helpTestProcess(plan, expected, dataMgr, metadata);
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
    @Test public void testDeclareWithQueryAssignment() throws Exception {
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
        helpTestProcess(plan, expected, dataMgr, metadata);
    }
    
    @Test public void testDefect8693() throws Exception {
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
		helpTestProcess(plan, 5, dataMgr, metadata);									 
    }
    
    @Test public void testWhileWithSubquery() throws Exception {
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
		helpTestProcess(plan, 0, dataMgr, metadata);									 
    }
    
    @Test public void testDefect18404() throws Exception {
        String procedure = "CREATE PROCEDURE  "; //$NON-NLS-1$
        procedure = procedure + "BEGIN\n"; //$NON-NLS-1$
        procedure = procedure + "DECLARE integer var1 = 5 + (select count(e2) from pm1.g1);\n"; //$NON-NLS-1$
        procedure = procedure + "ROWS_UPDATED = ROWS_UPDATED + var1;\n"; //$NON-NLS-1$
        procedure = procedure + "END"; //$NON-NLS-1$
    
        String userUpdateStr = "UPDATE vm1.g1 SET e1='x'"; //$NON-NLS-1$
    
        FakeMetadataFacade metadata = FakeMetadataFactory.exampleUpdateProc(FakeMetadataObject.Props.UPDATE_PROCEDURE, procedure);
        FakeDataManager dataMgr = exampleDataManager(metadata);
		ProcessorPlan plan = getProcedurePlan(userUpdateStr, metadata);
		helpTestProcess(plan, 8, dataMgr, metadata);									 
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
    @Test public void testRemovalOfNonJoinCritWithReference() throws Exception {
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
        helpTestProcess(plan, expected, dataMgr, metadata);
        
        assertTrue(!plan.requiresTransaction(false));
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
    @Test public void testRemovalOfNonJoinCritWithReference2() throws Exception {
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
        helpTestProcess(plan, expected, dataMgr, metadata);
    }
    
    @Test public void testUpdateDeleteTemp() throws Exception {
        String proc = "CREATE VIRTUAL PROCEDURE " + //$NON-NLS-1$
        		"BEGIN " + //$NON-NLS-1$
                " select e1, e2, e3, e4 into #t1 from pm1.g1;\n" + //$NON-NLS-1$
                " update #t1 set e1 = 1 where e4 < 2;\n" + //$NON-NLS-1$
                " delete from #t1 where e4 > 2;\n" + //$NON-NLS-1$
                " select e1 from #t1;\n" + //$NON-NLS-1$
        		"END"; //$NON-NLS-1$

        FakeMetadataFacade metadata = createProcedureMetadata(proc);
        String userQuery = "SELECT * FROM (EXEC pm1.sq1()) as proc"; //$NON-NLS-1$
        FakeDataManager dataMgr = exampleDataManager2(metadata);
        ProcessorPlan plan = getProcedurePlan(userQuery, metadata, TestOptimizer.getGenericFinder());

        List[] expected = new List[] {
                Arrays.asList( new Object[] { String.valueOf(1) } ), 
        };
        helpTestProcess(plan, expected, dataMgr, metadata);
    }
    
    @Test public void testTempSubqueryInput() throws Exception {
        String proc = "CREATE VIRTUAL PROCEDURE " + //$NON-NLS-1$
        		"BEGIN " + //$NON-NLS-1$
        		" create local temporary table t1 (e1 string);\n" + //$NON-NLS-1$
                " select e1 into t1 from pm1.g1;\n" + //$NON-NLS-1$
                " select e2 from (exec pm1.sq2((select max(e1) from t1))) x;\n" + //$NON-NLS-1$
        		"END"; //$NON-NLS-1$

        FakeMetadataFacade metadata = createProcedureMetadata(proc);
        String userQuery = "SELECT * FROM (EXEC pm1.sq1()) as proc"; //$NON-NLS-1$
        FakeDataManager dataMgr = exampleDataManager2(metadata);
        ProcessorPlan plan = getProcedurePlan(userQuery, metadata, TestOptimizer.getGenericFinder());

        List[] expected = new List[] {
                Arrays.asList( 51 ),
        };
        helpTestProcess(plan, expected, dataMgr, metadata);
    }
    
    @Test public void testUnambiguousVirtualProc() throws Exception {
        String userQuery = "EXEC MMSP6('1')"; //$NON-NLS-1$
        QueryMetadataInterface metadata = FakeMetadataFactory.exampleBQTCached();
        ProcessorPlan plan = getProcedurePlan(userQuery, metadata, TestOptimizer.getGenericFinder());

        List[] expected = new List[] {
                Arrays.asList( "1" ),
        };
        helpTestProcess(plan, expected, new HardcodedDataManager(), metadata);
    }
    
    @Test public void testParameterAssignments() throws Exception {
        TransformationMetadata metadata = RealMetadataFactory.exampleBQTCached();
        String userQuery = "EXEC TEIIDSP7(1)"; //$NON-NLS-1$
        HardcodedDataManager dataMgr = new HardcodedDataManager();
        ProcessorPlan plan = getProcedurePlan(userQuery, metadata);
        dataMgr.addData("x = EXEC spTest9(1)", new List[] {Arrays.asList(3)});
        dataMgr.addData("EXEC spTest11(3, null)", new List[] {Arrays.asList("1", 1, null), Arrays.asList(null, null, 4)});
        List[] expected = new List[] {Arrays.asList("34")};
        helpTestProcess(plan, expected, dataMgr, metadata);
    }
    
    @Test public void testNonQueryPushdownValidation() throws Exception {
        
        FakeMetadataFacade metadata = FakeMetadataFactory.example1();
        
        FakeMetadataObject pm1 = metadata.getStore().findObject("pm1",FakeMetadataObject.MODEL); //$NON-NLS-1$
        
        FakeMetadataObject rs2 = FakeMetadataFactory.createResultSet("pm1.rs1", pm1, new String[] { "e1" }, new String[] { DataTypeManager.DefaultDataTypes.STRING }); //$NON-NLS-1$ //$NON-NLS-2$
        FakeMetadataObject rs2p1 = FakeMetadataFactory.createParameter("ret", 1, ParameterInfo.RESULT_SET, DataTypeManager.DefaultDataTypes.OBJECT, rs2);  //$NON-NLS-1$
        FakeMetadataObject in = FakeMetadataFactory.createParameter("pm1.sq1.in1", 2, SPParameter.IN, DataTypeManager.DefaultDataTypes.INTEGER, null); //$NON-NLS-1$

        StringBuffer procedure = new StringBuffer("CREATE VIRTUAL PROCEDURE \n"); //$NON-NLS-1$
        procedure.append("BEGIN\n"); //$NON-NLS-1$
        procedure.append("create local temporary table x (y string);\n"); //$NON-NLS-1$
        procedure.append("declare string s = 'foo';\n"); //$NON-NLS-1$
        procedure.append("update x set y = in1 || s;\n"); //$NON-NLS-1$
        procedure.append("update pm1.g1 set e1 = lookup('pm1.g1', 'e1', 'e2', in1);\n"); //$NON-NLS-1$
        procedure.append("exec pm1.sq2(in1 || 'foo');\n"); //$NON-NLS-1$
        procedure.append("END"); //$NON-NLS-1$
        
        QueryNode sq2n1 = new QueryNode(procedure.toString()); //$NON-NLS-1$ 
        FakeMetadataObject sq1 = FakeMetadataFactory.createVirtualProcedure("pm1.sq1", pm1, Arrays.asList(new FakeMetadataObject[] { in, rs2p1 }), sq2n1);  //$NON-NLS-1$

        metadata.getStore().addObject(rs2);
        metadata.getStore().addObject(sq1);
        
        String userUpdateStr = "EXEC pm1.sq1(1)"; //$NON-NLS-1$
        
        FakeDataManager dataMgr = exampleDataManager(metadata);

        ProcessorPlan plan = getProcedurePlan(userUpdateStr, metadata);
        
        //Create expected results
        List[] expected = new List[0];           
        helpTestProcess(plan, expected, dataMgr, metadata);
    }
    
    @Test public void testReturnParamWithNoResultSetVirtual() throws Exception {
        String sql = "EXEC TEIIDSP8(51)";     //$NON-NLS-1$
        TransformationMetadata metadata = RealMetadataFactory.exampleBQTCached();
        ProcessorPlan plan = getProcedurePlan(sql, metadata);

        // Set up data
        FakeDataManager dataMgr = new FakeDataManager();
  
        // Create expected results
        List[] expected = new List[] { Arrays.asList(51) }; //$NON-NLS-1$
        helpTestProcess(plan, expected, dataMgr, metadata);
    }
    
    @Test(expected=QueryProcessingException.class) public void testParamsWithResultSetVirtualNotNull() throws Exception {
        String sql = "{? = call TEIIDSP9(51)}";     //$NON-NLS-1$
        TransformationMetadata metadata = RealMetadataFactory.exampleBQTCached();
        ProcessorPlan plan = getProcedurePlan(sql, metadata);

        FakeDataManager dataMgr = new FakeDataManager();
  
        helpTestProcess(plan, null, dataMgr, metadata);
    }

    @Test public void testParamsWithResultSetVirtual() throws Exception {
        String sql = "{? = call TEIIDSP9(1)}";     //$NON-NLS-1$
        TransformationMetadata metadata = RealMetadataFactory.exampleBQTCached();
        ProcessorPlan plan = getProcedurePlan(sql, metadata);

        FakeDataManager dataMgr = new FakeDataManager();
  
        List[] expected = new List[] { Arrays.asList("hello", null, null), 
        		Arrays.asList(null, 1, 10) }; //$NON-NLS-1$

        helpTestProcess(plan, expected, dataMgr, metadata);
    }

    private static final boolean DEBUG = false;
}
