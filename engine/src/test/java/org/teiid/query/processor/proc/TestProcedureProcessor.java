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

package org.teiid.query.processor.proc;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.teiid.query.processor.TestProcessor.helpGetPlan;
import static org.teiid.query.processor.TestProcessor.helpProcess;
import static org.teiid.query.processor.TestProcessor.sampleData1;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.junit.Test;
import org.mockito.Mockito;
import org.teiid.api.exception.query.QueryPlannerException;
import org.teiid.api.exception.query.QueryProcessingException;
import org.teiid.api.exception.query.QueryResolverException;
import org.teiid.api.exception.query.QueryValidatorException;
import org.teiid.client.metadata.ParameterInfo;
import org.teiid.client.xa.XATransactionException;
import org.teiid.core.TeiidException;
import org.teiid.core.TeiidProcessingException;
import org.teiid.core.types.ArrayImpl;
import org.teiid.core.types.DataTypeManager;
import org.teiid.dqp.service.TransactionContext;
import org.teiid.dqp.service.TransactionService;
import org.teiid.metadata.ColumnSet;
import org.teiid.metadata.MetadataStore;
import org.teiid.metadata.Procedure;
import org.teiid.metadata.ProcedureParameter;
import org.teiid.metadata.Schema;
import org.teiid.query.analysis.AnalysisRecord;
import org.teiid.query.mapping.relational.QueryNode;
import org.teiid.query.metadata.QueryMetadataInterface;
import org.teiid.query.metadata.TempMetadataAdapter;
import org.teiid.query.metadata.TempMetadataStore;
import org.teiid.query.metadata.TransformationMetadata;
import org.teiid.query.optimizer.QueryOptimizer;
import org.teiid.query.optimizer.TestOptimizer;
import org.teiid.query.optimizer.capabilities.BasicSourceCapabilities;
import org.teiid.query.optimizer.capabilities.CapabilitiesFinder;
import org.teiid.query.optimizer.capabilities.DefaultCapabilitiesFinder;
import org.teiid.query.optimizer.capabilities.SourceCapabilities.Capability;
import org.teiid.query.parser.ParseInfo;
import org.teiid.query.parser.QueryParser;
import org.teiid.query.processor.FakeDataManager;
import org.teiid.query.processor.HardcodedDataManager;
import org.teiid.query.processor.ProcessorDataManager;
import org.teiid.query.processor.ProcessorPlan;
import org.teiid.query.processor.TestProcessor;
import org.teiid.query.resolver.QueryResolver;
import org.teiid.query.resolver.TestProcedureResolving;
import org.teiid.query.rewriter.QueryRewriter;
import org.teiid.query.sql.lang.Command;
import org.teiid.query.sql.lang.SPParameter;
import org.teiid.query.unittest.RealMetadataFactory;
import org.teiid.query.util.CommandContext;
import org.teiid.query.validator.Validator;
import org.teiid.query.validator.ValidatorFailure;
import org.teiid.query.validator.ValidatorReport;
import org.teiid.translator.ExecutionFactory.TransactionSupport;

@SuppressWarnings({"unchecked", "rawtypes", "nls"})
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
            if ( capabilitiesFinder == null ) {
                capabilitiesFinder = new DefaultCapabilitiesFinder();
            }
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

    private FakeDataManager exampleDataManager(QueryMetadataInterface metadata) throws TeiidException {
        FakeDataManager dataMgr = new FakeDataManager();

        dataMgr.registerTuples(
            metadata,
            "pm1.g1", new List[] {
                    Arrays.asList( new Object[] { "First", Integer.valueOf(5), new Boolean(true), new Double(1.003)} ), //$NON-NLS-1$
                    Arrays.asList( new Object[] { "Second", Integer.valueOf(15), new Boolean(true), new Double(2.003)} ), //$NON-NLS-1$
                    Arrays.asList( new Object[] { "Third", Integer.valueOf(51), new Boolean(true), new Double(3.003)} ) //$NON-NLS-1$
                    } );

        dataMgr.registerTuples(
            metadata,
            "pm1.g2", new List[] {
                    Arrays.asList( new Object[] { "First", Integer.valueOf(5), new Boolean(true), new Double(1.003)} ), //$NON-NLS-1$
                    Arrays.asList( new Object[] { "Second", Integer.valueOf(15), new Boolean(true), new Double(2.003)} ), //$NON-NLS-1$
                    Arrays.asList( new Object[] { "Third", Integer.valueOf(51), new Boolean(true), new Double(3.003)} ) //$NON-NLS-1$
                    } );

        return dataMgr;
    }

    private FakeDataManager exampleDataManager2(QueryMetadataInterface metadata) throws TeiidException {
        FakeDataManager dataMgr = new FakeDataManager();

        dataMgr.registerTuples(
            metadata,
            "pm1.g1", new List[] {
                    Arrays.asList( new Object[] { "First", Integer.valueOf(5), new Boolean(true), new Double(1.003)} ), //$NON-NLS-1$
                    Arrays.asList( new Object[] { "Second", Integer.valueOf(15), new Boolean(true), new Double(2.003)} ), //$NON-NLS-1$
                    Arrays.asList( new Object[] { "Third", Integer.valueOf(51), new Boolean(true), new Double(3.003)} ) //$NON-NLS-1$
                    } );

        dataMgr.registerTuples(
            metadata,
            "pm1.g2", new List[] {
                    Arrays.asList( new Object[] { "First", Integer.valueOf(5), new Boolean(true), new Double(1.003)} ), //$NON-NLS-1$
                    Arrays.asList( new Object[] { "Second", Integer.valueOf(15), new Boolean(true), new Double(2.003)} ), //$NON-NLS-1$
                    Arrays.asList( new Object[] { "Third", Integer.valueOf(51), new Boolean(true), new Double(3.003)} ) //$NON-NLS-1$
                    } );

        dataMgr.registerTuples(
            metadata,
            "pm2.g1", new List[] {
                    Arrays.asList( new Object[] { "First", Integer.valueOf(5), new Boolean(true), new Double(1.003)} ), //$NON-NLS-1$
                    Arrays.asList( new Object[] { "Second", Integer.valueOf(15), new Boolean(true), new Double(2.003)} ), //$NON-NLS-1$
                    Arrays.asList( new Object[] { "Third", Integer.valueOf(51), new Boolean(true), new Double(3.003)} ) //$NON-NLS-1$
                    } );

        dataMgr.registerTuples(
            metadata,
            "pm2.g2", new List[] {
                    Arrays.asList( new Object[] { "First", Integer.valueOf(5), new Boolean(true), new Double(1.003)} ), //$NON-NLS-1$
                    Arrays.asList( new Object[] { "Second", Integer.valueOf(15), new Boolean(true), new Double(2.003)} ), //$NON-NLS-1$
                    Arrays.asList( new Object[] { "Third", Integer.valueOf(51), new Boolean(true), new Double(3.003)} ) //$NON-NLS-1$
                    } );

        return dataMgr;
    }

    private FakeDataManager exampleDataManagerPm5(QueryMetadataInterface metadata) throws TeiidException {
        FakeDataManager dataMgr = new FakeDataManager();

        dataMgr.registerTuples(
            metadata,
            "pm5.g3", new List[] {
                    Arrays.asList( new Object[] { "First", new Short((short)5), new Boolean(true), new Double(1.003)} ), //$NON-NLS-1$
                    Arrays.asList( new Object[] { "Second", new Short((short)15), new Boolean(true), new Double(2.003)} ), //$NON-NLS-1$
                    Arrays.asList( new Object[] { "Third", new Short((short)51), new Boolean(true), new Double(3.003)} ) //$NON-NLS-1$
                    } );

        return dataMgr;
    }

    @Test public void testVirtualProcedure() throws Exception {
        String userUpdateStr = "EXEC pm1.vsp2()"; //$NON-NLS-1$

        QueryMetadataInterface metadata = RealMetadataFactory.example1Cached();

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

        QueryMetadataInterface metadata = RealMetadataFactory.example1Cached();

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

        QueryMetadataInterface metadata = RealMetadataFactory.example1Cached();

        FakeDataManager dataMgr = exampleDataManager(metadata);

        ProcessorPlan plan = getProcedurePlan(userUpdateStr, metadata);

        // Create expected results
        List[] expected = new List[] {
            Arrays.asList(new Object[] { "Third"})};         //$NON-NLS-1$
        helpTestProcess(plan, expected, dataMgr, metadata);
    }

    @Test public void testVirtualProcedure3() throws Exception {
        String userUpdateStr = "EXEC pm1.vsp4()"; //$NON-NLS-1$

        QueryMetadataInterface metadata = RealMetadataFactory.example1Cached();

        FakeDataManager dataMgr = exampleDataManager(metadata);

        ProcessorPlan plan = getProcedurePlan(userUpdateStr, metadata);

        // Create expected results
        List[] expected = new List[] {
            Arrays.asList(new Object[] { "First"})};         //$NON-NLS-1$
        helpTestProcess(plan, expected, dataMgr, metadata);
    }

    @Test public void testVirtualProcedure4() throws Exception {
        String userUpdateStr = "EXEC pm1.vsp5()"; //$NON-NLS-1$

        QueryMetadataInterface metadata = RealMetadataFactory.example1Cached();

        FakeDataManager dataMgr = exampleDataManager(metadata);

        ProcessorPlan plan = getProcedurePlan(userUpdateStr, metadata);

        // Create expected results
        List[] expected = new List[] {
            Arrays.asList(new Object[] { "First"})};         //$NON-NLS-1$
        helpTestProcess(plan, expected, dataMgr, metadata);
    }

    @Test public void testVirtualProcedure5() throws Exception {
        String userUpdateStr = "EXEC pm1.vsp6()"; //$NON-NLS-1$

        QueryMetadataInterface metadata = RealMetadataFactory.example1Cached();

        FakeDataManager dataMgr = exampleDataManager(metadata);

        ProcessorPlan plan = getProcedurePlan(userUpdateStr, metadata);

        // Create expected results
        List[] expected = new List[] {
            Arrays.asList(new Object[] { "Second"})};         //$NON-NLS-1$
        helpTestProcess(plan, expected, dataMgr, metadata);
    }

    @Test public void testVirtualProcedure6() throws Exception {
        String userUpdateStr = "EXEC pm1.vsp7(5)"; //$NON-NLS-1$

        QueryMetadataInterface metadata = RealMetadataFactory.example1Cached();

        FakeDataManager dataMgr = exampleDataManager(metadata);

        ProcessorPlan plan = getProcedurePlan(userUpdateStr, metadata);

        // Create expected results
        List[] expected = new List[] {
            Arrays.asList(new Object[] { "Second"})};         //$NON-NLS-1$
        helpTestProcess(plan, expected, dataMgr, metadata);
    }

    @Test public void testVirtualProcedure7() throws Exception {
        String userUpdateStr = "EXEC pm1.vsp8(51)"; //$NON-NLS-1$

        QueryMetadataInterface metadata = RealMetadataFactory.example1Cached();

        FakeDataManager dataMgr = exampleDataManager(metadata);

        ProcessorPlan plan = getProcedurePlan(userUpdateStr, metadata);

        // Create expected results
        List[] expected = new List[] {
            Arrays.asList(new Object[] { "Third"})};         //$NON-NLS-1$
        helpTestProcess(plan, expected, dataMgr, metadata);
    }

    @Test public void testVirtualProcedure8() throws Exception {
        String userUpdateStr = "EXEC pm1.vsp9(51)"; //$NON-NLS-1$

        QueryMetadataInterface metadata = RealMetadataFactory.example1Cached();

        FakeDataManager dataMgr = exampleDataManager(metadata);

        ProcessorPlan plan = getProcedurePlan(userUpdateStr, metadata);

        // Create expected results
        List[] expected = new List[] {
            Arrays.asList(new Object[] { "Third"})};         //$NON-NLS-1$
        helpTestProcess(plan, expected, dataMgr, metadata);
    }

    @Test public void testVirtualProcedure9() throws Exception {
        String userUpdateStr = "EXEC pm1.vsp10(51)"; //$NON-NLS-1$

        QueryMetadataInterface metadata = RealMetadataFactory.example1Cached();

        FakeDataManager dataMgr = exampleDataManager(metadata);

        ProcessorPlan plan = getProcedurePlan(userUpdateStr, metadata);

        // Create expected results
        List[] expected = new List[] {};
        helpTestProcess(plan, expected, dataMgr, metadata);
    }


    @Test public void testVirtualProcedure10() throws Exception {
        String userUpdateStr = "EXEC pm1.vsp13()"; //$NON-NLS-1$

        QueryMetadataInterface metadata = RealMetadataFactory.example1Cached();

        FakeDataManager dataMgr = exampleDataManager(metadata);

        ProcessorPlan plan = getProcedurePlan(userUpdateStr, metadata);

        // Create expected results
        List[] expected = new List[] {
            Arrays.asList(new Object[] { "Third", Integer.valueOf(5)})};         //$NON-NLS-1$
        helpTestProcess(plan, expected, dataMgr, metadata);
    }

    @Test public void testVirtualProcedure11() throws Exception {
        String userUpdateStr = "EXEC pm1.vsp14()";     //$NON-NLS-1$
        QueryMetadataInterface metadata = RealMetadataFactory.example1Cached();
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
        QueryMetadataInterface metadata = RealMetadataFactory.example1Cached();
        // Set up data
        FakeDataManager dataMgr = exampleDataManager(metadata);

        ProcessorPlan plan = getProcedurePlan(userUpdateStr, metadata);

        dataMgr.registerTuples(
            metadata,
            "pm1.g2", new List[] {
                    Arrays.asList( new Object[] { "First", Integer.valueOf(5), new Boolean(true), new Double(1.003)} ), //$NON-NLS-1$
                    Arrays.asList( new Object[] { "Third", Integer.valueOf(51), new Boolean(true), new Double(3.003)} ) //$NON-NLS-1$
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
        QueryMetadataInterface metadata = RealMetadataFactory.example1Cached();

        // Set up data
        FakeDataManager dataMgr = exampleDataManager(metadata);

        ProcessorPlan plan = getProcedurePlan(userUpdateStr, metadata);

        dataMgr.registerTuples(
            metadata,
            "pm1.g2", new List[] {
                    Arrays.asList( new Object[] { "First", Integer.valueOf(5), new Boolean(true), new Double(1.003)} ), //$NON-NLS-1$
                    Arrays.asList( new Object[] { "Third", Integer.valueOf(51), new Boolean(true), new Double(3.003)} ) //$NON-NLS-1$
                    } );

        // Create expected results
        List[] expected = new List[] {
            Arrays.asList(new Object[] { "First"}),  //$NON-NLS-1$
            Arrays.asList(new Object[] { "Third"})};           //$NON-NLS-1$
        helpTestProcess(plan, expected, dataMgr, metadata);
    }

    @Test public void testVirtualProcedure14() throws Exception {
        String userUpdateStr = "EXEC pm1.vsp17()";     //$NON-NLS-1$
        QueryMetadataInterface metadata = RealMetadataFactory.example1Cached();
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
        QueryMetadataInterface metadata = RealMetadataFactory.example1Cached();

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
        QueryMetadataInterface metadata = RealMetadataFactory.example1Cached();
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
        QueryMetadataInterface metadata = RealMetadataFactory.example1Cached();
        //Set up data
        FakeDataManager dataMgr = exampleDataManager(metadata);

        ProcessorPlan plan = getProcedurePlan(userUpdateStr, metadata);

        // Create expected results
        List[] expected = new List[] {
            Arrays.asList(new Object[] { "First", Integer.valueOf(5)}),  //$NON-NLS-1$
            Arrays.asList(new Object[] { "Second", Integer.valueOf(15)}),  //$NON-NLS-1$
            Arrays.asList(new Object[] { "Third", Integer.valueOf(51)}),  //$NON-NLS-1$
            Arrays.asList(new Object[] { "Fourth", Integer.valueOf(7)})};           //$NON-NLS-1$
        helpTestProcess(plan, expected, dataMgr, metadata);
    }

    @Test public void testVirtualProcedure18() throws Exception {
        String userUpdateStr = "EXEC pm1.vsp22(7)";     //$NON-NLS-1$
        QueryMetadataInterface metadata = RealMetadataFactory.example1Cached();
        //Set up data
        FakeDataManager dataMgr = exampleDataManager(metadata);

        ProcessorPlan plan = getProcedurePlan(userUpdateStr, metadata);

        // Create expected results
        List[] expected = new List[] {
            Arrays.asList(new Object[] { "Second", Integer.valueOf(15)}),  //$NON-NLS-1$
            Arrays.asList(new Object[] { "Third", Integer.valueOf(51)}) }; //$NON-NLS-1$
        helpTestProcess(plan, expected, dataMgr, metadata);
    }

    @Test public void testVirtualProcedure19() throws Exception {
        String userUpdateStr = "EXEC pm1.vsp23(7)";     //$NON-NLS-1$
        QueryMetadataInterface metadata = RealMetadataFactory.example1Cached();
        //Set up data
        FakeDataManager dataMgr = exampleDataManager(metadata);

        ProcessorPlan plan = getProcedurePlan(userUpdateStr, metadata);

        // Create expected results
        List[] expected = new List[] {
            Arrays.asList(new Object[] { "Second", Integer.valueOf(15)})}; //$NON-NLS-1$
        helpTestProcess(plan, expected, dataMgr, metadata);
    }

    @Test public void testVirtualProcedure19WithBlockedException() throws Exception {
        String userUpdateStr = "EXEC pm1.vsp23(7)";     //$NON-NLS-1$
        QueryMetadataInterface metadata = RealMetadataFactory.example1Cached();
        //Set up data
        FakeDataManager dataMgr = exampleDataManager(metadata);

        ProcessorPlan plan = getProcedurePlan(userUpdateStr, metadata);
        // Create expected results
        List[] expected = new List[] {
            Arrays.asList(new Object[] { "Second", Integer.valueOf(15)})}; //$NON-NLS-1$
        helpTestProcess(plan, expected, dataMgr, metadata);
    }

    @Test public void testVirtualProcedureNoDataInTempTable() throws Exception {
        String userUpdateStr = "EXEC pm1.vsp25()";     //$NON-NLS-1$
        QueryMetadataInterface metadata = RealMetadataFactory.example1Cached();

        //Set up data
        FakeDataManager dataMgr = exampleDataManager(metadata);

        ProcessorPlan plan = getProcedurePlan(userUpdateStr, metadata);

        // Create expected results
        List[] expected = new List[] {};
        helpTestProcess(plan, expected, dataMgr, metadata);
    }

    @Test public void testVirtualProcedure30() throws Exception {
        String userUpdateStr = "EXEC pm1.vsp30()";     //$NON-NLS-1$
        QueryMetadataInterface metadata = RealMetadataFactory.example1Cached();

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
        QueryMetadataInterface metadata = RealMetadataFactory.example1Cached();

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

        QueryMetadataInterface metadata = RealMetadataFactory.example1Cached();

        FakeDataManager dataMgr = exampleDataManager(metadata);

        ProcessorPlan plan = getProcedurePlan(userUpdateStr, metadata);

        // Create expected results
        List[] expected = new List[] {
            Arrays.asList(new Object[] { "Second"})};         //$NON-NLS-1$
        helpTestProcess(plan, expected, dataMgr, metadata);
    }

    @Test public void testDefect16193() throws Exception {
        String userUpdateStr = "EXEC pm1.vsp35(51)";     //$NON-NLS-1$
        QueryMetadataInterface metadata = RealMetadataFactory.example1Cached();
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
        QueryMetadataInterface metadata = RealMetadataFactory.example1Cached();
        ProcessorPlan plan = getProcedurePlan(userUpdateStr, metadata);


        // Set up data
        HardcodedDataManager dataMgr = new HardcodedDataManager();
        dataMgr.addData("INSERT INTO pm1.g1 (e2) VALUES (5)", new List[] {Arrays.asList(1)});

        // Create expected results
        List[] expected = new List[] {
            Arrays.asList("1")};
        helpTestProcess(plan, expected, dataMgr, metadata);
    }

    @Test public void testDefect16649_1() throws Exception {
        String userUpdateStr = "EXEC pm1.vsp38()";     //$NON-NLS-1$
        QueryMetadataInterface metadata = RealMetadataFactory.example1Cached();
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
        QueryMetadataInterface metadata = RealMetadataFactory.example1Cached();
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
        QueryMetadataInterface metadata = RealMetadataFactory.example1Cached();
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
        QueryMetadataInterface metadata = RealMetadataFactory.example1Cached();
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
        QueryMetadataInterface metadata = RealMetadataFactory.example1Cached();
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
        QueryMetadataInterface metadata = RealMetadataFactory.example1Cached();

        //Set up data
        FakeDataManager dataMgr = exampleDataManager(metadata);

        ProcessorPlan plan = getProcedurePlan(userUpdateStr, metadata);

        dataMgr.registerTuples(
            metadata,
            "pm1.g2", new List[] {
                    Arrays.asList( new Object[] { "First", Integer.valueOf(5), new Boolean(true), new Double(1.003)} ), //$NON-NLS-1$
                    Arrays.asList( new Object[] { "Third", Integer.valueOf(51), new Boolean(true), new Double(3.003)} ) //$NON-NLS-1$
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
        QueryMetadataInterface metadata = RealMetadataFactory.example1Cached();

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

    @Test public void testDefect19982() throws Exception {
        String userUpdateStr = "EXEC pm1.vsp55(5)";     //$NON-NLS-1$
        QueryMetadataInterface metadata = RealMetadataFactory.example1Cached();
        ProcessorPlan plan = getProcedurePlan(userUpdateStr, metadata);

        // Set up data
        FakeDataManager dataMgr = exampleDataManager(metadata);

        // Create expected results
        List[] expected = new List[] {
            Arrays.asList(new Object[] { "First", Integer.valueOf(5)}),  //$NON-NLS-1$
            Arrays.asList(new Object[] { "Second", Integer.valueOf(5)}), //$NON-NLS-1$
            Arrays.asList(new Object[] { "Third", Integer.valueOf(5)})};           //$NON-NLS-1$
        helpTestProcess(plan, expected, dataMgr, metadata);
    }

    @Test public void testCase3521() throws Exception {
        String userUpdateStr = "EXEC pm1.vsp1()"; //$NON-NLS-1$

        QueryMetadataInterface metadata = RealMetadataFactory.example1Cached();

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

    @Test public void testDynamicCommandWithIntoExpression() throws Exception {

        //Test INTO clause with expression
        TransformationMetadata metadata = RealMetadataFactory.example1();
        String query = "CREATE VIRTUAL PROCEDURE BEGIN\n" //$NON-NLS-1$ //$NON-NLS-2$
            + "execute string 'SELECT e1 FROM pm1.g1 WHERE e1 = ''First''' as x string into #temp; declare string VARIABLES.RESULT = select x from #temp;select VARIABLES.RESULT; END";

        addProc(metadata, query);

        String userUpdateStr = "EXEC pm1.sq2()"; //$NON-NLS-1$

        FakeDataManager dataMgr = exampleDataManager(metadata);

        ProcessorPlan plan = getProcedurePlan(userUpdateStr, metadata);

        //Create expected results
        List[] expected = new List[] {
            Arrays.asList(new Object[] { "First" }),  //$NON-NLS-1$
            };
        helpTestProcess(plan, expected, dataMgr, metadata);
      }

    @Test public void testDynamicCommandWithIntoRegularTable() throws Exception {
        TransformationMetadata metadata = RealMetadataFactory.example1();
        String query = "CREATE VIRTUAL PROCEDURE BEGIN\n" //$NON-NLS-1$ //$NON-NLS-2$
            + "execute string 'SELECT * FROM pm1.g1 WHERE e1 = ''First''' as a string, b integer, c boolean, d double into pm1.g1; declare string VARIABLES.RESULT = select count(e1) from pm1.g1;select VARIABLES.RESULT; END";

        addProc(metadata, query);

        String userUpdateStr = "EXEC pm1.sq2()"; //$NON-NLS-1$

        FakeDataManager dataMgr = exampleDataManager(metadata);

        ProcessorPlan plan = getProcedurePlan(userUpdateStr, metadata);

        //Create expected results
        List[] expected = new List[] {
            Arrays.asList("3"),  //$NON-NLS-1$
            };
        helpTestProcess(plan, expected, dataMgr, metadata);

        //make sure the insert looks good
        assertTrue(dataMgr.getQueries().toString(), dataMgr.getQueries().contains("INSERT INTO pm1.g1 (e1, e2, e3, e4) VALUES ('First', 5, TRUE, 1.003)"));
    }

    @Test public void testDynamicUpdateInto() throws Exception {

        TransformationMetadata metadata = RealMetadataFactory.example1Cached();
        String query = "BEGIN " //$NON-NLS-1$ //$NON-NLS-2$
            + " execute immediate 'delete from pm1.g1' as v integer into #temp; select * from #temp with return; end";

        FakeDataManager dataMgr = exampleDataManager(metadata);

        ProcessorPlan plan = helpGetPlan(query, metadata);

        helpProcess(plan, dataMgr, new List[] {Arrays.asList(0)});
    }

    private void addProc(TransformationMetadata metadata, String query) {
        addProc(metadata, "sq2", query, new String[] { "e1" }, new String[] { DataTypeManager.DefaultDataTypes.STRING }, new String[0], new String[0]);
    }

    private void addProc(TransformationMetadata metadata, String name, String query, String[] rsCols, String[] rsTypes, String[] params, String[] paramTypes) {
        Schema pm1 = metadata.getMetadataStore().getSchema("PM1"); //$NON-NLS-1$
        pm1.getProcedures().remove(name.toUpperCase());
        ColumnSet<Procedure> rs2 = RealMetadataFactory.createResultSet("rs1", rsCols, rsTypes);
        QueryNode sq2n1 = new QueryNode(query); //$NON-NLS-1$
        ArrayList<ProcedureParameter> procParams = new ArrayList<ProcedureParameter>(params.length);
        for (int i = 0; i < params.length; i++) {
            procParams.add(RealMetadataFactory.createParameter(params[i], SPParameter.IN, paramTypes[i]));
        }
        Procedure sq1 = RealMetadataFactory.createVirtualProcedure(name, pm1, procParams, sq2n1);  //$NON-NLS-1$
        sq1.setResultSet(rs2);
    }

    @Test public void testDynamicCommandWithIntoAndLoop() throws Exception {

        //Test INTO clause with loop
        TransformationMetadata metadata = RealMetadataFactory.example1();

        StringBuffer procedure = new StringBuffer("CREATE VIRTUAL PROCEDURE \n"); //$NON-NLS-1$
        procedure.append("BEGIN\n"); //$NON-NLS-1$
        procedure.append("declare integer VARIABLES.e2_total=0;\n"); //$NON-NLS-1$
        procedure.append("execute string 'SELECT e1, e2 FROM pm1.g1' as e1 string, e2 integer into #temp;\n"); //$NON-NLS-1$
        procedure.append("loop on (Select e2 from #temp where e2 > 2) as mycursor\n"); //$NON-NLS-1$
        procedure.append("BEGIN\n"); //$NON-NLS-1$
        procedure.append("IF (mycursor.e2>5) \n"); //$NON-NLS-1$
        procedure.append("VARIABLES.e2_total=VARIABLES.e2_total+mycursor.e2;\n"); //$NON-NLS-1$
        procedure.append("END\n"); //$NON-NLS-1$
        procedure.append("SELECT cast(VARIABLES.e2_total as string);\n"); //$NON-NLS-1$
        procedure.append("END"); //$NON-NLS-1$

        addProc(metadata, procedure.toString());

        String userUpdateStr = "EXEC pm1.sq2()"; //$NON-NLS-1$

        FakeDataManager dataMgr = exampleDataManager(metadata);

        ProcessorPlan plan = getProcedurePlan(userUpdateStr, metadata);

        //Create expected results
        List[] expected = new List[] {
            Arrays.asList(new Object[] { "66"}),
            };
        helpTestProcess(plan, expected, dataMgr, metadata);
    }

    @Test public void testDynamicCommandWithParameter() throws Exception {
        TransformationMetadata metadata = RealMetadataFactory.example1();

        addProc(metadata, "sq2", "CREATE VIRTUAL PROCEDURE BEGIN\n" //$NON-NLS-1$ //$NON-NLS-2$
                        + "execute string 'SELECT e1, e2 FROM pm1.g1 WHERE e1=pm1.sq2.in' as e1 string, e2 integer; END", new String[] { "e1", "e2" }
        , new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.INTEGER }, new String[] {"in"}, new String[] {DataTypeManager.DefaultDataTypes.STRING});

        String userUpdateStr = "EXEC pm1.sq2('First')"; //$NON-NLS-1$

        FakeDataManager dataMgr = exampleDataManager(metadata);

        ProcessorPlan plan = getProcedurePlan(userUpdateStr, metadata);

        // Create expected results
        List[] expected = new List[] {
                Arrays.asList(new Object[] { "First", Integer.valueOf(5) }),  //$NON-NLS-1$
        };
        helpTestProcess(plan, expected, dataMgr, metadata);
    }

    @Test public void testMultipleReturnable() throws Exception {
        TransformationMetadata metadata = RealMetadataFactory.example1();

        addProc(metadata, "sq2", "CREATE VIRTUAL PROCEDURE BEGIN\n" //$NON-NLS-1$ //$NON-NLS-2$
                        + "SELECT e1, e2 FROM pm1.g1; select e1, e2 from pm1.g2; END", new String[] { "e1", "e2" }
        , new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.INTEGER }, new String[] {"in"}, new String[] {DataTypeManager.DefaultDataTypes.STRING});

        String userUpdateStr = "EXEC pm1.sq2('First')"; //$NON-NLS-1$

        HardcodedDataManager dataMgr = new HardcodedDataManager();
        dataMgr.addData("SELECT pm1.g1.e1, pm1.g1.e2 FROM pm1.g1", new List<?>[0]);
        dataMgr.addData("SELECT pm1.g2.e1, pm1.g2.e2 FROM pm1.g2", new List<?>[] {Arrays.asList("a", 1)});

        ProcessorPlan plan = getProcedurePlan(userUpdateStr, metadata);

        List[] expected = new List<?>[] {Arrays.asList("a", 1)};
        helpTestProcess(plan, expected, dataMgr, metadata);
        assertEquals(6, dataMgr.getCommandHistory().size());
    }

    /**
     * Should return the first results
     */
    @Test public void testReturnable1() throws Exception {
        TransformationMetadata metadata = RealMetadataFactory.example1();

        addProc(metadata, "sq2", "CREATE VIRTUAL PROCEDURE BEGIN\n" //$NON-NLS-1$ //$NON-NLS-2$
                        + "SELECT e1, e2 FROM pm1.g1; select e1, e2 from pm1.g2 without return; END", new String[] { "e1", "e2" }
        , new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.INTEGER }, new String[] {"in"}, new String[] {DataTypeManager.DefaultDataTypes.STRING});

        String userUpdateStr = "EXEC pm1.sq2('First')"; //$NON-NLS-1$

        HardcodedDataManager dataMgr = new HardcodedDataManager();
        dataMgr.addData("SELECT pm1.g1.e1, pm1.g1.e2 FROM pm1.g1", new List<?>[0]);
        dataMgr.addData("SELECT pm1.g2.e1, pm1.g2.e2 FROM pm1.g2", new List<?>[] {Arrays.asList("a", 1)});

        ProcessorPlan plan = getProcedurePlan(userUpdateStr, metadata);

        List[] expected = new List<?>[0];
        helpTestProcess(plan, expected, dataMgr, metadata);
        assertEquals(6, dataMgr.getCommandHistory().size());
    }

    @Test public void testDynamicCommandWithUsing() throws Exception {
        TransformationMetadata metadata = RealMetadataFactory.example1();

        addProc(metadata, "sq2", "CREATE VIRTUAL PROCEDURE BEGIN\n" //$NON-NLS-1$ //$NON-NLS-2$
                        + "execute string 'SELECT e1, e2 FROM pm1.g1 WHERE e1=using.id' using id=pm1.sq2.in; END", new String[] { "e1", "e2" }
        , new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.INTEGER }, new String[] {"in"}, new String[] {DataTypeManager.DefaultDataTypes.STRING});

        String userUpdateStr = "EXEC pm1.sq2('First')"; //$NON-NLS-1$

        FakeDataManager dataMgr = exampleDataManager(metadata);

        ProcessorPlan plan = getProcedurePlan(userUpdateStr, metadata);

        // Create expected results
        List[] expected = new List[] {
                Arrays.asList(new Object[] { "First", Integer.valueOf(5) }),  //$NON-NLS-1$
        };
        helpTestProcess(plan, expected, dataMgr, metadata);
    }

    @Test public void testDynamicCommandWithVariable() throws Exception {
        TransformationMetadata metadata = RealMetadataFactory.example1();

        addProc(metadata, "sq2", "CREATE VIRTUAL PROCEDURE BEGIN\n" //$NON-NLS-1$ //$NON-NLS-2$
                        + "declare string VARIABLES.x; VARIABLES.x = pm1.sq2.in; execute string 'SELECT e1, e2 FROM pm1.g1 WHERE e1=VARIABLES.x'; END", new String[] { "e1", "e2" }
        , new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.INTEGER }, new String[] {"in"}, new String[] {DataTypeManager.DefaultDataTypes.STRING});

        String userUpdateStr = "EXEC pm1.sq2('First')"; //$NON-NLS-1$

        FakeDataManager dataMgr = exampleDataManager(metadata);

        ProcessorPlan plan = getProcedurePlan(userUpdateStr, metadata);

        // Create expected results
        List[] expected = new List[] {
                Arrays.asList(new Object[] { "First", Integer.valueOf(5) }),  //$NON-NLS-1$
        };
        helpTestProcess(plan, expected, dataMgr, metadata);
    }

    @Test public void testDynamicCommandValidationFails() throws Exception {
        TransformationMetadata metadata = RealMetadataFactory.example1();

        addProc(metadata, "sq2", "CREATE VIRTUAL PROCEDURE BEGIN\n" //$NON-NLS-1$ //$NON-NLS-2$
                        + "declare object VARIABLES.x; execute string 'SELECT xmlelement(name elem, x)'; select '1', 2; END", new String[] { "e1", "e2" }
        , new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.INTEGER }, new String[] {"in"}, new String[] {DataTypeManager.DefaultDataTypes.STRING});

        String userUpdateStr = "EXEC pm1.sq2('First')"; //$NON-NLS-1$

        FakeDataManager dataMgr = exampleDataManager(metadata);

        ProcessorPlan plan = getProcedurePlan(userUpdateStr, metadata);

        try {
            helpTestProcess(plan, null, dataMgr, metadata);
            fail("exception expected");
        } catch (QueryProcessingException e) {
        }
    }

    @Test public void testDynamicCommandWithSingleSelect() throws Exception {
        TransformationMetadata metadata = RealMetadataFactory.example1();

        addProc(metadata, "CREATE VIRTUAL PROCEDURE BEGIN\n" //$NON-NLS-1$ //$NON-NLS-2$
                + "execute string 'SELECT 26'; END");

        String userUpdateStr = "EXEC pm1.sq2()"; //$NON-NLS-1$

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
        TransformationMetadata metadata = RealMetadataFactory.example1();

        addProc(metadata, "sq2", "CREATE VIRTUAL PROCEDURE BEGIN\n" //$NON-NLS-1$ //$NON-NLS-2$
                        + "declare string VARIABLES.x; VARIABLES.x = 'a'; execute string 'SELECT e2 ' || ' FROM pm1.g1 ' || ' where e1=pm1.sq2.in'; END", new String[] { "e1" }
        , new String[] { DataTypeManager.DefaultDataTypes.STRING }, new String[] {"in"}, new String[] {DataTypeManager.DefaultDataTypes.STRING});

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
        TransformationMetadata metadata = RealMetadataFactory.example1();

        addProc(metadata, "sq2", "CREATE VIRTUAL PROCEDURE BEGIN\n" //$NON-NLS-1$ //$NON-NLS-2$
                        + "execute string 'EXEC pm1.sq2(''First'')' as e1 string, e2 integer; END", new String[] { "e1", "e2" }
        , new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.INTEGER }, new String[] {"in"}, new String[] {DataTypeManager.DefaultDataTypes.STRING});

        String userUpdateStr = "EXEC pm1.sq2('First')"; //$NON-NLS-1$

        FakeDataManager dataMgr = exampleDataManager(metadata);

        ProcessorPlan plan = getProcedurePlan(userUpdateStr, metadata);

        helpTestProcessFailure(plan,
                               dataMgr,
                               "TEIID30168 Couldn't execute the dynamic SQL command \"EXECUTE IMMEDIATE 'EXEC pm1.sq2(''First'')' AS e1 string, e2 integer\" with the SQL statement \"EXEC pm1.sq2('First')\" due to: TEIID30347 There is a recursive invocation of group 'pm1.sq2'. Please correct the SQL.", metadata); //$NON-NLS-1$
    }

    @Test(expected=QueryPlannerException.class) public void testDynamicCommandIncorrectProjectSymbolCount() throws Exception {
        //Tests dynamic query with incorrect number of elements
        TransformationMetadata metadata = RealMetadataFactory.example1();

        addProc(metadata, "sq1", "CREATE VIRTUAL PROCEDURE BEGIN\n" //$NON-NLS-1$ //$NON-NLS-2$
                        + "SELECT pm1.g1.e1 FROM pm1.g1; END", new String[] { "e1" }
        , new String[] { DataTypeManager.DefaultDataTypes.STRING }, new String[] {"in"}, new String[] {DataTypeManager.DefaultDataTypes.STRING});

        addProc(metadata, "sq2", "CREATE VIRTUAL PROCEDURE BEGIN\n" //$NON-NLS-1$ //$NON-NLS-2$
                        + "execute string 'EXEC pm1.sq1(''First'')' as e1 string, e2 integer; END", new String[] { "e1" }
        , new String[] { DataTypeManager.DefaultDataTypes.STRING }, new String[] {"in"}, new String[] {DataTypeManager.DefaultDataTypes.STRING});

        String userUpdateStr = "EXEC pm1.sq2('test')"; //$NON-NLS-1$

        getProcedurePlan(userUpdateStr, metadata);
     }

    @Test public void testDynamicCommandPositional() throws Exception {
        TransformationMetadata metadata = RealMetadataFactory.example1();

        addProc(metadata, "sq2", "CREATE VIRTUAL PROCEDURE BEGIN\n" //$NON-NLS-1$ //$NON-NLS-2$
                        + "execute string 'select e1 as x, e2 from pm1.g1'; END", new String[] { "e1", "e2" }
        , new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.STRING }, new String[] {"in"}, new String[] {DataTypeManager.DefaultDataTypes.STRING});

        String userUpdateStr = "EXEC pm1.sq2('test')"; //$NON-NLS-1$

        FakeDataManager dataMgr = exampleDataManager(metadata);

        ProcessorPlan plan = getProcedurePlan(userUpdateStr, metadata);

        helpTestProcess(plan, new List[] {Arrays.asList("First", "5"), //$NON-NLS-1$ //$NON-NLS-2$
                Arrays.asList("Second", "15"), //$NON-NLS-1$ //$NON-NLS-2$
                Arrays.asList("Third", "51")}, dataMgr, metadata); //$NON-NLS-1$ //$NON-NLS-2$
     }

    @Test public void testDynamicCommandIncorrectProjectSymbolDatatypes() throws Exception {
        TransformationMetadata metadata = RealMetadataFactory.example1();

        addProc(metadata, "sq2", "CREATE VIRTUAL PROCEDURE BEGIN\n" //$NON-NLS-1$ //$NON-NLS-2$
                                                                        + "execute string 'select e1 from pm1.g1'; END", new String[] { "e1"}, new String[] { DataTypeManager.DefaultDataTypes.INTEGER}, new String[0], new String[0]); //$NON-NLS-1$

        String userUpdateStr = "EXEC pm1.sq2()"; //$NON-NLS-1$

        FakeDataManager dataMgr = exampleDataManager(metadata);

        ProcessorPlan plan = getProcedurePlan(userUpdateStr, metadata);

        helpTestProcessFailure(plan, dataMgr, "TEIID30168 Couldn't execute the dynamic SQL command \"EXECUTE IMMEDIATE 'select e1 from pm1.g1'\" with the SQL statement \"select e1 from pm1.g1\" due to: The datatype 'string' for element 'e1' in the dynamic SQL cannot be implicitly converted to 'integer'.", metadata); //$NON-NLS-1$
     }

    @Test public void testDynamicCommandWithTwoDynamicStatements() throws Exception {
        //Tests dynamic query with two consecutive DynamicCommands. The first without an AS clause and returning different results.
        TransformationMetadata metadata = RealMetadataFactory.example1();

        addProc(metadata, "sq1", "CREATE VIRTUAL PROCEDURE BEGIN\n" //$NON-NLS-1$ //$NON-NLS-2$
                        + "execute string 'select e1 as x, e2 from pm1.g1'; END", new String[] { "e1", "e2" }
        , new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.STRING }, new String[0], new String[0]);

        String userUpdateStr = "EXEC pm1.sq1()"; //$NON-NLS-1$

        FakeDataManager dataMgr = exampleDataManager(metadata);

        ProcessorPlan plan = getProcedurePlan(userUpdateStr, metadata);

        // Create expected results
        List[] expected = new List[] {
            Arrays.asList(new Object[] { "First", "5"}),  //$NON-NLS-1$
            Arrays.asList(new Object[] { "Second", "15"}), //$NON-NLS-1$
            Arrays.asList(new Object[] { "Third", "51"})};           //$NON-NLS-1$

        helpTestProcess(plan, expected, dataMgr, metadata);
     }

    @Test public void testAssignmentWithCase() throws Exception {
        TransformationMetadata metadata = RealMetadataFactory.example1();

        String sql = new StringBuffer("declare integer caseValue = ") //$NON-NLS-1$
        .append("CASE") //$NON-NLS-1$
        .append(" WHEN pm1.sq1.param='a' THEN 0") //$NON-NLS-1$
        .append(" WHEN pm1.sq1.param='b' THEN 1") //$NON-NLS-1$
        .append(" WHEN pm1.sq1.param='c' THEN 2") //$NON-NLS-1$
        .append(" WHEN pm1.sq1.param='d' THEN 3") //$NON-NLS-1$
        .append(" ELSE 9999") //$NON-NLS-1$
        .append(" END").toString(); //$NON-NLS-1$

        addProc(metadata, "sq1", "CREATE VIRTUAL PROCEDURE BEGIN\n" //$NON-NLS-1$ //$NON-NLS-2$
                                        + sql + "; SELECT caseValue; END", new String[] { "e1"}, new String[] { DataTypeManager.DefaultDataTypes.INTEGER}, new String[] {"param"}, new String[] {DataTypeManager.DefaultDataTypes.STRING}); //$NON-NLS-1$

        String userUpdateStr = "EXEC pm1.sq1('d')"; //$NON-NLS-1$

        FakeDataManager dataMgr = exampleDataManager(metadata);

        ProcessorPlan plan = getProcedurePlan(userUpdateStr, metadata);

        // Create expected results
        List[] expected = new List[] {
                Arrays.asList(new Object[] { Integer.valueOf(3) }),
        };
        helpTestProcess(plan, expected, dataMgr, metadata);
    }

    @Test public void testDynamicCommandInsertIntoTempTableWithDifferentDatatypeFromSource() throws Exception {
        //Tests dynamic query with insert into a temp table using data returned from a physical table.
        //See defect 23394
        TransformationMetadata metadata = RealMetadataFactory.example1();

        addProc(metadata, "sq2", "CREATE VIRTUAL PROCEDURE BEGIN\n" //$NON-NLS-1$ //$NON-NLS-2$
                + "execute string 'select e1,e2 from pm5.g3' as e1 string, e2 integer INTO #temp; select * from #temp; END", new String[] { "e1", "e2"}, new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.INTEGER}, new String[0], new String[0]); //$NON-NLS-1$

        String userUpdateStr = "EXEC pm1.sq2()"; //$NON-NLS-1$

        FakeDataManager dataMgr = exampleDataManagerPm5(metadata);

        ProcessorPlan plan = getProcedurePlan(userUpdateStr, metadata);

        // Create expected results
        List[] expected = new List[] {
            Arrays.asList(new Object[] { "First", Integer.valueOf(5)}),  //$NON-NLS-1$
            Arrays.asList(new Object[] { "Second", Integer.valueOf(15)}), //$NON-NLS-1$
            Arrays.asList(new Object[] { "Third", Integer.valueOf(51)})};           //$NON-NLS-1$

        helpTestProcess(plan, expected, dataMgr, metadata);
     }

    @Test public void testDynamicCommandWithVariableOnly() throws Exception {
        //Tests dynamic query with only a variable that represents thte entire dynamic query.
        TransformationMetadata metadata = RealMetadataFactory.example1();

        addProc(metadata, "sq1", "CREATE VIRTUAL PROCEDURE BEGIN\n" //$NON-NLS-1$ //$NON-NLS-2$
                + "DECLARE string VARIABLES.CRIT = 'select e1, e2 from pm5.g3 where e2=using.id'; execute string VARIABLES.CRIT USING ID = pm1.sq1.param; END", new String[] { "e1", "e2"}, new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.SHORT}, new String[] {"param"}, new String[] {DataTypeManager.DefaultDataTypes.SHORT}); //$NON-NLS-1$

        String userUpdateStr = "EXEC pm1.sq1(convert(5,short))"; //$NON-NLS-1$

        FakeDataManager dataMgr = exampleDataManagerPm5(metadata);

        ProcessorPlan plan = getProcedurePlan(userUpdateStr, metadata);

        // Create expected results
        List[] expected = new List[] { Arrays.asList(new Object[] { "First", new Short((short)5)})};           //$NON-NLS-1$

        helpTestProcess(plan, expected, dataMgr, metadata);
     }

    @Test public void testVirtualProcedureWithCreate() throws Exception{
        String userUpdateStr = "EXEC pm1.vsp60()"; //$NON-NLS-1$

        QueryMetadataInterface metadata = RealMetadataFactory.example1Cached();

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

        QueryMetadataInterface metadata = RealMetadataFactory.example1Cached();

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

        QueryMetadataInterface metadata = RealMetadataFactory.example1Cached();

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

        TransformationMetadata metadata = RealMetadataFactory.example1();

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
        procedure.append("SELECT cast(VARIABLES.e2_total as string);\n"); //$NON-NLS-1$
        procedure.append("END"); //$NON-NLS-1$

        addProc(metadata, procedure.toString());

        String userUpdateStr = "EXEC pm1.sq2()"; //$NON-NLS-1$

        FakeDataManager dataMgr = exampleDataManager(metadata);

        ProcessorPlan plan = getProcedurePlan(userUpdateStr, metadata);

        //Create expected results
        List[] expected = new List[] {
            Arrays.asList("3"),
            };
        helpTestProcess(plan, expected, dataMgr, metadata);
    }

    @Test public void testLoopsWithBreak() throws Exception {

        TransformationMetadata metadata = RealMetadataFactory.example1();

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
        procedure.append("SELECT cast(VARIABLES.e2_total as string);\n"); //$NON-NLS-1$
        procedure.append("END"); //$NON-NLS-1$

        addProc(metadata, procedure.toString());

        String userUpdateStr = "EXEC pm1.sq2()"; //$NON-NLS-1$

        FakeDataManager dataMgr = exampleDataManager(metadata);

        ProcessorPlan plan = getProcedurePlan(userUpdateStr, metadata);

        //Create expected results
        List[] expected = new List[] {
            Arrays.asList("76"),
            };
        helpTestProcess(plan, expected, dataMgr, metadata);
    }

    @Test public void testLoopsWithLabels() throws Exception {
        TransformationMetadata metadata = RealMetadataFactory.example1();

        StringBuffer procedure = new StringBuffer("CREATE VIRTUAL PROCEDURE \n"); //$NON-NLS-1$
        procedure.append("y: BEGIN\n"); //$NON-NLS-1$
        procedure.append("declare integer VARIABLES.e2_total=param1;\n"); //$NON-NLS-1$
        procedure.append("x: loop on (select e2 as x from pm1.g1) as mycursor\n"); //$NON-NLS-1$
        procedure.append("BEGIN\n"); //$NON-NLS-1$
        procedure.append("e2_total=e2_total+mycursor.x;\n"); //$NON-NLS-1$
        procedure.append("loop on (select e2 as x from pm1.g1) as mycursor1\n"); //$NON-NLS-1$
        procedure.append("BEGIN\n"); //$NON-NLS-1$
        procedure.append("if (e2_total < 5)\n"); //$NON-NLS-1$
        procedure.append("break x;\n"); //$NON-NLS-1$
        procedure.append("else if (e2_total > 50)\n"); //$NON-NLS-1$
        procedure.append("leave y;\n"); //$NON-NLS-1$
        procedure.append("e2_total=e2_total+mycursor1.x;"); //$NON-NLS-1$
        procedure.append("END\n"); //$NON-NLS-1$
        procedure.append("END\n"); //$NON-NLS-1$
        procedure.append("SELECT VARIABLES.e2_total;\n"); //$NON-NLS-1$
        procedure.append("END"); //$NON-NLS-1$

        addProc(metadata, "sq2", procedure.toString(), new String[] { "e1" }, new String[] { DataTypeManager.DefaultDataTypes.INTEGER }, new String[] {"param1"}, new String[] {DataTypeManager.DefaultDataTypes.INTEGER});

        String userUpdateStr = "EXEC pm1.sq2(1)"; //$NON-NLS-1$

        FakeDataManager dataMgr = exampleDataManager(metadata);

        ProcessorPlan plan = getProcedurePlan(userUpdateStr, metadata);

        //Create expected results
        List[] expected = new List[] {
            };
        helpTestProcess(plan, expected, dataMgr, metadata);

        expected = new List[] {
            Arrays.asList(0),
            };
        userUpdateStr = "EXEC pm1.sq2(-5)"; //$NON-NLS-1$
        plan = getProcedurePlan(userUpdateStr, metadata);
        helpTestProcess(plan, expected, dataMgr, metadata);
    }

    @Test public void testCreateWithoutDrop() throws Exception {

        TransformationMetadata metadata = RealMetadataFactory.example1();

        StringBuffer procedure = new StringBuffer("CREATE VIRTUAL PROCEDURE \n"); //$NON-NLS-1$
        procedure.append("BEGIN\n"); //$NON-NLS-1$
        procedure.append("create local temporary table t1 (e1 integer);\n"); //$NON-NLS-1$
        procedure.append("create local temporary table T1 (e1 integer);\n"); //$NON-NLS-1$
        procedure.append("SELECT cast(e1 as string) from t1;\n"); //$NON-NLS-1$
        procedure.append("END"); //$NON-NLS-1$

        addProc(metadata, procedure.toString());

        String userUpdateStr = "EXEC pm1.sq2()"; //$NON-NLS-1$

        FakeDataManager dataMgr = exampleDataManager(metadata);

        ProcessorPlan plan = getProcedurePlan(userUpdateStr, metadata);

        helpTestProcessFailure(plan, dataMgr, "TEIID30229 Temporary table \"T1\" already exists.", metadata); //$NON-NLS-1$
    }

    @Test(expected=QueryPlannerException.class) public void testDoubleDrop() throws Exception {

        TransformationMetadata metadata = RealMetadataFactory.example1();

        StringBuffer procedure = new StringBuffer("CREATE VIRTUAL PROCEDURE \n"); //$NON-NLS-1$
        procedure.append("BEGIN\n"); //$NON-NLS-1$
        procedure.append("create local temporary table t1 (e1 string);\n"); //$NON-NLS-1$
        procedure.append("select e1 into t1 from pm1.g1;\n"); //$NON-NLS-1$
        procedure.append("drop table t1;\n"); //$NON-NLS-1$
        procedure.append("drop table t1;\n"); //$NON-NLS-1$
        procedure.append("SELECT '1';\n"); //$NON-NLS-1$
        procedure.append("END"); //$NON-NLS-1$

        addProc(metadata, procedure.toString());

        String userUpdateStr = "EXEC pm1.sq2()"; //$NON-NLS-1$

        FakeDataManager dataMgr = exampleDataManager(metadata);

        getProcedurePlan(userUpdateStr, metadata);
    }

    /**
     * defect 23975
     */
    @Test public void testFunctionInput() throws Exception {
        MetadataStore metadataStore = new MetadataStore();
        Schema v1 = RealMetadataFactory.createVirtualModel("v1", metadataStore); //$NON-NLS-1$

        ProcedureParameter p1 = RealMetadataFactory.createParameter("in", ParameterInfo.IN, DataTypeManager.DefaultDataTypes.STRING);  //$NON-NLS-1$
        ColumnSet<Procedure> rs1 = RealMetadataFactory.createResultSet("v1.rs1", new String[] {"e1"}, new String[] { DataTypeManager.DefaultDataTypes.STRING }); //$NON-NLS-1$ //$NON-NLS-2$

        QueryNode n1 = new QueryNode("CREATE VIRTUAL PROCEDURE BEGIN declare string VARIABLES.x = '1'; exec v1.vp2(concat(x, v1.vp1.in)); END"); //$NON-NLS-1$ //$NON-NLS-2$
        Procedure vt1 = RealMetadataFactory.createVirtualProcedure("vp1", v1, Arrays.asList(p1), n1); //$NON-NLS-1$
        vt1.setResultSet(rs1);

        ProcedureParameter p2 = RealMetadataFactory.createParameter("in", ParameterInfo.IN, DataTypeManager.DefaultDataTypes.STRING);  //$NON-NLS-1$
        QueryNode n2 = new QueryNode("CREATE VIRTUAL PROCEDURE BEGIN select v1.vp2.in; end"); //$NON-NLS-1$ //$NON-NLS-2$
        Procedure vt2 = RealMetadataFactory.createVirtualProcedure("vp2", v1, Arrays.asList(p2), n2); //$NON-NLS-1$
        vt2.setResultSet(RealMetadataFactory.createResultSet("v1.rs1", new String[] {"e1"}, new String[] { DataTypeManager.DefaultDataTypes.STRING })); //$NON-NLS-1$ //$NON-NLS-2$

        String sql = "exec v1.vp1('1')"; //$NON-NLS-1$

        List[] expected = new List[] {
            Arrays.asList(new Object[] { "11" }), //$NON-NLS-1$
        };

        QueryMetadataInterface metadata = RealMetadataFactory.createTransformationMetadata(metadataStore, "foo");

        // Construct data manager with data
        // Plan query
        ProcessorPlan plan = getProcedurePlan(sql, metadata);
        // Run query
        helpTestProcess(plan, expected, new FakeDataManager(), metadata);
    }

    /**
     *  This is a slight variation of TestProcessor.testVariableInExecParam, where the proc wrapper can be
     *  removed after rewrite
     */
    @Test public void testReferenceForwarding() throws Exception {
        // Create query
        String sql = "EXEC pm1.vsp49()"; //$NON-NLS-1$

        TransformationMetadata metadata = RealMetadataFactory.example1();

        addProc(metadata, "sq2", "CREATE VIRTUAL PROCEDURE BEGIN if (1 = 2) begin declare integer x = 1; end SELECT e1, e2 FROM pm1.g1 WHERE e1=pm1.sq2.in; END",
                new String[] { "e1", "e2" }, new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.INTEGER }
        , new String[] {"in"}, new String[] {DataTypeManager.DefaultDataTypes.STRING});  //$NON-NLS-1$

        // Create expected results
        List[] expected = new List[] {
            Arrays.asList(new Object[] { "b", Integer.valueOf(2) }), //$NON-NLS-1$
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
        TransformationMetadata metadata = RealMetadataFactory.example1();

        StringBuffer procedure = new StringBuffer("CREATE VIRTUAL PROCEDURE \n") //$NON-NLS-1$
        .append("BEGIN\n") //$NON-NLS-1$
        .append("\n  create local temporary table #temp (e1 string, e2 string);") //$NON-NLS-1$
        .append("\n  insert into #temp (e1) values ('a');") //$NON-NLS-1$
        .append("\n  insert into #temp (e2) values ('b');") //$NON-NLS-1$
        .append("SELECT e2 as e1 from #temp;\n") //$NON-NLS-1$
        .append("END"); //$NON-NLS-1$

        addProc(metadata, procedure.toString());

        String userUpdateStr = "EXEC pm1.sq2()"; //$NON-NLS-1$

        FakeDataManager dataMgr = exampleDataManager(metadata);

        ProcessorPlan plan = getProcedurePlan(userUpdateStr, metadata);

        helpTestProcess(plan, new List[] {
            Arrays.asList(new Object[] {null}),
            Arrays.asList(new Object[] {"b"})}, dataMgr, metadata); //$NON-NLS-1$

    }

    @Test public void testEvaluatableSelectWithOrderBy() throws Exception {
        TransformationMetadata metadata = RealMetadataFactory.example1();

        StringBuffer procedure = new StringBuffer("CREATE VIRTUAL PROCEDURE \n") //$NON-NLS-1$
        .append("BEGIN\n") //$NON-NLS-1$
        .append("SELECT param from pm1.g1 order by param limit 1;\n") //$NON-NLS-1$
        .append("END"); //$NON-NLS-1$

        addProc(metadata, "sq1", procedure.toString(), new String[] { "e1" }, new String[] { DataTypeManager.DefaultDataTypes.STRING }, new String[] {"param"}, new String[] {DataTypeManager.DefaultDataTypes.STRING});

        String userUpdateStr = "EXEC pm1.sq1(1)"; //$NON-NLS-1$

        FakeDataManager dataMgr = exampleDataManager(metadata);

        ProcessorPlan plan = getProcedurePlan(userUpdateStr, metadata);

        helpTestProcess(plan, new List[] {
            Arrays.asList(new Object[] {"1"})}, dataMgr, metadata); //$NON-NLS-1$

    }

    @Test public void testEvaluatableLimit() throws Exception {
        TransformationMetadata metadata = RealMetadataFactory.example1();

        StringBuffer procedure = new StringBuffer("CREATE VIRTUAL PROCEDURE \n") //$NON-NLS-1$
        .append("BEGIN\n") //$NON-NLS-1$
        .append("SELECT e1 from pm1.g1 limit param;\n") //$NON-NLS-1$
        .append("END"); //$NON-NLS-1$

        addProc(metadata, "sq1", procedure.toString(), new String[] { "e1" }, new String[] { DataTypeManager.DefaultDataTypes.STRING }, new String[] {"param"}, new String[] {DataTypeManager.DefaultDataTypes.INTEGER});


        FakeDataManager dataMgr = exampleDataManager(metadata);

        String userUpdateStr = "EXEC pm1.sq1(1)"; //$NON-NLS-1$
        ProcessorPlan plan = getProcedurePlan(userUpdateStr, metadata);

        helpTestProcess(plan, new List[] {
            Arrays.asList(new Object[] {"First"})}, dataMgr, metadata); //$NON-NLS-1$

        userUpdateStr = "EXEC pm1.sq1(-1)"; //$NON-NLS-1$
        plan = getProcedurePlan(userUpdateStr, metadata);

        try {
            helpTestProcess(plan, new List[] {
                Arrays.asList(new Object[] {"First"})}, dataMgr, metadata); //$NON-NLS-1$
            fail();
        } catch (QueryValidatorException e) {
            //shouldn't allow -1
        }
    }

    @Test public void testEvaluatableLimit2() throws Exception {
        TransformationMetadata metadata = RealMetadataFactory.fromDDL("create foreign table t (c string); " +
                "create virtual procedure proc (p short) returns (c string) as select c from t limit p;", "vdb", "m");
        HardcodedDataManager dataMgr = new HardcodedDataManager();
        dataMgr.addData("SELECT m.t.c FROM m.t", new List<?>[] {Arrays.asList("a"), Arrays.asList("b")});
        String sql = "call proc(1)";
        ProcessorPlan plan = getProcedurePlan(sql, metadata);
        helpTestProcess(plan, new List[] {
            Arrays.asList(new Object[] {"a"})}, dataMgr, metadata); //$NON-NLS-1$
    }

    //should fail as the param type is incorrect
    @Test(expected=QueryPlannerException.class) public void testEvaluatableLimit1() throws Exception {
        TransformationMetadata metadata = RealMetadataFactory.example1();

        StringBuffer procedure = new StringBuffer("CREATE VIRTUAL PROCEDURE \n") //$NON-NLS-1$
        .append("BEGIN\n") //$NON-NLS-1$
        .append("SELECT e1 from pm1.g1 limit param;\n") //$NON-NLS-1$
        .append("END"); //$NON-NLS-1$

        addProc(metadata, "sq1", procedure.toString(), new String[] { "e1" }, new String[] { DataTypeManager.DefaultDataTypes.STRING }, new String[] {"param"}, new String[] {DataTypeManager.DefaultDataTypes.STRING});

        String userUpdateStr = "EXEC pm1.sq1(1)"; //$NON-NLS-1$

        getProcedurePlan(userUpdateStr, metadata);
    }

    @Test public void testEvaluatableSelectWithOrderBy1() throws Exception {
        TransformationMetadata metadata = RealMetadataFactory.example1();

        StringBuffer procedure = new StringBuffer("CREATE VIRTUAL PROCEDURE \n") //$NON-NLS-1$
        .append("BEGIN\n") //$NON-NLS-1$
        .append("SELECT param from pm1.g1 union select e1 from pm1.g1 order by param limit 2;\n") //$NON-NLS-1$
        .append("END"); //$NON-NLS-1$

        addProc(metadata, "sq1", procedure.toString(), new String[] { "e1" }, new String[] { DataTypeManager.DefaultDataTypes.STRING }, new String[] {"param"}, new String[] {DataTypeManager.DefaultDataTypes.STRING});

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
        .append("SELECT cast(e1 as string) FROM #TEMP;\n") //$NON-NLS-1$
        .append("END"); //$NON-NLS-1$

        QueryMetadataInterface metadata = createProcedureMetadata(procedure.toString());

        String userUpdateStr = "EXEC pm1.sq1()"; //$NON-NLS-1$

        FakeDataManager dataMgr = exampleDataManager(metadata);

        ProcessorPlan plan = getProcedurePlan(userUpdateStr, metadata);


        helpTestProcess(plan, new List[] {
            Arrays.asList("240"),
            Arrays.asList("637")}, dataMgr, metadata);
    }

    private QueryMetadataInterface createProcedureMetadata(String procedure) {
        TransformationMetadata metadata = RealMetadataFactory.example1();
        addProc(metadata, "sq1", procedure, new String[] { "e1" }, new String[] { DataTypeManager.DefaultDataTypes.STRING }, new String[0], new String[0]);
        return metadata;
    }

    @Test public void testTempTableTypeConversion() throws Exception {

        String procedure = "CREATE VIRTUAL PROCEDURE\n"; //$NON-NLS-1$
        procedure += "BEGIN\n";       //$NON-NLS-1$
        procedure += "CREATE local temporary table temp (x string, y integer);\n";       //$NON-NLS-1$
        procedure += "Select pm1.g1.e2 as e1, pm1.g1.e2 into temp from pm1.g1 order by pm1.g1.e2 limit 1;\n"; //$NON-NLS-1$
        procedure += "Select x from temp;\n"; //$NON-NLS-1$
        procedure += "END\n"; //$NON-NLS-1$

        QueryMetadataInterface metadata = createProcedureMetadata(procedure);

        String userUpdateStr = "EXEC pm1.sq1()"; //$NON-NLS-1$

        FakeDataManager dataMgr = exampleDataManager(metadata);

        ProcessorPlan plan = getProcedurePlan(userUpdateStr, metadata);

        helpTestProcess(plan, new List[] {
            Arrays.asList(new Object[] {"5"}), //$NON-NLS-1$
            }, dataMgr, metadata);
    }

    @Test public void testCase174806() throws Exception{
        String userUpdateStr = "EXEC pm1.vsp63()"; //$NON-NLS-1$

        QueryMetadataInterface metadata = RealMetadataFactory.example1Cached();

        FakeDataManager dataMgr = exampleDataManager(metadata);

        ProcessorPlan plan = getProcedurePlan(userUpdateStr, metadata);

        // Create expected results
        List[] expected = new List[] {
            Arrays.asList(new Object[] { "c"})};         //$NON-NLS-1$
        helpTestProcess(plan, expected, dataMgr, metadata);
    }

    @Test public void testJoinProcAndPhysicalModel() throws Exception {
        String userUpdateStr = "select a.e1 from (EXEC pm1.vsp46()) as a, pm1.g1 where a.e1=pm1.g1.e1";     //$NON-NLS-1$
        QueryMetadataInterface metadata = RealMetadataFactory.example1Cached();

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
     * is valid and fully resolvable.
     *
     * <p>This test is related to JBEDSP-818 in which the query in the
     * <code>LOOP</code> statement would fail due to a query being used as the
     * assigned value in the <code>DECLARE</code> statement.
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
        procedure += "   SELECT cast(VARIABLES.Var1 as string) AS e1;\n"; //$NON-NLS-1$
        procedure += "END\n"; //$NON-NLS-1$

        QueryMetadataInterface metadata = createProcedureMetadata(procedure);
        String userQuery = "SELECT e1 FROM (EXEC pm1.sq1()) as proc"; //$NON-NLS-1$
        FakeDataManager dataMgr = exampleDataManager(metadata);
        ProcessorPlan plan = getProcedurePlan(userQuery, metadata);

        List[] expected = new List[] {Arrays.asList("3")};
        helpTestProcess(plan, expected, dataMgr, metadata);
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

        TransformationMetadata metadata = RealMetadataFactory.example1();
        addProc(metadata, "sq1", proc, new String[] { "e1", "e2", "e3", "e4" }, new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.INTEGER, DataTypeManager.DefaultDataTypes.INTEGER }, new String[0], new String[0]);
        String userQuery = "SELECT * FROM (EXEC pm1.sq1()) as proc"; //$NON-NLS-1$
        FakeDataManager dataMgr = exampleDataManager2(metadata);
        ProcessorPlan plan = getProcedurePlan(userQuery, metadata);

        List[] expected = new List[] {
                Arrays.asList( new Object[] { "First", "First", Integer.valueOf(5), Integer.valueOf(5)} ), //$NON-NLS-1$ //$NON-NLS-2$
                Arrays.asList( new Object[] { "Second", null, Integer.valueOf(15), null} ), //$NON-NLS-1$
                Arrays.asList( new Object[] { "Third", null, Integer.valueOf(51), null} ) //$NON-NLS-1$
        };
        helpTestProcess(plan, expected, dataMgr, metadata);

        assertTrue(!plan.requiresTransaction(false));
    }

    @Test public void testDDLProcTransaction() throws Exception {
        String ddl = "create foreign procedure proc (x integer) options (updatecount 2);"; //$NON-NLS-1$

        TransformationMetadata metadata = RealMetadataFactory.fromDDL(ddl, "x", "y");
        String userQuery = "EXEC proc(1)"; //$NON-NLS-1$
        ProcessorPlan plan = getProcedurePlan(userQuery, metadata);

        assertTrue(plan.requiresTransaction(false));
    }

    @Test public void testDDLProcTransactionNonTransactionalJoin() throws Exception {
        String ddl = "create foreign procedure proc () returns table(col string);"
                + "create virtual procedure virt() as begin select * from proc, proc as x; end"; //$NON-NLS-1$

        TransformationMetadata metadata = RealMetadataFactory.fromDDL(ddl, "x", "y");
        String userQuery = "EXEC virt()"; //$NON-NLS-1$
        ProcessorPlan plan = getProcedurePlan(userQuery, metadata);

        assertTrue(plan.requiresTransaction(false));

        BasicSourceCapabilities bsc = new BasicSourceCapabilities();
        bsc.setSourceProperty(Capability.TRANSACTION_SUPPORT, TransactionSupport.NONE);
        plan = getProcedurePlan(userQuery, metadata, new DefaultCapabilitiesFinder(bsc));

        assertFalse(plan.requiresTransaction(false));
    }

    @Test public void testAnonProcTransaction() throws Exception {
        ProcedurePlan plan = (ProcedurePlan) TestProcessor.helpGetPlan("begin select 1; end", RealMetadataFactory.example1Cached());
        assertFalse(plan.requiresTransaction(false));

        plan = (ProcedurePlan) TestProcessor.helpGetPlan("begin select * from pm1.g1; end", RealMetadataFactory.example1Cached());
        assertNull(plan.requiresTransaction(true));

        plan = (ProcedurePlan) TestProcessor.helpGetPlan("begin insert into pm1.g1 (e1) values ('a'); end", RealMetadataFactory.example1Cached());
        assertNull(plan.requiresTransaction(false));

        plan = (ProcedurePlan) TestProcessor.helpGetPlan("begin if (true) insert into pm1.g1 (e1) values ('a'); else insert into pm1.g1 (e1) values ('b'); end", RealMetadataFactory.example1Cached());
        assertNull(plan.requiresTransaction(false));

        plan = (ProcedurePlan) TestProcessor.helpGetPlan("begin loop on (select e1 from pm1.g1) as x begin insert into pm1.g1 (e1) values (x.e1); end end", RealMetadataFactory.example1Cached());
        assertNull(plan.requiresTransaction(false));

        plan = (ProcedurePlan) TestProcessor.helpGetPlan("begin execute immediate 'select 1'; end", RealMetadataFactory.example1Cached());
        assertFalse(plan.requiresTransaction(false));

        plan = (ProcedurePlan) TestProcessor.helpGetPlan("begin execute immediate 'select 1'; end", RealMetadataFactory.example1Cached());
        assertNull(plan.requiresTransaction(true));

        plan = (ProcedurePlan) TestProcessor.helpGetPlan("begin execute immediate 'select 1' update *; end", RealMetadataFactory.example1Cached());
        assertTrue(plan.requiresTransaction(false));
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

        TransformationMetadata metadata = RealMetadataFactory.example1();
        addProc(metadata, "sq1", proc, new String[] { "e1", "e2", "e3", "e4" }, new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.INTEGER, DataTypeManager.DefaultDataTypes.INTEGER }, new String[0], new String[0]);
        String userQuery = "SELECT * FROM (EXEC pm1.sq1()) as proc"; //$NON-NLS-1$
        FakeDataManager dataMgr = exampleDataManager2(metadata);
        ProcessorPlan plan = getProcedurePlan(userQuery, metadata, TestOptimizer.getGenericFinder());

        List[] expected = new List[] {
                Arrays.asList( new Object[] { "First", "First", Integer.valueOf(5), Integer.valueOf(5)} ), //$NON-NLS-1$ //$NON-NLS-2$
                Arrays.asList( new Object[] { "Second", null, Integer.valueOf(15), null} ), //$NON-NLS-1$
                Arrays.asList( new Object[] { "Third", null, Integer.valueOf(51), null} ) //$NON-NLS-1$
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

        QueryMetadataInterface metadata = createProcedureMetadata(proc);
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
                " select cast(e2 as string) from (exec pm1.sq2((select max(e1) from t1))) x;\n" + //$NON-NLS-1$
                "END"; //$NON-NLS-1$

        QueryMetadataInterface metadata = createProcedureMetadata(proc);
        String userQuery = "SELECT * FROM (EXEC pm1.sq1()) as proc"; //$NON-NLS-1$
        FakeDataManager dataMgr = exampleDataManager2(metadata);
        ProcessorPlan plan = getProcedurePlan(userQuery, metadata, TestOptimizer.getGenericFinder());

        List[] expected = new List[] {
                Arrays.asList( "51" ),
        };
        helpTestProcess(plan, expected, dataMgr, metadata);
    }

    @Test public void testOuterTempTableExecuteImmediateTarget() throws Exception {
        String proc = "CREATE VIRTUAL PROCEDURE " + //$NON-NLS-1$
                "BEGIN " + //$NON-NLS-1$
                " create local temporary table t1 (e1 string);\n" + //$NON-NLS-1$
                " loop on (select 1 as a union all select 2) as c \n" +
                " begin \n" +
                " execute immediate 'select c.a' as e1 string into t1; \n" +
                " end \n" +
                " select * from t1;\n" + //$NON-NLS-1$
                "END"; //$NON-NLS-1$

        QueryMetadataInterface metadata = createProcedureMetadata(proc);
        String userQuery = "SELECT * FROM (EXEC pm1.sq1()) as proc"; //$NON-NLS-1$
        FakeDataManager dataMgr = exampleDataManager2(metadata);
        ProcessorPlan plan = getProcedurePlan(userQuery, metadata, TestOptimizer.getGenericFinder());

        List[] expected = new List[] {
                Arrays.asList( "1" ),
                Arrays.asList( "2" ),
        };
        helpTestProcess(plan, expected, dataMgr, metadata);
    }

    @Test public void testUnambiguousVirtualProc() throws Exception {
        String userQuery = "EXEC MMSP6('1')"; //$NON-NLS-1$
        QueryMetadataInterface metadata = RealMetadataFactory.exampleBQTCached();
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

        TransformationMetadata metadata = RealMetadataFactory.example1();

        StringBuffer procedure = new StringBuffer("CREATE VIRTUAL PROCEDURE \n"); //$NON-NLS-1$
        procedure.append("BEGIN\n"); //$NON-NLS-1$
        procedure.append("create local temporary table x (y string);\n"); //$NON-NLS-1$
        procedure.append("declare string s = 'foo';\n"); //$NON-NLS-1$
        procedure.append("update x set y = in1 || s;\n"); //$NON-NLS-1$
        procedure.append("update pm1.g1 set e1 = lookup('pm1.g1', 'e1', 'e2', in1);\n"); //$NON-NLS-1$
        procedure.append("exec pm1.sq2(in1 || 'foo');\n"); //$NON-NLS-1$
        procedure.append("END"); //$NON-NLS-1$

        addProc(metadata, "sq1", procedure.toString(), new String[] { "e1", "e2" }, new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.INTEGER }, new String[] {"in1"}, new String[] {DataTypeManager.DefaultDataTypes.INTEGER});

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

    @Test public void testBeginAtomic() throws Exception {
        String proc = "CREATE VIRTUAL PROCEDURE " + //$NON-NLS-1$
                "BEGIN ATOMIC" + //$NON-NLS-1$
                " select e1, e2, e3, e4 into #t1 from pm1.g1;\n" + //$NON-NLS-1$
                " update #t1 set e1 = 1 where e4 < 2;\n" + //$NON-NLS-1$
                " delete from #t1 where e4 > 2;\n" + //$NON-NLS-1$
                " select e2/\"in\" from #t1;\n" + //$NON-NLS-1$
                "END"; //$NON-NLS-1$
        TransformationMetadata tm = RealMetadataFactory.example1();
        addProc(tm, "sq1", proc, new String[] { "e1" },
                new String[] { DataTypeManager.DefaultDataTypes.INTEGER }, new String[] {"in"}, new String[] {DataTypeManager.DefaultDataTypes.INTEGER});
        FakeDataManager dataMgr = exampleDataManager(tm);
        CommandContext context = new CommandContext("pID", null, null, null, 1); //$NON-NLS-1$
        QueryMetadataInterface metadata = new TempMetadataAdapter(tm, new TempMetadataStore());
        context.setMetadata(metadata);

        TransactionContext tc = new TransactionContext();
        TransactionService ts = Mockito.mock(TransactionService.class);
        context.setTransactionService(ts);
        context.setTransactionContext(tc);
        String userQuery = "EXEC pm1.sq1(1)"; //$NON-NLS-1$
        ProcessorPlan plan = getProcedurePlan(userQuery, tm, TestOptimizer.getGenericFinder());
        List[] expected = new List[] {
                Arrays.asList(5),
        };
        TestProcessor.helpProcess(plan, context, dataMgr, expected);
        Mockito.verify(ts, Mockito.times(3)).begin(tc);
        Mockito.verify(ts, Mockito.times(3)).commit(tc);

        tc = new TransactionContext();
        ts = Mockito.mock(TransactionService.class);
        context.setTransactionService(ts);
        context.setTransactionContext(tc);
        userQuery = "EXEC pm1.sq1(0)"; //$NON-NLS-1$
        plan = getProcedurePlan(userQuery, tm, TestOptimizer.getGenericFinder());
        expected = null;
        try {
            TestProcessor.helpProcess(plan, context, dataMgr, expected);
            fail();
        } catch (TeiidProcessingException e) {

        }
        Mockito.verify(ts).begin(tc);
        Mockito.verify(ts, Mockito.times(2)).resume(tc);
        Mockito.verify(ts, Mockito.times(0)).commit(tc);
        Mockito.verify(ts).rollback(tc);
    }

    @Test public void testVarArgs() throws Exception {
        String ddl = "create foreign procedure proc (x integer, VARIADIC z integer); create virtual procedure vproc (x integer, VARIADIC z integer) returns integer as begin \"return\" = z[2] + array_length(z); call proc(x, z); end;";
        TransformationMetadata tm = TestProcedureResolving.createMetadata(ddl);
        String sql = "call vproc(1, 2, 3)"; //$NON-NLS-1$

        ProcessorPlan plan = getProcedurePlan(sql, tm);

        HardcodedDataManager dataManager = new HardcodedDataManager(tm);
        dataManager.addData("EXEC proc(1, 2, 3)", new List<?>[0]);
        // Create expected results
        List[] expected = new List[] { Arrays.asList(5) }; //$NON-NLS-1$
        helpTestProcess(plan, expected, dataManager, tm);
    }

    @Test public void testVarArgsNull() throws Exception {
        String ddl = "create foreign procedure proc (x integer, VARIADIC z integer not null); create virtual procedure vproc (x integer, VARIADIC z integer) returns integer as begin \"return\" = z[2] + array_length(z); call proc(x, z); end;";
        TransformationMetadata tm = TestProcedureResolving.createMetadata(ddl);
        String sql = "call vproc(1, cast(null as integer[]))"; //$NON-NLS-1$

        ProcessorPlan plan = getProcedurePlan(sql, tm);

        HardcodedDataManager dataManager = new HardcodedDataManager(tm);
        dataManager.addData("EXEC proc(1)", new List<?>[0]);
        // Create expected results
        List[] expected = new List[] { Collections.singletonList(null) }; //$NON-NLS-1$
        helpTestProcess(plan, expected, dataManager, tm);

        sql = "call vproc(x=>1, z=>null)"; //$NON-NLS-1$

        plan = getProcedurePlan(sql, tm);

        helpTestProcess(plan, expected, dataManager, tm);
    }

    @Test public void testVarArgsVirtNull() throws Exception {
        String ddl = "create virtual procedure vproc (x integer, VARIADIC z integer not null) returns (y integer) as begin select array_length(z); end;";
        TransformationMetadata tm = TestProcedureResolving.createMetadata(ddl);

        String sql = "call vproc(1, (select cast(null as integer[])))"; //$NON-NLS-1$

        ProcessorPlan plan = getProcedurePlan(sql, tm);

        HardcodedDataManager dataManager = new HardcodedDataManager();
        helpTestProcess(plan, new List[] {Collections.singletonList(null)}, dataManager, tm);
    }

    @Test public void testVarArgsVirtNotNull() throws Exception {
        String ddl = "create virtual procedure vproc (x integer, VARIADIC z integer NOT NULL) returns (y integer) as begin select array_length(z); end;";
        TransformationMetadata tm = TestProcedureResolving.createMetadata(ddl);

        String sql = "call vproc(1, null, 3)"; //$NON-NLS-1$

        try {
            getProcedurePlan(sql, tm);
            fail();
        } catch (QueryValidatorException e) {

        }

        sql = "call vproc(1, (select cast(null as integer)), 3)"; //$NON-NLS-1$

        ProcessorPlan plan = getProcedurePlan(sql, tm);

        HardcodedDataManager dataManager = new HardcodedDataManager();
        try {
            helpTestProcess(plan, null, dataManager, tm);
            fail();
        } catch (QueryValidatorException e) {

        }
    }

    @Test public void testVarArgsFunctionInVirt() throws Exception {
        String ddl = "create virtual procedure proc (VARIADIC z STRING) returns string as \"return\" = coalesce(null, null, z);";
        TransformationMetadata tm = TestProcedureResolving.createMetadata(ddl);
        String sql = "call proc(1, 2, 3)"; //$NON-NLS-1$

        ProcessorPlan plan = getProcedurePlan(sql, tm);

        HardcodedDataManager dataManager = new HardcodedDataManager(tm);
        // note that we're properly cast to string, even though we called with int
        List[] expected = new List[] { Arrays.asList("1") }; //$NON-NLS-1$
        helpTestProcess(plan, expected, dataManager, tm);
    }

    @Test public void testNestedBlock() throws Exception {
        String ddl = "create virtual procedure proc (z STRING) returns table (x string, y string) as begin declare string x = z; select x without return; begin select x, x; end end;";
        TransformationMetadata tm = TestProcedureResolving.createMetadata(ddl);
        String sql = "call proc('a')"; //$NON-NLS-1$

        ProcessorPlan plan = getProcedurePlan(sql, tm);

        HardcodedDataManager dataManager = new HardcodedDataManager(tm);
        List[] expected = new List[] { Arrays.asList("a", "a") }; //$NON-NLS-1$
        helpTestProcess(plan, expected, dataManager, tm);
    }

    @Test public void testReturnStatement() throws Exception {
        String ddl = "create virtual procedure proc (OUT a string RESULT, z STRING) returns table (x string, y string) as begin declare string x = z; select x without return; if (z = 'a') return 2; else if (z = 'b') return; begin select x, x; end end;";
        TransformationMetadata tm = TestProcedureResolving.createMetadata(ddl);
        String sql = "{? = call proc('a')}"; //$NON-NLS-1$

        ProcessorPlan plan = getProcedurePlan(sql, tm);

        HardcodedDataManager dataManager = new HardcodedDataManager(tm);
        List[] expected = new List[] { Arrays.asList(null, null, "2") }; //$NON-NLS-1$
        helpTestProcess(plan, expected, dataManager, tm);

        sql = "{? = call proc('b')}"; //$NON-NLS-1$
        plan = getProcedurePlan(sql, tm);
        expected = new List[] { Arrays.asList(null, null, null) }; //$NON-NLS-1$
        helpTestProcess(plan, expected, dataManager, tm);
    }

    @Test public void testReturnStatementWithDynamicCommad() throws Exception {
        String ddl = "create virtual procedure proc (z STRING) returns integer as begin execute immediate 'select '' || z || '''; return 1; end;";
        TransformationMetadata tm = TestProcedureResolving.createMetadata(ddl);
        String sql = "{? = call proc('a')}"; //$NON-NLS-1$

        ProcessorPlan plan = getProcedurePlan(sql, tm);

        HardcodedDataManager dataManager = new HardcodedDataManager(tm);
        List[] expected = new List[] { Arrays.asList(1) }; //$NON-NLS-1$
        helpTestProcess(plan, expected, dataManager, tm);
    }

    @Test public void testAnonBlock() throws Exception {
        String sql = "begin insert into #temp (e1) select e1 from pm1.g1; select * from #temp; end;"; //$NON-NLS-1$
        TransformationMetadata tm = RealMetadataFactory.example1Cached();
        ProcessorPlan plan = getProcedurePlan(sql, tm);

        HardcodedDataManager dataManager = new HardcodedDataManager(tm);
        dataManager.addData("SELECT g1.e1 FROM g1", new List<?>[] {Arrays.asList("a")});
        List[] expected = new List[] { Arrays.asList("a") }; //$NON-NLS-1$
        helpTestProcess(plan, expected, dataManager, tm);
    }

    @Test public void testDyanmicAnonBlockWithReturn() throws Exception {
        String sql = "begin execute immediate 'begin select 1; select 1, 2; end'; end"; //$NON-NLS-1$
        TransformationMetadata tm = RealMetadataFactory.example1Cached();
        ProcessorPlan plan = getProcedurePlan(sql, tm);

        HardcodedDataManager dataManager = new HardcodedDataManager(tm);
        List[] expected = new List[] {}; //$NON-NLS-1$
        helpTestProcess(plan, expected, dataManager, tm);

        sql = "begin execute immediate 'begin select 1; select 1, 2; end' as y string; end"; //$NON-NLS-1$
        tm = RealMetadataFactory.example1Cached();
        plan = getProcedurePlan(sql, tm);

        try {
            helpTestProcess(plan, expected, dataManager, tm);
            fail();
        } catch (QueryProcessingException e) {

        }
    }

    @Test(expected=QueryProcessingException.class) public void testDyanmicAnonBlockInto() throws Exception {
        String sql = "begin execute immediate 'begin select 2; end' as x integer into #temp; end"; //$NON-NLS-1$
        TransformationMetadata tm = RealMetadataFactory.example1Cached();
        ProcessorPlan plan = getProcedurePlan(sql, tm);

        HardcodedDataManager dataManager = new HardcodedDataManager(tm);
        List[] expected = new List[] {}; //$NON-NLS-1$
        helpTestProcess(plan, expected, dataManager, tm);
    }

    @Test public void testDyanmicAnonBlockRewrite() throws Exception {
        String sql = "begin insert into #temp values (1); declare integer x = 1; execute immediate 'begin x = 2; insert into #temp select x; end' without return; select * from #temp; end"; //$NON-NLS-1$
        TransformationMetadata tm = RealMetadataFactory.example1Cached();
        ProcessorPlan plan = getProcedurePlan(sql, tm);

        HardcodedDataManager dataManager = new HardcodedDataManager(tm);
        List[] expected = new List[] {Arrays.asList(1), Arrays.asList(2)}; //$NON-NLS-1$
        helpTestProcess(plan, expected, dataManager, tm);
    }

    /**
     * Should fail as the results conflict from multiple statements
     */
    @Test(expected=QueryValidatorException.class) public void testAnonBlockResolveFails() throws Exception {
        String sql = "begin insert into #temp (e1) select e1 from pm1.g1; select * from #temp; select * from pm1.g1; end;"; //$NON-NLS-1$
        TransformationMetadata tm = RealMetadataFactory.example1Cached();
        getProcedurePlan(sql, tm);
    }

    @Test public void testDepJoinFullProcessing() throws Exception {
        String sql = " BEGIN"
                + "\n    create local temporary table ssid_version (sysplex varchar, lpar varchar, ssid varchar, version varchar);"
                + "\n  insert into ssid_version(sysplex, lpar, ssid, version) values ('plex1', 'ca11', 'd91a', 'v5');"
                + "\n    insert into ssid_version(sysplex, lpar, ssid, version) values ('plex1', 'ca11', 'd91b', 'v6');"
                + "\n create local temporary table table_spaces_v5 (sysplex varchar, lpar varchar, ssid varchar, table_space_id varchar);"
                + "\n    insert into table_spaces_v5 (sysplex, lpar, ssid, table_space_id) values ('plex1', 'ca11', 'd91a', 'ts1');"
                + "\n create local temporary table table_spaces_v6 (sysplex varchar, lpar varchar, ssid varchar, table_space_id varchar);"
                + "\n    insert into table_spaces_v6 (sysplex, lpar, ssid, table_space_id) values ('plex1', 'ca11', 'd91b', 'ts2');"
                + "\n select table_space_id from ( select * from (select v.sysplex, v.lpar, v.ssid, t.table_space_id from ssid_version v join table_spaces_v5 t on t.sysplex=v.sysplex and t.lpar=v.lpar and t.ssid=v.ssid option makedep table_spaces_v5) t"
                + " union all select * from (select v.sysplex, v.lpar, v.ssid, t.table_space_id from ssid_version v join table_spaces_v6 t on t.sysplex=v.sysplex and t.lpar=v.lpar and t.ssid=v.ssid option makedep table_spaces_v6) t"
                + " ) t where ssid='d91a';"
                //+ " exception e"
                //+ " raise e.exception;"
                + "\n   END";

        TransformationMetadata tm = RealMetadataFactory.example1Cached();
        ProcessorPlan plan = getProcedurePlan(sql, tm);

        HardcodedDataManager dataManager = new HardcodedDataManager(tm);
        dataManager.addData("SELECT g1.e1 FROM g1", new List<?>[] {Arrays.asList("a")});
        List[] expected = new List[] { Arrays.asList("ts1") }; //$NON-NLS-1$
        helpTestProcess(plan, expected, dataManager, tm);
    }


    @Test public void testDynamicCommandWithIntoExpressionInNestedBlock() throws Exception {
        TransformationMetadata metadata = RealMetadataFactory.example1Cached();
        String query = "BEGIN\n"
                + "EXECUTE IMMEDIATE 'SELECT e1 FROM pm1.g1 WHERE e1 = ''First''' as x string into #temp;\n"
                + "declare string VARIABLES.RESULT = select x from #temp;\n"
                + "IF (VARIABLES.RESULT = 'First')\n"
                + "  BEGIN ATOMIC\n"
                + "  EXECUTE IMMEDIATE 'SELECT e1 FROM pm1.g1' AS x string;"
                + "  EXECUTE IMMEDIATE 'SELECT e1 FROM pm1.g1 WHERE e1 = ''Second''' as x string into #temp2 WITHOUT RETURN;\n"
                + "  VARIABLES.RESULT = select x from #temp2;\n" + "  END"
                + " select VARIABLES.RESULT;" + "END";

        FakeDataManager dataMgr = exampleDataManager(metadata);

        ProcessorPlan plan = getProcedurePlan(query, metadata);

        // Create expected results
        List[] expected = new List[] { Arrays.asList(new Object[] { "Second" }), //$NON-NLS-1$
        };
        helpTestProcess(plan, expected, dataMgr, metadata);
    }

    @Test public void testResultSetAtomic() throws Exception {
        String ddl =
                "create virtual procedure proc2 (x integer) returns table(y integer) as begin select 1; begin atomic select 2; end end;";
        TransformationMetadata tm = TestProcedureResolving.createMetadata(ddl);

        String sql = "call proc2(0)"; //$NON-NLS-1$

        ProcessorPlan plan = getProcedurePlan(sql, tm);

        HardcodedDataManager dataManager = new HardcodedDataManager(tm);

        helpTestProcess(plan, new List[] {Arrays.asList(2)}, dataManager, tm);
    }

    @Test public void testSubqueryArguments() {
        String sql = "select * from (EXEC pm1.sq3b((select min(e1) from pm1.g1), (select max(e2) from pm1.g1))) as x"; //$NON-NLS-1$

        ProcessorPlan plan = helpGetPlan(sql, RealMetadataFactory.example1Cached());
        FakeDataManager fdm = new FakeDataManager();
        fdm.setBlockOnce();
        sampleData1(fdm);
        helpProcess(plan, fdm, new List[] {Arrays.asList("a", 0), Arrays.asList("a", 3), Arrays.asList("a", 0), Arrays.asList("a", 3)});
    }

    @Test public void testDynamicInsert() throws Exception {
        String sql = "exec p1(1)"; //$NON-NLS-1$
        TransformationMetadata tm = RealMetadataFactory.fromDDL("create virtual procedure p1(a long) returns (res long) as "
                + "begin create local temporary table t (x string); execute immediate 'insert into t select ''a''';  end;", "x", "y");
        ProcessorPlan plan = getProcedurePlan(sql, tm);

        HardcodedDataManager dataManager = new HardcodedDataManager(tm);
        List[] expected = new List[] {  }; //$NON-NLS-1$
        helpTestProcess(plan, expected, dataManager, tm);
    }

    @Test(expected=TeiidProcessingException.class) public void testDynamicInsert1() throws Exception {
        String sql = "exec p1(1)"; //$NON-NLS-1$
        TransformationMetadata tm = RealMetadataFactory.fromDDL("create virtual procedure p1(a long) returns (res long) as "
                + "begin create local temporary table t (x string); execute immediate 'insert into t select ''a''' as res long;  end;", "x", "y");
        ProcessorPlan plan = getProcedurePlan(sql, tm);

        HardcodedDataManager dataManager = new HardcodedDataManager(tm);
        List[] expected = new List[] {  }; //$NON-NLS-1$
        helpTestProcess(plan, expected, dataManager, tm);
    }

    @Test(expected=TeiidProcessingException.class) public void testDynamicCreate() throws Exception {
        String sql = "exec p1(1)"; //$NON-NLS-1$
        TransformationMetadata tm = RealMetadataFactory.fromDDL("create virtual procedure p1(a long) returns (res long) as "
                + "begin execute immediate 'create local temporary table t (x string)' as res long;  end;", "x", "y");
        ProcessorPlan plan = getProcedurePlan(sql, tm);

        HardcodedDataManager dataManager = new HardcodedDataManager(tm);
        List[] expected = new List[] {  }; //$NON-NLS-1$
        helpTestProcess(plan, expected, dataManager, tm);
    }

    // TEIID-3267 OPTION NOCACHE causes ConcurrentModificationException
    @Test public void testOptionNocacheDynamic() throws Exception {
        TransformationMetadata metadata = RealMetadataFactory.example1();
        String proc = "CREATE VIRTUAL PROCEDURE\n" //$NON-NLS-1$
            + "BEGIN\n" //$NON-NLS-1$
            + "DECLARE string VARIABLES.strSql = 'select g1.e1 from vm1.g1 as g1, vm1.g2 as g2 where g1.e1=g2.e1 option nocache g1';\n" //$NON-NLS-1$
            + "EXECUTE IMMEDIATE VARIABLES.strSql AS id string;\n" //$NON-NLS-1$
            + "END"; //$NON-NLS-1$
        addProc(metadata, proc);
        String userUpdateStr = "EXEC pm1.sq2()"; //$NON-NLS-1$
        HardcodedDataManager hdm = new HardcodedDataManager(false);
        ProcessorPlan plan = getProcedurePlan(userUpdateStr, metadata);

        // expecting 0 row without an exception
        List[] expected = new List[] {}; //$NON-NLS-1$
        helpTestProcess(plan, expected, hdm, metadata);
    }

    @Test public void testUDF() throws Exception {
        TransformationMetadata metadata = RealMetadataFactory.fromDDL("CREATE VIRTUAL FUNCTION f1(VARIADIC e1 integer) RETURNS integer as return array_length(e1);", "x", "y");

        ProcessorPlan plan = helpGetPlan("select f1(1, 2, 1)", metadata);
        CommandContext cc = TestProcessor.createCommandContext();
        cc.setMetadata(metadata);
        helpProcess(plan, cc, new HardcodedDataManager(), new List[] {Arrays.asList(3)});
    }

    @Test public void testUDFCorrelated() throws Exception {
        TransformationMetadata metadata = RealMetadataFactory.fromDDL("CREATE VIRTUAL FUNCTION f1(x integer) RETURNS string as return (select e1 from g1 where e2 = x/2);; create foreign table g1 (e1 string, e2 integer);", "x", "y");

        ProcessorPlan plan = helpGetPlan("select * from g1, table ( select f1 (g1.e2)) t;", metadata);
        CommandContext cc = TestProcessor.createCommandContext();
        cc.setMetadata(metadata);
        HardcodedDataManager hdm = new HardcodedDataManager();
        hdm.addData("SELECT y.g1.e1, y.g1.e2 FROM y.g1", Arrays.asList("a", 1), Arrays.asList("b", 2));
        hdm.addData("SELECT y.g1.e2, y.g1.e1 FROM y.g1", Arrays.asList(1, "a"), Arrays.asList(2, "b"));
        hdm.setBlockOnce(true);
        helpProcess(plan, cc, hdm, new List[] {Arrays.asList("a", 1, null), Arrays.asList("b", 2, "a")});
    }

    @Test public void testDefaultExpression() throws Exception {
        TransformationMetadata metadata = RealMetadataFactory.fromDDL("CREATE foreign procedure f1(x string default 'current_database()' options (\"teiid_rel:default_handling\" 'expression')) RETURNS string", "x", "y");

        ProcessorPlan plan = helpGetPlan("exec f1()", metadata);
        CommandContext cc = TestProcessor.createCommandContext();
        cc.setMetadata(metadata);
        HardcodedDataManager hdm = new HardcodedDataManager(metadata);
        hdm.addData("EXEC f1('myvdb')", Arrays.asList("a"));
        hdm.setBlockOnce(true);
        helpProcess(plan, cc, hdm, new List[] {Arrays.asList("a")});
    }

    @Test public void testOmitDefault() throws Exception {
        TransformationMetadata metadata = RealMetadataFactory.fromDDL("CREATE foreign procedure f1(x string not null options (\"teiid_rel:default_handling\" 'omit')) RETURNS string;", "x", "y");

        ProcessorPlan plan = helpGetPlan("exec f1()", metadata);
        CommandContext cc = TestProcessor.createCommandContext();
        cc.setMetadata(metadata);
        HardcodedDataManager hdm = new HardcodedDataManager(metadata);
        hdm.addData("EXEC f1()", Arrays.asList("a"));
        helpProcess(plan, cc, hdm, new List[] {Arrays.asList("a")});
    }

    @Test public void testOmitDefaultWithDefault() throws Exception {
        TransformationMetadata metadata = RealMetadataFactory.fromDDL("CREATE foreign procedure f1(x string default truncate options (\"teiid_rel:default_handling\" 'omit')) RETURNS string", "x", "y");

        ProcessorPlan plan = helpGetPlan("exec f1()", metadata);
        CommandContext cc = TestProcessor.createCommandContext();
        cc.setMetadata(metadata);
        HardcodedDataManager hdm = new HardcodedDataManager(metadata);
        hdm.addData("EXEC f1()", Arrays.asList("a"));
        helpProcess(plan, cc, hdm, new List[] {Arrays.asList("a")});
    }

    @Test public void testVariadicParameterOrdering() throws Exception {
        TransformationMetadata metadata = RealMetadataFactory.fromDDL("CREATE PROCEDURE p1(VARIADIC parameters integer) returns integer[] AS BEGIN "
                + "return parameters; END;", "x", "y");
        StringBuilder sql = new StringBuilder("exec p1(0");
        int arraySize = 66000; //dependent upon the jre.  this is suffient to trigger the issue on oracle 1.8
        for (int i = 1; i < arraySize; i++) {
            sql.append(',').append(i);
        }
        sql.append(')');

        ProcessorPlan plan = helpGetPlan(sql.toString(), metadata);
        CommandContext cc = TestProcessor.createCommandContext();
        cc.setMetadata(metadata);

        Integer[] val = new Integer[arraySize];
        for (int i = 0; i < val.length; i++) {
            val[i] = i;
        }

        ArrayImpl expected = new ArrayImpl(val);

        HardcodedDataManager hdm = new HardcodedDataManager(metadata);
        helpProcess(plan, cc, hdm, new List[] {Arrays.asList(expected)});
    }

    @Test public void testDynamicClob() throws Exception {
        String sql = "exec p1()"; //$NON-NLS-1$
        TransformationMetadata tm = RealMetadataFactory.fromDDL("create virtual procedure p1() as "
                + "begin create local temporary table t (x string); execute immediate cast('select * from t' as clob); end;", "x", "y");
        ProcessorPlan plan = getProcedurePlan(sql, tm);

        HardcodedDataManager dataManager = new HardcodedDataManager(tm);
        List[] expected = new List[] {  }; //$NON-NLS-1$
        helpTestProcess(plan, expected, dataManager, tm);
    }

    @Test public void testTeiid5001() throws Exception {
        ParseInfo.REQUIRE_UNQUALIFIED_NAMES = false;
        try {
            String sql = "SELECT d.id FROM ( SELECT 'l1' as domain ) dim_md_domains_to_load, table(CALL testcase.proc_web_avg_visit_duration_empty(\"domain\" => domain)) x JOIN testcase.dim_md_date_ranges d ON true";

            TransformationMetadata tm = RealMetadataFactory.fromDDL("CREATE VIEW testcase.dim_md_date_ranges AS SELECT 1 as \"id\" union all select 2 "
                    + "CREATE VIRTUAL PROCEDURE proc_web_avg_visit_duration_empty( domain string ) RETURNS (i integer) AS BEGIN select 1; END", "x", "testcase");

            ProcessorPlan plan = getProcedurePlan(sql, tm);

            HardcodedDataManager dataManager = new HardcodedDataManager(tm);
            List[] expected = new List[] { Arrays.asList(1), Arrays.asList(2) }; //$NON-NLS-1$
            helpTestProcess(plan, expected, dataManager, tm);
        } finally {
            ParseInfo.REQUIRE_UNQUALIFIED_NAMES = true;
        }
    }

    @Test public void testNonDeterministicNow() throws Exception {
        String sql = "BEGIN\n" +
                "    declare timestamp ts1 = (select now());\n" +
                "    DECLARE integer c = 20000;\n" +
                "    WHILE (c > 0) \n" +
                "        BEGIN\n" +
                "            c= c-1; \n" +
                "        END\n" +
                "    declare timestamp ts2 = (select now());\n" +
                "    if (ts1 = ts2)"
                + "     raise sqlexception 'failed';" +
                "END ;";
        TransformationMetadata tm = RealMetadataFactory.example1Cached();

        ProcessorPlan plan = getProcedurePlan(sql, tm);

        HardcodedDataManager dataManager = new HardcodedDataManager(tm);
        List[] expected = new List[] { }; //$NON-NLS-1$
        helpTestProcess(plan, expected, dataManager, tm);
    }

    @Test public void testImplicitRecreate() throws Exception {
        String sql = "begin\n" +
                "    select * into #temp from (select 1 as a) x;\n" +
                "    drop table #temp;\n" +
                "    select * into #temp from (select 'a' as a) x;\n" +
                "end";
        TransformationMetadata tm = RealMetadataFactory.example1Cached();

        ProcessorPlan plan = getProcedurePlan(sql, tm);

        HardcodedDataManager dataManager = new HardcodedDataManager(tm);
        List[] expected = new List[] { }; //$NON-NLS-1$
        helpTestProcess(plan, expected, dataManager, tm);
    }

    @Test public void testExplictDropException() throws Exception {
        String sql = "begin create temporary table temp (a integer);\n" +
                "    select * into temp from (select 1 as a) x;\n" +
                "    drop table temp;\n" +
                "    select * into temp from (select 'a' as a) x;\n" +
                "end";
        TransformationMetadata tm = RealMetadataFactory.example1Cached();

        try {
            getProcedurePlan(sql, tm);
            fail();
        } catch (QueryResolverException e) {
            assertTrue(e.getMessage().contains("Group does not exist"));
        }
    }

    @Test public void testDoubleCreate() throws Exception {
        String sql = "begin "
                + "   create temporary table temp (a integer); "
                + "   create temporary table temp (a integer);\n" +
                "end";
        TransformationMetadata tm = RealMetadataFactory.example1Cached();

        //should plan just fine, but fail to process
        ProcessorPlan plan = getProcedurePlan(sql, tm);

        HardcodedDataManager dataManager = new HardcodedDataManager(tm);
        List[] expected = new List[] { }; //$NON-NLS-1$
        try {
            helpTestProcess(plan, expected, dataManager, tm);
            fail();
        } catch (QueryProcessingException e) {

        }
    }

    @Test public void testAnonDynamicAtomic() throws Exception {
        String sql = "begin atomic\n" +
                "execute immediate 'begin update pm1.g1 t set e2 = -1 where e1 = ''a''; error ''Test error''; end'; " +
                "end ";

        helpTestDyanmicTxn(sql);
    }

    @Test public void testAnonDynamicAtomicUpdateClause() throws Exception {
        String sql = "begin atomic\n" +
                "execute immediate 'begin update pm1.g1 t set e2 = -1 where e1 = ''a''; error ''Test error''; end' update 1; " +
                "end ";

        helpTestDyanmicTxn(sql);
    }

    private void helpTestDyanmicTxn(String sql)
            throws Exception, XATransactionException {
        TransformationMetadata tm = RealMetadataFactory.example1Cached();

        ProcessorPlan plan = TestProcessor.helpGetPlan(sql, tm, TestOptimizer.getGenericFinder());

        HardcodedDataManager dataManager = new HardcodedDataManager(tm);
        dataManager.addData("UPDATE g1 SET e2 = -1 WHERE g1.e1 = 'a'", Arrays.asList(1));
        List[] expected = new List[] { }; //$NON-NLS-1$
        CommandContext context = TestProcessor.createCommandContext();
        TransactionContext tc = new TransactionContext();
        TransactionService ts = Mockito.mock(TransactionService.class);
        context.setTransactionService(ts);
        context.setTransactionContext(tc);

        try {
            CommandContext.pushThreadLocalContext(context);
            TestProcessor.helpProcess(plan, context, dataManager, expected);
            fail();
        } catch (TeiidProcessingException e) {

        } finally {
            CommandContext.popThreadLocalContext();
        }

        Mockito.verify(ts, Mockito.times(1)).begin(tc);
        Mockito.verify(ts, Mockito.times(0)).commit(tc);
        Mockito.verify(ts, Mockito.times(1)).rollback(tc);
    }

    private static final boolean DEBUG = false;

}
