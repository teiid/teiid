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

package org.teiid.query.optimizer.relational;

import static org.junit.Assert.*;
import static org.teiid.query.processor.TestProcessor.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.Test;
import org.mockito.Mockito;
import org.teiid.api.exception.query.QueryPlannerException;
import org.teiid.api.exception.query.QueryProcessingException;
import org.teiid.api.exception.query.QueryValidatorException;
import org.teiid.client.metadata.ParameterInfo;
import org.teiid.core.TeiidException;
import org.teiid.core.TeiidProcessingException;
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
import org.teiid.query.optimizer.capabilities.CapabilitiesFinder;
import org.teiid.query.optimizer.capabilities.DefaultCapabilitiesFinder;
import org.teiid.query.parser.QueryParser;
import org.teiid.query.processor.FakeDataManager;
import org.teiid.query.processor.HardcodedDataManager;
import org.teiid.query.processor.ProcessorDataManager;
import org.teiid.query.processor.ProcessorPlan;
import org.teiid.query.processor.TestProcessor;
import org.teiid.query.processor.xml.TestXMLPlanningEnhancements;
import org.teiid.query.processor.xml.TestXMLProcessor;
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

@SuppressWarnings({"unchecked", "rawtypes", "nls"})
public class TestRelationalPlanner {
    
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
    
    private FakeDataManager exampleDataManager(QueryMetadataInterface metadata) throws TeiidException {
        FakeDataManager dataMgr = new FakeDataManager();
        
        dataMgr.registerTuples(metadata, "pm1.g1", new List[] {});
        
        dataMgr.registerTuples(metadata, "pm1.g2", new List[] {});
        
        return dataMgr;
    }
    
    // TEIID-3267 OPTION NOCACHE causes ConcurrentModificationException
    @Test public void testOptionNocacheWithinProcedure() throws Exception {
        TransformationMetadata metadata = RealMetadataFactory.example1();
        String proc = "CREATE VIRTUAL PROCEDURE\n" //$NON-NLS-1$
            + "BEGIN\n" //$NON-NLS-1$
            + "DECLARE string VARIABLES.strSql = 'select g1.e1 from vm1.g1 as g1, vm1.g2 as g2 where g1.e1=g2.e1 option nocache g1';\n" //$NON-NLS-1$
            + "EXECUTE IMMEDIATE VARIABLES.strSql AS id string;\n" //$NON-NLS-1$
            + "END"; //$NON-NLS-1$
        addProc(metadata, proc);
        String userUpdateStr = "EXEC pm1.sq2()"; //$NON-NLS-1$
        FakeDataManager dataMgr = exampleDataManager(metadata);
        ProcessorPlan plan = getProcedurePlan(userUpdateStr, metadata);
        
        // expecting 0 row without an exception
        List[] expected = new List[] {}; //$NON-NLS-1$
        helpTestProcess(plan, expected, dataMgr, metadata);
    }
    
    private static final boolean DEBUG = false;
    
}
