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

package org.teiid.query.optimizer.proc;

import java.util.Collections;

import org.junit.Test;
import org.teiid.api.exception.query.QueryMetadataException;
import org.teiid.api.exception.query.QueryValidatorException;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidProcessingException;
import org.teiid.metadata.Table.TriggerEvent;
import org.teiid.query.analysis.AnalysisRecord;
import org.teiid.query.metadata.QueryMetadataInterface;
import org.teiid.query.metadata.TempMetadataID;
import org.teiid.query.optimizer.QueryOptimizer;
import org.teiid.query.optimizer.capabilities.DefaultCapabilitiesFinder;
import org.teiid.query.parser.QueryParser;
import org.teiid.query.processor.ProcessorPlan;
import org.teiid.query.resolver.QueryResolver;
import org.teiid.query.rewriter.QueryRewriter;
import org.teiid.query.sql.lang.Command;
import org.teiid.query.sql.proc.CreateProcedureCommand;
import org.teiid.query.sql.symbol.GroupSymbol;
import org.teiid.query.unittest.RealMetadataFactory;
import org.teiid.query.validator.Validator;
import org.teiid.query.validator.ValidatorFailure;
import org.teiid.query.validator.ValidatorReport;

@SuppressWarnings("nls")
public class TestProcedurePlanner {

	// ################ getReplacementClause tests ################### 

	private ProcessorPlan helpPlanProcedure(String userQuery,
                                            String procedure,
                                            TriggerEvent procedureType) throws TeiidComponentException,
                                                                 QueryMetadataException, TeiidProcessingException {
        QueryMetadataInterface metadata = RealMetadataFactory.exampleUpdateProc(procedureType, procedure);

        QueryParser parser = QueryParser.getQueryParser();
        Command userCommand = userQuery != null ? parser.parseCommand(userQuery) : parser.parseCommand(procedure);
        
        if (userCommand instanceof CreateProcedureCommand) {
        	GroupSymbol gs = new GroupSymbol("proc");
        	gs.setMetadataID(new TempMetadataID("proc", Collections.EMPTY_LIST));
        	((CreateProcedureCommand)userCommand).setVirtualGroup(gs);
        }
        
        QueryResolver.resolveCommand(userCommand, metadata);
		ValidatorReport report = Validator.validate(userCommand, metadata);
        
        if (report.hasItems()) {
            ValidatorFailure firstFailure = report.getItems().iterator().next();
            throw new QueryValidatorException(firstFailure.getMessage());
        }
        userCommand = QueryRewriter.rewrite(userCommand, metadata, null);
        
        AnalysisRecord analysisRecord = new AnalysisRecord(false, DEBUG);
        
        try {
            return QueryOptimizer.optimizePlan(userCommand, metadata, null, new DefaultCapabilitiesFinder(), analysisRecord, null);
        } finally {
            if(DEBUG) {
                System.out.println(analysisRecord.getDebugLog());
            }
        }        
	}

    // =============================================================================
    // TESTS
    // =============================================================================
	
    // testing select into with virtual group in from clause
    @Test public void testCreateVirtualProcedure1() throws Exception  {
        String procedure = "CREATE VIRTUAL PROCEDURE  "; //$NON-NLS-1$
        procedure = procedure + "BEGIN\n"; //$NON-NLS-1$
        procedure = procedure + "SELECT e1 INTO #temptable FROM vm1.g1;\n"; //$NON-NLS-1$
        procedure = procedure + "SELECT e1 FROM #temptable;\n"; //$NON-NLS-1$
        procedure = procedure + "END\n"; //$NON-NLS-1$
        
        helpPlanProcedure(null, procedure,
                                     TriggerEvent.UPDATE);
    }  
    
    // testing select into with function in select clause
    @Test public void testCreateVirtualProcedure2() throws Exception {
        String procedure = "CREATE VIRTUAL PROCEDURE  "; //$NON-NLS-1$
        procedure = procedure + "BEGIN\n"; //$NON-NLS-1$
        procedure = procedure + "SELECT e1, convert(e2, string) INTO #temptable FROM vm1.g1;\n"; //$NON-NLS-1$
        procedure = procedure + "SELECT e1 FROM #temptable;\n"; //$NON-NLS-1$
        procedure = procedure + "END\n"; //$NON-NLS-1$
        
        helpPlanProcedure(null, procedure,
                                     TriggerEvent.UPDATE);
    }      
    
    // testing select into with function in select clause
    @Test public void testCreateVirtualProcedure3() throws Exception {
        String procedure = "CREATE VIRTUAL PROCEDURE  "; //$NON-NLS-1$
        procedure = procedure + "BEGIN\n"; //$NON-NLS-1$
        procedure = procedure + "SELECT e1, convert(e2, string) as a1 INTO #temptable FROM vm1.g1;\n"; //$NON-NLS-1$
        procedure = procedure + "SELECT e1 FROM #temptable;\n"; //$NON-NLS-1$
        procedure = procedure + "END\n"; //$NON-NLS-1$
        
        helpPlanProcedure(null, procedure,
                                     TriggerEvent.UPDATE);
    }
    
    @Test public void testCase4504() throws Exception { 
        String procedure = "CREATE VIRTUAL PROCEDURE  "; //$NON-NLS-1$ 
        procedure = procedure + "BEGIN\n"; //$NON-NLS-1$ 
        procedure = procedure + "SELECT y INTO #temptable FROM (select x.e1 as y from (select convert(pm1.g1.e1, date) e1 from pm1.g1) x) z;\n"; //$NON-NLS-1$ 
        procedure = procedure + "loop on (SELECT y FROM #temptable) as mycursor\n"; //$NON-NLS-1$ 
        procedure = procedure + "BEGIN\n"; //$NON-NLS-1$ 
        procedure = procedure + "select * from #temptable;\n"; //$NON-NLS-1$ 
        procedure = procedure + "END\n"; //$NON-NLS-1$ 
        procedure = procedure + "END\n"; //$NON-NLS-1$ 
         
        helpPlanProcedure(null, procedure, 
                                     TriggerEvent.UPDATE); 
    }

    // =============================================================================
    // FRAMEWORK
    // =============================================================================

    private static boolean DEBUG = false;

}
