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

package com.metamatrix.query.optimizer.proc;

import junit.framework.TestCase;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.api.exception.query.QueryMetadataException;
import com.metamatrix.api.exception.query.QueryParserException;
import com.metamatrix.api.exception.query.QueryPlannerException;
import com.metamatrix.api.exception.query.QueryResolverException;
import com.metamatrix.api.exception.query.QueryValidatorException;
import com.metamatrix.query.analysis.AnalysisRecord;
import com.metamatrix.query.metadata.QueryMetadataInterface;
import com.metamatrix.query.optimizer.QueryOptimizer;
import com.metamatrix.query.optimizer.capabilities.DefaultCapabilitiesFinder;
import com.metamatrix.query.parser.QueryParser;
import com.metamatrix.query.processor.ProcessorPlan;
import com.metamatrix.query.resolver.QueryResolver;
import com.metamatrix.query.rewriter.QueryRewriter;
import com.metamatrix.query.sql.lang.Command;
import com.metamatrix.query.unittest.FakeMetadataFactory;
import com.metamatrix.query.unittest.FakeMetadataObject;
import com.metamatrix.query.validator.Validator;
import com.metamatrix.query.validator.ValidatorFailure;
import com.metamatrix.query.validator.ValidatorReport;

public class TestProcedurePlanner extends TestCase {

	/**
	 * Constructor for TestGenerateCanonical.
	 * @param arg0
	 */
	public TestProcedurePlanner(String arg0) {
		super(arg0);
	}

	// ################ getReplacementClause tests ################### 

    private ProcessorPlan helpPlanProcedure(String procedure, String procedureType) throws QueryParserException, QueryResolverException, QueryValidatorException, QueryPlannerException, QueryMetadataException, MetaMatrixComponentException {
        return helpPlanProcedure(null, procedure, procedureType);
    }
    
	private ProcessorPlan helpPlanProcedure(String userQuery,
                                            String procedure,
                                            String procedureType) throws QueryParserException,
                                                                 QueryResolverException,
                                                                 MetaMatrixComponentException,
                                                                 QueryValidatorException,
                                                                 QueryPlannerException,
                                                                 QueryMetadataException {
        QueryMetadataInterface metadata = FakeMetadataFactory.exampleUpdateProc(procedureType, procedure);

        QueryParser parser = new QueryParser();
        Command userCommand = userQuery != null ? parser.parseCommand(userQuery) : parser.parseCommand(procedure);
        QueryResolver.resolveCommand(userCommand, metadata);
		ValidatorReport report = Validator.validate(userCommand, metadata);
        
        if (report.hasItems()) {
            ValidatorFailure firstFailure = (ValidatorFailure) report.getItems().iterator().next();
            throw new QueryValidatorException(firstFailure.getMessage());
        }
        QueryRewriter.rewrite(userCommand, null, metadata, null);
        
        AnalysisRecord analysisRecord = new AnalysisRecord(false, false, DEBUG);
        
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
	
    public void testCreateUpdateProcedure1() throws Exception {
        String procedure = "CREATE PROCEDURE "; //$NON-NLS-1$
        procedure = procedure + "BEGIN\n"; //$NON-NLS-1$
        procedure = procedure + "Declare String var1;\n";         //$NON-NLS-1$
        procedure = procedure + "if(var1 = 'x' or var1 = 'y')\n";         //$NON-NLS-1$
        procedure = procedure + "BEGIN\n";         //$NON-NLS-1$
        procedure = procedure + "Select pm1.g1.e2, Input.e2, CHANGING.e2, CHANGING.e1 from pm1.g1;\n"; //$NON-NLS-1$
        procedure = procedure + "END\n"; //$NON-NLS-1$
        procedure = procedure + "ROWS_UPDATED = 1;\n";         //$NON-NLS-1$
        procedure = procedure + "END\n";         //$NON-NLS-1$

        String userUpdateStr = "INSERT into vm1.g1 (e1) values('x')"; //$NON-NLS-1$
        
		helpPlanProcedure(userUpdateStr, procedure,
									 FakeMetadataObject.Props.INSERT_PROCEDURE);
    }
    
	// special variable CHANGING used with declared variable
    public void testCreateUpdateProcedure2() throws Exception {
        String procedure = "CREATE PROCEDURE  "; //$NON-NLS-1$
        procedure = procedure + "BEGIN\n"; //$NON-NLS-1$
        procedure = procedure + "DECLARE integer var1;\n"; //$NON-NLS-1$
        procedure = procedure + "if(CHANGING.e1 = 'true')\n";         //$NON-NLS-1$
        procedure = procedure + "BEGIN\n";         //$NON-NLS-1$
        procedure = procedure + "Select pm1.g1.e2, Input.e2 from pm1.g1;\n"; //$NON-NLS-1$
        procedure = procedure + "UPDATE pm1.g1 SET pm1.g1.e1 = INPUT.e1, pm1.g1.e2 = INPUT.e2;\n"; //$NON-NLS-1$
        procedure = procedure + "END\n"; //$NON-NLS-1$
        procedure = procedure + "ROWS_UPDATED = 1;\n";         //$NON-NLS-1$
        procedure = procedure + "END\n";         //$NON-NLS-1$

        String userUpdateStr = "UPDATE vm1.g1 SET e1='x'"; //$NON-NLS-1$
        
		helpPlanProcedure(userUpdateStr, procedure,
									 FakeMetadataObject.Props.UPDATE_PROCEDURE);
    }
    
	// special variable CHANGING and INPUT used in conpound criteria
    public void testCreateUpdateProcedure3() throws Exception {
        String procedure = "CREATE PROCEDURE  "; //$NON-NLS-1$
        procedure = procedure + "BEGIN\n"; //$NON-NLS-1$
        procedure = procedure + "DECLARE integer var1;\n"; //$NON-NLS-1$
        procedure = procedure + "if(CHANGING.e1=\"false\" and INPUT.e1=1)\n";         //$NON-NLS-1$
        procedure = procedure + "BEGIN\n";         //$NON-NLS-1$
        procedure = procedure + "Select pm1.g1.e2, Input.e2 from pm1.g1;\n"; //$NON-NLS-1$
        procedure = procedure + "UPDATE pm1.g1 SET pm1.g1.e1 = INPUT.e1, pm1.g1.e2 = INPUT.e2;\n"; //$NON-NLS-1$
        procedure = procedure + "END\n"; //$NON-NLS-1$
        procedure = procedure + "ROWS_UPDATED = 1;\n";         //$NON-NLS-1$
        procedure = procedure + "END\n";         //$NON-NLS-1$

        String userUpdateStr = "UPDATE vm1.g1 SET e1='x'"; //$NON-NLS-1$
        
		helpPlanProcedure(userUpdateStr, procedure,
									 FakeMetadataObject.Props.UPDATE_PROCEDURE);
    }
    
	// special variable CHANGING and INPUT used in conpound criteria, with declared variables
    public void testCreateUpdateProcedure4() throws Exception {
        String procedure = "CREATE PROCEDURE  "; //$NON-NLS-1$
        procedure = procedure + "BEGIN\n"; //$NON-NLS-1$
        procedure = procedure + "DECLARE integer var1;\n"; //$NON-NLS-1$
        procedure = procedure + "if(CHANGING.e4 ='true' and INPUT.e2=1 or var1 < 30)\n";         //$NON-NLS-1$
        procedure = procedure + "BEGIN\n";         //$NON-NLS-1$
        procedure = procedure + "Select pm1.g1.e2, Input.e2 from pm1.g1;\n"; //$NON-NLS-1$
        procedure = procedure + "UPDATE pm1.g1 SET pm1.g1.e1 = INPUT.e1, pm1.g1.e2 = INPUT.e2;\n"; //$NON-NLS-1$
        procedure = procedure + "END\n"; //$NON-NLS-1$
        procedure = procedure + "ROWS_UPDATED = 1;\n";         //$NON-NLS-1$
        procedure = procedure + "END\n";         //$NON-NLS-1$

        String userUpdateStr = "UPDATE vm1.g1 SET e1='x'"; //$NON-NLS-1$
        
		helpPlanProcedure(userUpdateStr, procedure,
									 FakeMetadataObject.Props.UPDATE_PROCEDURE);
    }
    
	// virtual group elements used in procedure(HAS CRITERIA)
    public void testCreateUpdateProcedure5() throws Exception {
        String procedure = "CREATE PROCEDURE  "; //$NON-NLS-1$
        procedure = procedure + "BEGIN\n"; //$NON-NLS-1$
        procedure = procedure + "DECLARE integer var1;\n"; //$NON-NLS-1$
        procedure = procedure + "ROWS_UPDATED = Select pm1.g1.e2 from pm1.g1 where HAS CRITERIA ON (vm1.g1.e1, vm1.g1.e1);\n"; //$NON-NLS-1$
        procedure = procedure + "UPDATE pm1.g1 SET pm1.g1.e1 = 'x', pm1.g1.e2 = var1;\n"; //$NON-NLS-1$
        procedure = procedure + "END\n"; //$NON-NLS-1$

        String userUpdateStr = "UPDATE vm1.g1 SET e1='x'"; //$NON-NLS-1$
        
		helpPlanProcedure(userUpdateStr, procedure,
									 FakeMetadataObject.Props.UPDATE_PROCEDURE);
    }
    
	// virtual group elements used in procedure in if statement(HAS CRITERIA)
    public void testCreateUpdateProcedure6() throws Exception {
        String procedure = "CREATE PROCEDURE  "; //$NON-NLS-1$
        procedure = procedure + "BEGIN\n"; //$NON-NLS-1$
        procedure = procedure + "DECLARE integer var1;\n"; //$NON-NLS-1$
        procedure = procedure + "if(HAS CRITERIA ON (vm1.g1.e1, vm1.g1.e1))\n";                 //$NON-NLS-1$
        procedure = procedure + "BEGIN\n";         //$NON-NLS-1$
        procedure = procedure + "ROWS_UPDATED = Select pm1.g1.e2 from pm1.g1 where HAS CRITERIA ON (vm1.g1.e1, vm1.g1.e1);\n"; //$NON-NLS-1$
        procedure = procedure + "UPDATE pm1.g1 SET pm1.g1.e1 = 'x', pm1.g1.e2 = var1;\n"; //$NON-NLS-1$
        procedure = procedure + "END\n"; //$NON-NLS-1$
        procedure = procedure + "END\n";         //$NON-NLS-1$

        String userUpdateStr = "UPDATE vm1.g1 SET e1='x'"; //$NON-NLS-1$
        
		helpPlanProcedure(userUpdateStr, procedure,
									 FakeMetadataObject.Props.UPDATE_PROCEDURE);
    }
    
	// testing rows updated incremented, Input and assignment statements
    public void testCreateUpdateProcedure7() throws Exception {
        String procedure = "CREATE PROCEDURE  "; //$NON-NLS-1$
        procedure = procedure + "BEGIN\n"; //$NON-NLS-1$
        procedure = procedure + "Select pm1.g1.e2 from pm1.g1;\n"; //$NON-NLS-1$
        procedure = procedure + "UPDATE pm1.g1 SET pm1.g1.e1 = INPUT.e1, pm1.g1.e2 = INPUT.e2;\n"; //$NON-NLS-1$
        procedure = procedure + "ROWS_UPDATED = 1;\n";         //$NON-NLS-1$
        procedure = procedure + "END\n"; //$NON-NLS-1$

        String userUpdateStr = "UPDATE vm1.g1 SET e2=40"; //$NON-NLS-1$
        
		helpPlanProcedure(userUpdateStr, procedure,
									 FakeMetadataObject.Props.UPDATE_PROCEDURE);
    }      
    
    // testing select into with virtual group in from clause
    public void testCreateVirtualProcedure1() throws Exception  {
        String procedure = "CREATE VIRTUAL PROCEDURE  "; //$NON-NLS-1$
        procedure = procedure + "BEGIN\n"; //$NON-NLS-1$
        procedure = procedure + "SELECT e1 INTO #temptable FROM vm1.g1;\n"; //$NON-NLS-1$
        procedure = procedure + "SELECT e1 FROM #temptable;\n"; //$NON-NLS-1$
        procedure = procedure + "END\n"; //$NON-NLS-1$
        
        helpPlanProcedure(procedure,
                                     FakeMetadataObject.Props.UPDATE_PROCEDURE);
    }  
    
    // testing select into with function in select clause
    public void testCreateVirtualProcedure2() throws Exception {
        String procedure = "CREATE VIRTUAL PROCEDURE  "; //$NON-NLS-1$
        procedure = procedure + "BEGIN\n"; //$NON-NLS-1$
        procedure = procedure + "SELECT e1, convert(e2, string) INTO #temptable FROM vm1.g1;\n"; //$NON-NLS-1$
        procedure = procedure + "SELECT e1 FROM #temptable;\n"; //$NON-NLS-1$
        procedure = procedure + "END\n"; //$NON-NLS-1$
        
        helpPlanProcedure(procedure,
                                     FakeMetadataObject.Props.UPDATE_PROCEDURE);
    }      
    
    // testing select into with function in select clause
    public void testCreateVirtualProcedure3() throws Exception {
        String procedure = "CREATE VIRTUAL PROCEDURE  "; //$NON-NLS-1$
        procedure = procedure + "BEGIN\n"; //$NON-NLS-1$
        procedure = procedure + "SELECT e1, convert(e2, string) as a1 INTO #temptable FROM vm1.g1;\n"; //$NON-NLS-1$
        procedure = procedure + "SELECT e1 FROM #temptable;\n"; //$NON-NLS-1$
        procedure = procedure + "END\n"; //$NON-NLS-1$
        
        helpPlanProcedure(procedure,
                                     FakeMetadataObject.Props.UPDATE_PROCEDURE);
    }
    
    public void testCase4504() throws Exception { 
        String procedure = "CREATE VIRTUAL PROCEDURE  "; //$NON-NLS-1$ 
        procedure = procedure + "BEGIN\n"; //$NON-NLS-1$ 
        procedure = procedure + "SELECT y INTO #temptable FROM (select x.e1 as y from (select convert(pm1.g1.e1, date) e1 from pm1.g1) x) z;\n"; //$NON-NLS-1$ 
        procedure = procedure + "loop on (SELECT y FROM #temptable) as mycursor\n"; //$NON-NLS-1$ 
        procedure = procedure + "BEGIN\n"; //$NON-NLS-1$ 
        procedure = procedure + "select * from #temptable;\n"; //$NON-NLS-1$ 
        procedure = procedure + "END\n"; //$NON-NLS-1$ 
        procedure = procedure + "END\n"; //$NON-NLS-1$ 
         
        helpPlanProcedure(procedure, 
                                     FakeMetadataObject.Props.UPDATE_PROCEDURE); 
    }

    // =============================================================================
    // FRAMEWORK
    // =============================================================================

    private static boolean DEBUG = false;

}
