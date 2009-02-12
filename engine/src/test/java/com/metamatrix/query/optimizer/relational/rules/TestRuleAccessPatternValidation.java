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

package com.metamatrix.query.optimizer.relational.rules;

import java.util.ArrayList;
import java.util.Collection;

import junit.framework.TestCase;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.api.exception.query.QueryMetadataException;
import com.metamatrix.api.exception.query.QueryPlannerException;
import com.metamatrix.query.analysis.AnalysisRecord;
import com.metamatrix.query.metadata.QueryMetadataInterface;
import com.metamatrix.query.optimizer.TestOptimizer;
import com.metamatrix.query.optimizer.capabilities.CapabilitiesFinder;
import com.metamatrix.query.optimizer.capabilities.SourceCapabilities;
import com.metamatrix.query.optimizer.relational.GenerateCanonical;
import com.metamatrix.query.optimizer.relational.OptimizerRule;
import com.metamatrix.query.optimizer.relational.PlanHints;
import com.metamatrix.query.optimizer.relational.RelationalPlanner;
import com.metamatrix.query.optimizer.relational.RuleStack;
import com.metamatrix.query.optimizer.relational.plantree.PlanNode;
import com.metamatrix.query.parser.QueryParser;
import com.metamatrix.query.resolver.QueryResolver;
import com.metamatrix.query.sql.lang.Command;
import com.metamatrix.query.sql.visitor.GroupCollectorVisitor;
import com.metamatrix.query.unittest.FakeMetadataFacade;
import com.metamatrix.query.unittest.FakeMetadataFactory;

/**
 * Tests {@link RuleChooseAccessPattern}
 */
public class TestRuleAccessPatternValidation extends TestCase {

    private static final FakeMetadataFacade METADATA = FakeMetadataFactory.example1();

    private PlanHints planHints;
    
    private static final boolean DEBUG = false;
   
    // ################################## FRAMEWORK ################################
    
    /**
     * Constructor for TestRuleChooseAccessPattern.
     */
    public TestRuleAccessPatternValidation(String name) {
        super(name);
    }

    public void setUp() {
    	this.planHints = new PlanHints();
    }

    // ################################## TEST HELPERS ################################

	/**
	 * @param command the query to be turned into a test query plan
	 * @param expectedChosenPredicates expected criteria predicates that should
	 * be below the access node after the rule is run
	 */
	private void helpTestAccessPatternValidation(String command) throws Exception {
		Collection groups = new ArrayList();
		PlanNode node = this.helpPlan(command, groups);

        if(DEBUG) {
            System.out.println("\nfinal plan node:\n"+node); //$NON-NLS-1$
        }
	}
	

	/**
	 * Parses and resolves the command, creates a canonical relational plan,
	 * and runs some of the optimizer rules, ending with the
	 * RuleChooseAccessPattern.
	 * @param command String command to parse, resolve and use for planning
	 * @param rules empty RuleStack
	 * @param groups Collection to add parsed and resolved GroupSymbols to
	 * @return the root PlanNode of the query plan
	 */
	private PlanNode helpPlan(String command, Collection groups) throws Exception {
		Command query = QueryParser.getQueryParser().parseCommand(command);
		QueryResolver.resolveCommand(query, METADATA);
		
		//save groups for later
		GroupCollectorVisitor.getGroups(query, groups);
		
		//Generate canonical plan
		PlanNode planNode = GenerateCanonical.generatePlan(query, planHints, METADATA);

		final RuleStack rules = RelationalPlanner.buildRules(planHints);

		PlanNode testPlan = helpExecuteRules(rules, planNode, METADATA, DEBUG);
		
		return testPlan;
	}

	/**
	 * Simulate execution of the QueryOptimizer rules stack
	 */
	private static PlanNode helpExecuteRules(RuleStack rules, PlanNode plan, QueryMetadataInterface metadata, boolean debug)
		throws QueryPlannerException, QueryMetadataException, MetaMatrixComponentException {

		while(! rules.isEmpty()) {
			if(debug) {
				System.out.println("\n============================================================================"); //$NON-NLS-1$
			}
			OptimizerRule rule = rules.pop();
			if(debug) {
				System.out.println("EXECUTING " + rule); //$NON-NLS-1$
			}

            CapabilitiesFinder finder = new CapabilitiesFinder() {
                private SourceCapabilities caps = TestOptimizer.getTypicalCapabilities();
                public SourceCapabilities findCapabilities(String modelName) throws MetaMatrixComponentException {
                    return caps;
                }
            };             
            plan = rule.execute(plan, metadata, finder, rules, new AnalysisRecord(false, false, debug), null);
			if(debug) {
				System.out.println("\nAFTER: \n" + plan); //$NON-NLS-1$
			}
		}
		return plan;
	}	
	

    // ################################## ACTUAL TESTS ################################
    
    /**
     * This test demonstrates that APs are ignored for inserts
     * CASE 3966
     */
    public void testInsertWithAccessPattern_Case3966() throws Exception {
        this.helpTestAccessPatternValidation( "insert into pm4.g1 (e1, e2, e3, e4) values('test', 1, convert('true', boolean) , convert('12', double) )" ); //$NON-NLS-1$
    }
    
    /**
     * This test demonstrates that a satisfied AP does not fail.  
     * Found testing fix for 3966.
     */
    public void testDeleteWithAccessPattern_Case3966() throws Exception {
        this.helpTestAccessPatternValidation( "delete from pm4.g1 where e1 = 'test' and e2 = 1" ); //$NON-NLS-1$ 
    }
    
    /**
     * This test demonstrates that unsatisfied AP fails.  
     * Found testing fix for 3966.
     */
    public void testDeleteWithAccessPattern_Case3966_2() throws Exception {
        try {
            this.helpTestAccessPatternValidation( "delete from pm4.g1" ); //$NON-NLS-1$ 
            fail("Expected QueryPlannerException, but did not get one"); //$NON-NLS-1$
        } catch (QueryPlannerException err) {
            //This SHOULD happen.
            final String msg = err.getMessage();
            final String expected = "Group has an access pattern which has not been met: group(s) [pm4.g1]; access pattern(s) [Access Pattern: Unsatisfied [pm4.g1.e1] History [[pm4.g1.e1]]]"; //$NON-NLS-1$
            assertEquals("Did not fail with expected QueryPlannerException", expected, msg); //$NON-NLS-1$
        }
    }
    
    public void testUpdateWithAccessPattern_Case3966() throws Exception {
        this.helpTestAccessPatternValidation( "update pm4.g1 set e1 = 'test1' where e1 = 'test' and e2 = 1" ); //$NON-NLS-1$ 
    }
    
    /**
     * This test demonstrates that unsatisfied AP fails.  
     * Found testing fix for 3966.
     */
    public void testUpdateWithAccessPattern_Case3966_2() throws Exception {
        try {
            this.helpTestAccessPatternValidation( "update pm4.g1 set e1 = 'test'" ); //$NON-NLS-1$ 
            fail("Expected QueryPlannerException, but did not get one"); //$NON-NLS-1$
        } catch (QueryPlannerException err) {
            //This SHOULD happen.
            final String msg = err.getMessage();
            final String expected = "Group has an access pattern which has not been met: group(s) [pm4.g1]; access pattern(s) [Access Pattern: Unsatisfied [pm4.g1.e1] History [[pm4.g1.e1]]]"; //$NON-NLS-1$
            assertEquals("Did not fail with expected QueryPlannerException", expected, msg); //$NON-NLS-1$
        }
    }
    
    /**
     * This test demonstrates that APs are ignored for inserts through a virtual layer
     * CASE 3966
     */
    public void testInsertWithAccessPattern_Case3966_VL() throws Exception {
        this.helpTestAccessPatternValidation( "insert into vm1.g37 (e1, e2, e3, e4) values('test', 1, convert('true', boolean) , convert('12', double) )" ); //$NON-NLS-1$
    }
    
    /**
     * This test demonstrates that a satisfied AP within a Delete 
     * through a virtual layer does not fail.  
     * Found testing fix for 3966.
     */
    public void testDeleteWithAccessPattern_Case3966_VL() throws Exception {
        this.helpTestAccessPatternValidation( "delete from vm1.g37 where e1 = 'test' and e2 = 1" ); //$NON-NLS-1$ 
    }
}
