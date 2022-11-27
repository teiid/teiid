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

package org.teiid.query.optimizer.relational.rules;

import static org.junit.Assert.*;

import org.junit.Test;
import org.teiid.api.exception.query.QueryMetadataException;
import org.teiid.api.exception.query.QueryPlannerException;
import org.teiid.core.TeiidComponentException;
import org.teiid.query.analysis.AnalysisRecord;
import org.teiid.query.metadata.QueryMetadataInterface;
import org.teiid.query.optimizer.TestOptimizer;
import org.teiid.query.optimizer.capabilities.CapabilitiesFinder;
import org.teiid.query.optimizer.capabilities.DefaultCapabilitiesFinder;
import org.teiid.query.optimizer.relational.OptimizerRule;
import org.teiid.query.optimizer.relational.RelationalPlanner;
import org.teiid.query.optimizer.relational.RuleStack;
import org.teiid.query.optimizer.relational.plantree.PlanNode;
import org.teiid.query.parser.QueryParser;
import org.teiid.query.resolver.QueryResolver;
import org.teiid.query.sql.lang.Command;
import org.teiid.query.unittest.RealMetadataFactory;
import org.teiid.query.util.CommandContext;


/**
 * Tests Access Patterns
 */
public class TestRuleAccessPatternValidation {

    private static final QueryMetadataInterface METADATA = RealMetadataFactory.example1Cached();

    private static final boolean DEBUG = false;

    private static CapabilitiesFinder FINDER = new DefaultCapabilitiesFinder(TestOptimizer.getTypicalCapabilities());

    /**
     * @param command the query to be turned into a test query plan
     */
    private void helpTestAccessPatternValidation(String command) throws Exception {
        PlanNode node = this.helpPlan(command);

        if(DEBUG) {
            System.out.println("\nfinal plan node:\n"+node); //$NON-NLS-1$
        }
    }


    /**
     * Parses and resolves the command, creates a canonical relational plan,
     * and runs some of the optimizer rules, ending with the
     * RuleChooseAccessPattern.
     * @param command String command to parse, resolve and use for planning
     * @return the root PlanNode of the query plan
     */
    private PlanNode helpPlan(String command) throws Exception {
        Command query = QueryParser.getQueryParser().parseCommand(command);
        QueryResolver.resolveCommand(query, METADATA);

        //Generate canonical plan
        RelationalPlanner p = new RelationalPlanner();
        p.initialize(query, null, METADATA, FINDER, null, new CommandContext());
        PlanNode planNode = p.generatePlan(query);
        RelationalPlanner planner = new RelationalPlanner();
        final RuleStack rules = planner.buildRules();

        PlanNode testPlan = helpExecuteRules(rules, planNode, METADATA, DEBUG);

        return testPlan;
    }

    /**
     * Simulate execution of the QueryOptimizer rules stack
     */
    private static PlanNode helpExecuteRules(RuleStack rules, PlanNode plan, QueryMetadataInterface metadata, boolean debug)
        throws QueryPlannerException, QueryMetadataException, TeiidComponentException {
        CommandContext context = new CommandContext();
        while(! rules.isEmpty()) {
            if(debug) {
                System.out.println("\n============================================================================"); //$NON-NLS-1$
            }
            OptimizerRule rule = rules.pop();
            if(debug) {
                System.out.println("EXECUTING " + rule); //$NON-NLS-1$
            }

            plan = rule.execute(plan, metadata, FINDER, rules, new AnalysisRecord(false, debug), context);
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
    @Test public void testInsertWithAccessPattern_Case3966() throws Exception {
        this.helpTestAccessPatternValidation( "insert into pm4.g1 (e1, e2, e3, e4) values('test', 1, convert('true', boolean) , convert('12', double) )" ); //$NON-NLS-1$
    }

    /**
     * This test demonstrates that a satisfied AP does not fail.
     * Found testing fix for 3966.
     */
    @Test public void testDeleteWithAccessPattern_Case3966() throws Exception {
        this.helpTestAccessPatternValidation( "delete from pm4.g1 where e1 = 'test' and e2 = 1" ); //$NON-NLS-1$
    }

    /**
     * This test demonstrates that unsatisfied AP fails.
     * Found testing fix for 3966.
     */
    @Test public void testDeleteWithAccessPattern_Case3966_2() throws Exception {
        try {
            this.helpTestAccessPatternValidation( "delete from pm4.g1" ); //$NON-NLS-1$
            fail("Expected QueryPlannerException, but did not get one"); //$NON-NLS-1$
        } catch (QueryPlannerException err) {
            //This SHOULD happen.
            final String msg = err.getMessage();
            final String expected = "TEIID30278 Group has an access pattern which has not been met: group(s) [pm4.g1]; access pattern(s) [Access Pattern: Unsatisfied [pm4.g1.e1] History [[pm4.g1.e1]]]"; //$NON-NLS-1$
            assertEquals("Did not fail with expected QueryPlannerException", expected, msg); //$NON-NLS-1$
        }
    }

    @Test public void testUpdateWithAccessPattern_Case3966() throws Exception {
        this.helpTestAccessPatternValidation( "update pm4.g1 set e1 = 'test1' where e1 = 'test' and e2 = 1" ); //$NON-NLS-1$
    }

    /**
     * This test demonstrates that unsatisfied AP fails.
     * Found testing fix for 3966.
     */
    @Test public void testUpdateWithAccessPattern_Case3966_2() throws Exception {
        try {
            this.helpTestAccessPatternValidation( "update pm4.g1 set e1 = 'test'" ); //$NON-NLS-1$
            fail("Expected QueryPlannerException, but did not get one"); //$NON-NLS-1$
        } catch (QueryPlannerException err) {
            //This SHOULD happen.
            final String msg = err.getMessage();
            final String expected = "TEIID30278 Group has an access pattern which has not been met: group(s) [pm4.g1]; access pattern(s) [Access Pattern: Unsatisfied [pm4.g1.e1] History [[pm4.g1.e1]]]"; //$NON-NLS-1$
            assertEquals("Did not fail with expected QueryPlannerException", expected, msg); //$NON-NLS-1$
        }
    }

    /**
     * This test demonstrates that APs are ignored for inserts through a virtual layer
     * CASE 3966
     */
    @Test public void testInsertWithAccessPattern_Case3966_VL() throws Exception {
        this.helpTestAccessPatternValidation( "insert into vm1.g37 (e1, e2, e3, e4) values('test', 1, convert('true', boolean) , convert('12', double) )" ); //$NON-NLS-1$
    }

    /**
     * This test demonstrates that a satisfied AP within a Delete
     * through a virtual layer does not fail.
     * Found testing fix for 3966.
     */
    @Test public void testDeleteWithAccessPattern_Case3966_VL() throws Exception {
        this.helpTestAccessPatternValidation( "delete from vm1.g37 where e1 = 'test' and e2 = 1" ); //$NON-NLS-1$
    }
}
