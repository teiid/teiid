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

package org.teiid.query.optimizer;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.junit.Test;
import org.teiid.api.exception.query.QueryMetadataException;
import org.teiid.api.exception.query.QueryPlannerException;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidProcessingException;
import org.teiid.query.analysis.AnalysisRecord;
import org.teiid.query.metadata.QueryMetadataInterface;
import org.teiid.query.optimizer.capabilities.BasicSourceCapabilities;
import org.teiid.query.optimizer.capabilities.CapabilitiesFinder;
import org.teiid.query.optimizer.capabilities.DefaultCapabilitiesFinder;
import org.teiid.query.optimizer.capabilities.FakeCapabilitiesFinder;
import org.teiid.query.parser.QueryParser;
import org.teiid.query.processor.BatchedUpdatePlan;
import org.teiid.query.processor.ProcessorPlan;
import org.teiid.query.processor.relational.AccessNode;
import org.teiid.query.processor.relational.BatchedUpdateNode;
import org.teiid.query.processor.relational.ProjectNode;
import org.teiid.query.processor.relational.RelationalNode;
import org.teiid.query.processor.relational.RelationalPlan;
import org.teiid.query.resolver.QueryResolver;
import org.teiid.query.rewriter.QueryRewriter;
import org.teiid.query.sql.LanguageObject;
import org.teiid.query.sql.lang.BatchedUpdateCommand;
import org.teiid.query.sql.lang.Command;
import org.teiid.query.unittest.RealMetadataFactory;
import org.teiid.query.validator.Validator;
import org.teiid.query.validator.ValidatorReport;



/**
 * @since 4.2
 */
public class TestBatchedUpdatePlanner {

    public static List<Command> helpGetCommands(String[] sql, QueryMetadataInterface md) throws TeiidComponentException, TeiidProcessingException  {
        if(DEBUG) System.out.println("\n####################################\n" + sql);  //$NON-NLS-1$
        List<Command> commands = new ArrayList<Command>(sql.length);
        for (int i = 0; i < sql.length; i++) {
            Command command = QueryParser.getQueryParser().parseCommand(sql[i]);
            QueryResolver.resolveCommand(command, md);
            ValidatorReport repo =  Validator.validate(command, md);
            Collection<LanguageObject> failures = new ArrayList<LanguageObject>();
            repo.collectInvalidObjects(failures);
            if (failures.size() > 0){
                fail("Exception during validation (" + repo); //$NON-NLS-1$
            }

            command = QueryRewriter.rewrite(command, md, null);
            commands.add(command);
        }
        return commands;
    }

    private BatchedUpdateCommand helpGetCommand(String[] sql, QueryMetadataInterface md) throws TeiidComponentException, TeiidProcessingException {
        BatchedUpdateCommand command = new BatchedUpdateCommand(helpGetCommands(sql, md));
        return command;
    }

    private BatchedUpdatePlan helpPlanCommand(Command command, QueryMetadataInterface md, CapabilitiesFinder capFinder, boolean shouldSucceed) throws QueryPlannerException, QueryMetadataException, TeiidComponentException {

        // plan
        ProcessorPlan plan = null;
        AnalysisRecord analysisRecord = new AnalysisRecord(false, DEBUG);
        if (shouldSucceed) {
            try {
                //do planning
                plan = QueryOptimizer.optimizePlan(command, md, null, capFinder, analysisRecord, null);

            } finally {
                if(DEBUG) {
                    System.out.println(analysisRecord.getDebugLog());
                }
            }
            return (BatchedUpdatePlan)plan;
        }
        Exception exception = null;
        try {
            //do planning
            QueryOptimizer.optimizePlan(command, md, null, capFinder, analysisRecord, null);

        } catch (QueryPlannerException e) {
            exception = e;
        } catch (TeiidComponentException e) {
            exception = e;
        } finally {
            if(DEBUG) {
                System.out.println(analysisRecord.getDebugLog());
            }
        }
        assertNotNull("Expected exception but did not get one.", exception); //$NON-NLS-1$
        return null;
    }

    public static CapabilitiesFinder getGenericFinder() {
        return new DefaultCapabilitiesFinder(new FakeCapabilities(true));
    }

    private BatchedUpdatePlan helpPlan(String[] sql, QueryMetadataInterface md) throws TeiidComponentException, QueryMetadataException, TeiidProcessingException {
        return helpPlan(sql, md, getGenericFinder(), true);
    }

    private BatchedUpdatePlan helpPlan(String[] sql, QueryMetadataInterface md, CapabilitiesFinder capFinder, boolean shouldSucceed) throws TeiidComponentException, QueryMetadataException, TeiidProcessingException {
        Command command = helpGetCommand(sql, md);

        if (capFinder == null){
            capFinder = getGenericFinder();
        }

        return helpPlanCommand(command, md, capFinder, shouldSucceed);
    }

    private void helpAssertIsBatchedPlan(RelationalPlan plan, boolean isBatchedPlan) {
        RelationalNode node = plan.getRootNode();
        if (node instanceof ProjectNode) {
            node = node.getChildren()[0];
        }
        if (isBatchedPlan) {
            assertTrue("Plan should have been a batched", node instanceof BatchedUpdateNode); //$NON-NLS-1$
        } else {
            assertTrue("Plan should not have been batched.", node instanceof AccessNode); //$NON-NLS-1$
        }
    }

    private void helpTestPlanner(String[] sql, boolean[] expectedBatching) throws QueryMetadataException, TeiidComponentException, TeiidProcessingException {
        BatchedUpdatePlan plan = helpPlan(sql, RealMetadataFactory.example1Cached());
        List plans = plan.getUpdatePlans();
        assertEquals("Number of child plans did not match expected", expectedBatching.length, plans.size()); //$NON-NLS-1$
        for (int i = 0; i < expectedBatching.length; i++) {
            helpAssertIsBatchedPlan((RelationalPlan)plans.get(i), expectedBatching[i]);
        }
    }

    private void helpTestPlanner(String[] sql, boolean[] expectedBatching, CapabilitiesFinder capFinder) throws QueryMetadataException, TeiidComponentException, TeiidProcessingException {
        BatchedUpdatePlan plan = helpPlan(sql, RealMetadataFactory.example1Cached(), capFinder, true);
        List plans = plan.getUpdatePlans();
        assertEquals("Number of child plans did not match expected", expectedBatching.length, plans.size()); //$NON-NLS-1$
        for (int i = 0; i < expectedBatching.length; i++) {
            helpAssertIsBatchedPlan((RelationalPlan)plans.get(i), expectedBatching[i]);
        }
    }

    @Test public void testPlannerAllCommandsBatched() throws Exception {
        String[] sql = {"INSERT INTO pm1.g1 (e1, e2, e3, e4) values ('string1', 1, {b'true'}, 1.0)", //$NON-NLS-1$
                        "INSERT INTO pm1.g2 (e1, e2, e3, e4) values ('string1', 1, {b'true'}, 1.0)", //$NON-NLS-1$
                        "DELETE FROM pm1.g1 WHERE e2 > 5000", //$NON-NLS-1$
                        "UPDATE pm1.g1 set e2 = -1 WHERE e2 = 4999" //$NON-NLS-1$
        };
        boolean[] expectedBatching = {true};
        helpTestPlanner(sql, expectedBatching);
    }

    @Test public void testPlannerNoCommandsBatched() throws Exception {
        String[] sql = {"INSERT INTO pm1.g1 (e1, e2, e3, e4) values ('string1', 1, {b'true'}, 1.0)", //$NON-NLS-1$
                        "INSERT INTO pm1.g2 (e1, e2, e3, e4) values ('string1', 1, {b'true'}, 1.0)", //$NON-NLS-1$
                        "DELETE FROM pm1.g1 WHERE e2 > 5000", //$NON-NLS-1$
                        "UPDATE pm1.g1 set e2 = -1 WHERE e2 = 4999" //$NON-NLS-1$
        };
        FakeCapabilitiesFinder finder = new FakeCapabilitiesFinder();
        finder.addCapabilities("pm1", new FakeCapabilities(false)); //$NON-NLS-1$
        boolean[] expectedBatching = {false, false, false, false};
        helpTestPlanner(sql, expectedBatching, finder);
    }

    @Test public void testPlannerSomeCommandsBatched() throws Exception {
        String[] sql = {"INSERT INTO pm1.g1 (e1, e2, e3, e4) values ('string1', 1, {b'true'}, 1.0)", //$NON-NLS-1$
                        "INSERT INTO pm1.g2 (e1, e2, e3, e4) values ('string1', 1, {b'true'}, 1.0)", //$NON-NLS-1$
                        "DELETE FROM pm2.g1 WHERE e2 > 5000", //$NON-NLS-1$
                        "INSERT INTO pm2.g1 (e1, e2, e3, e4) values ('5000', 5000, {b'true'}, 5000.0)", //$NON-NLS-1$
                        "UPDATE pm2.g1 set e2 = -1 WHERE e2 = 4999", //$NON-NLS-1$
                        "DELETE FROM pm1.g2 WHERE e2 = 50" //$NON-NLS-1$
        };
        FakeCapabilitiesFinder finder = new FakeCapabilitiesFinder();
        finder.addCapabilities("pm1", new FakeCapabilities(false)); //$NON-NLS-1$
        finder.addCapabilities("pm2", new FakeCapabilities(true)); //$NON-NLS-1$
        boolean[] expectedBatching = {false, false, true, false};
        helpTestPlanner(sql, expectedBatching, finder);
    }

    private static final class FakeCapabilities extends BasicSourceCapabilities {
        private boolean supportsBatching = false;
        private FakeCapabilities(boolean supportsBatching) {
            this.supportsBatching = supportsBatching;
        }
        public boolean supportsCapability(Capability capability) {
            return !capability.equals(Capability.BATCHED_UPDATES) || supportsBatching;
        }
    }

    private static final boolean DEBUG = false;

}
