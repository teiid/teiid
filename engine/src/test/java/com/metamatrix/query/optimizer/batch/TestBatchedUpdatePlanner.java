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

package com.metamatrix.query.optimizer.batch;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;

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
import com.metamatrix.query.optimizer.capabilities.CapabilitiesFinder;
import com.metamatrix.query.optimizer.capabilities.SourceCapabilities;
import com.metamatrix.query.parser.QueryParser;
import com.metamatrix.query.processor.ProcessorPlan;
import com.metamatrix.query.processor.batch.BatchedUpdatePlan;
import com.metamatrix.query.processor.relational.AccessNode;
import com.metamatrix.query.processor.relational.BatchedUpdateNode;
import com.metamatrix.query.processor.relational.ProjectNode;
import com.metamatrix.query.processor.relational.RelationalNode;
import com.metamatrix.query.processor.relational.RelationalPlan;
import com.metamatrix.query.resolver.QueryResolver;
import com.metamatrix.query.rewriter.QueryRewriter;
import com.metamatrix.query.sql.lang.BatchedUpdateCommand;
import com.metamatrix.query.sql.lang.Command;
import com.metamatrix.query.unittest.FakeMetadataFactory;
import com.metamatrix.query.validator.Validator;
import com.metamatrix.query.validator.ValidatorReport;


/** 
 * @since 4.2
 */
public class TestBatchedUpdatePlanner extends TestCase {

    public TestBatchedUpdatePlanner(String name) {
        super(name);
    }
    
    public static List helpGetCommands(String[] sql, QueryMetadataInterface md) throws QueryParserException, QueryResolverException, MetaMatrixComponentException, QueryValidatorException  { 
        if(DEBUG) System.out.println("\n####################################\n" + sql);  //$NON-NLS-1$
        List commands = new ArrayList(sql.length);
        for (int i = 0; i < sql.length; i++) {
            Command command = QueryParser.getQueryParser().parseCommand(sql[i]);
            QueryResolver.resolveCommand(command, md);
            ValidatorReport repo =  Validator.validate(command, md);
            Collection failures = new ArrayList();
            repo.collectInvalidObjects(failures);
            if (failures.size() > 0){
                fail("Exception during validation (" + repo); //$NON-NLS-1$
            }                    
            
            command = QueryRewriter.rewrite(command, null, md, null);
            commands.add(command);
        }
        return commands;
    }
    
    private BatchedUpdateCommand helpGetCommand(String[] sql, QueryMetadataInterface md) throws QueryParserException, QueryResolverException, QueryValidatorException, MetaMatrixComponentException { 
        BatchedUpdateCommand command = new BatchedUpdateCommand(helpGetCommands(sql, md));
        return command;
    }
    
    private BatchedUpdatePlan helpPlanCommand(Command command, QueryMetadataInterface md, CapabilitiesFinder capFinder, boolean shouldSucceed) throws QueryPlannerException, QueryMetadataException, MetaMatrixComponentException { 
        
        // plan
        ProcessorPlan plan = null;
        AnalysisRecord analysisRecord = new AnalysisRecord(false, false, DEBUG);
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
        } catch (MetaMatrixComponentException e) {
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
        CapabilitiesFinder finder = new CapabilitiesFinder() {
            private SourceCapabilities caps = new FakeCapabilities(true);
            public SourceCapabilities findCapabilities(String modelName) throws MetaMatrixComponentException {
                return caps;
            }
        };
        return finder;
    }

    private BatchedUpdatePlan helpPlan(String[] sql, QueryMetadataInterface md) throws QueryParserException, QueryResolverException, QueryValidatorException, MetaMatrixComponentException, QueryPlannerException, QueryMetadataException {
        return helpPlan(sql, md, getGenericFinder(), true);
    }
    
    private BatchedUpdatePlan helpPlan(String[] sql, QueryMetadataInterface md, CapabilitiesFinder capFinder, boolean shouldSucceed) throws QueryParserException, QueryResolverException, QueryValidatorException, MetaMatrixComponentException, QueryPlannerException, QueryMetadataException {
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
    
    private void helpTestPlanner(String[] sql, boolean[] expectedBatching) throws QueryParserException, QueryResolverException, QueryValidatorException, QueryPlannerException, QueryMetadataException, MetaMatrixComponentException {
        BatchedUpdatePlan plan = helpPlan(sql, FakeMetadataFactory.example1Cached());
        List plans = plan.getUpdatePlans();
        assertEquals("Number of child plans did not match expected", expectedBatching.length, plans.size()); //$NON-NLS-1$
        for (int i = 0; i < expectedBatching.length; i++) {
            helpAssertIsBatchedPlan((RelationalPlan)plans.get(i), expectedBatching[i]);
        }
    }    
    
    private void helpTestPlanner(String[] sql, boolean[] expectedBatching, CapabilitiesFinder capFinder) throws QueryParserException, QueryResolverException, QueryValidatorException, QueryPlannerException, QueryMetadataException, MetaMatrixComponentException {
        BatchedUpdatePlan plan = helpPlan(sql, FakeMetadataFactory.example1Cached(), capFinder, true);
        List plans = plan.getUpdatePlans();
        assertEquals("Number of child plans did not match expected", expectedBatching.length, plans.size()); //$NON-NLS-1$
        for (int i = 0; i < expectedBatching.length; i++) {
            helpAssertIsBatchedPlan((RelationalPlan)plans.get(i), expectedBatching[i]);
        }
    }    
    
    public void testPlannerAllCommandsBatched() throws Exception {
        String[] sql = {"INSERT INTO pm1.g1 (e1, e2, e3, e4) values ('string1', 1, {b'true'}, 1.0)", //$NON-NLS-1$
                        "INSERT INTO pm1.g2 (e1, e2, e3, e4) values ('string1', 1, {b'true'}, 1.0)", //$NON-NLS-1$
                        "DELETE FROM pm1.g1 WHERE e2 > 5000", //$NON-NLS-1$
                        "UPDATE pm1.g1 set e2 = -1 WHERE e2 = 4999" //$NON-NLS-1$
        };
        boolean[] expectedBatching = {true};
        helpTestPlanner(sql, expectedBatching);
    }
    
    public void testPlannerNoCommandsBatched() throws Exception {
        String[] sql = {"INSERT INTO pm1.g1 (e1, e2, e3, e4) values ('string1', 1, {b'true'}, 1.0)", //$NON-NLS-1$
                        "INSERT INTO pm1.g2 (e1, e2, e3, e4) values ('string1', 1, {b'true'}, 1.0)", //$NON-NLS-1$
                        "DELETE FROM pm1.g1 WHERE e2 > 5000", //$NON-NLS-1$
                        "UPDATE pm1.g1 set e2 = -1 WHERE e2 = 4999" //$NON-NLS-1$
        };
        FakeFinder finder = new FakeFinder();
        finder.setCapabilities("pm1", new FakeCapabilities(false)); //$NON-NLS-1$
        boolean[] expectedBatching = {false, false, false, false};
        helpTestPlanner(sql, expectedBatching, finder);
    }
    
    public void testPlannerSomeCommandsBatched() throws Exception {
        String[] sql = {"INSERT INTO pm1.g1 (e1, e2, e3, e4) values ('string1', 1, {b'true'}, 1.0)", //$NON-NLS-1$
                        "INSERT INTO pm1.g2 (e1, e2, e3, e4) values ('string1', 1, {b'true'}, 1.0)", //$NON-NLS-1$
                        "DELETE FROM pm2.g1 WHERE e2 > 5000", //$NON-NLS-1$
                        "INSERT INTO pm2.g1 (e1, e2, e3, e4) values ('5000', 5000, {b'true'}, 5000.0)", //$NON-NLS-1$
                        "UPDATE pm2.g1 set e2 = -1 WHERE e2 = 4999", //$NON-NLS-1$
                        "DELETE FROM pm1.g2 WHERE e2 = 50" //$NON-NLS-1$
        };
        FakeFinder finder = new FakeFinder();
        finder.setCapabilities("pm1", new FakeCapabilities(false)); //$NON-NLS-1$
        finder.setCapabilities("pm2", new FakeCapabilities(true)); //$NON-NLS-1$
        boolean[] expectedBatching = {false, false, true, false};
        helpTestPlanner(sql, expectedBatching, finder);
    }

    private static final class FakeCapabilities implements SourceCapabilities {
        private boolean supportsBatching = false;
        private FakeCapabilities(boolean supportsBatching) {
            this.supportsBatching = supportsBatching;
        }
        public Scope getScope() {return null;}
        public boolean supportsCapability(Capability capability) {
            return !capability.equals(Capability.BATCHED_UPDATES) || supportsBatching;
        }
        public boolean supportsFunction(String functionName) {return false;}
        // since 4.4
        public Object getSourceProperty(Capability propertyName) {return null;}
        
    }
    private static final class FakeFinder implements CapabilitiesFinder {
        private HashMap caps = new HashMap();
        private void setCapabilities(String modelName, SourceCapabilities cap) {
            caps.put(modelName, cap);
        }
        public SourceCapabilities findCapabilities(String modelName) throws MetaMatrixComponentException {
            return (SourceCapabilities)caps.get(modelName);
        }
}
    private static final boolean DEBUG = false;
    
}
