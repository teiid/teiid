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

package org.teiid.dqp.internal.process.multisource;

import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import org.junit.Test;
import org.teiid.adminapi.impl.ModelMetaData;
import org.teiid.adminapi.impl.VDBMetaData;
import org.teiid.common.buffer.TupleSource;
import org.teiid.core.id.IDGenerator;
import org.teiid.core.id.IntegerIDFactory;
import org.teiid.dqp.internal.process.DQPWorkContext;
import org.teiid.query.analysis.AnalysisRecord;
import org.teiid.query.metadata.QueryMetadataInterface;
import org.teiid.query.optimizer.QueryOptimizer;
import org.teiid.query.optimizer.TestOptimizer;
import org.teiid.query.optimizer.capabilities.CapabilitiesFinder;
import org.teiid.query.optimizer.capabilities.FakeCapabilitiesFinder;
import org.teiid.query.processor.HardcodedDataManager;
import org.teiid.query.processor.ProcessorDataManager;
import org.teiid.query.processor.ProcessorPlan;
import org.teiid.query.processor.TestProcessor;
import org.teiid.query.resolver.TestResolver;
import org.teiid.query.rewriter.QueryRewriter;
import org.teiid.query.sql.lang.Command;
import org.teiid.query.sql.symbol.ElementSymbol;
import org.teiid.query.sql.visitor.ElementCollectorVisitor;
import org.teiid.query.unittest.RealMetadataFactory;
import org.teiid.query.util.CommandContext;

/** 
 * It's important here that the MultiSourceCapabilityFinder is used since some capabilities 
 * will never be pushed to the source
 * 
 * @since 4.2
 */
public class TestMultiSourcePlanToProcessConverter {
    
    private final class MultiSourceDataManager extends HardcodedDataManager {
        
        public MultiSourceDataManager() {
            setMustRegisterCommands(false);
        }

        public TupleSource registerRequest(CommandContext context, Command command, String modelName, String connectorBindingId, int nodeID, int limit) throws org.teiid.core.TeiidComponentException {
        	assertNotNull(connectorBindingId);
        	
        	Collection<ElementSymbol> elements = ElementCollectorVisitor.getElements(command, true, true);
            
        	for (ElementSymbol symbol : elements) {
                if (symbol.getMetadataID() instanceof MultiSourceElement) {
                    fail("Query Contains a MultiSourceElement -- MultiSource expansion did not happen"); //$NON-NLS-1$
                }
            }
            return super.registerRequest(context, command, modelName, connectorBindingId, nodeID, limit);
        }
    }

    private static final boolean DEBUG = false;
    
    public void helpTestMultiSourcePlan(QueryMetadataInterface metadata, String userSql, String multiModel, int sourceCount, ProcessorDataManager dataMgr, List[] expectedResults, VDBMetaData vdb) throws Exception {
        
       DQPWorkContext dqpContext = RealMetadataFactory.buildWorkContext(metadata, vdb);
     
        Set<String> multiSourceModels = vdb.getMultiSourceModelNames();
        for (String model:multiSourceModels) {
            char sourceID = 'a';
            // by default every model has one binding associated, but for multi-source there were none assigned. 
            ModelMetaData m = vdb.getModel(model);
            int x = m.getSourceNames().size();
            for(int i=x; i<sourceCount; i++, sourceID++) {
            	 m.addSourceMapping("" + sourceID, "translator",  null); //$NON-NLS-1$ //$NON-NLS-2$
            }
        }
        
        MultiSourceMetadataWrapper wrapper = new MultiSourceMetadataWrapper(metadata, multiSourceModels); 
        AnalysisRecord analysis = new AnalysisRecord(false, DEBUG);
        
        Command command = TestResolver.helpResolve(userSql, wrapper);               
                
        // Plan
        command = QueryRewriter.rewrite(command, wrapper, null);
        FakeCapabilitiesFinder fakeFinder = new FakeCapabilitiesFinder();
        fakeFinder.addCapabilities(multiModel, TestOptimizer.getTypicalCapabilities()); 

        CapabilitiesFinder finder = new MultiSourceCapabilitiesFinder(fakeFinder, multiSourceModels);
        
        IDGenerator idGenerator = new IDGenerator();
        idGenerator.setDefaultFactory(new IntegerIDFactory());            
        
        Properties props = new Properties();
        CommandContext context = new CommandContext("0", "test", "user", null, vdb.getName(), vdb.getVersion(), props, false); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        context.setPlanToProcessConverter(new MultiSourcePlanToProcessConverter(metadata, idGenerator, analysis, finder, multiSourceModels, dqpContext, context));

        ProcessorPlan plan = QueryOptimizer.optimizePlan(command, wrapper, idGenerator, finder, analysis, context);
                        
        if(DEBUG) {
            System.out.println("\nMultiSource Plan:"); //$NON-NLS-1$
            System.out.println(plan);
        }
                
        TestProcessor.helpProcess(plan, context, dataMgr, expectedResults);                        
    }

    @Test public void testNoReplacement() throws Exception {
        final QueryMetadataInterface metadata = RealMetadataFactory.exampleMultiBinding();
        final String userSql = "SELECT * FROM MultiModel.Phys WHERE SOURCE_NAME = 'bogus'"; //$NON-NLS-1$
        final String multiModel = "MultiModel"; //$NON-NLS-1$
        final int sources = 2;
        final List[] expected = 
            new List[0];
        final ProcessorDataManager dataMgr = new MultiSourceDataManager();
        helpTestMultiSourcePlan(metadata, userSql, multiModel, sources, dataMgr, expected, RealMetadataFactory.exampleMultiBindingVDB());
    }

    @Test public void testSingleReplacement() throws Exception {
        final QueryMetadataInterface metadata = RealMetadataFactory.exampleMultiBinding();
        final String userSql = "SELECT * FROM MultiModel.Phys WHERE SOURCE_NAME = 'a'"; //$NON-NLS-1$
        final String multiModel = "MultiModel"; //$NON-NLS-1$
        final int sources = 2;
        final List[] expected = 
            new List[] { Arrays.asList(new Object[] { null, null, null}) };
        final HardcodedDataManager dataMgr = new MultiSourceDataManager();
        dataMgr.setMustRegisterCommands(false);
        helpTestMultiSourcePlan(metadata, userSql, multiModel, sources, dataMgr, expected, RealMetadataFactory.exampleMultiBindingVDB());
    }
    
    @Test public void testMultiReplacement() throws Exception {
        final QueryMetadataInterface metadata = RealMetadataFactory.exampleMultiBinding();
        final String userSql = "SELECT * FROM MultiModel.Phys"; //$NON-NLS-1$
        final String multiModel = "MultiModel"; //$NON-NLS-1$
        final int sources = 3;
        final List[] expected = 
            new List[] { Arrays.asList(new Object[] { null, null, null}),
                         Arrays.asList(new Object[] { null, null, null}),
                         Arrays.asList(new Object[] { null, null, null})};
        final ProcessorDataManager dataMgr = new MultiSourceDataManager();
        helpTestMultiSourcePlan(metadata, userSql, multiModel, sources, dataMgr, expected, RealMetadataFactory.exampleMultiBindingVDB());
    }
    
    @Test public void testMultiReplacementWithOrderBy() throws Exception {
        QueryMetadataInterface metadata = RealMetadataFactory.exampleMultiBinding();

        final String userSql = "SELECT * FROM MultiModel.Phys order by a"; //$NON-NLS-1$
        final String multiModel = "MultiModel"; //$NON-NLS-1$
        final int sources = 2;
        final List[] expected = new List[] {
            Arrays.asList("e", "z", "b"), //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
            Arrays.asList("f", "z", "b"), //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
            Arrays.asList("x", "z", "a"), //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
            Arrays.asList("y", "z", "a"), //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        };
        final HardcodedDataManager dataMgr = new HardcodedDataManager();
        dataMgr.addData("SELECT g_0.a, g_0.b, 'a' FROM MultiModel.Phys AS g_0", //$NON-NLS-1$
                        new List[] {
                            Arrays.asList("y", "z", "a"), //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
                            Arrays.asList("x", "z", "a")}); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        dataMgr.addData("SELECT g_0.a, g_0.b, 'b' FROM MultiModel.Phys AS g_0", //$NON-NLS-1$
                        new List[] {
                            Arrays.asList("e", "z", "b"), //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
                            Arrays.asList("f", "z", "b")}); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        helpTestMultiSourcePlan(metadata, userSql, multiModel, sources, dataMgr, expected, RealMetadataFactory.exampleMultiBindingVDB());
    }

    @Test public void testMultiReplacementWithLimit() throws Exception {
        final QueryMetadataInterface metadata = RealMetadataFactory.exampleMultiBinding();
        final String userSql = "SELECT distinct * FROM MultiModel.Phys order by a limit 1"; //$NON-NLS-1$
        final String multiModel = "MultiModel"; //$NON-NLS-1$
        final int sources = 2;
        final List[] expected = new List[] {
            Arrays.asList("e", "z", "b"), //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        };
        final HardcodedDataManager dataMgr = new HardcodedDataManager();
        dataMgr.addData("SELECT g_0.a, g_0.b, 'a' FROM MultiModel.Phys AS g_0", //$NON-NLS-1$
                        new List[] {
                            Arrays.asList("y", "z", "a"), //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
                            Arrays.asList("x", "z", "a")}); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        dataMgr.addData("SELECT g_0.a, g_0.b, 'b' FROM MultiModel.Phys AS g_0", //$NON-NLS-1$
                        new List[] {
                            Arrays.asList("e", "z", "b"), //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
                            Arrays.asList("f", "z", "b")}); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        helpTestMultiSourcePlan(metadata, userSql, multiModel, sources, dataMgr, expected, RealMetadataFactory.exampleMultiBindingVDB());
    }
    
    @Test public void testMultiDependentJoin() throws Exception {
        QueryMetadataInterface metadata = RealMetadataFactory.exampleMultiBinding();
        
        final String userSql = "SELECT a.a FROM MultiModel.Phys a inner join MultiModel.Phys b makedep on (a.a = b.a) order by a"; //$NON-NLS-1$
        final String multiModel = "MultiModel"; //$NON-NLS-1$
        final int sources = 2;
        final List[] expected = 
            new List[] { Arrays.asList(new Object[] { "x"}), //$NON-NLS-1$
                         Arrays.asList(new Object[] { "x"}), //$NON-NLS-1$
                         Arrays.asList(new Object[] { "x"}), //$NON-NLS-1$
                         Arrays.asList(new Object[] { "x"}), //$NON-NLS-1$
                         Arrays.asList(new Object[] { "y"}), //$NON-NLS-1$
                         Arrays.asList(new Object[] { "y"}), //$NON-NLS-1$
                         Arrays.asList(new Object[] { "y"}), //$NON-NLS-1$
                         Arrays.asList(new Object[] { "y"})}; //$NON-NLS-1$
                         
        final HardcodedDataManager dataMgr = new HardcodedDataManager();
        dataMgr.addData("SELECT g_0.a FROM MultiModel.Phys AS g_0",  //$NON-NLS-1$
                        new List[] { Arrays.asList(new Object[] { "x" }), //$NON-NLS-1$
                                     Arrays.asList(new Object[] { "y" })}); //$NON-NLS-1$
        dataMgr.addData("SELECT g_0.a FROM MultiModel.Phys AS g_0 WHERE g_0.a IN ('x', 'y')",  //$NON-NLS-1$
                        new List[] { Arrays.asList(new Object[] { "x" }), //$NON-NLS-1$
                                     Arrays.asList(new Object[] { "y" })}); //$NON-NLS-1$
        helpTestMultiSourcePlan(metadata, userSql, multiModel, sources, dataMgr, expected, RealMetadataFactory.exampleMultiBindingVDB());
    }
    
    @Test public void testSingleReplacementInDynamicCommand() throws Exception {
        final QueryMetadataInterface metadata = RealMetadataFactory.exampleMultiBinding();
        final String userSql = "exec Virt.sq1('a')"; //$NON-NLS-1$
        final String multiModel = "MultiModel"; //$NON-NLS-1$
        final int sources = 3;
        final List[] expected = new List[] { Arrays.asList(new Object[] { null, null}), };
        final ProcessorDataManager dataMgr = new MultiSourceDataManager();
        helpTestMultiSourcePlan(metadata, userSql, multiModel, sources, dataMgr, expected, RealMetadataFactory.exampleMultiBindingVDB());
    }
    
    @Test public void testSingleReplacementInDynamicCommandNullValue() throws Exception {
        final QueryMetadataInterface metadata = RealMetadataFactory.exampleMultiBinding();
        final String userSql = "exec Virt.sq1(null)"; //$NON-NLS-1$
        final String multiModel = "MultiModel"; //$NON-NLS-1$
        final int sources = 3;
        final List[] expected = new List[0];
        final ProcessorDataManager dataMgr = new MultiSourceDataManager();
        helpTestMultiSourcePlan(metadata, userSql, multiModel, sources, dataMgr, expected, RealMetadataFactory.exampleMultiBindingVDB());
    }
    
    @Test public void testMultiUpdateAll() throws Exception {
        final QueryMetadataInterface metadata = RealMetadataFactory.exampleMultiBinding();
        final String userSql = "update MultiModel.Phys set a = '1' where b = 'z'"; //$NON-NLS-1$
        final String multiModel = "MultiModel"; //$NON-NLS-1$
        final int sources = 3;
        final List[] expected = new List[] { Arrays.asList(3)};
        final MultiSourceDataManager dataMgr = new MultiSourceDataManager();
        dataMgr.setMustRegisterCommands(true);
        dataMgr.addData("UPDATE MultiModel.Phys SET a = '1' WHERE b = 'z'", new List[] {Arrays.asList(1)}); //$NON-NLS-1$
        helpTestMultiSourcePlan(metadata, userSql, multiModel, sources, dataMgr, expected, RealMetadataFactory.exampleMultiBindingVDB());
    }
    
    @Test public void testInsertMatching() throws Exception {
        final QueryMetadataInterface metadata = RealMetadataFactory.exampleMultiBinding();
        final String userSql = "INSERT INTO MultiModel.Phys(a, SOURCE_NAME) VALUES ('a', 'a')"; //$NON-NLS-1$
        final String multiModel = "MultiModel"; //$NON-NLS-1$
        final int sources = 3;
        final List[] expected = new List[] { Arrays.asList(1)};
        final MultiSourceDataManager dataMgr = new MultiSourceDataManager();
        dataMgr.setMustRegisterCommands(true);
        dataMgr.addData("INSERT INTO MultiModel.Phys (a) VALUES ('a')", new List[] {Arrays.asList(1)}); //$NON-NLS-1$
        helpTestMultiSourcePlan(metadata, userSql, multiModel, sources, dataMgr, expected, RealMetadataFactory.exampleMultiBindingVDB());
    }
    
    @Test public void testInsertNotMatching() throws Exception {
        final QueryMetadataInterface metadata = RealMetadataFactory.exampleMultiBinding();
        final String userSql = "INSERT INTO MultiModel.Phys(a, SOURCE_NAME) VALUES ('a', 'x')"; //$NON-NLS-1$
        final String multiModel = "MultiModel"; //$NON-NLS-1$
        final int sources = 3;
        final List[] expected = new List[] { Arrays.asList(0)};
        final MultiSourceDataManager dataMgr = new MultiSourceDataManager();
        dataMgr.setMustRegisterCommands(true);
        helpTestMultiSourcePlan(metadata, userSql, multiModel, sources, dataMgr, expected, RealMetadataFactory.exampleMultiBindingVDB());
    }
    
    @Test public void testInsertAll() throws Exception {
    	final QueryMetadataInterface metadata = RealMetadataFactory.exampleMultiBinding();
        final String userSql = "INSERT INTO MultiModel.Phys(a) VALUES ('a')"; //$NON-NLS-1$
        final String multiModel = "MultiModel"; //$NON-NLS-1$
        final int sources = 3;
        final List[] expected = new List[] { Arrays.asList(3)};
        final MultiSourceDataManager dataMgr = new MultiSourceDataManager();
        dataMgr.setMustRegisterCommands(true);
        dataMgr.addData("INSERT INTO MultiModel.Phys (a) VALUES ('a')", new List[] {Arrays.asList(1)}); //$NON-NLS-1$
        helpTestMultiSourcePlan(metadata, userSql, multiModel, sources, dataMgr, expected, RealMetadataFactory.exampleMultiBindingVDB());
    }
    
    @Test public void testProcedure() throws Exception {
    	final QueryMetadataInterface metadata = RealMetadataFactory.exampleMultiBinding();
        final String userSql = "exec MultiModel.proc('b', 'a')"; //$NON-NLS-1$
        final String multiModel = "MultiModel"; //$NON-NLS-1$
        final int sources = 3;
        final List[] expected = new List[] { Arrays.asList(1)};
        final MultiSourceDataManager dataMgr = new MultiSourceDataManager();
        dataMgr.setMustRegisterCommands(true);
        dataMgr.addData("EXEC MultiModel.proc('b')", new List[] {Arrays.asList(1)}); //$NON-NLS-1$
        helpTestMultiSourcePlan(metadata, userSql, multiModel, sources, dataMgr, expected, RealMetadataFactory.exampleMultiBindingVDB());
    }
    
    @Test public void testProcedureAll() throws Exception {
    	final QueryMetadataInterface metadata = RealMetadataFactory.exampleMultiBinding();
        final String userSql = "exec MultiModel.proc(\"in\"=>'b')"; //$NON-NLS-1$
        final String multiModel = "MultiModel"; //$NON-NLS-1$
        final int sources = 3;
        final List[] expected = new List[] { Arrays.asList(1), Arrays.asList(1), Arrays.asList(1)};
        final MultiSourceDataManager dataMgr = new MultiSourceDataManager();
        dataMgr.setMustRegisterCommands(true);
        dataMgr.addData("EXEC MultiModel.proc('b')", new List[] {Arrays.asList(1)}); //$NON-NLS-1$
        helpTestMultiSourcePlan(metadata, userSql, multiModel, sources, dataMgr, expected, RealMetadataFactory.exampleMultiBindingVDB());
    }

}
