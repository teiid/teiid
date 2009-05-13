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

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import org.teiid.dqp.internal.process.multisource.MultiSourceCapabilitiesFinder;
import org.teiid.dqp.internal.process.multisource.MultiSourceElement;
import org.teiid.dqp.internal.process.multisource.MultiSourceMetadataWrapper;
import org.teiid.dqp.internal.process.multisource.MultiSourcePlanToProcessConverter;

import junit.framework.TestCase;

import com.metamatrix.common.buffer.TupleSource;
import com.metamatrix.common.vdb.api.ModelInfo;
import com.metamatrix.core.id.IDGenerator;
import com.metamatrix.core.id.IntegerIDFactory;
import com.metamatrix.dqp.service.FakeVDBService;
import com.metamatrix.query.analysis.AnalysisRecord;
import com.metamatrix.query.metadata.QueryMetadataInterface;
import com.metamatrix.query.optimizer.QueryOptimizer;
import com.metamatrix.query.optimizer.TestOptimizer;
import com.metamatrix.query.optimizer.capabilities.CapabilitiesFinder;
import com.metamatrix.query.optimizer.capabilities.FakeCapabilitiesFinder;
import com.metamatrix.query.processor.HardcodedDataManager;
import com.metamatrix.query.processor.ProcessorDataManager;
import com.metamatrix.query.processor.ProcessorPlan;
import com.metamatrix.query.processor.TestProcessor;
import com.metamatrix.query.resolver.TestResolver;
import com.metamatrix.query.rewriter.QueryRewriter;
import com.metamatrix.query.sql.lang.Command;
import com.metamatrix.query.sql.symbol.ElementSymbol;
import com.metamatrix.query.sql.visitor.ElementCollectorVisitor;
import com.metamatrix.query.unittest.FakeMetadataFacade;
import com.metamatrix.query.unittest.FakeMetadataFactory;
import com.metamatrix.query.util.CommandContext;


/** 
 * It's important here that the MultiSourceCapabilityFinder is used since some capabilities 
 * will never be pushed to the source
 * 
 * @since 4.2
 */
public class TestMultiSourcePlanToProcessConverter extends TestCase {
    
    private final class MultiSourceDataManager extends HardcodedDataManager {
        
        public MultiSourceDataManager() {
            setMustRegisterCommands(false);
        }

        public TupleSource registerRequest(Object processorID, Command command, String modelName, String connectorBindingId, int nodeID) throws com.metamatrix.api.exception.MetaMatrixComponentException {
        	assertNotNull(connectorBindingId);
        	
        	Collection elements = ElementCollectorVisitor.getElements(command, true, true);
            
            for (Iterator i = elements.iterator(); i.hasNext();) {
                ElementSymbol symbol = (ElementSymbol)i.next();
                if (symbol.getMetadataID() instanceof MultiSourceElement) {
                    fail("Query Contains a MultiSourceElement -- MultiSource expansion did not happen"); //$NON-NLS-1$
                }
            }
            return super.registerRequest(processorID, command, modelName, connectorBindingId, nodeID);
        }
    }

    private static final boolean DEBUG = false;
    
    public void helpTestMultiSourcePlan(QueryMetadataInterface metadata, String userSql, String multiModel, int sourceCount, ProcessorDataManager dataMgr, List[] expectedResults) throws Exception {
        final String vdbName = "MyVDB"; //$NON-NLS-1$
        final String vdbVersion = "1"; //$NON-NLS-1$
     
        // Set up metadata and other dependencies
        FakeVDBService vdbService = new FakeVDBService();
        vdbService.addModel(vdbName, vdbVersion, multiModel, ModelInfo.PUBLIC, true); 
        
        char sourceID = 'a';
        for(int i=0; i<sourceCount; i++, sourceID++) {
            vdbService.addBinding(vdbName, vdbVersion, multiModel, "mmuuid:source_" + sourceID, "" + sourceID); //$NON-NLS-1$ //$NON-NLS-2$ 
        }
        Set<String> multiSourceModels = new HashSet<String>(vdbService.getMultiSourceModels(vdbName, vdbVersion));
        MultiSourceMetadataWrapper wrapper = new MultiSourceMetadataWrapper(metadata, multiSourceModels); 
        AnalysisRecord analysis = new AnalysisRecord(false, false, DEBUG);
        
        Command command = TestResolver.helpResolve(userSql, wrapper, analysis);               
                
        // Plan
        QueryRewriter.rewrite(command, null, wrapper, null);
        FakeCapabilitiesFinder fakeFinder = new FakeCapabilitiesFinder();
        fakeFinder.addCapabilities(multiModel, TestOptimizer.getTypicalCapabilities()); 

        CapabilitiesFinder finder = new MultiSourceCapabilitiesFinder(fakeFinder, multiSourceModels);
        
        IDGenerator idGenerator = new IDGenerator();
        idGenerator.setDefaultFactory(new IntegerIDFactory());            
        
        Properties props = new Properties();
        CommandContext context = new CommandContext("0", "test", "user", null, vdbName, vdbVersion, props, false, false); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        context.setPlanToProcessConverter(new MultiSourcePlanToProcessConverter(metadata, idGenerator, analysis, finder, multiSourceModels, vdbName, vdbService, vdbVersion));

        ProcessorPlan plan = QueryOptimizer.optimizePlan(command, wrapper, idGenerator, finder, analysis, context);
                        
        if(DEBUG) {
            System.out.println("\nMultiSource Plan:"); //$NON-NLS-1$
            System.out.println(plan);
        }
                
        TestProcessor.helpProcess(plan, context, dataMgr, expectedResults);                        
    }

    public void testNoReplacement() throws Exception {
        final QueryMetadataInterface metadata = FakeMetadataFactory.exampleMultiBinding();
        final String userSql = "SELECT * FROM MultiModel.Phys WHERE SOURCE_NAME = 'bogus'"; //$NON-NLS-1$
        final String multiModel = "MultiModel"; //$NON-NLS-1$
        final int sources = 2;
        final List[] expected = 
            new List[0];
        final ProcessorDataManager dataMgr = new MultiSourceDataManager();
        helpTestMultiSourcePlan(metadata, userSql, multiModel, sources, dataMgr, expected);
    }

    public void testSingleReplacement() throws Exception {
        final QueryMetadataInterface metadata = FakeMetadataFactory.exampleMultiBinding();
        final String userSql = "SELECT * FROM MultiModel.Phys WHERE SOURCE_NAME = 'a'"; //$NON-NLS-1$
        final String multiModel = "MultiModel"; //$NON-NLS-1$
        final int sources = 2;
        final List[] expected = 
            new List[] { Arrays.asList(new Object[] { null, null, null}) };
        final HardcodedDataManager dataMgr = new MultiSourceDataManager();
        dataMgr.setMustRegisterCommands(false);
        helpTestMultiSourcePlan(metadata, userSql, multiModel, sources, dataMgr, expected);
    }
    
    public void testMultiReplacement() throws Exception {
        final QueryMetadataInterface metadata = FakeMetadataFactory.exampleMultiBinding();
        final String userSql = "SELECT * FROM MultiModel.Phys"; //$NON-NLS-1$
        final String multiModel = "MultiModel"; //$NON-NLS-1$
        final int sources = 3;
        final List[] expected = 
            new List[] { Arrays.asList(new Object[] { null, null, null}),
                         Arrays.asList(new Object[] { null, null, null}),
                         Arrays.asList(new Object[] { null, null, null})};
        final ProcessorDataManager dataMgr = new MultiSourceDataManager();
        helpTestMultiSourcePlan(metadata, userSql, multiModel, sources, dataMgr, expected);
    }
    
    public void testMultiReplacementWithOrderBy() throws Exception {
        FakeMetadataFacade metadata = FakeMetadataFactory.exampleMultiBinding();

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
        helpTestMultiSourcePlan(metadata, userSql, multiModel, sources, dataMgr, expected);
    }

    public void testMultiReplacementWithLimit() throws Exception {
        final QueryMetadataInterface metadata = FakeMetadataFactory.exampleMultiBinding();
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
        helpTestMultiSourcePlan(metadata, userSql, multiModel, sources, dataMgr, expected);
    }
    
    public void testMultiDependentJoin() throws Exception {
        FakeMetadataFacade metadata = FakeMetadataFactory.exampleMultiBinding();
        
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
        helpTestMultiSourcePlan(metadata, userSql, multiModel, sources, dataMgr, expected);
    }
    
    public void testSingleReplacementInDynamicCommand() throws Exception {
        final QueryMetadataInterface metadata = FakeMetadataFactory.exampleMultiBinding();
        final String userSql = "exec Virt.sq1('a')"; //$NON-NLS-1$
        final String multiModel = "MultiModel"; //$NON-NLS-1$
        final int sources = 3;
        final List[] expected = new List[] { Arrays.asList(new Object[] { null, null}), };
        final ProcessorDataManager dataMgr = new MultiSourceDataManager();
        helpTestMultiSourcePlan(metadata, userSql, multiModel, sources, dataMgr, expected);
    }

}
