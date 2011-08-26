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

package org.teiid.query.processor;

import static org.junit.Assert.*;
import static org.teiid.query.optimizer.TestOptimizer.*;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.concurrent.Executor;

import org.junit.Test;
import org.teiid.client.metadata.ParameterInfo;
import org.teiid.common.buffer.BlockedException;
import org.teiid.common.buffer.BufferManager;
import org.teiid.common.buffer.BufferManagerFactory;
import org.teiid.common.buffer.TupleBuffer;
import org.teiid.common.buffer.TupleSource;
import org.teiid.common.buffer.impl.BufferManagerImpl;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidException;
import org.teiid.core.TeiidProcessingException;
import org.teiid.core.TeiidRuntimeException;
import org.teiid.core.types.DataTypeManager;
import org.teiid.core.types.XMLType;
import org.teiid.dqp.internal.process.CachedResults;
import org.teiid.dqp.internal.process.PreparedPlan;
import org.teiid.dqp.internal.process.QueryProcessorFactoryImpl;
import org.teiid.dqp.internal.process.SessionAwareCache;
import org.teiid.metadata.ColumnSet;
import org.teiid.metadata.MetadataStore;
import org.teiid.metadata.Procedure;
import org.teiid.metadata.Schema;
import org.teiid.metadata.Table;
import org.teiid.query.analysis.AnalysisRecord;
import org.teiid.query.function.FunctionTree;
import org.teiid.query.mapping.relational.QueryNode;
import org.teiid.query.metadata.QueryMetadataInterface;
import org.teiid.query.metadata.TempMetadataAdapter;
import org.teiid.query.metadata.TempMetadataStore;
import org.teiid.query.metadata.TransformationMetadata;
import org.teiid.query.optimizer.FakeFunctionMetadataSource;
import org.teiid.query.optimizer.QueryOptimizer;
import org.teiid.query.optimizer.TestOptimizer;
import org.teiid.query.optimizer.TestRuleRaiseNull;
import org.teiid.query.optimizer.TestOptimizer.ComparisonMode;
import org.teiid.query.optimizer.capabilities.BasicSourceCapabilities;
import org.teiid.query.optimizer.capabilities.CapabilitiesFinder;
import org.teiid.query.optimizer.capabilities.DefaultCapabilitiesFinder;
import org.teiid.query.optimizer.capabilities.FakeCapabilitiesFinder;
import org.teiid.query.optimizer.capabilities.SourceCapabilities.Capability;
import org.teiid.query.optimizer.relational.rules.RuleChooseDependent;
import org.teiid.query.parser.QueryParser;
import org.teiid.query.processor.relational.JoinNode;
import org.teiid.query.processor.relational.RelationalNode;
import org.teiid.query.processor.relational.RelationalPlan;
import org.teiid.query.resolver.QueryResolver;
import org.teiid.query.rewriter.QueryRewriter;
import org.teiid.query.sql.lang.Command;
import org.teiid.query.sql.lang.SPParameter;
import org.teiid.query.sql.symbol.Reference;
import org.teiid.query.sql.util.VariableContext;
import org.teiid.query.sql.visitor.ReferenceCollectorVisitor;
import org.teiid.query.tempdata.GlobalTableStoreImpl;
import org.teiid.query.tempdata.TempTableDataManager;
import org.teiid.query.tempdata.TempTableStore;
import org.teiid.query.unittest.RealMetadataFactory;
import org.teiid.query.unittest.TimestampUtil;
import org.teiid.query.util.CommandContext;
import org.teiid.query.validator.Validator;
import org.teiid.query.validator.ValidatorReport;

@SuppressWarnings({"nls", "unchecked"})
public class TestProcessor {

	// ################################## TEST HELPERS ################################

    private static final List[] SP1RS = new List[] { 
	            Arrays.asList(new Object[] { "a",   new Integer(0) }), //$NON-NLS-1$
	                    Arrays.asList(new Object[] { null,  new Integer(1)}),
	                    Arrays.asList(new Object[] { "a",   new Integer(3) }), //$NON-NLS-1$
	                    Arrays.asList(new Object[] { "c",   new Integer(1)}), //$NON-NLS-1$
	                    Arrays.asList(new Object[] { "b",   new Integer(2)}), //$NON-NLS-1$
	                    Arrays.asList(new Object[] { "a",   new Integer(0) }) //$NON-NLS-1$
	        };

	static Command helpParse(String sql) { 
        // parse
        try { 
            return QueryParser.getQueryParser().parseCommand(sql);
        } catch(TeiidException e) { 
            throw new TeiidRuntimeException(e);
        }
    }

	public static ProcessorPlan helpGetPlan(String sql, QueryMetadataInterface metadata) {
		return helpGetPlan(sql, metadata, new DefaultCapabilitiesFinder());
    }
	
	public static ProcessorPlan helpGetPlan(String sql, QueryMetadataInterface metadata, CapabilitiesFinder finder) { 
		if(DEBUG) System.out.println("\n####################################\n" + sql);  //$NON-NLS-1$

        Command command = helpParse(sql);   
        
    	ProcessorPlan process = helpGetPlan(command, metadata, finder);
        
        return process;
    }

    static ProcessorPlan helpGetPlan(Command command, QueryMetadataInterface metadata) {
        return helpGetPlan(command, metadata, new DefaultCapabilitiesFinder());
    }
    
	static ProcessorPlan helpGetPlan(Command command, QueryMetadataInterface metadata, CapabilitiesFinder capFinder) {
        CommandContext context = createCommandContext();
	    try {
			return helpGetPlan(command, metadata, capFinder, context);
		} catch (TeiidException e) {
			throw new RuntimeException(e);
		}
    }
	
    static ProcessorPlan helpGetPlan(Command command, QueryMetadataInterface metadata, CapabilitiesFinder capFinder, CommandContext context) throws TeiidException {
		if(DEBUG) System.out.println("\n####################################\n" + command); //$NON-NLS-1$
		AnalysisRecord analysisRecord = new AnalysisRecord(false, DEBUG);
		if (!(metadata instanceof TempMetadataAdapter)) {
			metadata = new TempMetadataAdapter(metadata, new TempMetadataStore());
		}
		context.setMetadata(metadata);
        try {
			QueryResolver.resolveCommand(command, metadata);
        
			ValidatorReport repo  = Validator.validate(command, metadata);
	        Collection failures = new ArrayList();
	        repo.collectInvalidObjects(failures);
	        if (failures.size() > 0){
	            fail("Exception during validation (" + repo); //$NON-NLS-1$
	        }        
			command = QueryRewriter.rewrite(command, metadata, context);
	        ProcessorPlan process = QueryOptimizer.optimizePlan(command, metadata, null, capFinder, analysisRecord, context);
			if(DEBUG) System.out.println("\n" + process); //$NON-NLS-1$
	        //per defect 10022, clone this plan before processing, just to make sure
	        //a cloned plan with correlated subquery references (or any cloned plan) can be processed
	        process = process.clone();
	        
	        assertNotNull("Output elements of process plan are null", process.getOutputElements()); //$NON-NLS-1$

			return process;
		} finally {
            if(DEBUG) {
                System.out.println(analysisRecord.getDebugLog());
            }
        }
	}

    public static void helpProcess(ProcessorPlan plan, ProcessorDataManager dataManager, List[] expectedResults) {    
        CommandContext context = createCommandContext();
        try {
			helpProcess(plan, context, dataManager, expectedResults);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
    }
    
    public static void helpProcess(ProcessorPlan plan, CommandContext context, ProcessorDataManager dataManager, List[] expectedResults) throws Exception {
        ProcessorPlan clonePlan = plan.clone();
        
        // Process twice to test reset and clone
        doProcess(plan, dataManager, expectedResults, context);
        plan.reset();
        doProcess(plan, dataManager, expectedResults, context);

        // Execute cloned of original plan
        doProcess(clonePlan, dataManager, expectedResults, context);
    }

    private void helpProcessException(ProcessorPlan plan, ProcessorDataManager dataManager) {
        helpProcessException(plan, dataManager, null);
    }
    
    private void helpProcessException(ProcessorPlan plan, ProcessorDataManager dataManager, String expectedErrorMessage) {
    	TupleBuffer tsId = null;
    	BufferManager bufferMgr = null;
        try {   
            bufferMgr = BufferManagerFactory.getStandaloneBufferManager();
            CommandContext context = new CommandContext("0", "test", null, null, 1); //$NON-NLS-1$ //$NON-NLS-2$
            QueryProcessor processor = new QueryProcessor(plan, context, bufferMgr, dataManager);
            processor.setNonBlocking(true);
            BatchCollector collector = processor.createBatchCollector();
            tsId = collector.collectTuples();
            fail("Expected error during processing, but got none."); //$NON-NLS-1$
        } catch(TeiidException e) {
            // ignore - this is expected
            if(expectedErrorMessage != null) {
                assertEquals(expectedErrorMessage, e.getMessage());
            }
        } finally {
        	if (tsId != null) {
        		tsId.remove();
        	}
        }
    }
        
    public static void doProcess(ProcessorPlan plan, ProcessorDataManager dataManager, List[] expectedResults, CommandContext context) throws Exception {
    	BufferManager bufferMgr = context.getBufferManager();
    	if (bufferMgr == null) {
	        BufferManagerImpl bm = BufferManagerFactory.createBufferManager();
	        bm.setProcessorBatchSize(context.getProcessorBatchSize());
	        bm.setConnectorBatchSize(context.getProcessorBatchSize());
	        context.setBufferManager(bm);
	        bufferMgr = bm;
    	}
    	context.getNextRand(0);
        if (context.getTempTableStore() == null) {
        	context.setTempTableStore(new TempTableStore(context.getConnectionID()));
        }
        if (context.getGlobalTableStore() == null) {
        	GlobalTableStoreImpl gts = new GlobalTableStoreImpl(bufferMgr, context.getMetadata());
        	context.setGlobalTableStore(gts);
        }
        if (!(dataManager instanceof TempTableDataManager)) {
    	    SessionAwareCache<CachedResults> cache = new SessionAwareCache<CachedResults>();
    	    cache.setBufferManager(bufferMgr);
    	    Executor executor = new Executor() {
    			@Override
    			public void execute(Runnable command) {
    				command.run();
    			}
    	    };        	
        	dataManager = new TempTableDataManager(dataManager, bufferMgr, executor, cache);
        }        
        if (context.getQueryProcessorFactory() == null) {
        	context.setQueryProcessorFactory(new QueryProcessorFactoryImpl(bufferMgr, dataManager, new DefaultCapabilitiesFinder(), null, context.getMetadata()));
        }
        TupleBuffer id = null;
        try {
            QueryProcessor processor = new QueryProcessor(plan, context, bufferMgr, dataManager);
            //processor.setNonBlocking(true);
            BatchCollector collector = processor.createBatchCollector();
            for (int i = 0; i < 100; i++) {
            	try {
            		id = collector.collectTuples();
            		break;
            	} catch (BlockedException e) {
            		
            	}
            }
            if (id == null) {
            	fail("did not complete processing");
            }
            if ( expectedResults != null ) {
            	examineResults(expectedResults, bufferMgr, id);
            }
        } finally {
        	if (id != null) {
        		id.remove();
        	}
        }
    }

    /** 
     * @param expectedResults
     * @param bufferMgr
     * @param tsID
     * @throws TeiidComponentException
     * @throws TeiidProcessingException 
     * @since 4.3
     */
    static void examineResults(List[] expectedResults,BufferManager bufferMgr,TupleBuffer tsID) 
        throws TeiidComponentException,SQLException, TeiidProcessingException {
        
        // Create QueryResults from TupleSource
        TupleSource ts = tsID.createIndexedTupleSource();
        int count = tsID.getRowCount();   

		if(DEBUG) {
            System.out.println("\nResults:\n" + tsID.getSchema()); //$NON-NLS-1$
            TupleSource ts2 = tsID.createIndexedTupleSource();
            for(int j=0; j<count; j++) {
                System.out.println("" + j + ": " + ts2.nextTuple());	 //$NON-NLS-1$ //$NON-NLS-2$
            }    
            ts2.closeSource();
        }
        
        // Compare actual to expected row count
        assertEquals("Did not get expected row count: ", expectedResults.length, count); //$NON-NLS-1$
     
        // Walk results and compare
        for(int i=0; i<count; i++) { 
            List record = ts.nextTuple();
            
            //handle xml
            if(record.size() == 1 && expectedResults[i].size() == 1){
            	Object cellValue = record.get(0);
            	if(cellValue instanceof XMLType){
                    XMLType id =  (XMLType)cellValue; 
                    String actualDoc = id.getString(); 
                	if (expectedResults[i].size() == 1) {
                		compareDocuments((String)expectedResults[i].get(0), actualDoc);
                        continue;
                	}
            	} else if (cellValue instanceof Object[]) {
            		assertArrayEquals((Object[])expectedResults[i].get(0), (Object[])cellValue);
            		continue;
            	}
            }
            
            assertEquals("Row " + i + " does not match expected: ", expectedResults[i], record);                 //$NON-NLS-1$ //$NON-NLS-2$
        }
        ts.closeSource();
    }
    
	public static void compareDocuments(String expectedDoc, String actualDoc) {
		StringTokenizer tokens1 = new StringTokenizer(expectedDoc, "\r\n"); //$NON-NLS-1$
		StringTokenizer tokens2 = new StringTokenizer(actualDoc, "\r\n");//$NON-NLS-1$
		while(tokens1.hasMoreTokens()){
			String token1 = tokens1.nextToken().trim();
			if(!tokens2.hasMoreTokens()){
				fail("XML doc mismatch: expected=" + token1 + "\nactual=none");//$NON-NLS-1$ //$NON-NLS-2$
			}
			String token2 = tokens2.nextToken().trim();
			assertEquals("XML doc mismatch: ", token1, token2); //$NON-NLS-1$
		}
		if(tokens2.hasMoreTokens()){
			fail("XML doc mismatch: expected=none\nactual=" + tokens2.nextToken().trim());//$NON-NLS-1$
		}
	}

	public static CommandContext createCommandContext() {
		Properties props = new Properties();
		props.setProperty("soap_host", "my.host.com"); //$NON-NLS-1$ //$NON-NLS-2$
		props.setProperty("soap_port", "12345"); //$NON-NLS-1$ //$NON-NLS-2$
		CommandContext context = new CommandContext("0", "test", "user", null, "myvdb", 1, props, DEBUG); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
        context.setProcessorBatchSize(BufferManager.DEFAULT_PROCESSOR_BATCH_SIZE);
        context.setConnectorBatchSize(BufferManager.DEFAULT_CONNECTOR_BATCH_SIZE);
        context.setBufferManager(BufferManagerFactory.getStandaloneBufferManager());
        context.setPreparedPlanCache(new SessionAwareCache<PreparedPlan>());
		return context;
	}   
    	
    public static void sampleData1(FakeDataManager dataMgr) {
        try { 
        	FakeDataStore.sampleData1(dataMgr, RealMetadataFactory.example1Cached());
        } catch(Throwable e) { 
        	throw new RuntimeException(e);
        }
    }                    
    
    private void sampleData2(FakeDataManager dataMgr) {
		try {
			FakeDataStore.sampleData2(dataMgr);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
    }                  

    private void sampleData2a(FakeDataManager dataMgr) {
        QueryMetadataInterface metadata = RealMetadataFactory.example1Cached();
    
        try { 
            dataMgr.registerTuples(
                metadata,
                "pm1.g1", new List[] { 
					    Arrays.asList(new Object[] { "a",   new Integer(0),     Boolean.FALSE,  new Double(2.0) }), //$NON-NLS-1$
					    Arrays.asList(new Object[] { "b",   new Integer(1),     Boolean.TRUE,   null }), //$NON-NLS-1$
					    Arrays.asList(new Object[] { "c",   new Integer(2),     Boolean.FALSE,  new Double(0.0) }), //$NON-NLS-1$
					    } );       
                
            dataMgr.registerTuples(
                metadata,
                "pm2.g1", new List[] { 
					    Arrays.asList(new Object[] { "b",   new Integer(0),     Boolean.FALSE,  new Double(2.0) }), //$NON-NLS-1$
					    Arrays.asList(new Object[] { "b",   new Integer(7),     Boolean.FALSE,  new Double(2.0) }), //$NON-NLS-1$
					    Arrays.asList(new Object[] { "d",   new Integer(3),     Boolean.TRUE,   new Double(7.0) }), //$NON-NLS-1$
					    Arrays.asList(new Object[] { "e",   new Integer(1),     Boolean.TRUE,   null }), //$NON-NLS-1$
					    } );      

            dataMgr.registerTuples(
                metadata,
                "pm4.g1", new List[] { 
					    Arrays.asList(new Object[] { "aa",   new Integer(0),     Boolean.FALSE,  new Double(2.0) }), //$NON-NLS-1$
					    Arrays.asList(new Object[] { "bb",   new Integer(1),     Boolean.TRUE,   null }), //$NON-NLS-1$
					    Arrays.asList(new Object[] { "cc",   new Integer(2),     Boolean.FALSE,  new Double(0.0) }), //$NON-NLS-1$
					    } );              
            
        } catch(TeiidException e) { 
        	throw new RuntimeException(e);
        }
    }    
    
    public static void sampleData2b(FakeDataManager dataMgr, QueryMetadataInterface metadata) {
        try { 
            dataMgr.registerTuples(
                metadata,
                "pm1.g1", new List[] { 
					    Arrays.asList(new Object[] { "aa ",   new Integer(0),     Boolean.FALSE,  new Double(2.0) }), //$NON-NLS-1$
					    Arrays.asList(new Object[] { "bb   ",   new Integer(1),     Boolean.TRUE,   null }), //$NON-NLS-1$
					    Arrays.asList(new Object[] { "cc  ",   new Integer(2),     Boolean.FALSE,  new Double(0.0) }), //$NON-NLS-1$
					    } );       
                
            dataMgr.registerTuples(
                metadata,
                "pm2.g1", new List[] { 
					    Arrays.asList(new Object[] { "b",   new Integer(0),     Boolean.FALSE,  new Double(2.0) }), //$NON-NLS-1$
					    Arrays.asList(new Object[] { "d",   new Integer(3),     Boolean.TRUE,   new Double(7.0) }), //$NON-NLS-1$
					    Arrays.asList(new Object[] { "e",   new Integer(1),     Boolean.TRUE,   null }), //$NON-NLS-1$
					    } );      

            dataMgr.registerTuples(
                metadata,
                "pm4.g1", new List[] { 
					    Arrays.asList(new Object[] { "aa ",   new Integer(0),     Boolean.FALSE,  new Double(2.0) }), //$NON-NLS-1$
					    Arrays.asList(new Object[] { "bb   ",   new Integer(1),     Boolean.TRUE,   null }), //$NON-NLS-1$
					    Arrays.asList(new Object[] { "cc  ",   new Integer(2),     Boolean.FALSE,  new Double(0.0) }), //$NON-NLS-1$
					    } );       
        } catch(TeiidException e) { 
        	throw new RuntimeException(e);
        }
    }    
    
    private void sampleDataBQT1(FakeDataManager dataMgr) {
        QueryMetadataInterface metadata = RealMetadataFactory.exampleBQTCached();
    
        try { 
            // Group bqt1.smalla
        
            List[] tuples = new List[20];
            for(int i=0; i<tuples.length; i++) {
                tuples[i] = new ArrayList(17);
                tuples[i].add(new Integer(i));
                for(int j=0; j<16; j++) {
                    tuples[i].add(null);    
                }    
            }
        
            dataMgr.registerTuples(metadata, "bqt1.smalla", tuples); //$NON-NLS-1$

            // Group bqt2.mediumb
            tuples = new List[20];
            for(int i=0; i<tuples.length; i++) {
                tuples[i] = new ArrayList(17);
                tuples[i].add(new Integer(i));
                for(int j=0; j<16; j++) {
                    tuples[i].add(null);    
                }    
            }
        
            dataMgr.registerTuples(metadata, "bqt2.mediumb", tuples); //$NON-NLS-1$
        } catch(TeiidException e) { 
        	throw new RuntimeException(e);
        }
    }

	private void sampleDataBQT2(FakeDataManager dataMgr) {
        QueryMetadataInterface metadata = RealMetadataFactory.exampleBQTCached();
    
        String[] groups = new String[] {"bqt1.smalla", "bqt2.smalla", "bqt3.smalla" }; //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    
        try { 
            for(int i=0; i<groups.length; i++) {
                String groupName = groups[i];
    
                List[] tuples = new List[30];
                for(int row=0; row<tuples.length; row++) {
                    tuples[row] = new ArrayList(17);
                    tuples[row].add(new Integer(row));
                    for(int col=0; col<16; col++) {
                        tuples[row].add(null);    
                    }    
                }
        
                dataMgr.registerTuples(metadata, groupName, tuples);
            }

        } catch(TeiidException e) { 
        	throw new RuntimeException(e);
        }
    }
    
    /**
     * Just want to register two rows of all the integral types to test AVG 
     * @param dataMgr
     * @since 4.2
     */
    private void sampleDataBQT_defect11682(FakeDataManager dataMgr) {
        QueryMetadataInterface metadata = RealMetadataFactory.exampleBQTCached();
    
        try { 
            List[] tuples = new List[2];
            for(int i=1; i<=tuples.length; i++) {
                int index=i-1;
                tuples[index] = new ArrayList(17);
                tuples[index].add(new Integer(i)); //IntKey
                tuples[index].add(null);
                tuples[index].add(new Integer(i));
                tuples[index].add(null);
                tuples[index].add(new Float(i));
                tuples[index].add(new Long(i));
                tuples[index].add(new Double(i));
                tuples[index].add(new Byte((byte)i));
                tuples[index].add(null);
                tuples[index].add(null);
                tuples[index].add(null);
                tuples[index].add(null);
                tuples[index].add(null);
                tuples[index].add(new Short((short)i));
                tuples[index].add(new BigInteger(i+"")); //$NON-NLS-1$
                tuples[index].add(new BigDecimal(i+".0")); //$NON-NLS-1$
                tuples[index].add(null);
            }
            
            dataMgr.registerTuples(metadata, "bqt1.smalla", tuples); //$NON-NLS-1$ 
            
        } catch(TeiidException e) { 
        	throw new RuntimeException(e);
        }
    }    

    private void sampleDataBQTSmall(FakeDataManager dataMgr) {
        QueryMetadataInterface metadata = RealMetadataFactory.exampleBQTCached();
    
        try { 
            List[] tuples = new List[1];
            for(int i=0; i<tuples.length; i++) {
                tuples[i] = new ArrayList(17);
                tuples[i].add(new Integer(i));
                for(int j=0; j<16; j++) {
                    tuples[i].add(null);    
                }    
            }
        
            dataMgr.registerTuples(metadata, "bqt1.smalla", tuples); //$NON-NLS-1$ 

        } catch(TeiidException e) { 
        	throw new RuntimeException(e);
        }
    }

    private List createRowWithTimestamp(String tsStr) {
        Timestamp ts = Timestamp.valueOf(tsStr);
        return Arrays.asList(new Object[] {
            new Integer(0), "a", new Integer(1), "a",  //$NON-NLS-1$ //$NON-NLS-2$
            null, null, null, null, null, null, ts, null, null, null, null, null, null   
        });
    }
            
    private void sampleDataBQT_case1566(FakeDataManager dataMgr) throws Exception {
        QueryMetadataInterface metadata = RealMetadataFactory.exampleBQTCached();
    
        dataMgr.registerTuples(metadata, "bqt1.smalla", new List[] { //$NON-NLS-1$ 
                createRowWithTimestamp("2002-01-01 10:00:00"), //$NON-NLS-1$
                createRowWithTimestamp("2002-01-01 14:00:00"), //$NON-NLS-1$
                createRowWithTimestamp("2002-01-02 10:00:00"), //$NON-NLS-1$
                createRowWithTimestamp("2002-01-02 14:00:00"), //$NON-NLS-1$
                createRowWithTimestamp("2002-01-02 19:00:00.01"), //$NON-NLS-1$
                } );       
    }                
                
    static List getProcResultSetSymbols(List params){
    	List result = new ArrayList();
    	Iterator iter = params.iterator();
    	while(iter.hasNext()){
    		SPParameter param = (SPParameter)iter.next();
    		if(param.getResultSetColumns() != null){
    			result.addAll(param.getResultSetColumns());
    		}
    	}
    	iter = params.iterator();
    	while(iter.hasNext()){
    		SPParameter param = (SPParameter)iter.next();
            if(param.getParameterType() == ParameterInfo.INOUT || param.getParameterType() == ParameterInfo.RETURN_VALUE) {
                result.add(param.getParameterSymbol());
            }
    	}
    	return result;
    }      
    
    @Test public void test1() { 
        // Create query 
        String sql = "SELECT pm1.g1.e1, e2, pm1.g1.e3 as a, e4 as b FROM pm1.g1"; //$NON-NLS-1$
        
        // Create expected results
        List[] expected = new List[] { 
            Arrays.asList(new Object[] { "a",   new Integer(0),     Boolean.FALSE,  new Double(2.0) }), //$NON-NLS-1$
            Arrays.asList(new Object[] { null,  new Integer(1),     Boolean.FALSE,  new Double(1.0) }),
            Arrays.asList(new Object[] { "a",   new Integer(3),     Boolean.TRUE,   new Double(7.0) }), //$NON-NLS-1$
            Arrays.asList(new Object[] { "c",   new Integer(1),     Boolean.TRUE,   null }), //$NON-NLS-1$
            Arrays.asList(new Object[] { "b",   new Integer(2),     Boolean.FALSE,  new Double(0.0) }), //$NON-NLS-1$
            Arrays.asList(new Object[] { "a",   new Integer(0),     Boolean.FALSE,  new Double(2.0) }) //$NON-NLS-1$
        };    
    
        // Construct data manager with data
        FakeDataManager dataManager = new FakeDataManager();
        sampleData1(dataManager);
        
        // Plan query
        ProcessorPlan plan = helpGetPlan(sql, RealMetadataFactory.example1Cached());
        
        // Run query
        helpProcess(plan, dataManager, expected);
	}

	@Test public void test2() { 
        // Create query 
        String sql = "SELECT COUNT(*) FROM pm1.g1"; //$NON-NLS-1$
        
        // Create expected results
        List[] expected = new List[] { 
            Arrays.asList(new Object[] { new Integer(6) })
        };    
    
        // Construct data manager with data
        FakeDataManager dataManager = new FakeDataManager();
        sampleData1(dataManager);
        
        // Plan query
        ProcessorPlan plan = helpGetPlan(sql, RealMetadataFactory.example1Cached());
        
        // Run query
        helpProcess(plan, dataManager, expected);
	}

	@Test public void test3() { 
        // Create query 
        String sql = "SELECT COUNT(*), COUNT(e1), COUNT(distinct e1), COUNT(distinct e2), COUNT(distinct e3), COUNT(distinct e4) FROM pm1.g1"; //$NON-NLS-1$
        
        // Create expected results
        List[] expected = new List[] { 
            Arrays.asList(new Object[] { new Integer(6), new Integer(5), new Integer(3), new Integer(4), new Integer(2), new Integer(4) }),
        };    
    
        // Construct data manager with data
        FakeDataManager dataManager = new FakeDataManager();
        sampleData1(dataManager);
        
        // Plan query
        ProcessorPlan plan = helpGetPlan(sql, RealMetadataFactory.example1Cached());
        
        // Run query
        helpProcess(plan, dataManager, expected);
	}
 
	/** see also integer average defect 11682 */
    @Test public void test4() { 
        // Create query 
        String sql = "SELECT MIN(e2), MAX(e2), SUM(e2), AVG(e2), SUM(distinct e2), AVG(distinct e2) FROM pm1.g1"; //$NON-NLS-1$
        
        // Create expected results
        List[] expected = new List[] { 
            Arrays.asList(new Object[] { new Integer(0), new Integer(3), new Long(7), new Double(1.1666666666666667), new Long(6), new Double(1.5) }),
        };    
    
        // Construct data manager with data
        FakeDataManager dataManager = new FakeDataManager();
        sampleData1(dataManager);
        
        // Plan query
        ProcessorPlan plan = helpGetPlan(sql, RealMetadataFactory.example1Cached());
        
        // Run query
        helpProcess(plan, dataManager, expected);
	}
    
	@Test public void test5() { 
        // Create query 
        String sql = "SELECT MIN(e4), MAX(e4), SUM(e4), AVG(e4), SUM(distinct e4), AVG(distinct e4) FROM pm1.g1"; //$NON-NLS-1$
        
        // Create expected results
        List[] expected = new List[] { 
            Arrays.asList(new Object[] { new Double(0.0), new Double(7.0), new Double(12.0), new Double(2.4), new Double(10.0), new Double(2.5) }),
        };    
    
        // Construct data manager with data
        FakeDataManager dataManager = new FakeDataManager();
        sampleData1(dataManager);
        
        // Plan query
        ProcessorPlan plan = helpGetPlan(sql, RealMetadataFactory.example1Cached());
        
        // Run query
        helpProcess(plan, dataManager, expected);
	}

	@Test public void test7() { 
        // Create query 
        String sql = "SELECT * FROM vm1.g1"; //$NON-NLS-1$
        
        // Create expected results
        List[] expected = new List[] { 
            Arrays.asList(new Object[] { "a",   new Integer(0),     Boolean.FALSE,  new Double(2.0) }), //$NON-NLS-1$
            Arrays.asList(new Object[] { null,  new Integer(1),     Boolean.FALSE,  new Double(1.0) }),
            Arrays.asList(new Object[] { "a",   new Integer(3),     Boolean.TRUE,   new Double(7.0) }), //$NON-NLS-1$
            Arrays.asList(new Object[] { "c",   new Integer(1),     Boolean.TRUE,   null }), //$NON-NLS-1$
            Arrays.asList(new Object[] { "b",   new Integer(2),     Boolean.FALSE,  new Double(0.0) }), //$NON-NLS-1$
            Arrays.asList(new Object[] { "a",   new Integer(0),     Boolean.FALSE,  new Double(2.0) }) //$NON-NLS-1$
        };    
    
        // Construct data manager with data
        FakeDataManager dataManager = new FakeDataManager();
        sampleData1(dataManager);
        
        // Plan query
        ProcessorPlan plan = helpGetPlan(sql, RealMetadataFactory.example1Cached());
        
        // Run query
        helpProcess(plan, dataManager, expected);
	}

	@Test public void test8() { 
        // Create query 
        String sql = "SELECT * FROM vm1.g2 order by 1, 2, 3"; //$NON-NLS-1$
        
        // Create expected results
        List[] expected = new List[] { 
            Arrays.asList(new Object[] { "a",   new Integer(0),     Boolean.FALSE,  new Double(2.0) }), //$NON-NLS-1$
            Arrays.asList(new Object[] { "a",   new Integer(0),     Boolean.FALSE,  new Double(2.0) }), //$NON-NLS-1$
            Arrays.asList(new Object[] { "a",   new Integer(0),     Boolean.FALSE,  new Double(2.0) }), //$NON-NLS-1$
            Arrays.asList(new Object[] { "a",   new Integer(0),     Boolean.FALSE,  new Double(2.0) }), //$NON-NLS-1$
            Arrays.asList(new Object[] { "a",   new Integer(0),     Boolean.TRUE,   new Double(7.0) }), //$NON-NLS-1$
            Arrays.asList(new Object[] { "a",   new Integer(0),     Boolean.TRUE,   new Double(7.0) }), //$NON-NLS-1$
            Arrays.asList(new Object[] { "a",   new Integer(3),     Boolean.FALSE,  new Double(2.0) }), //$NON-NLS-1$
            Arrays.asList(new Object[] { "a",   new Integer(3),     Boolean.FALSE,  new Double(2.0) }), //$NON-NLS-1$
            Arrays.asList(new Object[] { "a",   new Integer(3),     Boolean.TRUE,   new Double(7.0) }), //$NON-NLS-1$
            Arrays.asList(new Object[] { "b",   new Integer(2),     Boolean.FALSE,  new Double(0.0) }), //$NON-NLS-1$
            Arrays.asList(new Object[] { "c",   new Integer(1),     Boolean.TRUE,  null }) //$NON-NLS-1$
        };    
    
        // Construct data manager with data
        FakeDataManager dataManager = new FakeDataManager();
        sampleData1(dataManager);
        
        // Plan query
        ProcessorPlan plan = helpGetPlan(sql, RealMetadataFactory.example1Cached());
        
        // Run query
        helpProcess(plan, dataManager, expected);
	}
    
	@Test public void test9() { 
        // Create query 
        String sql = "SELECT * FROM vm1.g4 order by e1"; //$NON-NLS-1$
        
        // Create expected results
        List[] expected = new List[] { 
            Arrays.asList(new Object[] { null }),
            Arrays.asList(new Object[] { "0" }), //$NON-NLS-1$
            Arrays.asList(new Object[] { "0" }), //$NON-NLS-1$
            Arrays.asList(new Object[] { "1" }), //$NON-NLS-1$
            Arrays.asList(new Object[] { "1" }), //$NON-NLS-1$
            Arrays.asList(new Object[] { "2" }), //$NON-NLS-1$
            Arrays.asList(new Object[] { "3" }), //$NON-NLS-1$
            Arrays.asList(new Object[] { "a" }), //$NON-NLS-1$
            Arrays.asList(new Object[] { "a" }), //$NON-NLS-1$
            Arrays.asList(new Object[] { "a" }), //$NON-NLS-1$
            Arrays.asList(new Object[] { "b" }), //$NON-NLS-1$
            Arrays.asList(new Object[] { "c" }) //$NON-NLS-1$
        };    
    
        // Construct data manager with data
        FakeDataManager dataManager = new FakeDataManager();
        sampleData1(dataManager);
        
        // Plan query
        ProcessorPlan plan = helpGetPlan(sql, RealMetadataFactory.example1Cached());

        // Run query
        helpProcess(plan, dataManager, expected);
	}
    
	@Test public void test10() { 
        // Create query 
        String sql = "SELECT e1 FROM vm1.g4 where e1 = 'a'"; //$NON-NLS-1$
        
        // Create expected results
        List[] expected = new List[] { 
            Arrays.asList(new Object[] { "a" }), //$NON-NLS-1$
            Arrays.asList(new Object[] { "a" }), //$NON-NLS-1$
            Arrays.asList(new Object[] { "a" }) //$NON-NLS-1$
        };    
    
        // Construct data manager with data
        FakeDataManager dataManager = new FakeDataManager();
        sampleData1(dataManager);
        
        // Plan query
        ProcessorPlan plan = helpGetPlan(sql, RealMetadataFactory.example1Cached());

        // Run query
        helpProcess(plan, dataManager, expected);
	}

    @Test public void testBooleanComparisonGT() { 
        // Create query 
        String sql = "SELECT pm1.g1.e3 FROM pm1.g1 WHERE e3 > {b'false'}"; //$NON-NLS-1$
        
        // Create expected results
        List[] expected = new List[] { 
            Arrays.asList(new Object[] { Boolean.TRUE }), 
            Arrays.asList(new Object[] { Boolean.TRUE }) 
        };    
    
        // Construct data manager with data
        FakeDataManager dataManager = new FakeDataManager();
        sampleData1(dataManager);
        
        // Plan query
        ProcessorPlan plan = helpGetPlan(sql, RealMetadataFactory.example1Cached());
        
        // Run query
        helpProcess(plan, dataManager, expected);
    }

    @Test public void testBooleanComparisonGE() { 
        // Create query 
        String sql = "SELECT pm1.g1.e3 FROM pm1.g1 WHERE e3 >= {b'false'}"; //$NON-NLS-1$
        
        // Create expected results
        List[] expected = new List[] { 
            Arrays.asList(new Object[] { Boolean.FALSE }), 
            Arrays.asList(new Object[] { Boolean.FALSE }), 
            Arrays.asList(new Object[] { Boolean.TRUE }), 
            Arrays.asList(new Object[] { Boolean.TRUE }), 
            Arrays.asList(new Object[] { Boolean.FALSE }), 
            Arrays.asList(new Object[] { Boolean.FALSE }) 
        };    
    
        // Construct data manager with data
        FakeDataManager dataManager = new FakeDataManager();
        sampleData1(dataManager);
        
        // Plan query
        ProcessorPlan plan = helpGetPlan(sql, RealMetadataFactory.example1Cached());
        
        // Run query
        helpProcess(plan, dataManager, expected);
    }

    @Test public void testBooleanComparisonLT() { 
        // Create query 
        String sql = "SELECT pm1.g1.e3 FROM pm1.g1 WHERE e3 < {b'true'}"; //$NON-NLS-1$
        
        // Create expected results
        List[] expected = new List[] { 
            Arrays.asList(new Object[] { Boolean.FALSE }), 
            Arrays.asList(new Object[] { Boolean.FALSE }), 
            Arrays.asList(new Object[] { Boolean.FALSE }), 
            Arrays.asList(new Object[] { Boolean.FALSE }) 
        };    
    
        // Construct data manager with data
        FakeDataManager dataManager = new FakeDataManager();
        sampleData1(dataManager);
        
        // Plan query
        ProcessorPlan plan = helpGetPlan(sql, RealMetadataFactory.example1Cached());
        
        // Run query
        helpProcess(plan, dataManager, expected);
    }

    @Test public void testBooleanComparisonLE() { 
        // Create query 
        String sql = "SELECT pm1.g1.e3 FROM pm1.g1 WHERE e3 <= {b'true'}"; //$NON-NLS-1$
        
        // Create expected results
        List[] expected = new List[] { 
            Arrays.asList(new Object[] { Boolean.FALSE }), 
            Arrays.asList(new Object[] { Boolean.FALSE }), 
            Arrays.asList(new Object[] { Boolean.TRUE }), 
            Arrays.asList(new Object[] { Boolean.TRUE }), 
            Arrays.asList(new Object[] { Boolean.FALSE }), 
            Arrays.asList(new Object[] { Boolean.FALSE }) 
        };    
    
        // Construct data manager with data
        FakeDataManager dataManager = new FakeDataManager();
        sampleData1(dataManager);
        
        // Plan query
        ProcessorPlan plan = helpGetPlan(sql, RealMetadataFactory.example1Cached());
        
        // Run query
        helpProcess(plan, dataManager, expected);
    }
 
    @Test public void testConcatOperator() { 
        // Create query 
        String sql = "SELECT pm1.g1.e1 || e2 AS x FROM pm1.g1"; //$NON-NLS-1$
        
        // Create expected results
        List[] expected = new List[] { 
            Arrays.asList(new Object[] { "a0" }), //$NON-NLS-1$
            Arrays.asList(new Object[] { null }),
            Arrays.asList(new Object[] { "a3" }), //$NON-NLS-1$
            Arrays.asList(new Object[] { "c1" }), //$NON-NLS-1$
            Arrays.asList(new Object[] { "b2" }), //$NON-NLS-1$
            Arrays.asList(new Object[] { "a0" }) //$NON-NLS-1$
        };    
    
        // Construct data manager with data
        FakeDataManager dataManager = new FakeDataManager();
        sampleData1(dataManager);
        
        // Plan query
        ProcessorPlan plan = helpGetPlan(sql, RealMetadataFactory.example1Cached());
        
        // Run query
        helpProcess(plan, dataManager, expected);
    }

 	/** Duplicates defect #4841: SELECT e1 a, e1 b FROM pm1.g1 order by a */
 	@Test public void testDefect4841_1() { 
        // Create query 
        String sql = "SELECT e1 a, e1 b FROM pm1.g1 order by a"; //$NON-NLS-1$
        
        // Create expected results
        List[] expected = new List[] { 
            Arrays.asList(new Object[] { null,  null }),
            Arrays.asList(new Object[] { "a",   "a" }), //$NON-NLS-1$ //$NON-NLS-2$
            Arrays.asList(new Object[] { "a",   "a" }), //$NON-NLS-1$ //$NON-NLS-2$
            Arrays.asList(new Object[] { "a",   "a" }), //$NON-NLS-1$ //$NON-NLS-2$
            Arrays.asList(new Object[] { "b",   "b" }), //$NON-NLS-1$ //$NON-NLS-2$
            Arrays.asList(new Object[] { "c",   "c" }) //$NON-NLS-1$ //$NON-NLS-2$
		};    
        
        // Construct data manager with data
        FakeDataManager dataManager = new FakeDataManager();
        sampleData1(dataManager);
        
        // Plan query
        ProcessorPlan plan = helpGetPlan(sql, RealMetadataFactory.example1Cached());

        // Run query
        helpProcess(plan, dataManager, expected);
	}

    /** Duplicates defect #4841: SELECT e1 a, e1 b FROM pm1.g1 order by a, b desc */
    @Test public void testDefect4841_2() { 
        // Create query 
        String sql = "SELECT e1 a, e1 b FROM pm1.g1 order by a"; //$NON-NLS-1$
        
        // Create expected results
        List[] expected = new List[] { 
            Arrays.asList(new Object[] { null,  null }),
            Arrays.asList(new Object[] { "a",   "a" }), //$NON-NLS-1$ //$NON-NLS-2$
            Arrays.asList(new Object[] { "a",   "a" }), //$NON-NLS-1$ //$NON-NLS-2$
            Arrays.asList(new Object[] { "a",   "a" }), //$NON-NLS-1$ //$NON-NLS-2$
            Arrays.asList(new Object[] { "b",   "b" }), //$NON-NLS-1$ //$NON-NLS-2$
            Arrays.asList(new Object[] { "c",   "c" }) //$NON-NLS-1$ //$NON-NLS-2$
        };    
        
        // Construct data manager with data
        FakeDataManager dataManager = new FakeDataManager();
        sampleData1(dataManager);
        
        // Plan query
        ProcessorPlan plan = helpGetPlan(sql, RealMetadataFactory.example1Cached());

        // Run query
        helpProcess(plan, dataManager, expected);
    }

    /** Duplicates defect #5292: SELECT DISTINCT e1, e1 a FROM pm1.g1 */
    @Test public void testDefect5292_1() { 
        // Create query 
        String sql = "SELECT DISTINCT e1, e1 a FROM pm1.g1"; //$NON-NLS-1$
        
        // Create expected results
        List[] expected = new List[] { 
            Arrays.asList(new Object[] { null,  null }),
            Arrays.asList(new Object[] { "a",   "a" }), //$NON-NLS-1$ //$NON-NLS-2$
            Arrays.asList(new Object[] { "b",   "b" }), //$NON-NLS-1$ //$NON-NLS-2$
            Arrays.asList(new Object[] { "c",   "c" }) //$NON-NLS-1$ //$NON-NLS-2$
        };    
        
        // Construct data manager with data
        FakeDataManager dataManager = new FakeDataManager();
        sampleData1(dataManager);
        
        // Plan query
        ProcessorPlan plan = helpGetPlan(sql, RealMetadataFactory.example1Cached());

        // Run query
        helpProcess(plan, dataManager, expected);
    }

    /** Duplicates defect #5292: SELECT DISTINCT e1, e1 a FROM pm1.g1 ORDER BY a */
    @Test public void testDefect5292_2() { 
        // Create query 
        String sql = "SELECT DISTINCT e1, e1 a FROM pm1.g1 ORDER BY a"; //$NON-NLS-1$
        
        // Create expected results
        List[] expected = new List[] { 
            Arrays.asList(new Object[] { null,  null }),
            Arrays.asList(new Object[] { "a",   "a" }), //$NON-NLS-1$ //$NON-NLS-2$
            Arrays.asList(new Object[] { "b",   "b" }), //$NON-NLS-1$ //$NON-NLS-2$
            Arrays.asList(new Object[] { "c",   "c" }) //$NON-NLS-1$ //$NON-NLS-2$
        };    
        
        // Construct data manager with data
        FakeDataManager dataManager = new FakeDataManager();
        sampleData1(dataManager);
        
        // Plan query
        ProcessorPlan plan = helpGetPlan(sql, RealMetadataFactory.example1Cached());

        // Run query
        helpProcess(plan, dataManager, expected);
    }

    /** Duplicates defect #5004: SELECT COUNT(*) FROM pm1.g1 WHERE e1='xxxx' */
    @Test public void testDefect5004() { 
        // Create query 
        String sql = "SELECT COUNT(*) FROM pm1.g1 WHERE e1='xxxx'"; //$NON-NLS-1$
        
        // Create expected results
        List[] expected = new List[] { 
            Arrays.asList(new Object[] { new Integer(0) })
        };    
        
        // Construct data manager with data
        FakeDataManager dataManager = new FakeDataManager();
        sampleData1(dataManager);
        
        // Plan query
        ProcessorPlan plan = helpGetPlan(sql, RealMetadataFactory.example1Cached());

        // Run query
        helpProcess(plan, dataManager, expected);
    }
    
    /**
     * Test to ensure that multiple empty batches are handled by the grouping node as well
     */
    @Test public void testDefect5004a() throws Exception { 
        // Create query 
        String sql = "SELECT COUNT(*) FROM pm1.g1 WHERE e1='xxxx'"; //$NON-NLS-1$
        
        // Create expected results
        List[] expected = new List[] { 
            Arrays.asList(new Object[] { new Integer(0) })
        };    
        
        // Construct data manager with data
        FakeDataManager dataManager = new FakeDataManager();
        sampleData1(dataManager);

        CommandContext context = createCommandContext();
        context.setProcessorBatchSize(2);
        context.setConnectorBatchSize(2);
        context.setMetadata(RealMetadataFactory.example1Cached());
        
        // Plan query
        ProcessorPlan plan = helpGetPlan(helpParse(sql), RealMetadataFactory.example1Cached(), new DefaultCapabilitiesFinder(), context);

        // Run query
        helpProcess(plan, context, dataManager, expected);
    }

    /** SELECT COUNT(e2), MIN(e2), MAX(e2), SUM(e2), AVG(e2) FROM pm1.g1 WHERE e2=-999999 */
    @Test public void test13() { 
        // Create query 
        String sql = "SELECT COUNT(e2), MIN(e2), MAX(e2), SUM(e2), AVG(e2) FROM pm1.g1 WHERE e2=-999999"; //$NON-NLS-1$
        
        // Create expected results
        List[] expected = new List[] {
            Arrays.asList(new Object[] {new Integer(0), null, null, null, null})
        };    
        
        // Construct data manager with data
        FakeDataManager dataManager = new FakeDataManager();
        sampleData1(dataManager);
        
        // Plan query
        ProcessorPlan plan = helpGetPlan(sql, RealMetadataFactory.example1Cached());

        // Run query
        helpProcess(plan, dataManager, expected);
    }

    /**
     * This test uncovered a bug in the FakeDataManager; the element
     * symbol in the atomic query criteria has a canonical name 
     * of "Y.e4", but the FakeDataManager sends a Map of ElementSymbols
     * having the unaliased names.  The first symbol cannot be found
     * in the Map due to the implementation of Symbol.equals() being
     * based entirely on the canonical name, which causes a NPE.
     * (Alex says this wasn't previously a problem because aliased groups
     * did not previously get pushed down to the source.)
     */
    @Test public void testCriteriaAliasedGroup() {
        String sql = "select e1, e2 FROM pm2.g1 Y WHERE 2.0 = e4"; //$NON-NLS-1$

        // Create expected results
        List[] expected = new List[] {
            Arrays.asList(new Object[] { "b", new Integer(0) }), //$NON-NLS-1$
        };

        // Construct data manager with data
        FakeDataManager dataManager = new FakeDataManager();
        sampleData2(dataManager);

        // Plan query
        ProcessorPlan plan = helpGetPlan(sql, RealMetadataFactory.example1Cached());

        // Run query
        helpProcess(plan, dataManager, expected);
    }

    /** SELECT e1 FROM pm1.g1 WHERE 'abc' = 'xyz' */
    @Test public void testCriteriaComparesUnequalConstants() { 
        // Create query 
        String sql = "SELECT e1 FROM pm1.g1 WHERE 'abc' = 'xyz'"; //$NON-NLS-1$
        
        // Create expected results
        List[] expected = new List[] { 
        };    
        
        // Construct data manager with data
        FakeDataManager dataManager = new FakeDataManager();
        sampleData1(dataManager);
        
        // Plan query
        ProcessorPlan plan = helpGetPlan(sql, RealMetadataFactory.example1Cached());

        // Run query
        helpProcess(plan, dataManager, expected);
    }
    
     /** SELECT pm1.g1.e1, pm2.g1.e1 FROM pm1.g1 RIGHT OUTER JOIN pm2.g1 ON pm1.g1.e1=pm2.g1.e1 */
     @Test public void testRightOuterJoin1() { 
        // Create query 
        String sql = "SELECT pm1.g1.e1, pm2.g1.e1 FROM pm1.g1 RIGHT OUTER JOIN pm2.g1 ON pm1.g1.e1=pm2.g1.e1"; //$NON-NLS-1$
        
        // Create expected results
        List[] expected = new List[] { 
            Arrays.asList(new Object[] { "b", "b" }), //$NON-NLS-1$ //$NON-NLS-2$
            Arrays.asList(new Object[] { null, "d" }), //$NON-NLS-1$
            Arrays.asList(new Object[] { null, "e" }) //$NON-NLS-1$
        };    
        
        // Construct data manager with data
        FakeDataManager dataManager = new FakeDataManager();
        sampleData2(dataManager);
        
        // Plan query
        ProcessorPlan plan = helpGetPlan(sql, RealMetadataFactory.example1Cached());

        // Run query
        helpProcess(plan, dataManager, expected);
    }

     /** SELECT pm1.g1.e1, pm2.g1.e1 FROM pm1.g1 LEFT OUTER JOIN pm2.g1 ON pm1.g1.e1=pm2.g1.e1 */
     @Test public void testLeftOuterJoin1() { 
        // Create query 
        String sql = "SELECT pm1.g1.e1, pm2.g1.e1 FROM pm1.g1 LEFT OUTER JOIN pm2.g1 ON pm1.g1.e1=pm2.g1.e1"; //$NON-NLS-1$
        
        // Create expected results
        List[] expected = new List[] { 
            Arrays.asList(new Object[] { "a", null }), //$NON-NLS-1$
            Arrays.asList(new Object[] { "b", "b" }), //$NON-NLS-1$ //$NON-NLS-2$
            Arrays.asList(new Object[] { "c", null }) //$NON-NLS-1$
        };    
        
        // Construct data manager with data
        FakeDataManager dataManager = new FakeDataManager();
        sampleData2(dataManager);
        
        // Plan query
        ProcessorPlan plan = helpGetPlan(sql, RealMetadataFactory.example1Cached());

        // Run query
        helpProcess(plan, dataManager, expected);
    }    

    /** SELECT pm1.g1.e1, pm2.g1.e1 FROM pm1.g1 FULL OUTER JOIN pm2.g1 ON pm1.g1.e1=pm2.g1.e1 */
    @Test public void testFullOuterJoin1() throws Exception { 
        // Create query 
        String sql = "SELECT pm1.g1.e1, pm2.g1.e1 FROM pm1.g1 FULL OUTER JOIN pm2.g1 ON pm1.g1.e1=pm2.g1.e1"; //$NON-NLS-1$
        
        // Create expected results
        List[] expected = new List[] { 
            Arrays.asList(new Object[] { "a", null }), //$NON-NLS-1$
            Arrays.asList(new Object[] { "b", "b" }), //$NON-NLS-1$ //$NON-NLS-2$
            Arrays.asList(new Object[] { "c", null }), //$NON-NLS-1$
            Arrays.asList(new Object[] { null, "d" }), //$NON-NLS-1$
            Arrays.asList(new Object[] { null, "e" }) //$NON-NLS-1$
        };    
        
        // Construct data manager with data
        FakeDataManager dataManager = new FakeDataManager();
        sampleData2(dataManager);
        
        // Plan query
        ProcessorPlan plan = helpGetPlan(sql, RealMetadataFactory.example1Cached());

        // Run query
        helpProcess(plan, dataManager, expected);
    }   
    
    @Test public void testFullOuterJoin2() throws Exception { 
        // Create query 
        String sql = "SELECT a.e4 c0, b.e4 c1 FROM pm1.g1 a FULL OUTER JOIN pm1.g1 b ON a.e4=b.e4 order by c0"; //$NON-NLS-1$
        
        // Create expected results
        List[] expected = new List[] { 
            Arrays.asList(new Object[] { null, null }),
            Arrays.asList(new Object[] { null, null }), 
            Arrays.asList(new Object[] { new Double(0), new Double(0) }),
            Arrays.asList(new Object[] { new Double(2), new Double(2) }), 
        };    
        
        // Construct data manager with data
        FakeDataManager dataManager = new FakeDataManager();
        sampleData2(dataManager);
        
        // Plan query
        ProcessorPlan plan = helpGetPlan(sql, RealMetadataFactory.example1Cached());

        // Run query
        helpProcess(plan, dataManager, expected);
    }   
    
    @Test public void testSortedFullOuterJoin() throws Exception { 
        // Create query 
        String sql = "SELECT pm1.g1.e2, pm2.g1.e2 FROM pm1.g1 FULL OUTER JOIN pm2.g1 ON pm1.g1.e2=pm2.g1.e2 and pm1.g1.e2 > 3"; //$NON-NLS-1$
        
        // Create expected results
        List[] expected = new List[] { 
            Arrays.asList(new Object[] { null, 2 }), //$NON-NLS-1$
            Arrays.asList(new Object[] { 3, null }), //$NON-NLS-1$ //$NON-NLS-2$
            Arrays.asList(new Object[] { null, 3 }), //$NON-NLS-1$
            Arrays.asList(new Object[] { null, 4 }), //$NON-NLS-1$
        };    
        
        HardcodedDataManager hdm = new HardcodedDataManager();
        hdm.addData("SELECT g_0.e2 AS c_0 FROM pm1.g1 AS g_0 ORDER BY c_0", new List[] {
        		Arrays.asList(3),
        });
        hdm.addData("SELECT g_0.e2 AS c_0 FROM pm2.g1 AS g_0 ORDER BY c_0", new List[] {
        		Arrays.asList(2),
        		Arrays.asList(3),
        		Arrays.asList(4),
        		
        });
        ProcessorPlan plan = helpGetPlan(helpParse(sql), RealMetadataFactory.example1Cached(), TestOptimizer.getGenericFinder());
        CommandContext cc = createCommandContext();
        cc.setProcessorBatchSize(2);
        helpProcess(plan, cc, hdm, expected);
    } 
    
    @Test public void testFullOuterJoin3() throws Exception { 
        // Create query 
        String sql = "SELECT a.e4 c0, b.e4 c1 FROM pm1.g1 b FULL OUTER JOIN (select e4, 1 x from pm1.g1 union all select e4, 2 from pm1.g1) a ON a.e4=b.e4 and a.x = 2 order by c0"; //$NON-NLS-1$
        
        // Create expected results
        List[] expected = new List[] { 
            Arrays.asList(new Object[] { null, null }),
            Arrays.asList(new Object[] { null, null }), 
            Arrays.asList(new Object[] { null, null }),
            Arrays.asList(new Object[] { new Double(0), new Double(0) }),
            Arrays.asList(new Object[] { new Double(0), null }),
            Arrays.asList(new Object[] { new Double(2), new Double(2) }), 
            Arrays.asList(new Object[] { new Double(2), null }),
        };    
        
        // Construct data manager with data
        FakeDataManager dataManager = new FakeDataManager();
        sampleData2(dataManager);
        
        // Plan query
        ProcessorPlan plan = helpGetPlan(sql, RealMetadataFactory.example1Cached());

        // Run query
        helpProcess(plan, dataManager, expected);
    }   

     /** SELECT x.e1, pm2.g1.e1 FROM (SELECT e1 FROM pm1.g1) AS x LEFT OUTER JOIN pm2.g1 ON x.e1=pm2.g1.e1 */
     @Test public void testLeftOuterJoinWithInlineView() { 
        // Create query 
        String sql = "SELECT x.e1, pm2.g1.e1 FROM (SELECT e1 FROM pm1.g1) AS x LEFT OUTER JOIN pm2.g1 ON x.e1=pm2.g1.e1"; //$NON-NLS-1$
        
        // Create expected results
        List[] expected = new List[] { 
            Arrays.asList(new Object[] { "a", null }), //$NON-NLS-1$
            Arrays.asList(new Object[] { "b", "b" }), //$NON-NLS-1$ //$NON-NLS-2$
            Arrays.asList(new Object[] { "c", null }) //$NON-NLS-1$
        };    
        
        // Construct data manager with data
        FakeDataManager dataManager = new FakeDataManager();
        sampleData2(dataManager);
        
        // Plan query
        ProcessorPlan plan = helpGetPlan(sql, RealMetadataFactory.example1Cached());

        // Run query
        helpProcess(plan, dataManager, expected);
    }  
    
    /** SELECT * FROM vm1.g5 ORDER BY expr */
    @Test public void testDefect5273_1() {
        // Create query 
        String sql = "SELECT expr FROM vm1.g5 ORDER BY expr"; //$NON-NLS-1$
        
        // Create expected results
        List[] expected = new List[] { 
            Arrays.asList(new Object[] { null }),
            Arrays.asList(new Object[] { "aval" }), //$NON-NLS-1$
            Arrays.asList(new Object[] { "aval" }), //$NON-NLS-1$
            Arrays.asList(new Object[] { "aval" }), //$NON-NLS-1$
            Arrays.asList(new Object[] { "bval" }), //$NON-NLS-1$
            Arrays.asList(new Object[] { "cval" }) //$NON-NLS-1$
        };    

        // Construct data manager with data
        FakeDataManager dataManager = new FakeDataManager();
        sampleData1(dataManager);
        
        // Plan query
        ProcessorPlan plan = helpGetPlan(sql, RealMetadataFactory.example1Cached());

        // Run query
        helpProcess(plan, dataManager, expected);        
    }

    /** SELECT expr AS e FROM vm1.g5 ORDER BY e */
    @Test public void testDefect5273_2() {
        // Create query 
        String sql = "SELECT expr AS e FROM vm1.g5 ORDER BY e"; //$NON-NLS-1$
        
        // Create expected results
        List[] expected = new List[] { 
            Arrays.asList(new Object[] { null }),
            Arrays.asList(new Object[] { "aval" }), //$NON-NLS-1$
            Arrays.asList(new Object[] { "aval" }), //$NON-NLS-1$
            Arrays.asList(new Object[] { "aval" }), //$NON-NLS-1$
            Arrays.asList(new Object[] { "bval" }), //$NON-NLS-1$
            Arrays.asList(new Object[] { "cval" }) //$NON-NLS-1$
        };    

        // Construct data manager with data
        FakeDataManager dataManager = new FakeDataManager();
        sampleData1(dataManager);
        
        // Plan query
        ProcessorPlan plan = helpGetPlan(sql, RealMetadataFactory.example1Cached());

        // Run query
        helpProcess(plan, dataManager, expected);        
    }

    /** SELECT e2 AS e FROM vm1.g5 ORDER BY e */
    @Test public void testDefect5273_3() {
        // Create query 
        String sql = "SELECT e2 AS e FROM vm1.g5 ORDER BY e"; //$NON-NLS-1$
        
        // Create expected results
        List[] expected = new List[] { 
            Arrays.asList(new Object[] { new Integer(0) }),
            Arrays.asList(new Object[] { new Integer(0) }),
            Arrays.asList(new Object[] { new Integer(1) }),
            Arrays.asList(new Object[] { new Integer(1) }),
            Arrays.asList(new Object[] { new Integer(2) }),
            Arrays.asList(new Object[] { new Integer(3) })
        };    

        // Construct data manager with data
        FakeDataManager dataManager = new FakeDataManager();
        sampleData1(dataManager);
        
        // Plan query
        ProcessorPlan plan = helpGetPlan(sql, RealMetadataFactory.example1Cached());

        // Run query
        helpProcess(plan, dataManager, expected);        
    }

    /** SELECT e AS f FROM vm1.g6 ORDER BY f */
    @Test public void testDefect5273_4() {
        // Create query 
        String sql = "SELECT e AS f FROM vm1.g6 ORDER BY f"; //$NON-NLS-1$
        
        // Create expected results
        List[] expected = new List[] { 
            Arrays.asList(new Object[] { null }),
            Arrays.asList(new Object[] { "aval" }), //$NON-NLS-1$
            Arrays.asList(new Object[] { "aval" }), //$NON-NLS-1$
            Arrays.asList(new Object[] { "aval" }), //$NON-NLS-1$
            Arrays.asList(new Object[] { "bval" }), //$NON-NLS-1$
            Arrays.asList(new Object[] { "cval" }) //$NON-NLS-1$
        };    

        // Construct data manager with data
        FakeDataManager dataManager = new FakeDataManager();
        sampleData1(dataManager);
        
        // Plan query
        ProcessorPlan plan = helpGetPlan(sql, RealMetadataFactory.example1Cached());

        // Run query
        helpProcess(plan, dataManager, expected);        
    }        

    /** SELECT e AS f FROM vm1.g7 ORDER BY f */
    @Test public void testDefect5273_5() {
        // Create query 
        String sql = "SELECT e AS f FROM vm1.g7 ORDER BY f"; //$NON-NLS-1$
        
        // Create expected results
        List[] expected = new List[] { 
            Arrays.asList(new Object[] { null }),
            Arrays.asList(new Object[] { "a0" }), //$NON-NLS-1$
            Arrays.asList(new Object[] { "a0" }), //$NON-NLS-1$
            Arrays.asList(new Object[] { "a3" }), //$NON-NLS-1$
            Arrays.asList(new Object[] { "b2" }), //$NON-NLS-1$
            Arrays.asList(new Object[] { "c1" }) //$NON-NLS-1$
        };    

        // Construct data manager with data
        FakeDataManager dataManager = new FakeDataManager();
        sampleData1(dataManager);
        
        // Plan query
        ProcessorPlan plan = helpGetPlan(sql, RealMetadataFactory.example1Cached());

        // Run query
        helpProcess(plan, dataManager, expected);        
    }        

    /** SELECT e AS f FROM vm1.g7 ORDER BY f */
    @Test public void testDefect5273_6() {
        // Create query 
        String sql = "SELECT e AS f FROM vm1.g8 ORDER BY f"; //$NON-NLS-1$
        
        // Create expected results
        List[] expected = new List[] { 
            Arrays.asList(new Object[] { null }),
            Arrays.asList(new Object[] { "aval" }), //$NON-NLS-1$
            Arrays.asList(new Object[] { "aval" }), //$NON-NLS-1$
            Arrays.asList(new Object[] { "aval" }), //$NON-NLS-1$
            Arrays.asList(new Object[] { "bval" }), //$NON-NLS-1$
            Arrays.asList(new Object[] { "cval" }) //$NON-NLS-1$
        };    

        // Construct data manager with data
        FakeDataManager dataManager = new FakeDataManager();
        sampleData1(dataManager);
        
        // Plan query
        ProcessorPlan plan = helpGetPlan(sql, RealMetadataFactory.example1Cached());

        // Run query
        helpProcess(plan, dataManager, expected);        
    }        

    @Test public void testFalseCriteria1() { 
        // Create query 
        String sql = "SELECT 5 FROM pm1.g1 WHERE 0=1"; //$NON-NLS-1$
        
        // Create expected results
        List[] expected = new List[] { 
        };    
    
        // Construct data manager with data
        FakeDataManager dataManager = new FakeDataManager();
        sampleData1(dataManager);
        
        // Plan query
        ProcessorPlan plan = helpGetPlan(sql, RealMetadataFactory.example1Cached());
        
        // Run query
        helpProcess(plan, dataManager, expected);
    }

    @Test public void testFalseCriteria2() { 
        // Create query 
        String sql = "SELECT count(*) FROM pm1.g1 WHERE 0=1"; //$NON-NLS-1$
        
        // Create expected results
        List[] expected = new List[] { 
            Arrays.asList(new Object[] { new Integer(0) }),
        };    
    
        // Construct data manager with data
        FakeDataManager dataManager = new FakeDataManager();
        sampleData1(dataManager);
        
        // Plan query
        ProcessorPlan plan = helpGetPlan(sql, RealMetadataFactory.example1Cached());
        
        // Run query
        helpProcess(plan, dataManager, expected);
    }

    @Test public void testSubquery1() {
   		// Create query
   		String sql = "SELECT e1 FROM (SELECT e1 FROM pm1.g1) AS x"; //$NON-NLS-1$
   		
   		// Create expected results
   		List[] expected = new List[] {
   			Arrays.asList(new Object[] { "a" }),	 //$NON-NLS-1$
   			Arrays.asList(new Object[] { null }),
   			Arrays.asList(new Object[] { "a" }), //$NON-NLS-1$
   			Arrays.asList(new Object[] { "c" }), //$NON-NLS-1$
   			Arrays.asList(new Object[] { "b" }), //$NON-NLS-1$
   			Arrays.asList(new Object[] { "a" }) //$NON-NLS-1$
   		}; 	
   		
   		// Construct data manager with data
   		FakeDataManager dataManager = new FakeDataManager();
   		sampleData1(dataManager);
   		
    	// Plan query
    	ProcessorPlan plan = helpGetPlan(sql, RealMetadataFactory.example1Cached());

   		// Run query
   		helpProcess(plan, dataManager, expected);
    }

	@Test public void testSubquerySimple() {
		// Create query
		String sql = "SELECT e1 FROM (SELECT e1 FROM pm1.g1) AS x"; //$NON-NLS-1$

		// Create expected results
		List[] expected = new List[] {
			Arrays.asList(new Object[] { "a" }), //$NON-NLS-1$
			Arrays.asList(new Object[] { null }),
			Arrays.asList(new Object[] { "a" }), //$NON-NLS-1$
			Arrays.asList(new Object[] { "c" }), //$NON-NLS-1$
			Arrays.asList(new Object[] { "b" }), //$NON-NLS-1$
			Arrays.asList(new Object[] { "a" }) //$NON-NLS-1$
		};

		// Construct data manager with data
		FakeDataManager dataManager = new FakeDataManager();
		sampleData1(dataManager);

		// Plan query
		ProcessorPlan plan = helpGetPlan(sql, RealMetadataFactory.example1Cached());

		// Run query
		helpProcess(plan, dataManager, expected);
	}
	
	@Test public void testCritInSubquery() {
		// Create query
		String sql = "SELECT e1 FROM (SELECT e1 FROM pm1.g1 WHERE e1 = 'a') AS x"; //$NON-NLS-1$

		// Create expected results
		List[] expected = new List[] {
			Arrays.asList(new Object[] { "a" }), //$NON-NLS-1$
			Arrays.asList(new Object[] { "a" }), //$NON-NLS-1$
			Arrays.asList(new Object[] { "a" }) //$NON-NLS-1$
		};

		// Construct data manager with data
		FakeDataManager dataManager = new FakeDataManager();
		sampleData1(dataManager);

		// Plan query
		ProcessorPlan plan = helpGetPlan(sql, RealMetadataFactory.example1Cached());

		// Run query
		helpProcess(plan, dataManager, expected);
	}
	
	@Test public void testCritAboveSubquery() {
		// Create query
		String sql = "SELECT e1 FROM (SELECT e1 FROM pm1.g1) AS x WHERE e1 = 'a'"; //$NON-NLS-1$

		// Create expected results
		List[] expected = new List[] {
			Arrays.asList(new Object[] { "a" }), //$NON-NLS-1$
			Arrays.asList(new Object[] { "a" }), //$NON-NLS-1$
			Arrays.asList(new Object[] { "a" }) //$NON-NLS-1$
		};

		// Construct data manager with data
		FakeDataManager dataManager = new FakeDataManager();
		sampleData1(dataManager);

		// Plan query
		ProcessorPlan plan = helpGetPlan(sql, RealMetadataFactory.example1Cached());

		// Run query
		helpProcess(plan, dataManager, expected);
	}    

	@Test public void testSubqueryInJoinPredicate() {
		// Create query
		String sql = "SELECT x.e1 FROM (SELECT e1 FROM pm1.g1) AS x JOIN (SELECT e1 FROM pm1.g1) y ON x.e1=y.e1 order by x.e1"; //$NON-NLS-1$

		// Create expected results
		List[] expected = new List[] {
            Arrays.asList(new Object[] { "a" }), //$NON-NLS-1$
            Arrays.asList(new Object[] { "a" }), //$NON-NLS-1$
            Arrays.asList(new Object[] { "a" }), //$NON-NLS-1$
            Arrays.asList(new Object[] { "a" }), //$NON-NLS-1$
            Arrays.asList(new Object[] { "a" }), //$NON-NLS-1$
            Arrays.asList(new Object[] { "a" }), //$NON-NLS-1$
            Arrays.asList(new Object[] { "a" }), //$NON-NLS-1$
            Arrays.asList(new Object[] { "a" }), //$NON-NLS-1$
            Arrays.asList(new Object[] { "a" }), //$NON-NLS-1$
            Arrays.asList(new Object[] { "b" }), //$NON-NLS-1$
            Arrays.asList(new Object[] { "c" }), //$NON-NLS-1$
		};

		// Construct data manager with data
		FakeDataManager dataManager = new FakeDataManager();
		sampleData1(dataManager);

		// Plan query
		ProcessorPlan plan = helpGetPlan(sql, RealMetadataFactory.example1Cached());

		// Run query
		helpProcess(plan, dataManager, expected);
	}

	@Test public void testSubqueryWithRenaming() {
		// Create query
		String sql = "SELECT x.a FROM (SELECT e1 AS a FROM pm1.g1) AS x"; //$NON-NLS-1$

		// Create expected results
		List[] expected = new List[] {
			Arrays.asList(new Object[] { "a" }), //$NON-NLS-1$
			Arrays.asList(new Object[] { null }),
			Arrays.asList(new Object[] { "a" }), //$NON-NLS-1$
			Arrays.asList(new Object[] { "c" }), //$NON-NLS-1$
			Arrays.asList(new Object[] { "b" }), //$NON-NLS-1$
			Arrays.asList(new Object[] { "a" }) //$NON-NLS-1$
		};

		// Construct data manager with data
		FakeDataManager dataManager = new FakeDataManager();
		sampleData1(dataManager);

		// Plan query
		ProcessorPlan plan = helpGetPlan(sql, RealMetadataFactory.example1Cached());

		// Run query
		helpProcess(plan, dataManager, expected);
	}

    @Test public void testNestedSubquery() {
        // Create query
        String sql = "SELECT x.a FROM (SELECT e1 AS a FROM (SELECT e1 FROM pm1.g1) AS y) AS x"; //$NON-NLS-1$

        // Create expected results
        List[] expected = new List[] {
            Arrays.asList(new Object[] { "a" }), //$NON-NLS-1$
            Arrays.asList(new Object[] { null }),
            Arrays.asList(new Object[] { "a" }), //$NON-NLS-1$
            Arrays.asList(new Object[] { "c" }), //$NON-NLS-1$
            Arrays.asList(new Object[] { "b" }), //$NON-NLS-1$
            Arrays.asList(new Object[] { "a" }) //$NON-NLS-1$
        };

        // Construct data manager with data
        FakeDataManager dataManager = new FakeDataManager();
        sampleData1(dataManager);

        // Plan query
        ProcessorPlan plan = helpGetPlan(sql, RealMetadataFactory.example1Cached());

        // Run query
        helpProcess(plan, dataManager, expected);
    }

	/**
	 * Tests a single Subquery IN clause criteria
	 */
	@Test public void testSubqueryINClause() {
		String sql = "SELECT e1 FROM pm1.g1 WHERE e2 IN (SELECT e2 FROM pm2.g1)"; //$NON-NLS-1$

		// Create expected results
		List[] expected = new List[] {
			Arrays.asList(new Object[] { "a" }), //$NON-NLS-1$
			Arrays.asList(new Object[] { "b" }) //$NON-NLS-1$
		};

		// Construct data manager with data
		FakeDataManager dataManager = new FakeDataManager();
		sampleData2(dataManager);

		// Plan query
		ProcessorPlan plan = helpGetPlan(sql, RealMetadataFactory.example1Cached());

		// Run query
		helpProcess(plan, dataManager, expected);
	}

	/**
	 * Tests a single Subquery IN clause criteria with nulls
	 * in sample data
	 */
	@Test public void testSubqueryINClauseWithNulls() {
		String sql = "SELECT e1 FROM pm1.g1 WHERE e4 IN (SELECT e4 FROM pm2.g1)"; //$NON-NLS-1$


		// Create expected results
		List[] expected = new List[] {
			Arrays.asList(new Object[] { "a" }) //$NON-NLS-1$
		};

		// Construct data manager with data
		FakeDataManager dataManager = new FakeDataManager();
		sampleData2(dataManager);

		// Plan query
		ProcessorPlan plan = helpGetPlan(sql, RealMetadataFactory.example1Cached());

		// Run query
		helpProcess(plan, dataManager, expected);
	}
	
	/**
	 * Tests a single Subquery IN clause criteria with nulls
	 * in sample data
	 */
	@Test public void testSubqueryINClauseWithNulls2() {
		String sql = "SELECT e1 FROM pm1.g1 WHERE e2 IN (SELECT e4 FROM pm2.g1)"; //$NON-NLS-1$

		// Create expected results
		List[] expected = new List[] {
			Arrays.asList(new Object[] { "c" }) //$NON-NLS-1$
		};

		// Construct data manager with data
		FakeDataManager dataManager = new FakeDataManager();
		sampleData2(dataManager);

		// Plan query
		ProcessorPlan plan = helpGetPlan(sql, RealMetadataFactory.example1Cached());

		// Run query
		helpProcess(plan, dataManager, expected);
	}	

	/**
	 * Tests a compound criteria of two subqueries in IN clauses
	 */
	@Test public void testSubqueryINClauses() {
		String sql = "SELECT e1 FROM pm1.g1 WHERE e2 IN (SELECT e2 FROM pm2.g1) AND e1 IN (SELECT e1 FROM pm2.g1)"; //$NON-NLS-1$

		// Create expected results
		List[] expected = new List[] {
			Arrays.asList(new Object[] { "b" }) //$NON-NLS-1$
		};

		// Construct data manager with data
		FakeDataManager dataManager = new FakeDataManager();
		sampleData2(dataManager);

		// Plan query
		ProcessorPlan plan = helpGetPlan(sql, RealMetadataFactory.example1Cached());

		// Run query
		helpProcess(plan, dataManager, expected);
	}

    /**
     * Tests a compound criteria of a subquery in IN clause and another type of
     * criteria
     */
    @Test public void testSubqueryINClauseMixedCriteria() {
        String sql = "SELECT e1 FROM pm1.g1 WHERE e2 IN (SELECT e2 FROM pm2.g1) AND e1 IN ('b')"; //$NON-NLS-1$

        // Create expected results
        List[] expected = new List[] {
            Arrays.asList(new Object[] { "b" }) //$NON-NLS-1$
        };

        // Construct data manager with data
        FakeDataManager dataManager = new FakeDataManager();
        sampleData2(dataManager);

        // Plan query
        ProcessorPlan plan = helpGetPlan(sql, RealMetadataFactory.example1Cached());

        // Run query
        helpProcess(plan, dataManager, expected);
    }

	/**
	 * Tests a compound criteria of a subquery in IN clause and another type of
	 * criteria
	 */
	@Test public void testSubqueryINClauseMixedCriteria2() {
		String sql = "SELECT e1 FROM pm1.g1 WHERE e2 IN (SELECT e2 FROM pm2.g1) AND NOT (e1 = 'a')"; //$NON-NLS-1$

		// Create expected results
		List[] expected = new List[] {
			Arrays.asList(new Object[] { "b" }) //$NON-NLS-1$
		};

		// Construct data manager with data
		FakeDataManager dataManager = new FakeDataManager();
		sampleData2(dataManager);

		// Plan query
		ProcessorPlan plan = helpGetPlan(sql, RealMetadataFactory.example1Cached());

		// Run query
		helpProcess(plan, dataManager, expected);
	}

	/**
	 * Tests nesting of Subquery IN clause criteria
	 */
	@Test public void testNestedSubqueryINClauses() {
		String sql = "SELECT e1 FROM pm1.g1 WHERE e2 IN (SELECT e2 FROM pm2.g1 WHERE e1 IN (SELECT e1 FROM pm1.g1))"; //$NON-NLS-1$

		// Create expected results
		List[] expected = new List[] {
			Arrays.asList(new Object[] { "a" }) //$NON-NLS-1$
		};

		// Construct data manager with data
		FakeDataManager dataManager = new FakeDataManager();
		sampleData2(dataManager);

		// Plan query
		ProcessorPlan plan = helpGetPlan(sql, RealMetadataFactory.example1Cached());

		// Run query
		helpProcess(plan, dataManager, expected);
	}	

	@Test public void testSubqueryXML() {
		// Create query
		String sql = "SELECT * FROM (SELECT * FROM xmltest.doc1) AS x"; //$NON-NLS-1$

		// Create expected results
		List[] expected = new List[] { 
			Arrays.asList(new Object[] { 
                "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" + //$NON-NLS-1$
                    "<root><node1><node2><node3/></node2></node1></root>"             //$NON-NLS-1$
            })
		};    

		// Construct data manager with data
		FakeDataManager dataManager = new FakeDataManager();
		sampleData1(dataManager);

		// Plan query
		ProcessorPlan plan = helpGetPlan(sql, RealMetadataFactory.example1Cached());

		// Run query
		helpProcess(plan, dataManager, expected);
	}

    /**
     * Tests a single Subquery EXISTS predicate criteria
     */
    @Test public void testSubqueryExistsPredicate() {
        String sql = "SELECT e1 FROM pm1.g1 WHERE EXISTS (SELECT e2 FROM pm2.g1)"; //$NON-NLS-1$

        // Create expected results
        List[] expected = new List[] {
            Arrays.asList(new Object[] { "a" }), //$NON-NLS-1$
            Arrays.asList(new Object[] { "b" }), //$NON-NLS-1$
            Arrays.asList(new Object[] { "c" }) //$NON-NLS-1$
        };

        // Construct data manager with data
        FakeDataManager dataManager = new FakeDataManager();
        sampleData2(dataManager);

        // Plan query
        ProcessorPlan plan = helpGetPlan(sql, RealMetadataFactory.example1Cached());

        // Run query
        helpProcess(plan, dataManager, expected);
    }

    /**
     * Tests a single Subquery EXISTS predicate criteria 
     * where the subquery returns no rows
     */
    @Test public void testSubqueryExistsPredicate2() {
        String sql = "SELECT e1 FROM pm1.g1 WHERE EXISTS (SELECT e2 FROM pm2.g1 WHERE e1 = 'ZZTop')"; //$NON-NLS-1$

        // Create expected results
        List[] expected = new List[0];

        // Construct data manager with data
        FakeDataManager dataManager = new FakeDataManager();
        sampleData2(dataManager);

        // Plan query
        ProcessorPlan plan = helpGetPlan(sql, RealMetadataFactory.example1Cached());

        // Run query
        helpProcess(plan, dataManager, expected);
    }
    
    /**
     * Tests a single Subquery in compare predicate criteria
     */
    @Test public void testSubqueryComparePredicate() {
        String sql = "SELECT e1 FROM pm1.g1 WHERE e2 = ANY (SELECT e2 FROM pm2.g1)"; //$NON-NLS-1$

        // Create expected results
        List[] expected = new List[] {
            Arrays.asList(new Object[] { "a" }), //$NON-NLS-1$
            Arrays.asList(new Object[] { "b" }) //$NON-NLS-1$
        };

        // Construct data manager with data
        FakeDataManager dataManager = new FakeDataManager();
        sampleData2(dataManager);

        // Plan query
        ProcessorPlan plan = helpGetPlan(sql, RealMetadataFactory.example1Cached());

        // Run query
        helpProcess(plan, dataManager, expected);
    }    

    /**
     * Tests a single Subquery in compare predicate criteria
     */
    @Test public void testSubqueryComparePredicate2() {
        String sql = "SELECT e1 FROM pm1.g1 WHERE e2 = SOME (SELECT e2 FROM pm2.g1)"; //$NON-NLS-1$

        // Create expected results
        List[] expected = new List[] {
            Arrays.asList(new Object[] { "a" }), //$NON-NLS-1$
            Arrays.asList(new Object[] { "b" }) //$NON-NLS-1$
        };

        // Construct data manager with data
        FakeDataManager dataManager = new FakeDataManager();
        sampleData2(dataManager);

        // Plan query
        ProcessorPlan plan = helpGetPlan(sql, RealMetadataFactory.example1Cached());

        // Run query
        helpProcess(plan, dataManager, expected);
    }    

    /**
     * Tests a single Subquery in compare predicate criteria
     */
    @Test public void testSubqueryComparePredicate3() {
        String sql = "SELECT e1 FROM pm1.g1 WHERE e2 = ALL (SELECT e2 FROM pm2.g1)"; //$NON-NLS-1$

        // Create expected results
        List[] expected = new List[0];

        // Construct data manager with data
        FakeDataManager dataManager = new FakeDataManager();
        sampleData2(dataManager);

        // Plan query
        ProcessorPlan plan = helpGetPlan(sql, RealMetadataFactory.example1Cached());

        // Run query
        helpProcess(plan, dataManager, expected);
    } 

    /**
     * Tests a single Subquery in compare predicate criteria
     */
    @Test public void testSubqueryComparePredicate4() {
        String sql = "SELECT e1 FROM pm1.g1 WHERE e2 <= ALL (SELECT e2 FROM pm2.g1)"; //$NON-NLS-1$

        // Create expected results
        List[] expected = new List[] {
            Arrays.asList(new Object[] { "a" }) //$NON-NLS-1$
        };

        // Construct data manager with data
        FakeDataManager dataManager = new FakeDataManager();
        sampleData2(dataManager);

        // Plan query
        ProcessorPlan plan = helpGetPlan(sql, RealMetadataFactory.example1Cached());

        // Run query
        helpProcess(plan, dataManager, expected);
    } 

    /**
     * Tests a single Subquery in compare predicate criteria
     */
    @Test public void testSubqueryComparePredicate5() {
        String sql = "SELECT e1 FROM pm1.g1 WHERE e2 < SOME (SELECT e2 FROM pm2.g1)"; //$NON-NLS-1$

        // Create expected results
        List[] expected = new List[] {
            Arrays.asList(new Object[] { "a" }), //$NON-NLS-1$
            Arrays.asList(new Object[] { "b" }), //$NON-NLS-1$
            Arrays.asList(new Object[] { "c" }) //$NON-NLS-1$
        };

        // Construct data manager with data
        FakeDataManager dataManager = new FakeDataManager();
        sampleData2(dataManager);

        // Plan query
        ProcessorPlan plan = helpGetPlan(sql, RealMetadataFactory.example1Cached());

        // Run query
        helpProcess(plan, dataManager, expected);
    }
    
    /**
     * Tests a single Subquery in compare predicate criteria
     */
    @Test public void testSubqueryComparePredicate5a() {
        String sql = "SELECT e1 FROM pm2.g1 WHERE e2 < SOME (SELECT e2 FROM pm1.g1)"; //$NON-NLS-1$

        // Create expected results
        List[] expected = new List[] {
            Arrays.asList(new Object[] { "b" }), //$NON-NLS-1$
            Arrays.asList(new Object[] { "e" }) //$NON-NLS-1$
        };

        // Construct data manager with data
        FakeDataManager dataManager = new FakeDataManager();
        sampleData2(dataManager);

        // Plan query
        ProcessorPlan plan = helpGetPlan(sql, RealMetadataFactory.example1Cached());

        // Run query
        helpProcess(plan, dataManager, expected);
    }    

    /**
     * Tests a single Subquery in compare predicate criteria
     * without predicate quantifier
     */
    @Test public void testSubqueryComparePredicate6() {
        String sql = "SELECT e1 FROM pm1.g1 WHERE e2 < (SELECT e2 FROM pm2.g1 WHERE e1 = 'e')"; //$NON-NLS-1$

        // Create expected results
        List[] expected = new List[] {
            Arrays.asList(new Object[] { "a" }) //$NON-NLS-1$
        };

        // Construct data manager with data
        FakeDataManager dataManager = new FakeDataManager();
        sampleData2(dataManager);

        // Plan query
        ProcessorPlan plan = helpGetPlan(sql, RealMetadataFactory.example1Cached());

        // Run query
        helpProcess(plan, dataManager, expected);
    }

    /**
     * Tests a single Subquery in compare predicate criteria
     */
    @Test public void testSubqueryComparePredicateNested() {
        String sql = "SELECT e1 FROM pm1.g1 WHERE e2 < SOME (SELECT e2 FROM pm2.g1 WHERE EXISTS (SELECT e2 FROM pm2.g1))"; //$NON-NLS-1$

        // Plan query
        ProcessorPlan plan = helpGetPlan(sql, RealMetadataFactory.example1Cached());

        // Run query
        // Construct data manager with data
        HardcodedDataManager dataManager = new HardcodedDataManager();
        dataManager.addData("SELECT pm1.g1.e2, pm1.g1.e1 FROM pm1.g1", new List[] { Arrays.asList(new Object[] { new Integer(0), "a" }), //$NON-NLS-1$ //$NON-NLS-2$
                                                                      Arrays.asList(new Object[] { new Integer(1), "b" }), //$NON-NLS-1$
                                                                      Arrays.asList(new Object[] { new Integer(2), "c" }), //$NON-NLS-1$
                                                                      });
        
        dataManager.addData("SELECT pm2.g1.e2 FROM pm2.g1", new List[] {  //$NON-NLS-1$
                                                                 Arrays.asList(new Object[] { new Integer(0) }),
                                                                 Arrays.asList(new Object[] { new Integer(3) }),
                                                                 Arrays.asList(new Object[] { new Integer(1) }),                    
                                                                  });              
        // Create expected results
        List[] expected = new List[] {
            Arrays.asList(new Object[] { "a" }), //$NON-NLS-1$
            Arrays.asList(new Object[] { "b" }), //$NON-NLS-1$
            Arrays.asList(new Object[] { "c" }) //$NON-NLS-1$
        };

        helpProcess(plan, dataManager, expected);
    }
    
    /**
     * Tests a scalar subquery in the SELECT clause
     */
    @Test public void testSubqueryScalar() {
        String sql = "SELECT e1, (SELECT e2 FROM pm2.g1 WHERE e1 = 'b') FROM pm1.g1"; //$NON-NLS-1$

        // Create expected results
        List[] expected = new List[] {
            Arrays.asList(new Object[] { "a", new Integer(0) }), //$NON-NLS-1$
            Arrays.asList(new Object[] { "b", new Integer(0) }), //$NON-NLS-1$
            Arrays.asList(new Object[] { "c", new Integer(0) }) //$NON-NLS-1$
        };

        // Construct data manager with data
        FakeDataManager dataManager = new FakeDataManager();
        sampleData2(dataManager);

        // Plan query
        ProcessorPlan plan = helpGetPlan(sql, RealMetadataFactory.example1Cached());

        // Run query
        helpProcess(plan, dataManager, expected);
    }

    /**
     * Tests a scalar subquery which returns no rows in the SELECT clause
     */
    @Test public void testSubqueryScalar2() {
        String sql = "SELECT e1, (SELECT e2 FROM pm2.g1 WHERE e1 = 'a') FROM pm1.g1"; //$NON-NLS-1$

        // Create expected results
        List[] expected = new List[] {
            Arrays.asList(new Object[] { "a", null }), //$NON-NLS-1$
            Arrays.asList(new Object[] { "b", null }), //$NON-NLS-1$
            Arrays.asList(new Object[] { "c", null }) //$NON-NLS-1$
        };

        // Construct data manager with data
        FakeDataManager dataManager = new FakeDataManager();
        sampleData2(dataManager);

        // Plan query
        ProcessorPlan plan = helpGetPlan(sql, RealMetadataFactory.example1Cached());

        // Run query
        helpProcess(plan, dataManager, expected);
    }

    /**
     * Tests a scalar subquery which returns more than one rows
     * causes the expected Exception
     */
    @Test public void testSubqueryScalarException() throws Exception {
        String sql = "SELECT e1, (SELECT e2 FROM pm2.g1) FROM pm1.g1"; //$NON-NLS-1$

        // Construct data manager with data
        FakeDataManager dataManager = new FakeDataManager();
        sampleData2(dataManager);

        // Plan query
        ProcessorPlan plan = helpGetPlan(sql, RealMetadataFactory.example1Cached());

        // Run query
        helpProcessException(plan, dataManager);
    }
    
    @Test public void testSubqueryScalarInTransformation() {
        String sql = "select * from vm1.g25"; //$NON-NLS-1$

        // Create expected results
        List[] expected = new List[]{
            Arrays.asList(new Object[] { "a",   new Integer(0),     Boolean.FALSE,  new Double(2.0),  new Double(0.0) }), //$NON-NLS-1$
            Arrays.asList(new Object[] { null,  new Integer(1),     Boolean.FALSE,  new Double(1.0),  new Double(0.0) }),
            Arrays.asList(new Object[] { "a",   new Integer(3),     Boolean.TRUE,   new Double(7.0),  new Double(0.0) }), //$NON-NLS-1$
            Arrays.asList(new Object[] { "c",   new Integer(1),     Boolean.TRUE,   null,             new Double(0.0) }), //$NON-NLS-1$
            Arrays.asList(new Object[] { "b",   new Integer(2),     Boolean.FALSE,  new Double(0.0),  new Double(0.0) }), //$NON-NLS-1$
            Arrays.asList(new Object[] { "a",   new Integer(0),     Boolean.FALSE,  new Double(2.0),  new Double(0.0) }) //$NON-NLS-1$
        };

        // Construct data manager with data
        FakeDataManager dataManager = new FakeDataManager();
        sampleData1(dataManager);

        // Plan query
        ProcessorPlan plan = helpGetPlan(sql, RealMetadataFactory.example1Cached());

        // Run query
        helpProcess(plan, dataManager, expected);
    }  

    @Test public void testSubqueryScalarInTransformation2() {
        String sql = "select * from vm1.g25 where e5 = 0.0"; //$NON-NLS-1$

        // Create expected results
        List[] expected = new List[]{
            Arrays.asList(new Object[] { "a",   new Integer(0),     Boolean.FALSE,  new Double(2.0),  new Double(0.0) }), //$NON-NLS-1$
            Arrays.asList(new Object[] { null,  new Integer(1),     Boolean.FALSE,  new Double(1.0),  new Double(0.0) }),
            Arrays.asList(new Object[] { "a",   new Integer(3),     Boolean.TRUE,   new Double(7.0),  new Double(0.0) }), //$NON-NLS-1$
            Arrays.asList(new Object[] { "c",   new Integer(1),     Boolean.TRUE,   null,             new Double(0.0) }), //$NON-NLS-1$
            Arrays.asList(new Object[] { "b",   new Integer(2),     Boolean.FALSE,  new Double(0.0),  new Double(0.0) }), //$NON-NLS-1$
            Arrays.asList(new Object[] { "a",   new Integer(0),     Boolean.FALSE,  new Double(2.0),  new Double(0.0) }) //$NON-NLS-1$
        };

        // Construct data manager with data
        FakeDataManager dataManager = new FakeDataManager();
        sampleData1(dataManager);

        // Plan query
        ProcessorPlan plan = helpGetPlan(sql, RealMetadataFactory.example1Cached());

        // Run query
        helpProcess(plan, dataManager, expected);
    }    

    @Test public void testCorrelatedSubquery_CASE2022() {
        String sql = "select * from BQT2_V WHERE BQT2_V.IntKey < 50"; //$NON-NLS-1$

        // Create expected results
        List[] expected = new List[0];

        // Construct data manager with data
        FakeDataManager dataManager = new FakeDataManager();
        sampleDataBQT1(dataManager);
        sampleDataBQT2(dataManager);

        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.CRITERIA_COMPARE_EQ, true);
        caps.setCapabilitySupport(Capability.CRITERIA_COMPARE_ORDERED, true);
        caps.setCapabilitySupport(Capability.CRITERIA_QUANTIFIED_ALL, true);
        caps.setCapabilitySupport(Capability.CRITERIA_IN, true);
        caps.setCapabilitySupport(Capability.CRITERIA_IN_SUBQUERY, true);
        caps.setCapabilitySupport(Capability.QUERY_SUBQUERIES_SCALAR, true);
        caps.setCapabilitySupport(Capability.QUERY_SUBQUERIES_CORRELATED, true);
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES, true);
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES_MIN, true);
        caps.setCapabilitySupport(Capability.QUERY_FROM_GROUP_ALIAS, true);
        caps.setSourceProperty(Capability.MAX_IN_CRITERIA_SIZE, new Integer(1000));
        capFinder.addCapabilities("BQT1", caps); //$NON-NLS-1$
        capFinder.addCapabilities("BQT2", caps); //$NON-NLS-1$
        // Plan query
        ProcessorPlan plan = helpGetPlan(helpParse(sql), RealMetadataFactory.exampleBQTCached(), capFinder);

        // Run query
        helpProcess(plan, dataManager, expected);
    } 
    @Test public void testCorrelatedSubquery1() {
        String sql = "Select e1, e2, e4 from pm1.g1 where e2 in (select e2 FROM pm2.g1 WHERE pm1.g1.e4 = pm2.g1.e4)"; //$NON-NLS-1$

        // Create expected results
        List[] expected = new List[] {
            Arrays.asList(new Object[] { "a", new Integer(0), new Double(2.0) }) //$NON-NLS-1$
        };

        // Construct data manager with data
        FakeDataManager dataManager = new FakeDataManager();
        sampleData2(dataManager);

        // Plan query
        ProcessorPlan plan = helpGetPlan(sql, RealMetadataFactory.example1Cached());

        // Run query
        helpProcess(plan, dataManager, expected);

    }
    
    /**
     * There is a bug when the second query in a UNION ALL has a correlated subquery, and both
     * the outer and inner query are selecting from the same virtual group, and aliasing them
     * differently to distinguish between them.  The generated atomic query has screwed up
     * aliasing. 
     */
    @Test public void testCorrelatedSubqueryCase3667() {

        HardcodedDataManager dataManager = new HardcodedDataManager();
        
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.CRITERIA_IN_SUBQUERY, true);
        caps.setCapabilitySupport(Capability.CRITERIA_IN, true);
        caps.setCapabilitySupport(Capability.CRITERIA_COMPARE_EQ, true);
        caps.setCapabilitySupport(Capability.QUERY_FROM_GROUP_ALIAS, true);
        caps.setCapabilitySupport(Capability.QUERY_SUBQUERIES_SCALAR, true);
        caps.setCapabilitySupport(Capability.QUERY_SUBQUERIES_CORRELATED, true);
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES, true);
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES_MAX, true);
        caps.setCapabilitySupport(Capability.QUERY_UNION, true);
        caps.setCapabilitySupport(Capability.QUERY_SET_ORDER_BY, true);
        caps.setCapabilitySupport(Capability.QUERY_ORDERBY, true);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$
        capFinder.addCapabilities("pm2", caps); //$NON-NLS-1$

        // Plan query
        String sql = "Select e1, e2, e4 from pm2.g1 where 1=2 " + //$NON-NLS-1$
           "UNION ALL Select e1, e2, e4 from vm1.g1 outg1 where outg1.e2 in (select ing1.e2 FROM vm1.g1 ing1 WHERE outg1.e4 = ing1.e4)";//$NON-NLS-1$
        Command command = helpParse(sql);   
        ProcessorPlan plan = helpGetPlan(command, RealMetadataFactory.example1Cached(), capFinder);
        
        // Run query
        List[] expected = new List[] {
            Arrays.asList(new Object[] { "aString", new Integer(22), new Double(22.0) }), //$NON-NLS-1$
        };

        dataManager.addData("SELECT g_0.e1, g_0.e2, g_0.e4 FROM pm1.g1 AS g_0 WHERE g_0.e2 IN (SELECT g_1.e2 FROM pm1.g1 AS g_1 WHERE g_1.e4 = g_0.e4)",  //$NON-NLS-1$
                            expected);
        
        helpProcess(plan, dataManager, expected);
    }    
    
    /** control query, this test passes */
    @Test public void testCorrelatedSubqueryCase3667a() {

        HardcodedDataManager dataManager = new HardcodedDataManager();
        
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.CRITERIA_IN_SUBQUERY, true);
        caps.setCapabilitySupport(Capability.CRITERIA_IN, true);
        caps.setCapabilitySupport(Capability.CRITERIA_COMPARE_EQ, true);
        caps.setCapabilitySupport(Capability.QUERY_FROM_GROUP_ALIAS, true);
        caps.setCapabilitySupport(Capability.QUERY_SUBQUERIES_SCALAR, true);
        caps.setCapabilitySupport(Capability.QUERY_SUBQUERIES_CORRELATED, true);
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES, true);
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES_MAX, true);
        caps.setCapabilitySupport(Capability.QUERY_UNION, true);
        caps.setCapabilitySupport(Capability.QUERY_SET_ORDER_BY, true);
        caps.setCapabilitySupport(Capability.QUERY_ORDERBY, true);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$

        // Plan query
        String sql = "Select e1, e2, e4 from vm1.g1 outg1 where outg1.e2 in (select ing1.e2 FROM vm1.g1 ing1 WHERE outg1.e4 = ing1.e4)";//$NON-NLS-1$
        Command command = helpParse(sql);   
        ProcessorPlan plan = helpGetPlan(command, RealMetadataFactory.example1Cached(), capFinder);
        
        // Run query
        List[] expected = new List[] {
            Arrays.asList(new Object[] { "aString", new Integer(22), new Double(22.0) }), //$NON-NLS-1$
        };
        
        dataManager.addData("SELECT g_0.e1, g_0.e2, g_0.e4 FROM pm1.g1 AS g_0 WHERE g_0.e2 IN (SELECT g_1.e2 FROM pm1.g1 AS g_1 WHERE g_1.e4 = g_0.e4)",  //$NON-NLS-1$
                            expected);
        
        helpProcess(plan, dataManager, expected);
    }     

    @Test public void testCorrelatedSubquery2() {
        String sql = "Select e1, e2 from pm1.g1 where e2 in (select e2 FROM pm2.g1 WHERE pm1.g1.e4 = pm2.g1.e4)"; //$NON-NLS-1$

        // Create expected results
        List[] expected = new List[] {
            Arrays.asList(new Object[] { "a", new Integer(0) }) //$NON-NLS-1$
        };

        // Construct data manager with data
        FakeDataManager dataManager = new FakeDataManager();
        sampleData2(dataManager);

        // Plan query
        ProcessorPlan plan = helpGetPlan(sql, RealMetadataFactory.example1Cached());

        // Run query
        helpProcess(plan, dataManager, expected);

    }

    @Test public void testCorrelatedSubquery3() {
        String sql = "Select e1, (select e2 FROM pm2.g1 WHERE pm1.g1.e4 = pm2.g1.e4) from pm1.g1"; //$NON-NLS-1$

        // Create expected results
        List[] expected = new List[] {
            Arrays.asList(new Object[] { "a", new Integer(0) }), //$NON-NLS-1$
            Arrays.asList(new Object[] { "b", null }), //$NON-NLS-1$
            Arrays.asList(new Object[] { "c", null }) //$NON-NLS-1$
        };

        // Construct data manager with data
        FakeDataManager dataManager = new FakeDataManager();
        sampleData2(dataManager);

        // Plan query
        ProcessorPlan plan = helpGetPlan(sql, RealMetadataFactory.example1Cached());

        // Run query
        helpProcess(plan, dataManager, expected);
    }
    
    @Test public void testCorrelatedSubquery3a() {
        String sql = "Select e1, (select e2 FROM pm2.g1 WHERE X.e4 = e4) from pm1.g1 X"; //$NON-NLS-1$

        // Create expected results
        List[] expected = new List[] {
            Arrays.asList(new Object[] { "a", new Integer(0) }), //$NON-NLS-1$
            Arrays.asList(new Object[] { "b", null }), //$NON-NLS-1$
            Arrays.asList(new Object[] { "c", null }) //$NON-NLS-1$
        };

        // Construct data manager with data
        FakeDataManager dataManager = new FakeDataManager();
        sampleData2(dataManager);

        // Plan query
        ProcessorPlan plan = helpGetPlan(sql, RealMetadataFactory.example1Cached());

        // Run query
        helpProcess(plan, dataManager, expected);
    }    

    @Test public void testCorrelatedSubquery3b() {
        String sql = "Select e1, (select e2 FROM pm2.g1 Y WHERE X.e4 = e4) from pm1.g1 X"; //$NON-NLS-1$

        // Create expected results
        List[] expected = new List[] {
            Arrays.asList(new Object[] { "a", new Integer(0) }), //$NON-NLS-1$
            Arrays.asList(new Object[] { "b", null }), //$NON-NLS-1$
            Arrays.asList(new Object[] { "c", null }) //$NON-NLS-1$
        };

        // Construct data manager with data
        FakeDataManager dataManager = new FakeDataManager();
        sampleData2(dataManager);

        // Plan query
        ProcessorPlan plan = helpGetPlan(sql, RealMetadataFactory.example1Cached());

        // Run query
        helpProcess(plan, dataManager, expected);
    }

    @Test public void testCorrelatedSubquery3c() {
        String sql = "Select e1, (select e2 FROM pm2.g1 Y WHERE X.e4 = Y.e4) from pm1.g1 X"; //$NON-NLS-1$

        // Create expected results
        List[] expected = new List[] {
            Arrays.asList(new Object[] { "a", new Integer(0) }), //$NON-NLS-1$
            Arrays.asList(new Object[] { "b", null }), //$NON-NLS-1$
            Arrays.asList(new Object[] { "c", null }) //$NON-NLS-1$
        };

        // Construct data manager with data
        FakeDataManager dataManager = new FakeDataManager();
        sampleData2(dataManager);

        // Plan query
        ProcessorPlan plan = helpGetPlan(sql, RealMetadataFactory.example1Cached());

        // Run query
        helpProcess(plan, dataManager, expected);
    }
    
    @Test public void testCorrelatedSubquery4() {
        String sql = "Select e1, e2 from pm1.g1 X where e2 in (select e2 FROM pm2.g1 WHERE X.e4 = pm2.g1.e4)"; //$NON-NLS-1$

        // Create expected results
        List[] expected = new List[] {
            Arrays.asList(new Object[] { "a", new Integer(0) }) //$NON-NLS-1$
        };

        // Construct data manager with data
        FakeDataManager dataManager = new FakeDataManager();
        sampleData2(dataManager);

        // Plan query
        ProcessorPlan plan = helpGetPlan(sql, RealMetadataFactory.example1Cached());

        // Run query
        helpProcess(plan, dataManager, expected);
    }    

    @Test public void testCorrelatedSubquery4a() {
        String sql = "Select e1, e2 from pm1.g1 X where e2 = some (select e2 FROM pm2.g1 WHERE X.e4 = pm2.g1.e4)"; //$NON-NLS-1$

        // Create expected results
        List[] expected = new List[] {
            Arrays.asList(new Object[] { "a", new Integer(0) }) //$NON-NLS-1$
        };

        // Construct data manager with data
        FakeDataManager dataManager = new FakeDataManager();
        sampleData2(dataManager);

        // Plan query
        ProcessorPlan plan = helpGetPlan(sql, RealMetadataFactory.example1Cached());

        // Run query
        helpProcess(plan, dataManager, expected);
    }   

    @Test public void testCorrelatedSubquery_defect9968() {
        String sql = "Select e1, e2 from pm1.g1 X where e2 in (select max(X.e2) FROM pm2.g1 WHERE X.e4 = pm2.g1.e4)"; //$NON-NLS-1$

        // Create expected results
        List[] expected = new List[] {
            Arrays.asList(new Object[] { "a", new Integer(0) }) //$NON-NLS-1$
        };

        // Construct data manager with data
        FakeDataManager dataManager = new FakeDataManager();
        sampleData2(dataManager);

        // Plan query
        ProcessorPlan plan = helpGetPlan(sql, RealMetadataFactory.example1Cached());

        // Run query
        helpProcess(plan, dataManager, expected);
    } 

    @Test public void testCorrelatedSubquery_defect9968a() {
        String sql = "Select e1, e2 from pm1.g1 X where e2 in (select ((X.e2)/2) as e FROM pm2.g1 WHERE X.e4 = pm2.g1.e4)"; //$NON-NLS-1$

        // Create expected results
        List[] expected = new List[] {
            Arrays.asList(new Object[] { "a", new Integer(0) }) //$NON-NLS-1$
        };

        // Construct data manager with data
        FakeDataManager dataManager = new FakeDataManager();
        sampleData2(dataManager);

        // Plan query
        ProcessorPlan plan = helpGetPlan(sql, RealMetadataFactory.example1Cached());

        // Run query
        helpProcess(plan, dataManager, expected);
    } 

    @Test public void testCorrelatedSubquery_defect9968b() {
        String sql = "Select e1, e2 from pm1.g1 X where e2 in (select (select e2 as e FROM pm2.g1 WHERE X.e4 = pm2.g1.e4) as e FROM pm2.g1)"; //$NON-NLS-1$

        // Create expected results
        List[] expected = new List[] {
            Arrays.asList(new Object[] { "a", new Integer(0) }) //$NON-NLS-1$
        };

        // Construct data manager with data
        FakeDataManager dataManager = new FakeDataManager();
        sampleData2(dataManager);

        // Plan query
        ProcessorPlan plan = helpGetPlan(sql, RealMetadataFactory.example1Cached());

        // Run query
        helpProcess(plan, dataManager, expected);
    } 

    @Test public void testCorrelatedSubquery_defect10021() {
        String sql = "Select e1, e2 from table1 X where e4 in (select max(Y.e4) FROM table1 Y WHERE X.e4 = Y.e4)"; //$NON-NLS-1$

        // Create expected results
        List[] expected = new List[] {
            Arrays.asList(new Object[] { "a", new Integer(0) }), //$NON-NLS-1$
            Arrays.asList(new Object[] { "c", new Integer(2) }) //$NON-NLS-1$
        };

        // Construct data manager with data
        FakeDataManager dataManager = new FakeDataManager();
        sampleData2(dataManager);

        // Plan query
        ProcessorPlan plan = helpGetPlan(sql, RealMetadataFactory.example1Cached());

        // Run query
        helpProcess(plan, dataManager, expected);
    } 
    
    /** 
     * Note the subquery has a multi-column result - conceptually this is
     * legal for the EXISTS predicate
     */
    @Test public void testCorrelatedSubquery5() {
        String sql = "Select * from pm1.g1 where exists (select * FROM pm2.g1 WHERE pm1.g1.e1 = e1)"; //$NON-NLS-1$

        // Create expected results
        List[] expected = new List[]{
            Arrays.asList(new Object[] { "b",   new Integer(1),     Boolean.TRUE,   null }) //$NON-NLS-1$
        };

        // Construct data manager with data
        FakeDataManager dataManager = new FakeDataManager();
        sampleData2(dataManager);

        // Plan query
        ProcessorPlan plan = helpGetPlan(sql, RealMetadataFactory.example1Cached());

        // Run query
        helpProcess(plan, dataManager, expected);
    }       

    /** 
     * Count the # of parent rows for which no child rows exist
     */
    @Test public void testCorrelatedSubquery6() {
        String sql = "Select count(*) from pm1.g1 where not (exists (select * FROM pm2.g1 WHERE pm1.g1.e1 = e1))"; //$NON-NLS-1$

        // Create expected results
        List[] expected = new List[]{
            Arrays.asList(new Object[] { new Integer(2) })
        };

        // Construct data manager with data
        FakeDataManager dataManager = new FakeDataManager();
        sampleData2(dataManager);

        // Plan query
        ProcessorPlan plan = helpGetPlan(sql, RealMetadataFactory.example1Cached());

        // Run query
        helpProcess(plan, dataManager, expected);
    }
    
    /** 
     * Select e2, e4, and the avg of e4 for each group of e1
     */
    @Test public void testCorrelatedSubquery7() {
        String sql = "select e2, e4, (select avg(e4) FROM pm1.g1 Y WHERE X.e1 = e1) from pm1.g1 X"; //$NON-NLS-1$

        // Create expected results
        List[] expected = new List[]{
            Arrays.asList(new Object[] { new Integer(0), new Double(2.0), new Double(3.6666666666666665) }),
            Arrays.asList(new Object[] { new Integer(1), new Double(1.0), null }),
            Arrays.asList(new Object[] { new Integer(3), new Double(7.0), new Double(3.6666666666666665) }),
            Arrays.asList(new Object[] { new Integer(1), null ,           null }),
            Arrays.asList(new Object[] { new Integer(2), new Double(0.0), new Double(0.0) }),
            Arrays.asList(new Object[] { new Integer(0), new Double(2.0), new Double(3.6666666666666665) })
        };

        // Construct data manager with data
        FakeDataManager dataManager = new FakeDataManager();
        sampleData1(dataManager);

        // Plan query
        ProcessorPlan plan = helpGetPlan(sql, RealMetadataFactory.example1Cached());

        // Run query
        helpProcess(plan, dataManager, expected);
    }    

    /** 
     * Select e2, e4, and the avg of e4 for each group of e1
     */
    @Test public void testCorrelatedSubquery8() {
        String sql = "select X.e2, X.e4, (select avg(Y.e4) FROM pm1.g1 Y WHERE X.e1 = Y.e1) from pm1.g1 X"; //$NON-NLS-1$

        // Create expected results
        List[] expected = new List[]{
            Arrays.asList(new Object[] { new Integer(0), new Double(2.0), new Double(3.6666666666666665) }),
            Arrays.asList(new Object[] { new Integer(1), new Double(1.0), null }),
            Arrays.asList(new Object[] { new Integer(3), new Double(7.0), new Double(3.6666666666666665) }),
            Arrays.asList(new Object[] { new Integer(1), null ,           null }),
            Arrays.asList(new Object[] { new Integer(2), new Double(0.0), new Double(0.0) }),
            Arrays.asList(new Object[] { new Integer(0), new Double(2.0), new Double(3.6666666666666665) })
        };

        // Construct data manager with data
        FakeDataManager dataManager = new FakeDataManager();
        sampleData1(dataManager);

        // Plan query
        ProcessorPlan plan = helpGetPlan(sql, RealMetadataFactory.example1Cached());

        // Run query
        helpProcess(plan, dataManager, expected);
    }   

    @Test public void testCorrelatedSubqueryVirtualLayer1() {
        String sql = "Select e1, e2 from vm1.g1 X where e2 in (select e2 FROM vm1.g1 Y WHERE X.e4 = Y.e4)"; //$NON-NLS-1$

        // Create expected results
        List[] expected = new List[] {
            Arrays.asList(new Object[] { "a",   new Integer(0) }), //$NON-NLS-1$
            Arrays.asList(new Object[] { null,  new Integer(1) }),
            Arrays.asList(new Object[] { "a",   new Integer(3) }), //$NON-NLS-1$
            Arrays.asList(new Object[] { "b",   new Integer(2) }), //$NON-NLS-1$
            Arrays.asList(new Object[] { "a",   new Integer(0) }) //$NON-NLS-1$
        };

        // Construct data manager with data
        FakeDataManager dataManager = new FakeDataManager();
        sampleData1(dataManager);

        // Plan query
        ProcessorPlan plan = helpGetPlan(sql, RealMetadataFactory.example1Cached());

        // Run query
        helpProcess(plan, dataManager, expected);

    }

    @Test public void testCorrelatedSubqueryVirtualLayer2() {
        String sql = "Select e1, e2 from vm1.g2 where e2 in (select e2 FROM vm1.g1 WHERE vm1.g2.e4 = vm1.g1.e4) order by e1, e2"; //$NON-NLS-1$

        // Create expected results
        List[] expected = new List[] {
            Arrays.asList(new Object[] { "a",   new Integer(0) }), //$NON-NLS-1$
            Arrays.asList(new Object[] { "a",   new Integer(0) }), //$NON-NLS-1$
            Arrays.asList(new Object[] { "a",   new Integer(0) }), //$NON-NLS-1$
            Arrays.asList(new Object[] { "a",   new Integer(0) }), //$NON-NLS-1$
            Arrays.asList(new Object[] { "a",   new Integer(3) }), //$NON-NLS-1$
            Arrays.asList(new Object[] { "b",   new Integer(2) }) //$NON-NLS-1$
        };

        // Construct data manager with data
        FakeDataManager dataManager = new FakeDataManager();
        sampleData1(dataManager);

        // Plan query
        ProcessorPlan plan = helpGetPlan(sql, RealMetadataFactory.example1Cached());

        // Run query
        helpProcess(plan, dataManager, expected);

    }

    @Test public void testCorrelatedSubqueryVirtualLayer3() {
        String sql = "Select e2 from vm1.g6 where e2 in (select e2 FROM vm1.g1 WHERE vm1.g6.e = e1)"; //$NON-NLS-1$

        // Create expected results
        List[] expected = new List[0];

        // Construct data manager with data
        FakeDataManager dataManager = new FakeDataManager();
        sampleData1(dataManager);

        // Plan query
        ProcessorPlan plan = helpGetPlan(sql, RealMetadataFactory.example1Cached());

        // Run query
        helpProcess(plan, dataManager, expected);
    }

    @Test public void testCorrelatedSubqueryVirtualLayer4() {
        String sql = "Select e2 from vm1.g7 where e2 in (select e2 FROM vm1.g1 WHERE vm1.g7.e = e1)"; //$NON-NLS-1$

        // Create expected results
        List[] expected = new List[0];

        // Construct data manager with data
        FakeDataManager dataManager = new FakeDataManager();
        sampleData1(dataManager);

        // Plan query
        ProcessorPlan plan = helpGetPlan(sql, RealMetadataFactory.example1Cached());

        // Run query
        helpProcess(plan, dataManager, expected);
    }

    @Test public void testCorrelatedSubqueryVirtualLayer5() {
        String sql = "Select e1 from vm1.g4 where not (e1 in (select e1 FROM vm1.g1 WHERE vm1.g4.e1 = e1)) order by e1"; //$NON-NLS-1$

        // Create expected results
        List[] expected = new List[] {
            Arrays.asList(new Object[] { "0" }), //$NON-NLS-1$
            Arrays.asList(new Object[] { "0" }), //$NON-NLS-1$
            Arrays.asList(new Object[] { "1" }), //$NON-NLS-1$
            Arrays.asList(new Object[] { "1" }), //$NON-NLS-1$
            Arrays.asList(new Object[] { "2" }), //$NON-NLS-1$
            Arrays.asList(new Object[] { "3" }) //$NON-NLS-1$
        };
        
        // Construct data manager with data
        FakeDataManager dataManager = new FakeDataManager();
        sampleData1(dataManager);

        // Plan query
        ProcessorPlan plan = helpGetPlan(sql, RealMetadataFactory.example1Cached());

        // Run query
        helpProcess(plan, dataManager, expected);
    }

    @Test public void testCorrelatedSubqueryVirtualLayer6() {
        String sql = "Select e2 from vm1.g1 X where e2 in (select X.e2 FROM vm1.g1)"; //$NON-NLS-1$

        // Create expected results
        List[] expected = new List[] {
            Arrays.asList(new Object[] { new Integer(0) }),
            Arrays.asList(new Object[] { new Integer(1) }),
            Arrays.asList(new Object[] { new Integer(3) }),
            Arrays.asList(new Object[] { new Integer(1) }),
            Arrays.asList(new Object[] { new Integer(2) }),
            Arrays.asList(new Object[] { new Integer(0) })
        };

        // Construct data manager with data
        FakeDataManager dataManager = new FakeDataManager();
        sampleData1(dataManager);

        // Plan query
        ProcessorPlan plan = helpGetPlan(sql, RealMetadataFactory.example1Cached());

        // Run query
        helpProcess(plan, dataManager, expected);
    }

    @Test public void testCorrelatedSubqueryVirtualLayer6a() {
        String sql = "Select e2 from vm1.g1 where e2 in (select vm1.g1.e2 FROM vm1.g5)"; //$NON-NLS-1$

        // Create expected results
        List[] expected = new List[] {
            Arrays.asList(new Object[] { new Integer(0) }),
            Arrays.asList(new Object[] { new Integer(1) }),
            Arrays.asList(new Object[] { new Integer(3) }),
            Arrays.asList(new Object[] { new Integer(1) }),
            Arrays.asList(new Object[] { new Integer(2) }),
            Arrays.asList(new Object[] { new Integer(0) })
        };


        // Construct data manager with data
        FakeDataManager dataManager = new FakeDataManager();
        sampleData1(dataManager);

        // Plan query
        ProcessorPlan plan = helpGetPlan(sql, RealMetadataFactory.example1Cached());

        // Run query
        helpProcess(plan, dataManager, expected);
    }

    @Test public void testCorrelatedSubqueryVirtualLayer6b() {
        String sql = "Select e2 from vm1.g1 where e2 in (select vm1.g1.e2 FROM vm1.g2)"; //$NON-NLS-1$

        // Create expected results
        List[] expected = new List[] {
            Arrays.asList(new Object[] { new Integer(0) }),
            Arrays.asList(new Object[] { new Integer(1) }),
            Arrays.asList(new Object[] { new Integer(3) }),
            Arrays.asList(new Object[] { new Integer(1) }),
            Arrays.asList(new Object[] { new Integer(2) }),
            Arrays.asList(new Object[] { new Integer(0) })
        };

        // Construct data manager with data
        FakeDataManager dataManager = new FakeDataManager();
        sampleData1(dataManager);

        // Plan query
        ProcessorPlan plan = helpGetPlan(sql, RealMetadataFactory.example1Cached());

        // Run query
        helpProcess(plan, dataManager, expected);
    }

    @Test public void testCorrelatedSubqueryVirtualLayer6c() {
        String sql = "Select e2 from vm1.g7 where e2 in (select vm1.g7.e FROM vm1.g1)"; //$NON-NLS-1$

        // Create expected results
        List[] expected = new List[0];

        // Construct data manager with data
        FakeDataManager dataManager = new FakeDataManager();
        sampleData1(dataManager);

        // Plan query
        ProcessorPlan plan = helpGetPlan(sql, RealMetadataFactory.example1Cached());

        // Run query
        helpProcess(plan, dataManager, expected);
    }

    @Test public void testCorrelatedSubqueryVirtualLayer6e() {
        String sql = "Select e2 from vm1.g1 where e2 in (select vm1.g1.e2 FROM vm1.g2a)"; //$NON-NLS-1$

        // Create expected results
        List[] expected = new List[] {
            Arrays.asList(new Object[] { new Integer(0) }),
            Arrays.asList(new Object[] { new Integer(1) }),
            Arrays.asList(new Object[] { new Integer(3) }),
            Arrays.asList(new Object[] { new Integer(1) }),
            Arrays.asList(new Object[] { new Integer(2) }),
            Arrays.asList(new Object[] { new Integer(0) })
        };


        // Construct data manager with data
        FakeDataManager dataManager = new FakeDataManager();
        sampleData1(dataManager);

        // Plan query
        ProcessorPlan plan = helpGetPlan(sql, RealMetadataFactory.example1Cached());

        // Run query
        helpProcess(plan, dataManager, expected);
    }

    @Test public void testCorrelatedSubqueryVirtualLayer7() {
        String sql = "Select e2 from vm1.g7 where e2 in (select vm1.g7.e FROM vm1.g1 WHERE vm1.g7.e = e1)"; //$NON-NLS-1$

        // Create expected results
        List[] expected = new List[0];

        // Construct data manager with data
        FakeDataManager dataManager = new FakeDataManager();
        sampleData1(dataManager);

        // Plan query
        ProcessorPlan plan = helpGetPlan(sql, RealMetadataFactory.example1Cached());

        // Run query
        helpProcess(plan, dataManager, expected);
    }

    @Test public void testCorrelatedSubqueryInTransformation() {
        String sql = "Select * from vm1.g21"; //$NON-NLS-1$

        // Create expected results
        List[] expected = new List[]{
            Arrays.asList(new Object[] { "b",   new Integer(1),     Boolean.TRUE,   null }) //$NON-NLS-1$
        };

        // Construct data manager with data
        FakeDataManager dataManager = new FakeDataManager();
        sampleData2(dataManager);

        // Plan query
        ProcessorPlan plan = helpGetPlan(sql, RealMetadataFactory.example1Cached());

        // Run query
        helpProcess(plan, dataManager, expected);
    } 
    
    @Test public void testCorrelatedSubqueryInTransformation2() {
        String sql = "Select * from vm1.g20"; //$NON-NLS-1$

        // Create expected results
        List[] expected = new List[]{
            Arrays.asList(new Object[] { "a",   new Integer(0),     Boolean.FALSE,  new Double(2.0) }), //$NON-NLS-1$
            Arrays.asList(new Object[] { "a",   new Integer(3),     Boolean.TRUE,   new Double(7.0) }), //$NON-NLS-1$
            Arrays.asList(new Object[] { "c",   new Integer(1),     Boolean.TRUE,   null }), //$NON-NLS-1$
            Arrays.asList(new Object[] { "b",   new Integer(2),     Boolean.FALSE,  new Double(0.0) }), //$NON-NLS-1$
            Arrays.asList(new Object[] { "a",   new Integer(0),     Boolean.FALSE,  new Double(2.0) }) //$NON-NLS-1$
        };

        // Construct data manager with data
        FakeDataManager dataManager = new FakeDataManager();
        sampleData1(dataManager);

        // Plan query
        ProcessorPlan plan = helpGetPlan(sql, RealMetadataFactory.example1Cached());

        // Run query
        helpProcess(plan, dataManager, expected);
    }    

    @Test public void testCorrelatedSubqueryInTransformation3() {
        String sql = "Select * from vm1.g19 order by e1"; //$NON-NLS-1$

        // Create expected results
        List[] expected = new List[]{
            Arrays.asList(new Object[] { "0" }), //$NON-NLS-1$
            Arrays.asList(new Object[] { "0" }), //$NON-NLS-1$
            Arrays.asList(new Object[] { "1" }), //$NON-NLS-1$
            Arrays.asList(new Object[] { "1" }), //$NON-NLS-1$
            Arrays.asList(new Object[] { "2" }), //$NON-NLS-1$
            Arrays.asList(new Object[] { "3" }) //$NON-NLS-1$
        };

        // Construct data manager with data
        FakeDataManager dataManager = new FakeDataManager();
        sampleData1(dataManager);

        // Plan query
        ProcessorPlan plan = helpGetPlan(sql, RealMetadataFactory.example1Cached());

        // Run query
        helpProcess(plan, dataManager, expected);
    }
    
    /**
     * User correlated subquery, and one of the virtual group transformations
     * also has a correlated subquery
     */
    @Test public void testCorrelatedSubqueryInTransformation4() {
        String sql = "Select * from vm1.g20 where exists (Select * from vm1.g19 where convert(vm1.g20.e2, string) = vm1.g19.e1)"; //$NON-NLS-1$

        // Create expected results
        List[] expected = new List[]{
            Arrays.asList(new Object[] { "a",   new Integer(0),     Boolean.FALSE,  new Double(2.0) }), //$NON-NLS-1$
            Arrays.asList(new Object[] { "a",   new Integer(3),     Boolean.TRUE,   new Double(7.0) }), //$NON-NLS-1$
            Arrays.asList(new Object[] { "c",   new Integer(1),     Boolean.TRUE,   null }), //$NON-NLS-1$
            Arrays.asList(new Object[] { "b",   new Integer(2),     Boolean.FALSE,  new Double(0.0) }), //$NON-NLS-1$
            Arrays.asList(new Object[] { "a",   new Integer(0),     Boolean.FALSE,  new Double(2.0) }) //$NON-NLS-1$
        };

        // Construct data manager with data
        FakeDataManager dataManager = new FakeDataManager();
        sampleData1(dataManager);

        // Plan query
        ProcessorPlan plan = helpGetPlan(sql, RealMetadataFactory.example1Cached());

        // Run query
        helpProcess(plan, dataManager, expected);
    }     

    /**
     * User correlated subquery, and one of the virtual group transformations
     * also has a correlated subquery
     */
    @Test public void testCorrelatedSubqueryInTransformation5() {
        String sql = "Select * from vm1.g19 where exists (Select e2 from vm1.g20 where convert(e2, string) = vm1.g19.e1) order by e1"; //$NON-NLS-1$

        // Create expected results
        List[] expected = new List[]{
            Arrays.asList(new Object[] { "0" }), //$NON-NLS-1$
            Arrays.asList(new Object[] { "0" }), //$NON-NLS-1$
            Arrays.asList(new Object[] { "1" }), //$NON-NLS-1$
            Arrays.asList(new Object[] { "1" }), //$NON-NLS-1$
            Arrays.asList(new Object[] { "2" }), //$NON-NLS-1$
            Arrays.asList(new Object[] { "3" }) //$NON-NLS-1$
        };

        // Construct data manager with data
        FakeDataManager dataManager = new FakeDataManager();
        sampleData1(dataManager);

        // Plan query
        ProcessorPlan plan = helpGetPlan(sql, RealMetadataFactory.example1Cached());

        // Run query
        helpProcess(plan, dataManager, expected);
    }   

    @Test public void testCorrelatedSubqueryInTransformation6() {
        String sql = "Select * from vm1.g21 where e2 = some (Select e2 from pm1.g1 where e1 = vm1.g21.e1)"; //$NON-NLS-1$

        // Create expected results
        List[] expected = new List[]{
            Arrays.asList(new Object[] { "b",   new Integer(1),     Boolean.TRUE,   null }) //$NON-NLS-1$
        };

        // Construct data manager with data
        FakeDataManager dataManager = new FakeDataManager();
        sampleData2(dataManager);

        // Plan query
        ProcessorPlan plan = helpGetPlan(sql, RealMetadataFactory.example1Cached());

        // Run query
        helpProcess(plan, dataManager, expected);
    }   

    @Test public void testCorrelatedSubqueryInTransformation7() {
        String sql = "Select * from vm1.g21 where exists (Select e2 from pm1.g2 where e4 = convert(vm1.g21.e2, double))"; //$NON-NLS-1$

        // Create expected results
        List[] expected = new List[]{
            Arrays.asList(new Object[] { "a",   new Integer(0),     Boolean.FALSE,  new Double(2.0) }), //$NON-NLS-1$
            Arrays.asList(new Object[] { "c",   new Integer(1),     Boolean.TRUE,   null }), //$NON-NLS-1$
            Arrays.asList(new Object[] { "b",   new Integer(2),     Boolean.FALSE,  new Double(0.0) }), //$NON-NLS-1$
            Arrays.asList(new Object[] { "a",   new Integer(0),     Boolean.FALSE,  new Double(2.0) }) //$NON-NLS-1$
        };

        // Construct data manager with data
        FakeDataManager dataManager = new FakeDataManager();
        sampleData1(dataManager);

        // Plan query
        ProcessorPlan plan = helpGetPlan(sql, RealMetadataFactory.example1Cached());

        // Run query
        helpProcess(plan, dataManager, expected);
    }

    @Test public void testCorrelatedSubqueryInTransformation8() {
        String sql = "Select * from vm1.g21 where exists (Select e2 from pm1.g1 where e4 = convert(vm1.g21.e2, double))"; //$NON-NLS-1$

        // Create expected results
        List[] expected = new List[]{
            Arrays.asList(new Object[] { "a",   new Integer(0),     Boolean.FALSE,  new Double(2.0) }), //$NON-NLS-1$
            Arrays.asList(new Object[] { "c",   new Integer(1),     Boolean.TRUE,   null }), //$NON-NLS-1$
            Arrays.asList(new Object[] { "b",   new Integer(2),     Boolean.FALSE,  new Double(0.0) }), //$NON-NLS-1$
            Arrays.asList(new Object[] { "a",   new Integer(0),     Boolean.FALSE,  new Double(2.0) }) //$NON-NLS-1$
        };

        // Construct data manager with data
        FakeDataManager dataManager = new FakeDataManager();
        sampleData1(dataManager);

        // Plan query
        ProcessorPlan plan = helpGetPlan(sql, RealMetadataFactory.example1Cached());

        // Run query
        helpProcess(plan, dataManager, expected);
    }  

    @Test public void testCorrelatedSubqueryInTransformation9() {
        String sql = "Select * from vm1.g22"; //$NON-NLS-1$

        // Create expected results
        List[] expected = new List[]{
            Arrays.asList(new Object[] { "a",   new Integer(0),     Boolean.FALSE,  new Double(2.0),  null }), //$NON-NLS-1$
            Arrays.asList(new Object[] { "a",   new Integer(3),     Boolean.TRUE,   new Double(7.0),  new Double(7.0) }), //$NON-NLS-1$
            Arrays.asList(new Object[] { "c",   new Integer(1),     Boolean.TRUE,   null,             null }),  //$NON-NLS-1$
            Arrays.asList(new Object[] { "b",   new Integer(2),     Boolean.FALSE,  new Double(0.0),  null }), //$NON-NLS-1$
            Arrays.asList(new Object[] { "a",   new Integer(0),     Boolean.FALSE,  new Double(2.0),  null }) //$NON-NLS-1$

        };

        // Construct data manager with data
        FakeDataManager dataManager = new FakeDataManager();
        sampleData1(dataManager);

        // Plan query
        ProcessorPlan plan = helpGetPlan(sql, RealMetadataFactory.example1Cached());

        // Run query
        helpProcess(plan, dataManager, expected);
    }  

    @Test public void testCorrelatedSubqueryInTransformation10() {
        String sql = "Select * from vm1.g23"; //$NON-NLS-1$

        // Create expected results
        List[] expected = new List[]{
            Arrays.asList(new Object[] { "a",   new Integer(0),     Boolean.FALSE,  new Double(2.0),  null }), //$NON-NLS-1$
            Arrays.asList(new Object[] { "a",   new Integer(3),     Boolean.TRUE,   new Double(7.0),  new Double(7.0) }), //$NON-NLS-1$
            Arrays.asList(new Object[] { "c",   new Integer(1),     Boolean.TRUE,   null,             null }),  //$NON-NLS-1$
            Arrays.asList(new Object[] { "b",   new Integer(2),     Boolean.FALSE,  new Double(0.0),  null }), //$NON-NLS-1$
            Arrays.asList(new Object[] { "a",   new Integer(0),     Boolean.FALSE,  new Double(2.0),  null }) //$NON-NLS-1$
        };

        // Construct data manager with data
        FakeDataManager dataManager = new FakeDataManager();
        sampleData1(dataManager);

        // Plan query
        ProcessorPlan plan = helpGetPlan(sql, RealMetadataFactory.example1Cached());

        // Run query
        helpProcess(plan, dataManager, expected);
    }  

    @Test public void testCorrelatedSubqueryInTransformation11() {
        String sql = "Select * from vm1.g24"; //$NON-NLS-1$

        // Create expected results
        List[] expected = new List[]{
            Arrays.asList(new Object[] { "a",   new Integer(0),     Boolean.FALSE,  new Double(2.0) }), //$NON-NLS-1$
            Arrays.asList(new Object[] { "a",   new Integer(3),     Boolean.TRUE,   new Double(7.0) }), //$NON-NLS-1$
//            Arrays.asList(new Object[] { "c",   new Integer(1),     Boolean.TRUE,   null }),
            Arrays.asList(new Object[] { "b",   new Integer(2),     Boolean.FALSE,  new Double(0.0) }), //$NON-NLS-1$
            Arrays.asList(new Object[] { "a",   new Integer(0),     Boolean.FALSE,  new Double(2.0) }) //$NON-NLS-1$
        };

        // Construct data manager with data
        FakeDataManager dataManager = new FakeDataManager();
        sampleData1(dataManager);

        // Plan query
        ProcessorPlan plan = helpGetPlan(sql, RealMetadataFactory.example1Cached());

        // Run query
        helpProcess(plan, dataManager, expected);
    }  

    @Test public void testCorrelatedSubqueryInTransformation12() {
        String sql = "Select * from vm1.g24 X where exists (Select * from vm1.g24 Y where X.e2 = Y.e2)"; //$NON-NLS-1$

        // Create expected results
        List[] expected = new List[]{
            Arrays.asList(new Object[] { "a",   new Integer(0),     Boolean.FALSE,  new Double(2.0) }), //$NON-NLS-1$
            Arrays.asList(new Object[] { "a",   new Integer(3),     Boolean.TRUE,   new Double(7.0) }), //$NON-NLS-1$
//            Arrays.asList(new Object[] { "c",   new Integer(1),     Boolean.TRUE,   null }),
            Arrays.asList(new Object[] { "b",   new Integer(2),     Boolean.FALSE,  new Double(0.0) }), //$NON-NLS-1$
            Arrays.asList(new Object[] { "a",   new Integer(0),     Boolean.FALSE,  new Double(2.0) }) //$NON-NLS-1$
        };

        // Construct data manager with data
        FakeDataManager dataManager = new FakeDataManager();
        sampleData1(dataManager);

        // Plan query
        ProcessorPlan plan = helpGetPlan(sql, RealMetadataFactory.example1Cached());

        // Run query
        helpProcess(plan, dataManager, expected);
    }  

    @Test public void testCorrelatedSubqueryInTransformation13() {
        String sql = "Select e1, e2, e3, e4, (select e4 from vm1.g25 where pm1.g1.e4 = e5 and e4=0.0) as e5 from pm1.g1"; //$NON-NLS-1$

        // Create expected results
        List[] expected = new List[]{
            Arrays.asList(new Object[] { "a",   new Integer(0),     Boolean.FALSE,  new Double(2.0),  null }), //$NON-NLS-1$
            Arrays.asList(new Object[] { null,  new Integer(1),     Boolean.FALSE,  new Double(1.0),  null }),
            Arrays.asList(new Object[] { "a",   new Integer(3),     Boolean.TRUE,   new Double(7.0),  null }), //$NON-NLS-1$
            Arrays.asList(new Object[] { "c",   new Integer(1),     Boolean.TRUE,   null,             null }), //$NON-NLS-1$
            Arrays.asList(new Object[] { "b",   new Integer(2),     Boolean.FALSE,  new Double(0.0),  new Double(0.0) }), //$NON-NLS-1$
            Arrays.asList(new Object[] { "a",   new Integer(0),     Boolean.FALSE,  new Double(2.0),  null }) //$NON-NLS-1$
        };

        // Construct data manager with data
        FakeDataManager dataManager = new FakeDataManager();
        sampleData1(dataManager);

        // Plan query
        ProcessorPlan plan = helpGetPlan(sql, RealMetadataFactory.example1Cached());

        // Run query
        helpProcess(plan, dataManager, expected);
    } 
    
    @Test public void testCorrelatedSubqueryInTransformation14() {
        String sql = "Select e1, e2, e3, e4, (select e4 from vm1.g26 where pm1.g1.e4 = e5 and e4=0.0) as e5 from pm1.g1"; //$NON-NLS-1$

        // Create expected results
        List[] expected = new List[]{
            Arrays.asList(new Object[] { "a",   new Integer(0),     Boolean.FALSE,  new Double(2.0),  null }), //$NON-NLS-1$
            Arrays.asList(new Object[] { null,  new Integer(1),     Boolean.FALSE,  new Double(1.0),  null }),
            Arrays.asList(new Object[] { "a",   new Integer(3),     Boolean.TRUE,   new Double(7.0),  null }), //$NON-NLS-1$
            Arrays.asList(new Object[] { "c",   new Integer(1),     Boolean.TRUE,   null,             null }), //$NON-NLS-1$
            Arrays.asList(new Object[] { "b",   new Integer(2),     Boolean.FALSE,  new Double(0.0),  new Double(0.0) }), //$NON-NLS-1$
            Arrays.asList(new Object[] { "a",   new Integer(0),     Boolean.FALSE,  new Double(2.0),  null }) //$NON-NLS-1$
        };

        // Construct data manager with data
        FakeDataManager dataManager = new FakeDataManager();
        sampleData1(dataManager);

        // Plan query
        ProcessorPlan plan = helpGetPlan(sql, RealMetadataFactory.example1Cached());

        // Run query
        helpProcess(plan, dataManager, expected);
    }     

    @Test public void testCorrelatedSubqueryInTransformation15() {
        String sql = "Select e1, e2, e3, e4, (select e4 from vm1.g23 where vm1.g22.e4 = e5) as e5 from vm1.g22 where e1 = 'a'"; //$NON-NLS-1$

        // Create expected results
        List[] expected = new List[]{
            Arrays.asList(new Object[] { "a",   new Integer(0),     Boolean.FALSE,  new Double(2.0),  null }), //$NON-NLS-1$
            Arrays.asList(new Object[] { "a",   new Integer(3),     Boolean.TRUE,   new Double(7.0),  new Double(7.0) }), //$NON-NLS-1$
            Arrays.asList(new Object[] { "a",   new Integer(0),     Boolean.FALSE,  new Double(2.0),  null }) //$NON-NLS-1$
        };

        // Construct data manager with data
        FakeDataManager dataManager = new FakeDataManager();
        sampleData1(dataManager);

        // Plan query
        ProcessorPlan plan = helpGetPlan(sql, RealMetadataFactory.example1Cached());

        // Run query
        helpProcess(plan, dataManager, expected);
    }  

    /** Test selecting a virtual element (e5) which is defined by a scalar subquery in the virtual transformation */
    @Test public void testCorrelatedSubqueryInTransformation15a() {
        String sql = "Select e1, e2, e3, e4 from pm1.g1 where exists (select * from vm1.g26 where pm1.g1.e3 = e3)"; //$NON-NLS-1$

        // Create expected results
        List[] expected = new List[]{
            Arrays.asList(new Object[] { "a",   new Integer(0),     Boolean.FALSE,  new Double(2.0) }), //$NON-NLS-1$
            Arrays.asList(new Object[] { null,  new Integer(1),     Boolean.FALSE,  new Double(1.0) }),
            Arrays.asList(new Object[] { "a",   new Integer(3),     Boolean.TRUE,   new Double(7.0) }), //$NON-NLS-1$
            Arrays.asList(new Object[] { "c",   new Integer(1),     Boolean.TRUE,   null,           }), //$NON-NLS-1$
            Arrays.asList(new Object[] { "b",   new Integer(2),     Boolean.FALSE,  new Double(0.0) }), //$NON-NLS-1$
            Arrays.asList(new Object[] { "a",   new Integer(0),     Boolean.FALSE,  new Double(2.0) }) //$NON-NLS-1$
        };

        // Construct data manager with data
        FakeDataManager dataManager = new FakeDataManager();
        sampleData1(dataManager);

        // Plan query
        ProcessorPlan plan = helpGetPlan(sql, RealMetadataFactory.example1Cached());

        // Run query
        helpProcess(plan, dataManager, expected);
    }  

    @Test public void testCorrelatedSubqueryInTransformation15b() {
        String sql = "Select e1, e2, e3, e4, (select e4 from vm1.g23 where vm1.g22.e4 = e5) as e5 from vm1.g22 where e1 = 'a' and exists (select * from vm1.g23 where vm1.g22.e3 = e3)"; //$NON-NLS-1$

        // Create expected results
        List[] expected = new List[]{
            Arrays.asList(new Object[] { "a",   new Integer(0),     Boolean.FALSE,  new Double(2.0),  null }), //$NON-NLS-1$
            Arrays.asList(new Object[] { "a",   new Integer(3),     Boolean.TRUE,   new Double(7.0),  new Double(7.0) }), //$NON-NLS-1$
            Arrays.asList(new Object[] { "a",   new Integer(0),     Boolean.FALSE,  new Double(2.0),  null }) //$NON-NLS-1$
        };

        // Construct data manager with data
        FakeDataManager dataManager = new FakeDataManager();
        sampleData1(dataManager);

        // Plan query
        ProcessorPlan plan = helpGetPlan(sql, RealMetadataFactory.example1Cached());

        // Run query
        helpProcess(plan, dataManager, expected);
    }  

    /** Test selecting a virtual element (e5) which is defined by a scalar subquery in the virtual transformation */
    @Test public void testCorrelatedSubqueryInTransformation15c() {
        String sql = "Select e1, e2, e3, e4 from pm1.g1 where exists (select e1, e2, e3, e4, e5 as e from vm1.g26 where pm1.g1.e3 = e3)"; //$NON-NLS-1$

        // Create expected results
        List[] expected = new List[]{
            Arrays.asList(new Object[] { "a",   new Integer(0),     Boolean.FALSE,  new Double(2.0) }), //$NON-NLS-1$
            Arrays.asList(new Object[] { null,  new Integer(1),     Boolean.FALSE,  new Double(1.0) }),
            Arrays.asList(new Object[] { "a",   new Integer(3),     Boolean.TRUE,   new Double(7.0) }), //$NON-NLS-1$
            Arrays.asList(new Object[] { "c",   new Integer(1),     Boolean.TRUE,   null,           }), //$NON-NLS-1$
            Arrays.asList(new Object[] { "b",   new Integer(2),     Boolean.FALSE,  new Double(0.0) }), //$NON-NLS-1$
            Arrays.asList(new Object[] { "a",   new Integer(0),     Boolean.FALSE,  new Double(2.0) }) //$NON-NLS-1$
        };

        // Construct data manager with data
        FakeDataManager dataManager = new FakeDataManager();
        sampleData1(dataManager);

        // Plan query
        ProcessorPlan plan = helpGetPlan(sql, RealMetadataFactory.example1Cached());

        // Run query
        helpProcess(plan, dataManager, expected);
    }   

    /** Test selecting a virtual element (e5) which is defined by a scalar subquery in the virtual transformation */
    @Test public void testCorrelatedSubqueryInTransformation15d() {
        String sql = "Select e1, e2, e3, e4 from pm1.g1 where exists (select e1, e2, e3, e4, ((e4 + e5)/(e4 + 1)) as e from vm1.g26 where pm1.g1.e3 = e3)"; //$NON-NLS-1$

        // Create expected results
        List[] expected = new List[]{
            Arrays.asList(new Object[] { "a",   new Integer(0),     Boolean.FALSE,  new Double(2.0) }), //$NON-NLS-1$
            Arrays.asList(new Object[] { null,  new Integer(1),     Boolean.FALSE,  new Double(1.0) }),
            Arrays.asList(new Object[] { "a",   new Integer(3),     Boolean.TRUE,   new Double(7.0) }), //$NON-NLS-1$
            Arrays.asList(new Object[] { "c",   new Integer(1),     Boolean.TRUE,   null,           }), //$NON-NLS-1$
            Arrays.asList(new Object[] { "b",   new Integer(2),     Boolean.FALSE,  new Double(0.0) }), //$NON-NLS-1$
            Arrays.asList(new Object[] { "a",   new Integer(0),     Boolean.FALSE,  new Double(2.0) }) //$NON-NLS-1$
        };

        // Construct data manager with data
        FakeDataManager dataManager = new FakeDataManager();
        sampleData1(dataManager);

        // Plan query
        ProcessorPlan plan = helpGetPlan(sql, RealMetadataFactory.example1Cached());

        // Run query
        helpProcess(plan, dataManager, expected);
    }

// Here is select * from vm1.g26
//  Arrays.asList(new Object[] { "a",   new Integer(0),     Boolean.FALSE,  new Double(2.0),  null }),
//  Arrays.asList(new Object[] { null,  new Integer(1),     Boolean.FALSE,  new Double(1.0),  null }),
//  Arrays.asList(new Object[] { "a",   new Integer(3),     Boolean.TRUE,   new Double(7.0),  null }),
//  Arrays.asList(new Object[] { "c",   new Integer(1),     Boolean.TRUE,   null,             null }),
//  Arrays.asList(new Object[] { "b",   new Integer(2),     Boolean.FALSE,  new Double(0.0),  new Double(0.0) }),
//  Arrays.asList(new Object[] { "a",   new Integer(0),     Boolean.FALSE,  new Double(2.0),  null })


    @Test public void testCorrelatedSubqueryInTransformation16() {
//        String sql = "Select e1, e2, e3, e4, (select e4 from vm1.g23 where vm1.g22.e4 = e5) as e5 from vm1.g22 where e1 = 'a'"/* and exists (select * from vm1.g23 where vm1.g22.e3 = e3)"*/;
        String sql = "select * from vm1.g26 where e5 = 0.0"; //$NON-NLS-1$

        // Create expected results
        List[] expected = new List[]{
//            Arrays.asList(new Object[] { "a",   new Integer(0),     Boolean.FALSE,  new Double(2.0),  null }),
//            Arrays.asList(new Object[] { null,  new Integer(1),     Boolean.FALSE,  new Double(1.0),  null }),
//            Arrays.asList(new Object[] { "a",   new Integer(3),     Boolean.TRUE,   new Double(7.0),  null }),
//            Arrays.asList(new Object[] { "c",   new Integer(1),     Boolean.TRUE,   null,             null }),
            Arrays.asList(new Object[] { "b",   new Integer(2),     Boolean.FALSE,  new Double(0.0),  new Double(0.0) }), //$NON-NLS-1$
//            Arrays.asList(new Object[] { "a",   new Integer(0),     Boolean.FALSE,  new Double(2.0),  null })
        };

        // Construct data manager with data
        FakeDataManager dataManager = new FakeDataManager();
        sampleData1(dataManager);

        // Plan query
        ProcessorPlan plan = helpGetPlan(sql, RealMetadataFactory.example1Cached());

        // Run query
        helpProcess(plan, dataManager, expected);
    }  

    @Test public void testCorrelatedSubqueriesNested() {
        String sql = 
                    "Select e1, e2, e4 from pm1.g1 where e2 < all (" + //$NON-NLS-1$
                    "select e2 from pm1.g2 where pm1.g1.e1 = e1 and exists("+ //$NON-NLS-1$
                    "select e2 from pm2.g1 where pm1.g2.e4 = pm2.g1.e4))"; //$NON-NLS-1$
        Command query = helpParse(sql);

        // Create expected results
        List[] expected = new List[] {
            Arrays.asList(new Object[] { "a", new Integer(0), new Double(2.0) }), //$NON-NLS-1$
            Arrays.asList(new Object[] { "b", new Integer(1), null }), //$NON-NLS-1$
            Arrays.asList(new Object[] { "c", new Integer(2), new Double(0.0) }) //$NON-NLS-1$
        };

        // Construct data manager with data
        FakeDataManager dataManager = new FakeDataManager();
        sampleData2(dataManager);

        // Plan query
        ProcessorPlan plan = helpGetPlan(query, RealMetadataFactory.example1Cached());

        // Run query
        helpProcess(plan, dataManager, expected);
    }

    /** defect 15124 */
    @Test public void testCorrelatedSubqueryAndInlineView() {
        String sql = "Select e1, (select e2 FROM pm2.g1 Y WHERE X.e4 = Y.e4) from (select * from pm1.g1) as X"; //$NON-NLS-1$

        // Create expected results
        List[] expected = new List[] {
            Arrays.asList(new Object[] { "a", new Integer(0) }), //$NON-NLS-1$
            Arrays.asList(new Object[] { "b", null }), //$NON-NLS-1$
            Arrays.asList(new Object[] { "c", null }) //$NON-NLS-1$
        };

        // Construct data manager with data
        FakeDataManager dataManager = new FakeDataManager();
        sampleData2(dataManager);

        // Plan query
        ProcessorPlan plan = helpGetPlan(sql, RealMetadataFactory.example1Cached());

        // Run query
        helpProcess(plan, dataManager, expected);        
    }
    
    /** defect 15124 */
    @Test public void testCorrelatedSubqueryAndInlineView2() {
        String sql = "Select e1 from (select * from pm1.g1) as X WHERE e2 IN (select e2 FROM pm2.g1 Y WHERE X.e4 = Y.e4)"; //$NON-NLS-1$

        // Create expected results
        List[] expected = new List[] {
            Arrays.asList(new Object[] { "a" }) //$NON-NLS-1$
        };

        // Construct data manager with data
        FakeDataManager dataManager = new FakeDataManager();
        sampleData2(dataManager);

        // Plan query
        ProcessorPlan plan = helpGetPlan(sql, RealMetadataFactory.example1Cached());

        // Run query
        helpProcess(plan, dataManager, expected);        
    }    

    /** defect 15124 */
    @Test public void testCorrelatedSubqueryAndInlineView3() {
        String sql = "Select e1, (select e2 FROM pm2.g1 Y WHERE X.e4 = Y.e4) from (select * from pm1.g1 UNION ALL select * from pm1.g1) as X"; //$NON-NLS-1$

        // Create expected results
        List[] expected = new List[] {
            Arrays.asList(new Object[] { "a", new Integer(0) }), //$NON-NLS-1$
            Arrays.asList(new Object[] { "b", null }), //$NON-NLS-1$
            Arrays.asList(new Object[] { "c", null }), //$NON-NLS-1$
            Arrays.asList(new Object[] { "a", new Integer(0) }), //$NON-NLS-1$
            Arrays.asList(new Object[] { "b", null }), //$NON-NLS-1$
            Arrays.asList(new Object[] { "c", null }) //$NON-NLS-1$
        };

        // Construct data manager with data
        FakeDataManager dataManager = new FakeDataManager();
        sampleData2(dataManager);

        // Plan query
        ProcessorPlan plan = helpGetPlan(sql, RealMetadataFactory.example1Cached());

        // Run query
        helpProcess(plan, dataManager, expected);        
    }    

    /** defect 15124 */
    @Test public void testCorrelatedSubqueryAndInlineView4() {
        String sql = "Select e1, (select e2 FROM pm2.g1 Y WHERE X.e4 = Y.e4) from (select pm1.g1.e1, (select pm1.g1.e2 from pm1.g1 Z where pm1.g1.e1 = Z.e1), pm1.g1.e3, pm1.g1.e4 from pm1.g1) as X"; //$NON-NLS-1$

        // Create expected results
        List[] expected = new List[] {
            Arrays.asList(new Object[] { "a", new Integer(0) }), //$NON-NLS-1$
            Arrays.asList(new Object[] { "b", null }), //$NON-NLS-1$
            Arrays.asList(new Object[] { "c", null }), //$NON-NLS-1$
        };

        // Construct data manager with data
        FakeDataManager dataManager = new FakeDataManager();
        sampleData2(dataManager);

        // Plan query
        ProcessorPlan plan = helpGetPlan(sql, RealMetadataFactory.example1Cached());

        // Run query
        helpProcess(plan, dataManager, expected);        
    }    
    
    @Test public void testXMLUnion_defect8373() {
        // Create query
        String sql = "SELECT * FROM xmltest.doc5"; //$NON-NLS-1$

        // Create expected results
        List[] expected = new List[] {
            Arrays.asList(new Object[] {
                "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" + //$NON-NLS-1$
                "<root><node1><node2>a</node2></node1><node1><node2/></node1><node1><node2>a</node2></node1><node1><node2>c</node2></node1><node1><node2>b</node2></node1><node1><node2>a</node2></node1><node1><node2>a</node2></node1><node1><node2/></node1><node1><node2>a</node2></node1><node1><node2>c</node2></node1><node1><node2>b</node2></node1><node1><node2>a</node2></node1></root>" //$NON-NLS-1$
            })
        };

        // Construct data manager with data
        FakeDataManager dataManager = new FakeDataManager();
        sampleData1(dataManager);

        // Plan query
        ProcessorPlan plan = helpGetPlan(sql, RealMetadataFactory.example1Cached());

        // Run query
        helpProcess(plan, dataManager, expected);
    }

    @Test public void testStoredQuery1() {
        // Create query
        String sql = "EXEC pm1.sq1()"; //$NON-NLS-1$

        // Create expected results
        List[] expected = SP1RS;    

        // Construct data manager with data
        FakeDataManager dataManager = new FakeDataManager();
        sampleData1(dataManager);

        // Plan query
        ProcessorPlan plan = helpGetPlan(sql, RealMetadataFactory.example1Cached());

        // Run query
        helpProcess(plan, dataManager, expected);
    }
    
    @Test public void testStoredQuery2() {
        // Create query
        String sql = "EXEC pm1.sq2(\'a\')"; //$NON-NLS-1$

        // Create expected results
        List[] expected = new List[] { 
            Arrays.asList(new Object[] { "a",   new Integer(0) }),  //$NON-NLS-1$
            Arrays.asList(new Object[] { "a",   new Integer(3) }),    //$NON-NLS-1$
            Arrays.asList(new Object[] { "a",   new Integer(0) })        //$NON-NLS-1$
        };    

        // Construct data manager with data
        FakeDataManager dataManager = new FakeDataManager();
        sampleData1(dataManager);

        // Plan query
        ProcessorPlan plan = helpGetPlan(sql, RealMetadataFactory.example1Cached());

        // Run query
        helpProcess(plan, dataManager, expected);
    }
        
    @Test public void testStoredQuery3() {
        // Create query
        String sql = "select x.e1 from (EXEC pm1.sq1()) as x"; //$NON-NLS-1$

        // Create expected results
        List[] expected = new List[] { 
            Arrays.asList(new Object[] { "a"}), //$NON-NLS-1$
            Arrays.asList(new Object[] { null}),
            Arrays.asList(new Object[] { "a" }), //$NON-NLS-1$
            Arrays.asList(new Object[] { "c" }), //$NON-NLS-1$
            Arrays.asList(new Object[] { "b" }), //$NON-NLS-1$
            Arrays.asList(new Object[] { "a" }) //$NON-NLS-1$
        };    

        // Construct data manager with data
        FakeDataManager dataManager = new FakeDataManager();
        sampleData1(dataManager);

        // Plan query
        ProcessorPlan plan = helpGetPlan(sql, RealMetadataFactory.example1Cached());

        // Run query
        helpProcess(plan, dataManager, expected);
    }
    
    @Test public void testStoredQuery4() {
        // Create query
        String sql = "EXEC pm1.sq5('a')"; //$NON-NLS-1$

        // Create expected results
        List[] expected = new List[] { 
            Arrays.asList(new Object[] { "a", new Integer(0)}),             //$NON-NLS-1$
            Arrays.asList(new Object[] { "a", new Integer(3) }),             //$NON-NLS-1$
            Arrays.asList(new Object[] { "a", new Integer(0) }) //$NON-NLS-1$
        };    

        // Construct data manager with data
        FakeDataManager dataManager = new FakeDataManager();
        sampleData1(dataManager);

        // Plan query
        ProcessorPlan plan = helpGetPlan(sql, RealMetadataFactory.example1Cached());

        // Run query
        helpProcess(plan, dataManager, expected);
    }
    
    @Test public void testStoredQuery5() {
        // Create query
        String sql = "EXEC pm1.sp1()"; //$NON-NLS-1$

        // Create expected results
        List[] expected = SP1RS;    

        // Construct data manager with data
        HardcodedDataManager dataManager = new HardcodedDataManager();
        dataManager.addData("EXEC pm1.sp1()", expected);

        // Plan query
        ProcessorPlan plan = helpGetPlan(sql, RealMetadataFactory.example1Cached());

        // Run query
        helpProcess(plan, dataManager, expected);
    }
    
    @Test public void testStoredQuery6() {
        // Create query
        String sql = "EXEC pm1.sqsp1()"; //$NON-NLS-1$

        // Create expected results
        List[] expected = new List[] { 
            Arrays.asList(new Object[] { "a"}), //$NON-NLS-1$
                    Arrays.asList(new Object[] { null}),
                    Arrays.asList(new Object[] { "a" }), //$NON-NLS-1$
                    Arrays.asList(new Object[] { "c"}), //$NON-NLS-1$
                    Arrays.asList(new Object[] { "b"}), //$NON-NLS-1$
                    Arrays.asList(new Object[] { "a" }) //$NON-NLS-1$
        };    

        // Construct data manager with data
        HardcodedDataManager dataManager = new HardcodedDataManager();
        dataManager.addData("EXEC pm1.sp1()", SP1RS);

        // Plan query
        ProcessorPlan plan = helpGetPlan(sql, RealMetadataFactory.example1Cached());

        // Run query
        helpProcess(plan, dataManager, expected);
    }
    
    @Test public void testStoredQuery7() {
        // Create query
        String sql = "EXEC pm1.sq17()"; //$NON-NLS-1$

        // Create expected results
        List[] expected = new List[] { 
            Arrays.asList(new Object[] { 
                "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" + //$NON-NLS-1$
                    "<root><node1><node2><node3/></node2></node1></root>"             //$NON-NLS-1$
            })
        };    

        // Construct data manager with data
        FakeDataManager dataManager = new FakeDataManager();
        sampleData1(dataManager);

        // Plan query
        ProcessorPlan plan = helpGetPlan(sql, RealMetadataFactory.example1Cached());

        // Run query
        helpProcess(plan, dataManager, expected);
    }    

    // implict type conversion of parameter
    @Test public void testStoredQuery8() {
        // Create query
        String sql = "EXEC pm1.sq5(5)"; //$NON-NLS-1$

        // Create expected results
        List[] expected = new List[] {
        };

        // Construct data manager with data
        FakeDataManager dataManager = new FakeDataManager();
        sampleData1(dataManager);

        // Plan query
        ProcessorPlan plan = helpGetPlan(sql, RealMetadataFactory.example1Cached());

        // Run query
        helpProcess(plan, dataManager, expected);
    }

    // function for parameter
    @Test public void testStoredQuery9() {
        // Create query
        String sql = "EXEC pm1.sq5(concat('a', ''))"; //$NON-NLS-1$

        // Create expected results
        List[] expected = new List[] {
            Arrays.asList(new Object[] { "a", new Integer(0)}), //$NON-NLS-1$
            Arrays.asList(new Object[] { "a", new Integer(3) }), //$NON-NLS-1$
            Arrays.asList(new Object[] { "a", new Integer(0) }) //$NON-NLS-1$
        };

        // Construct data manager with data
        FakeDataManager dataManager = new FakeDataManager();
        sampleData1(dataManager);

        // Plan query
        ProcessorPlan plan = helpGetPlan(sql, RealMetadataFactory.example1Cached());

        // Run query
        helpProcess(plan, dataManager, expected);
    }

    /** named parameters */
    @Test public void testStoredQuery10() {
        // Create query
        String sql = "EXEC pm1.sq3b(\"in\" = 'a', in3 = 'something')"; //$NON-NLS-1$

        // Create expected results
        List[] expected = new List[] {
            Arrays.asList(new Object[] { "a", new Integer(0)}), //$NON-NLS-1$
            Arrays.asList(new Object[] { "a", new Integer(3) }), //$NON-NLS-1$
            Arrays.asList(new Object[] { "a", new Integer(0) }) //$NON-NLS-1$
        };

        // Construct data manager with data
        FakeDataManager dataManager = new FakeDataManager();
        sampleData1(dataManager);

        // Plan query
        ProcessorPlan plan = helpGetPlan(sql, RealMetadataFactory.example1Cached());

        // Run query
        helpProcess(plan, dataManager, expected);
    }    
    
    @Test public void testInsert() {
        // Create query
        String sql = "Insert into pm1.g1 (pm1.g1.e1, pm1.g1.e2) values ('MyString', 1)"; //$NON-NLS-1$

        // Create expected results
        List[] expected = new List[] { 
            Arrays.asList(new Object[] { new Integer(1)})
        };    

        // Construct data manager with data
        HardcodedDataManager dataManager = new HardcodedDataManager();
        dataManager.addData("INSERT INTO pm1.g1 (pm1.g1.e1, pm1.g1.e2) VALUES ('MyString', 1)", new List[] {Arrays.asList(1)});

        // Plan query
        ProcessorPlan plan = helpGetPlan(sql, RealMetadataFactory.example1Cached());

        // Run query
        helpProcess(plan, dataManager, expected);
    }

     /** SELECT BQT1.SmallA.IntKey AS SmallA_IntKey, BQT2.MediumB.IntKey AS MediumB_IntKey FROM BQT1.SmallA FULL OUTER JOIN BQT2.MediumB ON BQT1.SmallA.IntKey = BQT2.MediumB.IntKey WHERE (BQT1.SmallA.IntKey >= 0) AND (BQT1.SmallA.IntKey <= 15) AND (BQT2.MediumB.IntKey >= 5) AND (BQT2.MediumB.IntKey <= 20) */
     @Test public void testDefect7770_FullOuter() { 
        // Create query 
        String sql = "SELECT BQT1.SmallA.IntKey AS SmallA_IntKey, BQT2.MediumB.IntKey AS MediumB_IntKey FROM BQT1.SmallA FULL OUTER JOIN BQT2.MediumB ON BQT1.SmallA.IntKey = BQT2.MediumB.IntKey WHERE (BQT1.SmallA.IntKey >= 0) AND (BQT1.SmallA.IntKey <= 15) AND (BQT2.MediumB.IntKey >= 5) AND (BQT2.MediumB.IntKey <= 20)"; //$NON-NLS-1$
        
        // Create expected results
        List[] expected = new List[] { 
            Arrays.asList(new Object[] { new Integer(5), new Integer(5) }),
            Arrays.asList(new Object[] { new Integer(6), new Integer(6) }),
            Arrays.asList(new Object[] { new Integer(7), new Integer(7) }),
            Arrays.asList(new Object[] { new Integer(8), new Integer(8) }),
            Arrays.asList(new Object[] { new Integer(9), new Integer(9) }),
            Arrays.asList(new Object[] { new Integer(10), new Integer(10) }),
            Arrays.asList(new Object[] { new Integer(11), new Integer(11) }),
            Arrays.asList(new Object[] { new Integer(12), new Integer(12) }),
            Arrays.asList(new Object[] { new Integer(13), new Integer(13) }),
            Arrays.asList(new Object[] { new Integer(14), new Integer(14) }),
            Arrays.asList(new Object[] { new Integer(15), new Integer(15) })
        };    
        
        // Construct data manager with data
        FakeDataManager dataManager = new FakeDataManager();
        sampleDataBQT1(dataManager);
        
        // Plan query
        ProcessorPlan plan = helpGetPlan(sql, RealMetadataFactory.exampleBQTCached());

        // Run query
        helpProcess(plan, dataManager, expected);
    }

     /** SELECT BQT1.SmallA.IntKey AS SmallA_IntKey, BQT2.MediumB.IntKey AS MediumB_IntKey FROM BQT1.SmallA RIGHT OUTER JOIN BQT2.MediumB ON BQT1.SmallA.IntKey = BQT2.MediumB.IntKey WHERE (BQT1.SmallA.IntKey >= 0) AND (BQT1.SmallA.IntKey <= 15) AND (BQT2.MediumB.IntKey >= 5) AND (BQT2.MediumB.IntKey <= 20) */
     @Test public void testDefect7770_RightOuter() { 
        // Create query 
        String sql = "SELECT BQT1.SmallA.IntKey AS SmallA_IntKey, BQT2.MediumB.IntKey AS MediumB_IntKey FROM BQT1.SmallA RIGHT OUTER JOIN BQT2.MediumB ON BQT1.SmallA.IntKey = BQT2.MediumB.IntKey WHERE (BQT1.SmallA.IntKey >= 0) AND (BQT1.SmallA.IntKey <= 15) AND (BQT2.MediumB.IntKey >= 5) AND (BQT2.MediumB.IntKey <= 20)"; //$NON-NLS-1$
        
        // Create expected results
        List[] expected = new List[] { 
            Arrays.asList(new Object[] { new Integer(5), new Integer(5) }),
            Arrays.asList(new Object[] { new Integer(6), new Integer(6) }),
            Arrays.asList(new Object[] { new Integer(7), new Integer(7) }),
            Arrays.asList(new Object[] { new Integer(8), new Integer(8) }),
            Arrays.asList(new Object[] { new Integer(9), new Integer(9) }),
            Arrays.asList(new Object[] { new Integer(10), new Integer(10) }),
            Arrays.asList(new Object[] { new Integer(11), new Integer(11) }),
            Arrays.asList(new Object[] { new Integer(12), new Integer(12) }),
            Arrays.asList(new Object[] { new Integer(13), new Integer(13) }),
            Arrays.asList(new Object[] { new Integer(14), new Integer(14) }),
            Arrays.asList(new Object[] { new Integer(15), new Integer(15) })
        };    
        
        // Construct data manager with data
        FakeDataManager dataManager = new FakeDataManager();
        sampleDataBQT1(dataManager);
        
        // Plan query
        ProcessorPlan plan = helpGetPlan(sql, RealMetadataFactory.exampleBQTCached());

        // Run query
        helpProcess(plan, dataManager, expected);
    }
    
     /** SELECT BQT1.SmallA.IntKey AS SmallA_IntKey, BQT2.MediumB.IntKey AS MediumB_IntKey FROM BQT1.SmallA LEFT OUTER JOIN BQT2.MediumB ON BQT1.SmallA.IntKey = BQT2.MediumB.IntKey WHERE (BQT1.SmallA.IntKey >= 0) AND (BQT1.SmallA.IntKey <= 15) AND (BQT2.MediumB.IntKey >= 5) AND (BQT2.MediumB.IntKey <= 20) */
     @Test public void testDefect7770_LeftOuter() { 
        // Create query 
        String sql = "SELECT BQT1.SmallA.IntKey AS SmallA_IntKey, BQT2.MediumB.IntKey AS MediumB_IntKey FROM BQT1.SmallA LEFT OUTER JOIN BQT2.MediumB ON BQT1.SmallA.IntKey = BQT2.MediumB.IntKey WHERE (BQT1.SmallA.IntKey >= 0) AND (BQT1.SmallA.IntKey <= 15) AND (BQT2.MediumB.IntKey >= 5) AND (BQT2.MediumB.IntKey <= 20)"; //$NON-NLS-1$
        
        // Create expected results
        List[] expected = new List[] { 
            Arrays.asList(new Object[] { new Integer(5), new Integer(5) }),
            Arrays.asList(new Object[] { new Integer(6), new Integer(6) }),
            Arrays.asList(new Object[] { new Integer(7), new Integer(7) }),
            Arrays.asList(new Object[] { new Integer(8), new Integer(8) }),
            Arrays.asList(new Object[] { new Integer(9), new Integer(9) }),
            Arrays.asList(new Object[] { new Integer(10), new Integer(10) }),
            Arrays.asList(new Object[] { new Integer(11), new Integer(11) }),
            Arrays.asList(new Object[] { new Integer(12), new Integer(12) }),
            Arrays.asList(new Object[] { new Integer(13), new Integer(13) }),
            Arrays.asList(new Object[] { new Integer(14), new Integer(14) }),
            Arrays.asList(new Object[] { new Integer(15), new Integer(15) })
        };    
        
        // Construct data manager with data
        FakeDataManager dataManager = new FakeDataManager();
        sampleDataBQT1(dataManager);
        
        // Plan query
        ProcessorPlan plan = helpGetPlan(sql, RealMetadataFactory.exampleBQTCached());

        // Run query
        helpProcess(plan, dataManager, expected);
    }

    @Test public void testReorder1() {
        // Create query
        String sql = "SELECT e1 AS x, {b'false'}, e2+e4, e3 FROM pm1.g1"; //$NON-NLS-1$

        // Create expected results
        List[] expected = new List[] {
            Arrays.asList(new Object[] { "a", Boolean.FALSE, new Double(2.0), Boolean.FALSE }), //$NON-NLS-1$
            Arrays.asList(new Object[] { null, Boolean.FALSE, new Double(2.0), Boolean.FALSE }),
            Arrays.asList(new Object[] { "a", Boolean.FALSE, new Double(10.0), Boolean.TRUE }), //$NON-NLS-1$
            Arrays.asList(new Object[] { "c", Boolean.FALSE, null, Boolean.TRUE }), //$NON-NLS-1$
            Arrays.asList(new Object[] { "b", Boolean.FALSE, new Double(2.0), Boolean.FALSE }), //$NON-NLS-1$
            Arrays.asList(new Object[] { "a", Boolean.FALSE, new Double(2.0), Boolean.FALSE }) //$NON-NLS-1$
        };

        // Construct data manager with data
        FakeDataManager dataManager = new FakeDataManager();
        sampleData1(dataManager);

        // Plan query
        ProcessorPlan plan = helpGetPlan(sql, RealMetadataFactory.example1Cached());

        // Run query
        helpProcess(plan, dataManager, expected);
    }

    @Test public void testTwoFullOuterJoins1() {
        // Create query
        String sql = "SELECT A.IntKey AS A_IntKey, B.IntKey AS B_IntKey, C.IntKey AS C_IntKey " +  //$NON-NLS-1$
        "FROM (BQT1.SmallA AS A FULL OUTER JOIN BQT2.SmallA AS B ON A.IntKey = B.IntKey) FULL OUTER JOIN BQT3.SmallA AS C ON B.IntKey = C.IntKey " +  //$NON-NLS-1$
        "WHERE (A.IntKey >= 0) AND (A.IntKey <= 15) " +  //$NON-NLS-1$
        "AND (B.IntKey >= 5) AND (B.IntKey <= 20) " +  //$NON-NLS-1$
        "AND (C.IntKey >= 10) AND (C.IntKey <= 30)"; //$NON-NLS-1$

        // Create expected results
        List[] expected = new List[] {
            Arrays.asList(new Object[] { new Integer(10), new Integer(10), new Integer(10) }),
            Arrays.asList(new Object[] { new Integer(11), new Integer(11), new Integer(11) }),
            Arrays.asList(new Object[] { new Integer(12), new Integer(12), new Integer(12) }),
            Arrays.asList(new Object[] { new Integer(13), new Integer(13), new Integer(13) }),
            Arrays.asList(new Object[] { new Integer(14), new Integer(14), new Integer(14) }),
            Arrays.asList(new Object[] { new Integer(15), new Integer(15), new Integer(15) })
        };

        // Construct data manager with data
        FakeDataManager dataManager = new FakeDataManager();
        sampleDataBQT2(dataManager);

        // Plan query
        ProcessorPlan plan = helpGetPlan(sql, RealMetadataFactory.exampleBQTCached());

        // Run query
        helpProcess(plan, dataManager, expected);
    }
    
    @Test public void testSelectDistinctOnBQT() {
       // Create query
       String sql = "SELECT DISTINCT IntKey FROM BQT1.SmallA"; //$NON-NLS-1$

       // Create expected results
       List[] expected = new List[] {
           Arrays.asList(new Object[] { new Integer(0) }),
           Arrays.asList(new Object[] { new Integer(1) }),
           Arrays.asList(new Object[] { new Integer(2) }),
           Arrays.asList(new Object[] { new Integer(3) }),
           Arrays.asList(new Object[] { new Integer(4) }),
           Arrays.asList(new Object[] { new Integer(5) }),
           Arrays.asList(new Object[] { new Integer(6) }),
           Arrays.asList(new Object[] { new Integer(7) }),
           Arrays.asList(new Object[] { new Integer(8) }),
           Arrays.asList(new Object[] { new Integer(9) }),
           Arrays.asList(new Object[] { new Integer(10) }),
           Arrays.asList(new Object[] { new Integer(11) }),
           Arrays.asList(new Object[] { new Integer(12) }),
           Arrays.asList(new Object[] { new Integer(13) }),
           Arrays.asList(new Object[] { new Integer(14) }),
           Arrays.asList(new Object[] { new Integer(15) }),
           Arrays.asList(new Object[] { new Integer(16) }),
           Arrays.asList(new Object[] { new Integer(17) }),
           Arrays.asList(new Object[] { new Integer(18) }),
           Arrays.asList(new Object[] { new Integer(19) })
       };

       // Construct data manager with data
       FakeDataManager dataManager = new FakeDataManager();
       sampleDataBQT1(dataManager);

       // Plan query
       ProcessorPlan plan = helpGetPlan(sql, RealMetadataFactory.exampleBQTCached());

       // Run query
       helpProcess(plan, dataManager, expected);
   }
   
   @Test public void testSelectWithNoFrom() { 
       // Create query 
       String sql = "SELECT 5"; //$NON-NLS-1$
        
       // Create expected results
       List[] expected = new List[] { 
           Arrays.asList(new Object[] { new Integer(5) })
       };    
    
       // Construct data manager with data
       FakeDataManager dataManager = new FakeDataManager();
       sampleData1(dataManager);
        
       // Plan query
       ProcessorPlan plan = helpGetPlan(sql, RealMetadataFactory.example1Cached());
        
       // Run query
       helpProcess(plan, dataManager, expected);
   }
   
   @Test public void testBetween() { 
       // Create query 
       String sql = "SELECT * FROM pm1.g1 WHERE e2 BETWEEN 1 AND 2"; //$NON-NLS-1$
        
       // Create expected results
       List[] expected = new List[] { 
           Arrays.asList(new Object[] { null,  new Integer(1),     Boolean.FALSE,  new Double(1.0) }),
           Arrays.asList(new Object[] { "c",   new Integer(1),     Boolean.TRUE,   null }), //$NON-NLS-1$
           Arrays.asList(new Object[] { "b",   new Integer(2),     Boolean.FALSE,  new Double(0.0) }) //$NON-NLS-1$
       };    
    
       // Construct data manager with data
       FakeDataManager dataManager = new FakeDataManager();
       sampleData1(dataManager);
        
       // Plan query
       ProcessorPlan plan = helpGetPlan(sql, RealMetadataFactory.example1Cached());
        
       // Run query
       helpProcess(plan, dataManager, expected);
   }

   /**
    * Test <code>QueryProcessor</code>'s ability to process a query containing 
    * a <code>CASE</code> expression in which a <code>BETWEEN</code> 
    * comparison is used in the queries <code>SELECT</code> statement.
    * <p>
    * For example:
    * <p>
    * SELECT CASE WHEN e2 BETWEEN 3 AND 5 THEN e2 ELSE -1 END FROM pm1.g1
    */
   @Test public void testBetweenInCase() { 
       // Create query 
       final String sql = "SELECT CASE WHEN e2 BETWEEN 3 AND 5 THEN e2 ELSE -1 END FROM pm1.g1"; //$NON-NLS-1$
        
       // Create expected results
       List[] expected = new List[] { 
               Arrays.asList(new Object[] { new Integer(-1) }),
               Arrays.asList(new Object[] { new Integer(-1) }),
               Arrays.asList(new Object[] { new Integer(3) }),
               Arrays.asList(new Object[] { new Integer(-1) }),
               Arrays.asList(new Object[] { new Integer(-1) }),
               Arrays.asList(new Object[] { new Integer(-1) }) 
       };    
    
       // Construct data manager with data
       FakeDataManager dataManager = new FakeDataManager();
       sampleData1(dataManager);

       // Plan query
       ProcessorPlan plan = helpGetPlan(sql, RealMetadataFactory.example1Cached());

       // Run query
       helpProcess(plan, dataManager, expected);
   }

   /**
    * Test <code>QueryProcessor</code>'s ability to process a query containing 
    * an aggregate SUM with a <code>CASE</code> expression in which a 
    * <code>BETWEEN</code> comparison is used in the queries <code>SELECT</code> 
    * statement.
    * <p>
    * For example:
    * <p>
    * SELECT SUM(CASE WHEN e2 BETWEEN 3 AND 5 THEN e2 ELSE -1 END) FROM pm1.g1
    */
   @Test public void testBetweenInCaseInSum() { 
       // Create query 
       final String sql = "SELECT SUM(CASE WHEN e2 BETWEEN 3 AND 5 THEN e2 ELSE -1 END) FROM pm1.g1"; //$NON-NLS-1$
        
       // Create expected results
       List[] expected = new List[] { 
           Arrays.asList(new Object[] { new Long(-2) })
       };    
    
       // Construct data manager with data
       FakeDataManager dataManager = new FakeDataManager();
       sampleData1(dataManager);

       // Plan query
       ProcessorPlan plan = helpGetPlan(sql, RealMetadataFactory.example1Cached());

       // Run query
       helpProcess(plan, dataManager, expected);
   }

   /**
    * Test <code>QueryProcessor</code>'s ability to process a query containing 
    * an aggregate SUM with a <code>CASE</code> expression in which a 
    * <code>BETWEEN</code> comparison is used in the queries <code>SELECT</code> 
    * statement and a GROUP BY is specified.
    * <p>
    * For example:
    * <p>
    * SELECT e1, SUM(CASE WHEN e2 BETWEEN 3 AND 5 THEN e2 ELSE -1 END) 
    * FROM pm1.g1 GROUP BY e1 ORDER BY e1
    */
   @Test public void testBetweenInCaseInSumWithGroupBy() { 
       // Create query 
       final String sql = "SELECT e1, SUM(CASE WHEN e2 BETWEEN 3 AND 5 THEN e2 ELSE -1 END) FROM pm1.g1 GROUP BY e1 ORDER BY e1"; //$NON-NLS-1$
        
       // Create expected results
       List[] expected = new List[] { 
    	       Arrays.asList(new Object[] { null,  new Long(-1) }),
    	       Arrays.asList(new Object[] { "a",   new Long(1) }), //$NON-NLS-1$
    	       Arrays.asList(new Object[] { "b",   new Long(-1) }), //$NON-NLS-1$
    	       Arrays.asList(new Object[] { "c",   new Long(-1) }) //$NON-NLS-1$
       };    
       
       // Construct data manager with data
       FakeDataManager dataManager = new FakeDataManager();
       sampleData1(dataManager);

       // Plan query
       ProcessorPlan plan = helpGetPlan(sql, RealMetadataFactory.example1Cached());

       // Run query
       helpProcess(plan, dataManager, expected);
   }

   /**
    * Test <code>QueryProcessor</code>'s ability to process a query containing 
    * an aggregate COUNT with a <code>CASE</code> expression in which a 
    * <code>BETWEEN</code> comparison is used in the queries <code>SELECT</code> 
    * statement.
    * <p>
    * For example:
    * <p>
    * SELECT COUNT(CASE WHEN e2 BETWEEN 3 AND 5 THEN e2 END) FROM pm1.g1
    */
   @Test public void testBetweenInCaseInCount() { 
       // Create query 
       final String sql = "SELECT COUNT(CASE WHEN e2 BETWEEN 3 AND 5 THEN e2 END) FROM pm1.g1"; //$NON-NLS-1$
        
       // Create expected results
       List[] expected = new List[] { 
           Arrays.asList(new Object[] { new Integer(1) })
       };    
    
       // Construct data manager with data
       FakeDataManager dataManager = new FakeDataManager();
       sampleData1(dataManager);

       // Plan query
       ProcessorPlan plan = helpGetPlan(sql, RealMetadataFactory.example1Cached());

       // Run query
       helpProcess(plan, dataManager, expected);
   }

   @Test public void testCase() { 
       // Create query 
       String sql = "SELECT e2, CASE e2 WHEN 1 THEN 2 ELSE 3 END FROM pm1.g1 WHERE e2 BETWEEN 1 AND 2"; //$NON-NLS-1$
        
       // Create expected results
       List[] expected = new List[] { 
           Arrays.asList(new Object[] { new Integer(1), new Integer(2) }),
           Arrays.asList(new Object[] { new Integer(1), new Integer(2) }),
           Arrays.asList(new Object[] { new Integer(2), new Integer(3) }) 
       };    
    
       // Construct data manager with data
       FakeDataManager dataManager = new FakeDataManager();
       sampleData1(dataManager);
        
       // Plan query
       ProcessorPlan plan = helpGetPlan(sql, RealMetadataFactory.example1Cached());
        
       // Run query
       helpProcess(plan, dataManager, expected);
   }
   
   @Test public void testSelectNoFrom1() { 
       // Create query 
       String sql = "SELECT 1"; //$NON-NLS-1$
        
       // Create expected results
       List[] expected = new List[] { 
           Arrays.asList(new Object[] { new Integer(1) })
       };    
    
       // Construct data manager with data
       FakeDataManager dataManager = new FakeDataManager();
       sampleData1(dataManager);
        
       // Plan query
       ProcessorPlan plan = helpGetPlan(sql, RealMetadataFactory.example1Cached());
        
       // Run query
       helpProcess(plan, dataManager, expected);
   }
   
   @Test public void testSelectNoFrom2() { 
       // Create query 
       String sql = "SELECT 1, {b'true'}, 2.0 AS x, {d'2003-11-04'}"; //$NON-NLS-1$
        
       // Create expected results
       List[] expected = new List[] { 
           Arrays.asList(new Object[] { new Integer(1), Boolean.TRUE, new Double(2.0), TimestampUtil.createDate(103, 10, 4)  })
       };    
    
       // Construct data manager with data
       FakeDataManager dataManager = new FakeDataManager();
       sampleData1(dataManager);
        
       // Plan query
       ProcessorPlan plan = helpGetPlan(sql, RealMetadataFactory.example1Cached());
        
       // Run query
       helpProcess(plan, dataManager, expected);
   }

   @Test public void testCase1566() throws Exception {
       // Create query
       String sql = "SELECT x, COUNT(*) FROM (SELECT convert(TimestampValue, date) AS x FROM bqt1.smalla) as y GROUP BY x"; //$NON-NLS-1$

       // Create expected results
       List[] expected = new List[] {
               Arrays.asList(new Object[] { java.sql.Date.valueOf("2002-01-01"), new Integer(2) }),
               Arrays.asList(new Object[] { java.sql.Date.valueOf("2002-01-02"), new Integer(3) }) };

       // Construct data manager with data
       FakeDataManager dataManager = new FakeDataManager();
       sampleDataBQT_case1566(dataManager);

       // Create capabilities
       FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
       BasicSourceCapabilities caps = new BasicSourceCapabilities();
       caps.setCapabilitySupport(Capability.QUERY_AGGREGATES, false);
       caps.setCapabilitySupport(Capability.QUERY_FROM_GROUP_ALIAS, true);
       capFinder.addCapabilities("BQT1", caps); //$NON-NLS-1$

       // Parse query
       Command command = QueryParser.getQueryParser().parseCommand(sql);

       // Plan query
       ProcessorPlan plan = helpGetPlan(command, RealMetadataFactory.exampleBQTCached(), capFinder);

       // Run query
       helpProcess(plan, dataManager, expected);
   }   
   
    @Test public void testDefect10976(){
        String sql = "SELECT * FROM vm1.g28"; //$NON-NLS-1$


        // Create expected results
        List[] expected = new List[] { 
            Arrays.asList(new Object[] { null, null }), 
            Arrays.asList(new Object[] { "A", "a" }), //$NON-NLS-1$ //$NON-NLS-2$
            Arrays.asList(new Object[] { "B", "b" }), //$NON-NLS-1$ //$NON-NLS-2$
            Arrays.asList(new Object[] { "C", "c" }) //$NON-NLS-1$ //$NON-NLS-2$
        };    
    
        // Construct data manager with data
        FakeDataManager dataManager = new FakeDataManager();
        sampleData1(dataManager);
        
        // Plan query
        ProcessorPlan plan = helpGetPlan(sql, RealMetadataFactory.example1Cached());
        
        // Run query
        helpProcess(plan, dataManager, expected);       
    }
         
    @Test public void testDefect10976_2(){
        String sql = "SELECT * FROM vm1.g29"; //$NON-NLS-1$


        // Create expected results
        List[] expected = new List[] { 
            Arrays.asList(new Object[] { null, null }), 
            Arrays.asList(new Object[] { "A", "a" }), //$NON-NLS-1$ //$NON-NLS-2$
            Arrays.asList(new Object[] { "B", "b" }), //$NON-NLS-1$ //$NON-NLS-2$
            Arrays.asList(new Object[] { "C", "c" }) //$NON-NLS-1$ //$NON-NLS-2$
        };    
    
        // Construct data manager with data
        FakeDataManager dataManager = new FakeDataManager();
        sampleData1(dataManager);
        
        // Plan query
        ProcessorPlan plan = helpGetPlan(sql, RealMetadataFactory.example1Cached());
        
        // Run query
        helpProcess(plan, dataManager, expected);       
    }         
    
    @Test public void testDefect10976_3(){
        String sql = "SELECT * FROM vm1.g30"; //$NON-NLS-1$


        // Create expected results
        List[] expected = new List[] { 
            Arrays.asList(new Object[] { null, null }), 
            Arrays.asList(new Object[] { "a", "a" }), //$NON-NLS-1$ //$NON-NLS-2$
            Arrays.asList(new Object[] { "b", "b" }), //$NON-NLS-1$ //$NON-NLS-2$
            Arrays.asList(new Object[] { "c", "c" }) //$NON-NLS-1$ //$NON-NLS-2$
        };    
    
        // Construct data manager with data
        FakeDataManager dataManager = new FakeDataManager();
        sampleData1(dataManager);
        
        // Plan query
        ProcessorPlan plan = helpGetPlan(sql, RealMetadataFactory.example1Cached());
        
        // Run query
        helpProcess(plan, dataManager, expected);       
    }

    @Test public void testDefect10976_4(){
        String sql = "SELECT * FROM vm1.g31 order by x"; //$NON-NLS-1$


        // Create expected results
        List[] expected = new List[] { 
            Arrays.asList(new Object[] { null, null }), 
            Arrays.asList(new Object[] { "a", "a" }), //$NON-NLS-1$ //$NON-NLS-2$
            Arrays.asList(new Object[] { "a", "a" }), //$NON-NLS-1$ //$NON-NLS-2$
            Arrays.asList(new Object[] { "a", "a" }), //$NON-NLS-1$ //$NON-NLS-2$
            Arrays.asList(new Object[] { "b", "b" }), //$NON-NLS-1$ //$NON-NLS-2$
            Arrays.asList(new Object[] { "c", "c" }) //$NON-NLS-1$ //$NON-NLS-2$
        };    
    
        // Construct data manager with data
        FakeDataManager dataManager = new FakeDataManager();
        sampleData1(dataManager);
        
        // Plan query
        ProcessorPlan plan = helpGetPlan(sql, RealMetadataFactory.example1Cached());
        
        // Run query
        helpProcess(plan, dataManager, expected);       
    }

    @Test public void testDefect10976_5(){
        String sql = "SELECT * FROM vm1.g32"; //$NON-NLS-1$


        // Create expected results
        List[] expected = new List[] { 
            Arrays.asList(new Object[] { null, null }), 
            Arrays.asList(new Object[] { "a", "a" }), //$NON-NLS-1$ //$NON-NLS-2$
            Arrays.asList(new Object[] { "b", "b" }), //$NON-NLS-1$ //$NON-NLS-2$
            Arrays.asList(new Object[] { "c", "c" }) //$NON-NLS-1$ //$NON-NLS-2$
        };    
    
        // Construct data manager with data
        FakeDataManager dataManager = new FakeDataManager();
        sampleData1(dataManager);
        
        // Plan query
        ProcessorPlan plan = helpGetPlan(sql, RealMetadataFactory.example1Cached());
        
        // Run query
        helpProcess(plan, dataManager, expected);       
    }
    
    @Test public void testDefect11236_MergeJoinWithFunctions() { 
       // Create query 
       String sql = "SELECT pm1.g1.e2, pm2.g1.e2 FROM pm1.g1, pm2.g1 WHERE pm1.g1.e2 = (pm2.g1.e2+1) order by pm1.g1.e2, pm2.g1.e2"; //$NON-NLS-1$
       boolean pushDown = false;
       boolean dependent = false;
       
       // Create expected results
       List[] expected = new List[] { 
           Arrays.asList(new Object[] { new Integer(1), new Integer(0) }), 
           Arrays.asList(new Object[] { new Integer(1), new Integer(0)  }), 
           Arrays.asList(new Object[] { new Integer(1), new Integer(0)  }), 
           Arrays.asList(new Object[] { new Integer(1), new Integer(0)  }), 
           Arrays.asList(new Object[] { new Integer(2), new Integer(1) }), 
           Arrays.asList(new Object[] { new Integer(2), new Integer(1)  }), 
           Arrays.asList(new Object[] { new Integer(3), new Integer(2)  }) 
       };    
        
       helpTestMergeJoinWithExpression(sql, pushDown, dependent, expected);
    }
    
    @Test public void testMergeJoinWithFunctionsPushDown() { 
        // Create query 
        String sql = "SELECT pm1.g1.e2, pm2.g1.e2 FROM pm1.g1, pm2.g1 WHERE pm1.g1.e2 = (pm2.g1.e2+1)"; //$NON-NLS-1$
        boolean pushDown = true;
        boolean dependent = false;
        
        // Create expected results
        List[] expected = new List[] { 
            Arrays.asList(new Object[] { new Integer(1), new Integer(0) }), 
            Arrays.asList(new Object[] { new Integer(1), new Integer(0)  }), 
            Arrays.asList(new Object[] { new Integer(1), new Integer(0)  }), 
            Arrays.asList(new Object[] { new Integer(1), new Integer(0)  }), 
            Arrays.asList(new Object[] { new Integer(2), new Integer(1) }), 
            Arrays.asList(new Object[] { new Integer(2), new Integer(1)  }), 
            Arrays.asList(new Object[] { new Integer(3), new Integer(2)  }) 
        };    
         
        helpTestMergeJoinWithExpression(sql, pushDown, dependent, expected);
    }
    
    @Test public void testMergeJoinWithFunctionsPushDownDependent() { 
        // Create query 
        String sql = "SELECT pm1.g1.e2, pm2.g1.e2 FROM pm1.g1, pm2.g1 WHERE pm1.g1.e2 = (pm2.g1.e2+1) option makedep pm1.g1"; //$NON-NLS-1$
        boolean pushDown = true;
        boolean dependent = true;
        
        // Create expected results
        List[] expected = new List[] { 
            Arrays.asList(new Object[] { new Integer(1), new Integer(0) }), 
            Arrays.asList(new Object[] { new Integer(1), new Integer(0)  }), 
            Arrays.asList(new Object[] { new Integer(1), new Integer(0)  }), 
            Arrays.asList(new Object[] { new Integer(1), new Integer(0)  }), 
            Arrays.asList(new Object[] { new Integer(2), new Integer(1) }), 
            Arrays.asList(new Object[] { new Integer(2), new Integer(1)  }), 
            Arrays.asList(new Object[] { new Integer(3), new Integer(2)  }) 
        };    
         
        helpTestMergeJoinWithExpression(sql, pushDown, dependent, expected);
    }


    /** 
     * @param sql
     * @param pushDown
     * @param expected
     */
    private void helpTestMergeJoinWithExpression(String sql,
                                                 boolean pushDown,
                                                 boolean dependent,
                                                 List[] expected) {
        // Construct data manager with data
           ProcessorDataManager dataManager = null;
           if (!pushDown) {
               FakeDataManager fakeDataManager = new FakeDataManager();
               sampleData1(fakeDataManager);
               dataManager = fakeDataManager;
           } else {
               HardcodedDataManager hardCoded = new HardcodedDataManager();
               List[] results = new List[] { 
                   Arrays.asList(new Object[] {new Integer(0), new Integer(1)}), 
                   Arrays.asList(new Object[] {new Integer(0), new Integer(1)}), 
                   Arrays.asList(new Object[] {new Integer(1), new Integer(2)}),
                   Arrays.asList(new Object[] {new Integer(1), new Integer(2)}), 
                   Arrays.asList(new Object[] {new Integer(2), new Integer(3)}), 
                   Arrays.asList(new Object[] {new Integer(3), new Integer(4)}), 
                   };
               hardCoded.addData("SELECT g_0.e2 AS c_0, (g_0.e2 + 1) AS c_1 FROM pm2.g1 AS g_0 ORDER BY c_1", results); //$NON-NLS-1$
               if (!dependent) {
                   results = new List[] { 
                       Arrays.asList(new Object[] {new Integer(0),}), 
                       Arrays.asList(new Object[] {new Integer(0),}), 
                       Arrays.asList(new Object[] {new Integer(1),}),
                       Arrays.asList(new Object[] {new Integer(1),}), 
                       Arrays.asList(new Object[] {new Integer(2),}), 
                       Arrays.asList(new Object[] {new Integer(3),}), 
                       };
                   hardCoded.addData("SELECT g_0.e2 AS c_0 FROM pm1.g1 AS g_0 ORDER BY c_0", results); //$NON-NLS-1$
               } else {
                   results = new List[] { 
                       Arrays.asList(new Object[] {new Integer(1),}),
                       Arrays.asList(new Object[] {new Integer(2),}),
                       Arrays.asList(new Object[] {new Integer(1),}), 
                       };
                   hardCoded.addData("SELECT g_0.e2 AS c_0 FROM pm1.g1 AS g_0 WHERE g_0.e2 IN (1, 2)", results); //$NON-NLS-1$
                   results = new List[] { 
                       Arrays.asList(new Object[] {new Integer(3),}), 
                       };
                   hardCoded.addData("SELECT g_0.e2 AS c_0 FROM pm1.g1 AS g_0 WHERE g_0.e2 IN (3, 4)", results); //$NON-NLS-1$
               }
               dataManager = hardCoded;
           }
            
           QueryMetadataInterface metadata = RealMetadataFactory.example1Cached();
           FakeCapabilitiesFinder finder = new FakeCapabilitiesFinder();
           BasicSourceCapabilities caps = TestOptimizer.getTypicalCapabilities();
           caps.setCapabilitySupport(Capability.CRITERIA_IN, pushDown);    
           caps.setCapabilitySupport(Capability.QUERY_ORDERBY, pushDown);
           caps.setSourceProperty(Capability.MAX_IN_CRITERIA_SIZE, new Integer(2));
           caps.setFunctionSupport("+", pushDown); //$NON-NLS-1$
           finder.addCapabilities("pm1", caps); //$NON-NLS-1$
           finder.addCapabilities("pm2", caps); //$NON-NLS-1$

           // Plan query
           ProcessorPlan plan = helpGetPlan(helpParse(sql), metadata, finder);

           // Run query
           helpProcess(plan, dataManager, expected);
    }
    
   @Test public void testCase2() { 
       // Create query 
       String sql = "SELECT e2, CASE e2 WHEN 1 THEN 2 END FROM pm1.g1 WHERE e2 BETWEEN 1 AND 2"; //$NON-NLS-1$
        
       // Create expected results
       List[] expected = new List[] { 
           Arrays.asList(new Object[] { new Integer(1), new Integer(2) }),
           Arrays.asList(new Object[] { new Integer(1), new Integer(2) }),
           Arrays.asList(new Object[] { new Integer(2), null }) 
       };    
    
       // Construct data manager with data
       FakeDataManager dataManager = new FakeDataManager();
       sampleData1(dataManager);
        
       // Plan query
       ProcessorPlan plan = helpGetPlan(sql, RealMetadataFactory.example1Cached());
        
       // Run query
       helpProcess(plan, dataManager, expected);
   }
   
    @Test public void testSortGroupCombination() throws Exception {
        String sql = "select e2, max(e1) from pm1.g1 x group by e2 order by e2 desc"; //$NON-NLS-1$

        FakeDataManager dataManager = new FakeDataManager();
        sampleData1(dataManager);
        ProcessorPlan plan = helpGetPlan(sql, RealMetadataFactory.example1Cached(), TestOptimizer.getGenericFinder());
        
        helpProcess(plan, dataManager, new List[] {
                        Arrays.asList(3, "a"),
                        Arrays.asList(2, "b"),
                        Arrays.asList(1, "c"),
                        Arrays.asList(0, "a")});
    }
    
    @Test public void testUnorderedLimitWithProc() throws Exception {
            String sql = "select e1 from (exec pm1.sq1()) x limit 1";
        
        FakeDataManager dataManager = new FakeDataManager();
        sampleData1(dataManager);
        ProcessorPlan plan = helpGetPlan(sql, RealMetadataFactory.example1Cached(), TestOptimizer.getGenericFinder());
        
        helpProcess(plan, dataManager, new List[] {Arrays.asList("a")});
    }
    
   @Test public void testCase3() { 
       // Create query 
       String sql = "SELECT e2, CASE e2 WHEN 1 THEN 2 ELSE null END FROM pm1.g1 WHERE e2 BETWEEN 1 AND 2"; //$NON-NLS-1$
        
       // Create expected results
       List[] expected = new List[] { 
           Arrays.asList(new Object[] { new Integer(1), new Integer(2) }),
           Arrays.asList(new Object[] { new Integer(1), new Integer(2) }),
           Arrays.asList(new Object[] { new Integer(2), null }) 
       };    
    
       // Construct data manager with data
       FakeDataManager dataManager = new FakeDataManager();
       sampleData1(dataManager);
        
       // Plan query
       ProcessorPlan plan = helpGetPlan(sql, RealMetadataFactory.example1Cached());
        
       // Run query
       helpProcess(plan, dataManager, expected);
   }   

   /** nested scalar subquery */
   @Test public void testCase4() { 
       // Create query 
       String nestedExpression = "(SELECT e1 FROM pm1.g2 WHERE e2 = 3)"; //$NON-NLS-1$
       String sql = "SELECT e2, CASE e2 WHEN 1 THEN " + nestedExpression + " ELSE null END FROM pm1.g1 WHERE e2 BETWEEN 1 AND 2"; //$NON-NLS-1$ //$NON-NLS-2$
        
       // Create expected results
       List[] expected = new List[] { 
           Arrays.asList(new Object[] { new Integer(1), "a" }), //$NON-NLS-1$
           Arrays.asList(new Object[] { new Integer(1), "a" }), //$NON-NLS-1$
           Arrays.asList(new Object[] { new Integer(2), null }) 
       };    
    
       // Construct data manager with data
       FakeDataManager dataManager = new FakeDataManager();
       sampleData1(dataManager);
        
       // Plan query
       ProcessorPlan plan = helpGetPlan(sql, RealMetadataFactory.example1Cached());
        
       // Run query
       helpProcess(plan, dataManager, expected);
   }  

    /** nested correlated scalar subquery */
    @Test public void testCase5() { 
        // Create query 
        String nestedExpression = "(SELECT e2 FROM pm1.g2 WHERE pm1.g1.e2 = (e4 + 2))"; //$NON-NLS-1$
        String sql = "SELECT e2, CASE e2 WHEN " + nestedExpression + " THEN 1 ELSE null END FROM pm1.g1"; //$NON-NLS-1$ //$NON-NLS-2$
        
        // Create expected results
        List[] expected = new List[] { 
            Arrays.asList(new Object[] { new Integer(0), null }), 
            Arrays.asList(new Object[] { new Integer(1), null }), 
            Arrays.asList(new Object[] { new Integer(3), null }), 
            Arrays.asList(new Object[] { new Integer(1), null }), 
            Arrays.asList(new Object[] { new Integer(2), new Integer(1) }), 
            Arrays.asList(new Object[] { new Integer(0), null }) 
        };    
    
        // Construct data manager with data
        FakeDataManager dataManager = new FakeDataManager();
        sampleData1(dataManager);
        
        // Plan query
        ProcessorPlan plan = helpGetPlan(sql, RealMetadataFactory.example1Cached());
        
        // Run query
        helpProcess(plan, dataManager, expected);
    }

    /** 
     * NOTE: this test depends on the ProcessorPlan being executed 
     * twice and reset in between, which currently is done in the 
     * helpProcess method  
     */
    @Test public void testDefect12135(){
        String sql = "SELECT pm1.g1.e1, pm1.g2.e1 FROM pm1.g1 LEFT OUTER JOIN pm1.g2 ON pm1.g1.e1=pm1.g2.e1"; //$NON-NLS-1$


        // Create expected results
        List[] expected = new List[] { 
            Arrays.asList(new Object[] { "a", "a" }), //$NON-NLS-1$ //$NON-NLS-2$
            Arrays.asList(new Object[] { "b", "b" }), //$NON-NLS-1$ //$NON-NLS-2$
            Arrays.asList(new Object[] { "b", "b" }), //$NON-NLS-1$ //$NON-NLS-2$
            Arrays.asList(new Object[] { "b", "b" }), //$NON-NLS-1$ //$NON-NLS-2$
            Arrays.asList(new Object[] { "c", null }) //$NON-NLS-1$ 
        };    
    
        // Construct data manager with data
        FakeDataManager dataManager = new FakeDataManager();
        sampleData2(dataManager);
        
        // Plan query
        ProcessorPlan plan = helpGetPlan(sql, RealMetadataFactory.example1Cached());
        
        // Run query
        helpProcess(plan, dataManager, expected);          
    }
    
    @Test public void testDefect12081(){
        String sql = "SELECT DISTINCT vm1.g1.e1, upper(vm1.g1.e1) as Nuge, pm1.g1.e1, upper(pm1.g1.e1) as Nuge FROM vm1.g1, pm1.g1"; //$NON-NLS-1$


        // Create expected results
        List[] expected = new List[] { 
            Arrays.asList(new Object[] { "a", "A", "a", "A" }), //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
            Arrays.asList(new Object[] { "a", "A", "b", "B" }), //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
            Arrays.asList(new Object[] { "a", "A", "c", "C" }), //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
            Arrays.asList(new Object[] { "b", "B", "a", "A" }), //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
            Arrays.asList(new Object[] { "b", "B", "b", "B" }), //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
            Arrays.asList(new Object[] { "b", "B", "c", "C" }), //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
            Arrays.asList(new Object[] { "c", "C", "a", "A" }), //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
            Arrays.asList(new Object[] { "c", "C", "b", "B" }), //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
            Arrays.asList(new Object[] { "c", "C", "c", "C" }) //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
        }; 
    
        // Construct data manager with data
        FakeDataManager dataManager = new FakeDataManager();
        sampleData2(dataManager);
        
        // Plan query
        ProcessorPlan plan = helpGetPlan(sql, RealMetadataFactory.example1Cached());
        
        // Run query
        helpProcess(plan, dataManager, expected);          
    }    

    @Test public void testDefect12081_2(){
        String sql = "SELECT DISTINCT vm1.g1b.e1, vm1.g1b.e1Upper, pm1.g1.e1, upper(pm1.g1.e1) as e1Upper FROM vm1.g1b, pm1.g1"; //$NON-NLS-1$

        // Create expected results
        List[] expected = new List[] { 
            Arrays.asList(new Object[] { "a", "A", "a", "A" }), //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
            Arrays.asList(new Object[] { "a", "A", "b", "B" }), //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
            Arrays.asList(new Object[] { "a", "A", "c", "C" }), //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
            Arrays.asList(new Object[] { "b", "B", "a", "A" }), //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
            Arrays.asList(new Object[] { "b", "B", "b", "B" }), //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
            Arrays.asList(new Object[] { "b", "B", "c", "C" }), //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
            Arrays.asList(new Object[] { "c", "C", "a", "A" }), //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
            Arrays.asList(new Object[] { "c", "C", "b", "B" }), //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
            Arrays.asList(new Object[] { "c", "C", "c", "C" }) //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
        };    
    
        // Construct data manager with data
        FakeDataManager dataManager = new FakeDataManager();
        sampleData2(dataManager);
        
        // Plan query
        ProcessorPlan plan = helpGetPlan(sql, RealMetadataFactory.example1Cached());
        
        // Run query
        helpProcess(plan, dataManager, expected);          
    }  
    
    @Test public void testDefect12081_3(){
        String sql = "SELECT DISTINCT vm1.g1b.e1, vm1.g1b.e1Upper, pm1.g1.e1, vm1.g1b.e1Upper FROM vm1.g1b, pm1.g1"; //$NON-NLS-1$

        // Create expected results
        List[] expected = new List[] { 
            Arrays.asList(new Object[] { "a", "A", "a", "A" }), //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
            Arrays.asList(new Object[] { "a", "A", "b", "A" }), //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
            Arrays.asList(new Object[] { "a", "A", "c", "A" }), //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
            Arrays.asList(new Object[] { "b", "B", "a", "B" }), //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
            Arrays.asList(new Object[] { "b", "B", "b", "B" }), //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
            Arrays.asList(new Object[] { "b", "B", "c", "B" }), //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
            Arrays.asList(new Object[] { "c", "C", "a", "C" }), //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
            Arrays.asList(new Object[] { "c", "C", "b", "C" }), //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
            Arrays.asList(new Object[] { "c", "C", "c", "C" }) //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
        };    
    
        // Construct data manager with data
        FakeDataManager dataManager = new FakeDataManager();
        sampleData2(dataManager);
        
        // Plan query
        ProcessorPlan plan = helpGetPlan(sql, RealMetadataFactory.example1Cached());
        
        // Run query
        helpProcess(plan, dataManager, expected);          
    }
    
    /**
     * Basically the same as above, but with a limit node between the dup removal and the project
     */
    @Test public void testDefect12081_4(){
        String sql = "SELECT DISTINCT e1, e1 FROM pm1.g1 where e1 = 'a' LIMIT 1"; //$NON-NLS-1$

        // Create expected results
        List[] expected = new List[] { 
            Arrays.asList(new Object[] { "a", "a"}), //$NON-NLS-1$ //$NON-NLS-2$ 
        };    
    
        // Construct data manager with data
        FakeDataManager dataManager = new FakeDataManager();
        sampleData2(dataManager);
        
        // Plan query
        ProcessorPlan plan = helpGetPlan(sql, RealMetadataFactory.example1Cached());
        
        // Run query
        helpProcess(plan, dataManager, expected);          
    }
    
    @Test public void testDefect12719(){
        String sql = "SELECT e1_, e2_, e2 FROM vm1.g34, pm1.g2 WHERE vm1.g34.e1_ = pm1.g2.e1 order by e1_, e2_"; //$NON-NLS-1$

        // Create expected results
        List[] expected = new List[] { 
            Arrays.asList(new Object[] { "a", new Integer(0), new Integer(1) }), //$NON-NLS-1$ 
            Arrays.asList(new Object[] { "b", new Integer(0), new Integer(0) }), //$NON-NLS-1$ 
            Arrays.asList(new Object[] { "b", new Integer(0), new Integer(5) }), //$NON-NLS-1$ 
            Arrays.asList(new Object[] { "b", new Integer(0), new Integer(2) }), //$NON-NLS-1$ 
            Arrays.asList(new Object[] { "b", new Integer(1), new Integer(0) }), //$NON-NLS-1$ 
            Arrays.asList(new Object[] { "b", new Integer(1), new Integer(5) }), //$NON-NLS-1$ 
            Arrays.asList(new Object[] { "b", new Integer(1), new Integer(2) }), //$NON-NLS-1$ 
            Arrays.asList(new Object[] { "d", new Integer(3), new Integer(2) }), //$NON-NLS-1$ 
        };    

		// Construct data manager with data
		FakeDataManager dataManager = new FakeDataManager();
		sampleData2(dataManager);
    
        // Plan query
        ProcessorPlan plan = helpGetPlan(sql, RealMetadataFactory.example1Cached());
        
        // Run query
        helpProcess(plan, dataManager, expected);
    }    
    
    @Test public void testDefect13034() {
		String sql = "SELECT CONCAT('http://', CONCAT(CASE WHEN (HOST IS NULL) OR (HOST = '') THEN 'soap_host' ELSE HOST END, CASE WHEN (PORT IS NULL) OR (PORT = '') THEN '/metamatrix-soap/services/DataService' ELSE CONCAT(':', CONCAT(PORT, '/metamatrix-soap/services/DataService')) END)) AS location " + //$NON-NLS-1$
			"FROM (SELECT env('soap_host') AS HOST, env('soap_port') AS PORT) AS props"; //$NON-NLS-1$
			
		// Create expected results
		List[] expected = new List[] { 
			Arrays.asList(new Object[] { "http://my.host.com:12345/metamatrix-soap/services/DataService" }), //$NON-NLS-1$ 
		};    
           
		// Plan query
		ProcessorPlan plan = helpGetPlan(sql, RealMetadataFactory.example1Cached());
        
		// Run query
		helpProcess(plan, new FakeDataManager(), expected);
		    	
    }
    
    /** see also integer average defect 11682 */
    @Test public void testIntAvgDefect11682() { 
        // Create query 
        String sql = "SELECT AVG(IntKey), AVG(IntNum), AVG(FloatNum), AVG(LongNum), AVG(DoubleNum), AVG(ByteNum), AVG(ShortValue), AVG(BigIntegerValue), AVG(BigDecimalValue) FROM BQT1.SmallA"; //$NON-NLS-1$
        
        // Create expected results
        List[] expected = new List[] { 
            Arrays.asList(new Object[] { new Double(1.5), new Double(1.5), new Double(1.5), new Double(1.5), new Double(1.5), new Double(1.5), new Double(1.5), new BigDecimal("1.500000000"), new BigDecimal("1.500000000") }),  //$NON-NLS-1$//$NON-NLS-2$
        };    
    
        // Construct data manager with data
        FakeDataManager dataManager = new FakeDataManager();
        sampleDataBQT_defect11682(dataManager);
        
        // Plan query
        ProcessorPlan plan = helpGetPlan(sql, RealMetadataFactory.exampleBQTCached());
        
        // Run query
        helpProcess(plan, dataManager, expected);
    }    
    
    @Test public void testNonJoinCriteriaInFrom() {
        String sql = "SELECT a.e1, b.e1, b.e2 FROM pm1.g1 a LEFT OUTER JOIN pm2.g1 b ON a.e1=b.e1 AND b.e2 = 0"; //$NON-NLS-1$

        // Create expected results
        List[] expected = new List[] { 
            Arrays.asList(new Object[] { "a", null, null }), //$NON-NLS-1$ 
            Arrays.asList(new Object[] { "b", "b", new Integer(0) }), //$NON-NLS-1$ //$NON-NLS-2$ 
            Arrays.asList(new Object[] { "c", null, null }), //$NON-NLS-1$ 
        };    

        // Construct data manager with data
        FakeDataManager dataManager = new FakeDataManager();
        sampleData2(dataManager);
    
        // Plan query
        ProcessorPlan plan = helpGetPlan(sql, RealMetadataFactory.example1Cached());
        
        // Run query
        helpProcess(plan, dataManager, expected);
    }
        
    @Test public void testNonJoinCriteriaInWhere() {
        String sql = "SELECT a.e1, b.e1, b.e2 FROM pm1.g1 a LEFT OUTER JOIN pm2.g1 b ON a.e1=b.e1 WHERE b.e2 = 0"; //$NON-NLS-1$

        // Create expected results
        List[] expected = new List[] { 
            Arrays.asList(new Object[] { "b", "b", new Integer(0) }), //$NON-NLS-1$ //$NON-NLS-2$ 
        };    

        // Construct data manager with data
        FakeDataManager dataManager = new FakeDataManager();
        sampleData2(dataManager);
    
        // Plan query
        ProcessorPlan plan = helpGetPlan(sql, RealMetadataFactory.example1Cached());
        
        // Run query
        helpProcess(plan, dataManager, expected);
    }
    
    @Test public void testNonJoinCriteriaInWhere2() {
        String sql = "SELECT a.e1, b.e1, b.e2 FROM pm1.g1 a LEFT OUTER JOIN pm1.g2 b ON a.e1=b.e1 WHERE (a.e2 + b.e2 = 1)"; //$NON-NLS-1$

        // Create expected results
        List[] expected = new List[] { 
            Arrays.asList(new Object[] { "a", "a", new Integer(1) }), //$NON-NLS-1$ //$NON-NLS-2$ 
            Arrays.asList(new Object[] { "b", "b", new Integer(0) }), //$NON-NLS-1$ //$NON-NLS-2$ 
        };    

        // Construct data manager with data
        FakeDataManager dataManager = new FakeDataManager();
        sampleData2(dataManager);
    
        // Plan query
        ProcessorPlan plan = helpGetPlan(sql, RealMetadataFactory.example1Cached());
        
        // Run query
        helpProcess(plan, dataManager, expected);
    }    
    
    @Test public void testNonJoinCriteriaInWhere3() {
        String sql = "SELECT a.e1, b.e1, b.e2 FROM pm1.g1 a LEFT OUTER JOIN pm1.g2 b ON a.e1=b.e1 WHERE (a.e2 = 0) OR (b.e2 = 0)"; //$NON-NLS-1$

        // Create expected results
        List[] expected = new List[] { 
            Arrays.asList(new Object[] { "a", "a", new Integer(1) }), //$NON-NLS-1$ //$NON-NLS-2$ 
            Arrays.asList(new Object[] { "b", "b", new Integer(0) }), //$NON-NLS-1$ //$NON-NLS-2$ 
        };    

        // Construct data manager with data
        FakeDataManager dataManager = new FakeDataManager();
        sampleData2(dataManager);
    
        // Plan query
        ProcessorPlan plan = helpGetPlan(sql, RealMetadataFactory.example1Cached());
        
        // Run query
        helpProcess(plan, dataManager, expected);
    }     
        
    @Test public void testNonJoinCriteriaInFromNestedInVirtual() {
        String sql = "SELECT a.e1, b.e1, b.e2 FROM pm1.g1 a LEFT OUTER JOIN (SELECT c.e1, d.e2 FROM pm2.g1 c JOIN pm2.g1 d ON c.e1=d.e1 AND d.e2 >= 0) b ON a.e1=b.e1 AND b.e2 = 0"; //$NON-NLS-1$

        // Create expected results
        List[] expected = new List[] { 
            Arrays.asList(new Object[] { "a", null, null }), //$NON-NLS-1$ 
            Arrays.asList(new Object[] { "b", "b", new Integer(0) }), //$NON-NLS-1$ //$NON-NLS-2$ 
            Arrays.asList(new Object[] { "c", null, null }), //$NON-NLS-1$ 
        };    

        // Construct data manager with data
        FakeDataManager dataManager = new FakeDataManager();
        sampleData2(dataManager);
    
        // Plan query
        ProcessorPlan plan = helpGetPlan(sql, RealMetadataFactory.example1Cached());
        
        // Run query
        helpProcess(plan, dataManager, expected);
    }
    
    @Test public void testNonJoinCriteriaInFromUsingDependentJoin() {
        String sql = "SELECT a.e1, b.e1, b.e2 FROM pm1.g1 a LEFT OUTER JOIN pm2.g1 b ON a.e1=b.e1 AND b.e2 = 0"; //$NON-NLS-1$

        // Create expected results
        List[] expected = new List[] { 
            Arrays.asList(new Object[] { "a", null, null }), //$NON-NLS-1$ 
            Arrays.asList(new Object[] { "b", "b", new Integer(0) }), //$NON-NLS-1$ //$NON-NLS-2$ 
            Arrays.asList(new Object[] { "c", null, null }), //$NON-NLS-1$ 
        };    

        // Construct data manager with data
        FakeDataManager dataManager = new FakeDataManager();
        sampleData2a(dataManager);
    
        // Plan query
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.CRITERIA_IN, true);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$
        capFinder.addCapabilities("pm2", caps); //$NON-NLS-1$
        
        Command command = helpParse(sql);   
        ProcessorPlan plan = helpGetPlan(command, RealMetadataFactory.example1Cached(), capFinder);

        // Run query
        helpProcess(plan, dataManager, expected);        
    }          
    
    @Test public void testDefect13700() {
        // Create expected results
        List[] expected = new List[] { 
            Arrays.asList(new Object[] { new Integer(10) }), 
        };    
           
        // Plan query
        ProcessorPlan plan = helpGetPlan("EXEC pm1.vsp36(5)", RealMetadataFactory.example1Cached()); //$NON-NLS-1$
        
        // Run query
        helpProcess(plan, new FakeDataManager(), expected);
        
    }
    
    @Test public void testDefect13920() throws Exception {

        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.CRITERIA_COMPARE_EQ, false);
        caps.setCapabilitySupport(Capability.CRITERIA_COMPARE_ORDERED, false);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$
        
        Command command = helpParse("SELECT e5, e2, e3, e4 FROM vm1.g1c WHERE e5 >= {ts'2004-08-01 00:00:00.0'}");   //$NON-NLS-1$
        ProcessorPlan plan = helpGetPlan(command,  
            RealMetadataFactory.example1Cached(), capFinder);

        // Construct data manager with data
        FakeDataManager dataManager = new FakeDataManager();
        dataManager.registerTuples(
                RealMetadataFactory.example1Cached(),
                "pm1.g1", new List[] { 
					    Arrays.asList(new Object[] { "Jan 01 2004 12:00:00",   new Integer(0),     Boolean.FALSE,  new Double(2.0) }), //$NON-NLS-1$
					    Arrays.asList(new Object[] { "Dec 31 2004 12:00:00",   new Integer(1),     Boolean.TRUE,   null }), //$NON-NLS-1$
					    Arrays.asList(new Object[] { "Aug 01 2004 12:00:00",   new Integer(2),     Boolean.FALSE,  new Double(0.0) }), //$NON-NLS-1$
					    } );       

        Calendar cal = Calendar.getInstance();
        cal.set(2004, Calendar.DECEMBER, 31, 0, 0, 0);
        cal.set(Calendar.MILLISECOND, 0);
        Timestamp t1 = new Timestamp(cal.getTime().getTime());
        cal.clear();
        cal.set(2004, Calendar.AUGUST, 1, 0, 0, 0);
        cal.set(Calendar.MILLISECOND, 0);
        Timestamp t2 = new Timestamp(cal.getTime().getTime());
        
        // Create expected results
        List[] expected = new List[] { 
            Arrays.asList(new Object[] { t1,   new Integer(1),     Boolean.TRUE,   null }), 
            Arrays.asList(new Object[] { t2,   new Integer(2),     Boolean.FALSE,  new Double(0.0) }), 
        };    
        
        // Run query
        helpProcess(plan, dataManager, expected);        
        
    }

    /** RLM Case 2077 
     * @throws Exception */
    @Test public void testComplexJoinExpressionsUsingDependentJoin() throws Exception {
        String sql = "SELECT a.e1, b.e1, b.e2 FROM pm1.g1 a, pm2.g1 b where rtrim(a.e1)=(b.e1 || b.e1)"; //$NON-NLS-1$

        // Create expected results
        List[] expected = new List[] { 
            Arrays.asList(new Object[] { "bb   ", "b", new Integer(0) }) //$NON-NLS-1$ //$NON-NLS-2$ 
        };    

        // Plan query
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.CRITERIA_IN, true);
        caps.setCapabilitySupport(Capability.QUERY_FROM_GROUP_ALIAS, true);
        caps.setSourceProperty(Capability.MAX_IN_CRITERIA_SIZE, new Integer(1000));
        caps.setFunctionSupport("||", true); //$NON-NLS-1$
        caps.setFunctionSupport("rtrim", true); //$NON-NLS-1$
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$
        capFinder.addCapabilities("pm2", caps); //$NON-NLS-1$

        QueryMetadataInterface metadata = RealMetadataFactory.example1();
        RealMetadataFactory.setCardinality("pm1.g1", RuleChooseDependent.DEFAULT_INDEPENDENT_CARDINALITY + 1000, metadata);
        RealMetadataFactory.setCardinality("pm2.g1", RuleChooseDependent.DEFAULT_INDEPENDENT_CARDINALITY - 1, metadata);
        
        // Construct data manager with data
        FakeDataManager dataManager = new FakeDataManager();
        sampleData2b(dataManager, metadata);

        Command command = helpParse(sql);   
        CommandContext context = createCommandContext();
        context.setMetadata(metadata);
        ProcessorPlan plan = helpGetPlan(command, metadata, capFinder,context);
        
        //Verify a dependent join (not merge join) was used
        assertTrue(plan instanceof RelationalPlan);
        RelationalPlan relationalPlan = (RelationalPlan)plan;
        RelationalNode project = relationalPlan.getRootNode();
        RelationalNode join = project.getChildren()[0];
        assertTrue("Expected instance of JoinNode (for dep join) but got " + join.getClass(), join instanceof JoinNode); //$NON-NLS-1$

        // Run query
        helpProcess(plan, context, dataManager, expected);        
    }     

    /** RLM Case 2077 
     * @throws Exception */
    @Test public void testComplexJoinExpressionsUsingDependentJoinWithAccessPattern() throws Exception {
        String sql = "SELECT a.e1, b.e1, b.e2 FROM pm4.g1 a, pm2.g1 b where rtrim(a.e1)=(b.e1 || b.e1)"; //$NON-NLS-1$

        // Create expected results
        List[] expected = new List[] { 
            Arrays.asList(new Object[] { "bb   ", "b", new Integer(0) }) //$NON-NLS-1$ //$NON-NLS-2$ 
        };    

        // Plan query
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.CRITERIA_IN, true);
        caps.setCapabilitySupport(Capability.QUERY_FROM_GROUP_ALIAS, true);
        caps.setSourceProperty(Capability.MAX_IN_CRITERIA_SIZE, new Integer(1));
        caps.setFunctionSupport("||", true); //$NON-NLS-1$
        caps.setFunctionSupport("rtrim", true); //$NON-NLS-1$
        capFinder.addCapabilities("pm4", caps); //$NON-NLS-1$
        capFinder.addCapabilities("pm2", caps); //$NON-NLS-1$

        QueryMetadataInterface metadata = RealMetadataFactory.example1();
        RealMetadataFactory.setCardinality("pm4.g1", RuleChooseDependent.DEFAULT_INDEPENDENT_CARDINALITY + 1000, metadata);
        RealMetadataFactory.setCardinality("pm2.g1", RuleChooseDependent.DEFAULT_INDEPENDENT_CARDINALITY - 1, metadata);
        
        // Construct data manager with data
        FakeDataManager dataManager = new FakeDataManager();
        sampleData2b(dataManager, metadata);
        
        Command command = helpParse(sql);   
        CommandContext context = createCommandContext();
        context.setMetadata(metadata);
        ProcessorPlan plan = helpGetPlan(command, metadata, capFinder, context);
        
        //Verify a dependent join (not merge join) was used
        assertTrue(plan instanceof RelationalPlan);
        RelationalPlan relationalPlan = (RelationalPlan)plan;
        RelationalNode project = relationalPlan.getRootNode();
        RelationalNode join = project.getChildren()[0];
        assertTrue("Expected instance of JoinNode (for dep join) but got " + join.getClass(), join instanceof JoinNode); //$NON-NLS-1$

        // Run query
        helpProcess(plan, context, dataManager, expected);        
    }      
    
    @Test public void testPushingCriteriaUnderJoinButNotToSource() {
        // Create query
        String sql = "SELECT A.IntKey AS A_IntKey, B.IntKey AS B_IntKey, C.IntKey AS C_IntKey " +  //$NON-NLS-1$
        "FROM (BQT1.SmallA AS A FULL OUTER JOIN BQT2.SmallA AS B ON A.IntKey = B.IntKey) LEFT OUTER JOIN BQT3.SmallA AS C ON B.IntKey = C.IntKey " +  //$NON-NLS-1$
        "WHERE (sin(A.IntKey) >= 0) " +  //$NON-NLS-1$
        "AND (C.IntKey >= 10)"; //$NON-NLS-1$

        // Create expected results
        List[] expected = new List[] {
            Arrays.asList(new Object[] { new Integer(13), new Integer(13), new Integer(13) }),
            Arrays.asList(new Object[] { new Integer(14), new Integer(14), new Integer(14) }),
            Arrays.asList(new Object[] { new Integer(15), new Integer(15), new Integer(15) }),
            Arrays.asList(new Object[] { new Integer(19), new Integer(19), new Integer(19) }),
            Arrays.asList(new Object[] { new Integer(20), new Integer(20), new Integer(20) }),
            Arrays.asList(new Object[] { new Integer(21), new Integer(21), new Integer(21) }),
            Arrays.asList(new Object[] { new Integer(26), new Integer(26), new Integer(26) }),
            Arrays.asList(new Object[] { new Integer(27), new Integer(27), new Integer(27) }),
            Arrays.asList(new Object[] { new Integer(28), new Integer(28), new Integer(28) })
        };

        // Construct data manager with data
        FakeDataManager dataManager = new FakeDataManager();
        sampleDataBQT2(dataManager);

        // Plan query
        ProcessorPlan plan = helpGetPlan(sql, RealMetadataFactory.exampleBQTCached());

        // Run query
        helpProcess(plan, dataManager, expected);
    }    
    
    @Test public void testPushdownLiteralInSelectUnderAggregate() {  
        String sql = "SELECT COUNT(*) FROM (SELECT '' AS y, a.IntKey FROM BQT1.SmallA a union all select '', b.intkey from bqt1.smallb b) AS x"; //$NON-NLS-1$

        // Create expected results
        List[] expected = new List[] { 
            Arrays.asList(new Object[] { new Integer(30) }) 
        };    

        // Construct data manager with data
        FakeDataManager dataManager = new FakeDataManager();
        sampleDataBQT2(dataManager);
    
        // Plan query
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_FROM_GROUP_ALIAS, true);
        caps.setCapabilitySupport(Capability.QUERY_FROM_JOIN_INNER, true);
        caps.setCapabilitySupport(Capability.QUERY_SELECT_EXPRESSION, true);
        caps.setCapabilitySupport(Capability.QUERY_UNION, true);
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES, true);
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES_COUNT, true);
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES_COUNT_STAR, true);
        capFinder.addCapabilities("BQT1", caps); //$NON-NLS-1$

        QueryMetadataInterface metadata = RealMetadataFactory.exampleBQTCached();
        
        Command command = helpParse(sql);   
        ProcessorPlan plan = helpGetPlan(command, metadata, capFinder);

        // Run query
        helpProcess(plan, dataManager, expected);        
    }

    @Test public void testPushdownLiteralInSelectWithOrderBy() {  
        String sql = "SELECT 1, concat('a', 'b' ) AS X FROM BQT1.SmallA where intkey = 0 " +  //$NON-NLS-1$
            "UNION ALL " +  //$NON-NLS-1$
            "select 2, 'Hello2' from BQT1.SmallA where intkey = 1 order by X desc"; //$NON-NLS-1$
        
        
        // Create expected results - would expect these to be:
        //    1, "ab"
        //    2, "Hello2"
        // but our fake tuple source is too dumb to return anything reasonable, so instead get:
        List[] expected = new List[] { 
            Arrays.asList(new Object[] { null, null }), 
        };    

        // Construct data manager with data
        FakeDataManager dataManager = new FakeDataManager();
        sampleDataBQTSmall(dataManager);
    
        // Plan query
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_SELECT_EXPRESSION, true);
        caps.setCapabilitySupport(Capability.QUERY_UNION, true);
        caps.setCapabilitySupport(Capability.QUERY_SET_ORDER_BY, true);
        caps.setCapabilitySupport(Capability.CRITERIA_COMPARE_EQ, true);
        capFinder.addCapabilities("BQT1", caps); //$NON-NLS-1$

        QueryMetadataInterface metadata = RealMetadataFactory.exampleBQTCached();
        
        Command command = helpParse(sql);   
        ProcessorPlan plan = helpGetPlan(command, metadata, capFinder);

        // Run query
        helpProcess(plan, dataManager, expected);        
    }
    
    /** defect 15348
     * @throws Exception */
    @Test public void testPreparedStatementDefect15348() throws Exception{
        String sql = "SELECT e1 from pm1.g1 where myrtrim(?)=e1"; //$NON-NLS-1$

        // Create expected results
        List[] expected = new List[] { 
            Arrays.asList(new Object[] { "a" }) //$NON-NLS-1$  
        };    

        // Construct data manager with data
        FakeDataManager dataManager = new FakeDataManager();
        sampleData2a(dataManager);
    
        // Plan query
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.CRITERIA_COMPARE_EQ, true);
        caps.setFunctionSupport("myrtrim", true); //$NON-NLS-1$
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$
        
        QueryMetadataInterface metadata = RealMetadataFactory.createTransformationMetadata(RealMetadataFactory.example1Cached().getMetadataStore(), "example1", new FunctionTree("foo", new FakeFunctionMetadataSource(), true));
        
        processPreparedStatement(sql, expected, dataManager, capFinder,
				metadata, Arrays.asList("a    "));        
    }

	static void processPreparedStatement(String sql, List[] expected,
			ProcessorDataManager dataManager, CapabilitiesFinder capFinder,
			QueryMetadataInterface metadata, List<?> values) throws Exception {
		Command command = helpParse(sql);   
        CommandContext context = createCommandContext();
        context.setMetadata(metadata);        
        ProcessorPlan plan = helpGetPlan(command, metadata, capFinder, context);
        
        // Collect reference, set value
        VariableContext vc = new VariableContext();
        Iterator<?> valIter = values.iterator();
        for (Reference ref : ReferenceCollectorVisitor.getReferences(command)) {
            vc.setGlobalValue(ref.getContextSymbol(),  valIter.next()); //$NON-NLS-1$
		}
        context.setVariableContext(vc);
        // Run query
        helpProcess(plan, context, dataManager, expected);
	}    

    /** defect 15348
     * @throws Exception */
    @Test public void testPreparedStatementDefect15348b() throws Exception{
        String sql = "SELECT e1 from pm4.g1 where myrtrim(concat(?, 'a  '))=e1"; //$NON-NLS-1$

        // Create expected results
        List[] expected = new List[] { 
            Arrays.asList(new Object[] { "aa" }) //$NON-NLS-1$  
        };    

        // Construct data manager with data
        FakeDataManager dataManager = new FakeDataManager();
        sampleData2a(dataManager);
    
        // Plan query
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.CRITERIA_COMPARE_EQ, true);
        caps.setFunctionSupport("myrtrim", true); //$NON-NLS-1$
        caps.setFunctionSupport("concat", true); //$NON-NLS-1$
        capFinder.addCapabilities("pm4", caps); //$NON-NLS-1$

        QueryMetadataInterface metadata = RealMetadataFactory.createTransformationMetadata(RealMetadataFactory.example1Cached().getMetadataStore(), "example1", new FunctionTree("foo", new FakeFunctionMetadataSource(), true));
        
        processPreparedStatement(sql, expected, dataManager, capFinder,
				metadata, Arrays.asList("a")); 
    }      

    @Test public void testSourceDoesntSupportGroupAlias() {  
        String sql = "SELECT a.IntKey, b.IntKey FROM BQT1.SmallA a, BQT1.SmallB b WHERE a.IntKey = 5 AND A.IntKey = b.IntKey"; //$NON-NLS-1$
        
        
        // Plan query
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.CRITERIA_COMPARE_EQ, true);
        caps.setCapabilitySupport(Capability.QUERY_FROM_GROUP_ALIAS, false);
        caps.setCapabilitySupport(Capability.QUERY_FROM_JOIN_INNER, true);
        capFinder.addCapabilities("BQT1", caps); //$NON-NLS-1$

        QueryMetadataInterface metadata = RealMetadataFactory.exampleBQTCached();
        
        Command command = helpParse(sql);   
        ProcessorPlan plan = helpGetPlan(command, metadata, capFinder);
        
        Set atomicQueries = TestOptimizer.getAtomicQueries(plan);
        assertEquals("Expected one query to get pushed down", 1, atomicQueries.size()); //$NON-NLS-1$
        String atomicSql = atomicQueries.iterator().next().toString();
        String expectedSql = "SELECT BQT1.SmallA.IntKey, BQT1.SmallB.IntKey FROM BQT1.SmallA, BQT1.SmallB WHERE (BQT1.SmallA.IntKey = BQT1.SmallB.IntKey) AND (BQT1.SmallA.IntKey = 5) AND (BQT1.SmallB.IntKey = 5)"; //$NON-NLS-1$
        assertEquals(expectedSql, atomicSql); 

        List[] expected = new List[] { 
                                      Arrays.asList(new Object[] { new Integer(5), new Integer(5)}), 
                                  };    
        
        HardcodedDataManager dataManager = new HardcodedDataManager();              
        dataManager.addData(expectedSql, expected);
        helpProcess(plan, dataManager, expected);        
    }

    @Test public void testSourceDoesntSupportGroupAliasOrCriteria() {  
        String sql = "SELECT a.IntKey, b.IntKey FROM BQT1.SmallA a, BQT1.SmallB b WHERE a.StringKey = '5' AND A.IntKey = b.IntKey"; //$NON-NLS-1$
        
        
        // Plan query
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_FROM_GROUP_ALIAS, false);
        caps.setCapabilitySupport(Capability.QUERY_FROM_JOIN_INNER, true);
        capFinder.addCapabilities("BQT1", caps); //$NON-NLS-1$

        QueryMetadataInterface metadata = RealMetadataFactory.exampleBQTCached();
        
        Command command = helpParse(sql);   
        ProcessorPlan plan = helpGetPlan(command, metadata, capFinder);
        
        Set atomicQueries = TestOptimizer.getAtomicQueries(plan);
        assertEquals("Expected 2 queries to get pushed down", 2, atomicQueries.size()); //$NON-NLS-1$
        
        String expectedSql = "SELECT BQT1.SmallA.StringKey, BQT1.SmallA.IntKey FROM BQT1.SmallA"; //$NON-NLS-1$
        String expectedSql2 = "SELECT BQT1.SmallB.IntKey FROM BQT1.SmallB"; //$NON-NLS-1$
        Set expectedQueries = new HashSet();
        expectedQueries.add(expectedSql);
        expectedQueries.add(expectedSql2);
        assertEquals(expectedQueries, atomicQueries); 

        List[] input1 = new List[] { 
                                    Arrays.asList(new Object[] { "5", new Integer(5)}), //$NON-NLS-1$ 
                                };    
        List[] input2 = new List[] {Arrays.asList(new Object[] {new Integer(5)}), 
        };
        HardcodedDataManager dataManager = new HardcodedDataManager();
        dataManager.addData(expectedSql, input1);
        dataManager.addData(expectedSql2, input2);
        
        List[] expected = new List[] {Arrays.asList(new Object[] {new Integer(5), new Integer(5)}), 
        };
        helpProcess(plan, dataManager, expected);        
    }

    /**
     * Same as testSourceDoesntSupportGroupAlias, but query is in an inline view and only 
     * the first column is selected.
     * 
     * @since 4.2
     */
    @Test public void testSourceDoesntSupportGroupAliasInVirtual() {  
        String sql = "SELECT x FROM (SELECT a.IntKey as x, b.IntKey as y FROM BQT1.SmallA a, BQT1.SmallB b WHERE a.IntKey = 5 AND A.IntKey = b.IntKey) AS z, BQT2.SmallA WHERE y = IntKey"; //$NON-NLS-1$
        
        
        // Plan query
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.CRITERIA_COMPARE_EQ, true);
        caps.setCapabilitySupport(Capability.QUERY_FROM_GROUP_ALIAS, false);
        caps.setCapabilitySupport(Capability.QUERY_FROM_JOIN_INNER, true);
        capFinder.addCapabilities("BQT1", caps); //$NON-NLS-1$
        capFinder.addCapabilities("BQT2", caps); //$NON-NLS-1$

        QueryMetadataInterface metadata = RealMetadataFactory.exampleBQTCached();
        
        Command command = helpParse(sql);   
        ProcessorPlan plan = helpGetPlan(command, metadata, capFinder);
        
        Set atomicQueries = TestOptimizer.getAtomicQueries(plan);
        assertEquals("Expected 2 queries to get pushed down", 2, atomicQueries.size()); //$NON-NLS-1$
        
        String expectedSql = "SELECT BQT1.SmallA.IntKey FROM BQT1.SmallA, BQT1.SmallB WHERE (BQT1.SmallA.IntKey = BQT1.SmallB.IntKey) AND (BQT1.SmallA.IntKey = 5) AND (BQT1.SmallB.IntKey = 5)"; //$NON-NLS-1$
        String expectedSql2 = "SELECT BQT2.SmallA.IntKey FROM BQT2.SmallA WHERE BQT2.SmallA.IntKey = 5"; //$NON-NLS-1$
        
        Set expectedQueries = new HashSet();
        expectedQueries.add(expectedSql);
        expectedQueries.add(expectedSql2);
        assertEquals(expectedQueries, atomicQueries); 

        List[] input1 = new List[] { 
                                      Arrays.asList(new Object[] { new Integer(5), new Integer(5)}), 
                                  };    
        List[] input2 = new List[] { 
                                    Arrays.asList(new Object[] { new Integer(5)}), 
                                };    
        HardcodedDataManager dataManager = new HardcodedDataManager();              
        dataManager.addData(expectedSql, input1);
        dataManager.addData(expectedSql2, input2);

        List[] expected = new List[] { 
                                      Arrays.asList(new Object[] { new Integer(5)}), 
                                  };    
        helpProcess(plan, dataManager, expected);        
    }

    @Test public void testCaseInGroupBy() {  
        String sql = "SELECT sum (IntKey), case when IntKey>=5000 then '5000 +' else '0-999' end " + //$NON-NLS-1$
        "FROM BQT1.SmallA GROUP BY case when IntKey>=5000 then '5000 +' else '0-999' end"; //$NON-NLS-1$
        
        // Plan query
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_SELECT_EXPRESSION, true);
        caps.setCapabilitySupport(Capability.QUERY_SEARCHED_CASE, true);
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES, true);
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES_SUM, true);
        caps.setCapabilitySupport(Capability.CRITERIA_COMPARE_ORDERED, true);
        caps.setCapabilitySupport(Capability.QUERY_FROM_INLINE_VIEWS, true);
        caps.setCapabilitySupport(Capability.QUERY_FROM_GROUP_ALIAS, true);
        capFinder.addCapabilities("BQT1", caps); //$NON-NLS-1$

        QueryMetadataInterface metadata = RealMetadataFactory.exampleBQTCached();
        
        Command command = helpParse(sql);   
        ProcessorPlan plan = helpGetPlan(command, metadata, capFinder);
        
        Set actualQueries = TestOptimizer.getAtomicQueries(plan);
        String expectedSql = "SELECT SUM(v_0.c_1), v_0.c_0 FROM (SELECT CASE WHEN g_0.IntKey >= 5000 THEN '5000 +' ELSE '0-999' END AS c_0, g_0.IntKey AS c_1 FROM BQT1.SmallA AS g_0) AS v_0 GROUP BY v_0.c_0"; //$NON-NLS-1$
        assertEquals(1, actualQueries.size());        
        assertEquals(expectedSql, actualQueries.iterator().next().toString()); 

        List[] input1 = new List[] { 
                                      Arrays.asList(new Object[] { new Integer(5), new Integer(10)}), 
                                  };    
        HardcodedDataManager dataManager = new HardcodedDataManager();              
        dataManager.addData(expectedSql, input1);

        List[] expected = new List[] { 
                                      Arrays.asList(new Object[] { new Integer(5), new Integer(10)}), 
                                  };    
        helpProcess(plan, dataManager, expected);        
    }

    @Test public void testCaseInGroupByAndHavingCantPush() {  
        String sql = "SELECT sum (IntKey), case when IntKey>=5000 then '5000 +' else '0-999' end " + //$NON-NLS-1$
        "FROM BQT1.SmallA GROUP BY case when IntKey>=5000 then '5000 +' else '0-999' end " + //$NON-NLS-1$
        "HAVING case when IntKey>=5000 then '5000 +' else '0-999' end = '5000 +'"; //$NON-NLS-1$
        
        // Plan query
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_SEARCHED_CASE, false);   // Can't push GROUP BY
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES, true);
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES_SUM, true);
        caps.setCapabilitySupport(Capability.CRITERIA_COMPARE_ORDERED, true);
        capFinder.addCapabilities("BQT1", caps); //$NON-NLS-1$

        QueryMetadataInterface metadata = RealMetadataFactory.exampleBQTCached();
        
        Command command = helpParse(sql);   
        ProcessorPlan plan = helpGetPlan(command, metadata, capFinder);
        
        Set actualQueries = TestOptimizer.getAtomicQueries(plan);
        String expectedSql = "SELECT BQT1.SmallA.IntKey FROM BQT1.SmallA"; //$NON-NLS-1$
        assertEquals(1, actualQueries.size());        
        assertEquals(expectedSql, actualQueries.iterator().next().toString()); 

        List[] input1 = new List[] { 
                                      Arrays.asList(new Object[] { new Integer(2)}),  
                                      Arrays.asList(new Object[] { new Integer(4)}),  
                                      Arrays.asList(new Object[] { new Integer(10000)}),  
                                      Arrays.asList(new Object[] { new Integer(10002)}),  
                                  };    
        HardcodedDataManager dataManager = new HardcodedDataManager();              
        dataManager.addData(expectedSql, input1);

        List[] expected = new List[] { 
                                      Arrays.asList(new Object[] { new Long(20002), "5000 +"}), //$NON-NLS-1$ 
                                  };    
        helpProcess(plan, dataManager, expected);        
    }

    @Test public void testCaseInGroupByAndHavingCantPush2() {  
        String sql = "SELECT sum (IntKey), case when IntKey>=5000 then '5000 +' else '0-999' end || 'x' " + //$NON-NLS-1$
        "FROM BQT1.SmallA GROUP BY case when IntKey>=5000 then '5000 +' else '0-999' end " + //$NON-NLS-1$
        "HAVING length(case when IntKey>=5000 then '5000 +' else '0-999' end) > 5"; //$NON-NLS-1$
        
        // Plan query
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_SEARCHED_CASE, false);   // Can't push GROUP BY
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES, true);
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES_SUM, true);
        capFinder.addCapabilities("BQT1", caps); //$NON-NLS-1$

        QueryMetadataInterface metadata = RealMetadataFactory.exampleBQTCached();
        
        Command command = helpParse(sql);   
        ProcessorPlan plan = helpGetPlan(command, metadata, capFinder);
        
        Set actualQueries = TestOptimizer.getAtomicQueries(plan);
        String expectedSql = "SELECT BQT1.SmallA.IntKey FROM BQT1.SmallA"; //$NON-NLS-1$
        assertEquals(1, actualQueries.size());        
        assertEquals(expectedSql, actualQueries.iterator().next().toString()); 

        List[] input1 = new List[] { 
                                      Arrays.asList(new Object[] { new Integer(2)}),  
                                      Arrays.asList(new Object[] { new Integer(4)}),  
                                      Arrays.asList(new Object[] { new Integer(10000)}),  
                                      Arrays.asList(new Object[] { new Integer(10002)}),  
                                  };    
        HardcodedDataManager dataManager = new HardcodedDataManager();              
        dataManager.addData(expectedSql, input1);

        List[] expected = new List[] { 
                                      Arrays.asList(new Object[] { new Long(20002), "5000 +x"}), //$NON-NLS-1$ 
                                  };    
        helpProcess(plan, dataManager, expected);        
    }

    @Test public void testCaseInGroupByAndHavingCantPush3() {  
        String sql = "SELECT s, c FROM (" + //$NON-NLS-1$
            "SELECT sum (IntKey) s, case when IntKey>=5000 then '5000 +' else '0-999' end || 'x' c " + //$NON-NLS-1$
            "FROM BQT1.SmallA GROUP BY case when IntKey>=5000 then '5000 +' else '0-999' end " + //$NON-NLS-1$
            ") AS x WHERE length(c) > 5 AND s = 20002"; //$NON-NLS-1$
        
        // Plan query
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_SEARCHED_CASE, false);   // Can't push GROUP BY
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES, true);
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES_SUM, true);
        capFinder.addCapabilities("BQT1", caps); //$NON-NLS-1$

        QueryMetadataInterface metadata = RealMetadataFactory.exampleBQTCached();
        
        Command command = helpParse(sql);   
        ProcessorPlan plan = helpGetPlan(command, metadata, capFinder);
        
        Set actualQueries = TestOptimizer.getAtomicQueries(plan);
        String expectedSql = "SELECT BQT1.SmallA.IntKey FROM BQT1.SmallA"; //$NON-NLS-1$
        assertEquals(1, actualQueries.size());        
        assertEquals(expectedSql, actualQueries.iterator().next().toString()); 

        List[] input1 = new List[] { 
                                      Arrays.asList(new Object[] { new Integer(2)}),  
                                      Arrays.asList(new Object[] { new Integer(4)}),  
                                      Arrays.asList(new Object[] { new Integer(10000)}),  
                                      Arrays.asList(new Object[] { new Integer(10002)}),  
                                  };    
        HardcodedDataManager dataManager = new HardcodedDataManager();              
        dataManager.addData(expectedSql, input1);

        List[] expected = new List[] { 
                                      Arrays.asList(new Object[] { new Long(20002), "5000 +x"}), //$NON-NLS-1$ 
                                  };    
        helpProcess(plan, dataManager, expected);        
    }

    @Test public void testFunctionOfAggregateCantPush() {  
        String sql = "SELECT StringKey || 'x', SUM(length(StringKey || 'x')) + 1 AS x FROM BQT1.SmallA GROUP BY StringKey || 'x' HAVING space(MAX(length((StringKey || 'x') || 'y'))) = '   '"; //$NON-NLS-1$
        
        // Plan query
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        capFinder.addCapabilities("BQT1", caps); //$NON-NLS-1$

        QueryMetadataInterface metadata = RealMetadataFactory.exampleBQTCached();
        
        Command command = helpParse(sql);   
        ProcessorPlan plan = helpGetPlan(command, metadata, capFinder);
        
        Set actualQueries = TestOptimizer.getAtomicQueries(plan);
        String expectedSql = "SELECT BQT1.SmallA.StringKey FROM BQT1.SmallA"; //$NON-NLS-1$
        assertEquals(1, actualQueries.size());        
        assertEquals(expectedSql, actualQueries.iterator().next().toString()); 

        List[] input1 = new List[] { 
                                      Arrays.asList(new Object[] { "0"}),   //$NON-NLS-1$
                                      Arrays.asList(new Object[] { "1"}),   //$NON-NLS-1$
                                      Arrays.asList(new Object[] { "10"}),   //$NON-NLS-1$
                                      Arrays.asList(new Object[] { "11"}),   //$NON-NLS-1$
                                      Arrays.asList(new Object[] { "100"}),   //$NON-NLS-1$
                                  };    
        HardcodedDataManager dataManager = new HardcodedDataManager();              
        dataManager.addData(expectedSql, input1);

        List[] expected = new List[] { 
                                      Arrays.asList(new Object[] { "0x", new Long(3)}), //$NON-NLS-1$ 
                                      Arrays.asList(new Object[] { "1x", new Long(3)}), //$NON-NLS-1$ 
                                  };    
        helpProcess(plan, dataManager, expected);        
    }
    
    
    @Test public void testCase2634() {
        
        String sql = "SELECT x, IntKey FROM (SELECT IntKey, 'a' AS x FROM BQT1.SmallA UNION ALL SELECT IntKey, 'b' AS x FROM BQT1.SmallB) as Z"; //$NON-NLS-1$
        
        // Plan query
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_SELECT_EXPRESSION, true);
        caps.setCapabilitySupport(Capability.QUERY_UNION, true);
        capFinder.addCapabilities("BQT1", caps); //$NON-NLS-1$

        QueryMetadataInterface metadata = RealMetadataFactory.exampleBQTCached();

        Command command = helpParse(sql);   
        ProcessorPlan plan = helpGetPlan(command, metadata, capFinder);
        
        Set actualQueries = TestOptimizer.getAtomicQueries(plan);
        String expectedSql = "SELECT 'a' AS c_0, BQT1.SmallA.IntKey AS c_1 FROM BQT1.SmallA UNION ALL SELECT 'b' AS c_0, BQT1.SmallB.IntKey AS c_1 FROM BQT1.SmallB"; //$NON-NLS-1$
        assertEquals(1, actualQueries.size());        
        assertEquals(expectedSql, actualQueries.iterator().next().toString()); 

        List[] input1 = new List[] { 
                                      Arrays.asList(new Object[] { "a", new Integer(0)}),   //$NON-NLS-1$
                                      Arrays.asList(new Object[] { "a", new Integer(1)}),   //$NON-NLS-1$
                                      Arrays.asList(new Object[] { "b", new Integer(0)}),   //$NON-NLS-1$
                                      Arrays.asList(new Object[] { "b", new Integer(1)}),   //$NON-NLS-1$
                                  };    
        HardcodedDataManager dataManager = new HardcodedDataManager();              
        dataManager.addData(expectedSql, input1);

        List[] expected = new List[] { 
                                      Arrays.asList(new Object[] { "a", new Integer(0)}),   //$NON-NLS-1$
                                      Arrays.asList(new Object[] { "a", new Integer(1)}),   //$NON-NLS-1$
                                      Arrays.asList(new Object[] { "b", new Integer(0)}),   //$NON-NLS-1$
                                      Arrays.asList(new Object[] { "b", new Integer(1)}),   //$NON-NLS-1$
                                  };    
        helpProcess(plan, dataManager, expected);          
        
    }
    
    @Test public void testQueryWithoutFromWithOrderBy() {
        
        String sql = "select 'three' as x ORDER BY x"; //$NON-NLS-1$

        // Create expected results
        List[] expected = new List[] { 
            Arrays.asList(new Object[] { "three"} ), //$NON-NLS-1$ 
        };       
        // Construct data manager with data
        FakeDataManager dataManager = new FakeDataManager();
        sampleData1(dataManager);   
        // Plan query
        ProcessorPlan plan = helpGetPlan(sql, RealMetadataFactory.example1Cached());       
        // Run query
        helpProcess(plan, dataManager, expected);         
        
    }

    @Test public void testQueryWithoutFromWithOrderBy2() {
        
        String sql = "select concat('three', ' sixteen') as x ORDER BY x"; //$NON-NLS-1$

        // Create expected results
        List[] expected = new List[] { 
            Arrays.asList(new Object[] { "three sixteen"} ), //$NON-NLS-1$ 
        };       
        // Construct data manager with data
        FakeDataManager dataManager = new FakeDataManager();
        sampleData1(dataManager);   
        // Plan query
        ProcessorPlan plan = helpGetPlan(sql, RealMetadataFactory.example1Cached());       
        // Run query
        helpProcess(plan, dataManager, expected);         
        
    }    

    @Test public void testQueryWithoutFromWithOrderBy3() {
        
        String sql = "SELECT CONCAT('yy', 'z') as c1234567890123456789012345678901234567890, " + //$NON-NLS-1$
                     "CONCAT('21', '12') AS EXPR ORDER BY c1234567890123456789012345678901234567890"; //$NON-NLS-1$

        // Create expected results
        List[] expected = new List[] { 
            Arrays.asList(new Object[] { "yyz", "2112"} ), //$NON-NLS-1$ //$NON-NLS-2$ 
        };       
        // Construct data manager with data
        FakeDataManager dataManager = new FakeDataManager();
        sampleData1(dataManager);   
        // Plan query
        ProcessorPlan plan = helpGetPlan(sql, RealMetadataFactory.example1Cached());       
        // Run query
        helpProcess(plan, dataManager, expected);         
        
    }     
    
    @Test public void testCase2507_3(){

        String sql = "SELECT CONCAT('yy', 'z') AS c1234567890123456789012345678901234567890, " + //$NON-NLS-1$
                            "CONCAT('21', '12') AS EXPR ORDER BY c1234567890123456789012345678901234567890"; //$NON-NLS-1$

        // Plan query
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.CRITERIA_COMPARE_EQ, true);
        caps.setCapabilitySupport(Capability.QUERY_ORDERBY, true);
        caps.setFunctionSupport("concat", true); //$NON-NLS-1$
        capFinder.addCapabilities("BQT1", caps); //$NON-NLS-1$

        QueryMetadataInterface metadata = RealMetadataFactory.exampleBQTCached();
        
        ProcessorPlan plan = TestOptimizer.helpPlan(sql,         
                                      metadata,
                                      null, capFinder,
                                      new String[0] ,
                                      TestOptimizer.SHOULD_SUCCEED );

        TestOptimizer.checkNodeTypes(plan, new int[] {
                                        0,      // Access
                                        0,      // DependentAccess
                                        0,      // DependentSelect
                                        0,      // DependentProject
                                        0,      // DupRemove
                                        0,      // Grouping
                                        0,      // Join
                                        0,      // MergeJoin
                                        0,      // Null
                                        0,      // PlanExecution
                                        1,      // Project
                                        0,      // Select
                                        0,      // Sort
                                        0       // UnionAll
                                    });        
        
        // TEST PROCESSING

        List[] expectedResults = new List[] { 
              Arrays.asList(new Object[] { "yyz", "2112"}), //$NON-NLS-1$ //$NON-NLS-2$ 
          };    
        
        // Construct data manager with data
        FakeDataManager dataManager = new FakeDataManager();
        sampleDataBQT1(dataManager);

        // Run query
        helpProcess(plan, dataManager, expectedResults);        
        
    }    

    @Test public void testMultiGroupJoinCriteria() {
        
        String sql = "SELECT X.NEWFIELD FROM " + //$NON-NLS-1$
                       "(SELECT SMALLA.STRINGNUM, " + //$NON-NLS-1$
                        "CASE WHEN SMALLA.STRINGNUM LIKE '1%' THEN SMALLA.INTKEY " + //$NON-NLS-1$
                             "WHEN SMALLA.STRINGNUM LIKE '2%' THEN SMALLB.INTNUM " + //$NON-NLS-1$
                             "WHEN SMALLA.STRINGNUM LIKE '3%' THEN MEDIUMA.INTKEY " + //$NON-NLS-1$
                       "END AS NEWFIELD " + //$NON-NLS-1$
                       "FROM BQT1.SMALLA, BQT1.SMALLB, BQT1.MEDIUMA " + //$NON-NLS-1$
                       "WHERE SMALLA.INTKEY = SMALLB.INTKEY AND SMALLA.INTKEY = MEDIUMA.INTKEY) AS X " + //$NON-NLS-1$
                     "WHERE X.NEWFIELD = -3"; //$NON-NLS-1$
        
        String expectedAtomic1 = "SELECT BQT1.SMALLB.INTKEY, BQT1.SMALLB.INTNUM FROM BQT1.SMALLB"; //$NON-NLS-1$
        String expectedAtomic2 = "SELECT BQT1.MEDIUMA.INTKEY FROM BQT1.MEDIUMA"; //$NON-NLS-1$
        String expectedAtomic3 = "SELECT BQT1.SMALLA.INTKEY, BQT1.SMALLA.STRINGNUM FROM BQT1.SMALLA"; //$NON-NLS-1$
        
        
        // Plan query
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        capFinder.addCapabilities("BQT1", caps); //$NON-NLS-1$
        
        QueryMetadataInterface metadata = RealMetadataFactory.exampleBQTCached();
        
        ProcessorPlan plan = TestOptimizer.helpPlan(sql,         
                          metadata,
                          null, capFinder,
                          new String[] {expectedAtomic1, expectedAtomic2, expectedAtomic3} ,
                          TestOptimizer.SHOULD_SUCCEED );        
        
        TestOptimizer.checkNodeTypes(plan, new int[] {
                            3,      // Access
                            0,      // DependentAccess
                            0,      // DependentSelect
                            0,      // DependentProject
                            0,      // DupRemove
                            0,      // Grouping
                            0,      // Join
                            2,      // MergeJoin
                            0,      // Null
                            0,      // PlanExecution
                            1,      // Project
                            0,      // Select
                            0,      // Sort
                            0       // UnionAll
                        });        
    
        // TEST PROCESSING
        
        List[] expectedResults = new List[] { 
        Arrays.asList(new Object[] { new Integer(-3) }), 
        };    
        
        // Construct data manager with data
        HardcodedDataManager dataManager = new HardcodedDataManager(); 
        List[] input1 = new List[] { 
            Arrays.asList(new Object[] { new Integer(1), new Integer(-4)}), 
            Arrays.asList(new Object[] { new Integer(2), new Integer(-3)}), 
            Arrays.asList(new Object[] { new Integer(3), new Integer(-2)}), 
        };    
        dataManager.addData(expectedAtomic1, input1);
        List[] input2 = new List[] { 
            Arrays.asList(new Object[] { new Integer(1)}), 
            Arrays.asList(new Object[] { new Integer(2)}), 
            Arrays.asList(new Object[] { new Integer(3)}), 
        };    
        dataManager.addData(expectedAtomic2, input2);
        List[] input3 = new List[] { 
            Arrays.asList(new Object[] { new Integer(1), new String("1")}), //$NON-NLS-1$ 
            Arrays.asList(new Object[] { new Integer(2), new String("2")}), //$NON-NLS-1$ 
            Arrays.asList(new Object[] { new Integer(3), new String("3")}), //$NON-NLS-1$ 
        };    
        dataManager.addData(expectedAtomic3, input3);
        
        // Run query
        helpProcess(plan, dataManager, expectedResults);        
        
    }    
    
    /**
     * Cross-source join with group by on top but no aggregate functions - running some special cases
     * where there are no "aggregate groups" (the groups of the aggregate expressions) because there
     * are no aggregate expressions.  In this case, need to switch the aggregate groups to be all the grouping
     * columns because they are all being "grouped on".    
     * 
     * @since 4.3
     */
    @Test public void testDefect18360(){

        String sql = "SELECT a.intkey, a.intnum FROM bqt1.smalla a join bqt2.mediumb b on a.stringkey = b.stringkey " + //$NON-NLS-1$
            "group by a.intkey, a.intnum"; //$NON-NLS-1$

        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        capFinder.addCapabilities("BQT1", new BasicSourceCapabilities()); //$NON-NLS-1$
        capFinder.addCapabilities("BQT2", new BasicSourceCapabilities()); //$NON-NLS-1$

        Command command = helpParse(sql);
        ProcessorPlan plan = helpGetPlan(command, RealMetadataFactory.exampleBQTCached(), capFinder);
        
        // Construct data manager with data        
        HardcodedDataManager dataManager = new HardcodedDataManager();
        List[] data1 = new List[] {
            Arrays.asList(new Object[] { "1", new Integer(1), new Integer(5) }), //$NON-NLS-1$
            Arrays.asList(new Object[] { "2", new Integer(2), new Integer(6) }), //$NON-NLS-1$
            Arrays.asList(new Object[] { "3", new Integer(3), new Integer(7) }), //$NON-NLS-1$
        };        
        dataManager.addData("SELECT bqt1.smalla.stringkey, bqt1.smalla.intkey, bqt1.smalla.intnum FROM bqt1.smalla", data1);  //$NON-NLS-1$

        List[] data2 = new List[] {
            Arrays.asList(new Object[] { "1" }), //$NON-NLS-1$
            Arrays.asList(new Object[] { "2" }), //$NON-NLS-1$
            Arrays.asList(new Object[] { "3" }), //$NON-NLS-1$
        };        
        dataManager.addData("SELECT bqt2.mediumb.stringkey FROM bqt2.mediumb", data2);  //$NON-NLS-1$

        // Run query
        List[] expectedResults = new List[] { 
            Arrays.asList(new Object[] { new Integer(1), new Integer(5) }), 
            Arrays.asList(new Object[] { new Integer(2), new Integer(6) }), 
            Arrays.asList(new Object[] { new Integer(3), new Integer(7) }), 
        };    
        helpProcess(plan, dataManager, expectedResults);          

    }    
    
    @Test public void testDefect17407(){
        String sql = "select pm1.g1.e1 from pm1.g1, g7 MAKEDEP WHERE pm1.g1.e2=g7.e2 order by e1"; //$NON-NLS-1$

        // Create expected results
        List[] expected = new List[] { 
            Arrays.asList(new Object[] { null}),
            Arrays.asList(new Object[] { null}),
            Arrays.asList(new Object[] { "a"}),//$NON-NLS-1$
            Arrays.asList(new Object[] { "a"}),//$NON-NLS-1$
            Arrays.asList(new Object[] { "a"}),//$NON-NLS-1$
            Arrays.asList(new Object[] { "a"}),//$NON-NLS-1$
            Arrays.asList(new Object[] { "a"}), //$NON-NLS-1$           
            Arrays.asList(new Object[] { "b"}),//$NON-NLS-1$
            Arrays.asList(new Object[] { "c"}),//$NON-NLS-1$
            Arrays.asList(new Object[] { "c"}),//$NON-NLS-1$
        };       
        // Construct data manager with data
        FakeDataManager dataManager = new FakeDataManager();
        sampleData1(dataManager);   
        // Plan query
        ProcessorPlan plan = helpGetPlan(sql, RealMetadataFactory.example1Cached());       
        // Run query
        helpProcess(plan, dataManager, expected); 
    }
    
    @Test public void testDecodeAsCriteria() { 
        // Create query 
        String sql = "SELECT x.foo, e2 FROM (select decodestring(e1, 'a,q,b,w') as foo, e2 from vm1.g1) as x where x.foo = 'w'"; //$NON-NLS-1$
        
        // Create expected results
        List[] expected = new List[] { 
                Arrays.asList(new Object[] { "w", new Integer(2) }), //$NON-NLS-1$
        };    
    
        // Construct data manager with data
        FakeDataManager dataManager = new FakeDataManager();
        sampleData1(dataManager);
        
        // Plan query
        ProcessorPlan plan = helpGetPlan(sql, RealMetadataFactory.example1Cached());

        // Run query
        helpProcess(plan, dataManager, expected);
    }
    
    @Test public void testInputParamInNestedExecParam() { 
        // Create query 
        String sql = "EXEC pm1.vsp48('a')"; //$NON-NLS-1$
        
        // Create expected results
        List[] expected = new List[] { 
            Arrays.asList(new Object[] { "a" }), //$NON-NLS-1$
            Arrays.asList(new Object[] { "a" }), //$NON-NLS-1$
            Arrays.asList(new Object[] { "a" }), //$NON-NLS-1$
        };    
    
        // Construct data manager with data
        FakeDataManager dataManager = new FakeDataManager();
        sampleData1(dataManager);
        
        // Plan query
        ProcessorPlan plan = helpGetPlan(sql, RealMetadataFactory.example1Cached());

        // Run query
        helpProcess(plan, dataManager, expected);
    }

    @Test public void testVariableInExecParam() { 
        // Create query 
        String sql = "EXEC pm1.vsp49()"; //$NON-NLS-1$
        
        // Create expected results
        List[] expected = new List[] { 
            Arrays.asList(new Object[] { "b", new Integer(2) }), //$NON-NLS-1$
        };    
    
        // Construct data manager with data
        FakeDataManager dataManager = new FakeDataManager();
        sampleData1(dataManager);
        
        // Plan query
        ProcessorPlan plan = helpGetPlan(sql, RealMetadataFactory.example1Cached());

        // Run query
        helpProcess(plan, dataManager, expected);
    }
    
    @Test public void testVariableInNestedExecParam() { 
        // Create query 
        String sql = "EXEC pm1.vsp50()"; //$NON-NLS-1$
        
        // Create expected results
        List[] expected = new List[] { 
            Arrays.asList(new Object[] { "b" }), //$NON-NLS-1$
        };    
    
        // Construct data manager with data
        FakeDataManager dataManager = new FakeDataManager();
        sampleData1(dataManager);
        
        // Plan query
        ProcessorPlan plan = helpGetPlan(sql, RealMetadataFactory.example1Cached());

        // Run query
        helpProcess(plan, dataManager, expected);
    }

    @Test public void testVariableInNestedExecParamInLoop() { 
        // Create query 
        String sql = "EXEC pm1.vsp51()"; //$NON-NLS-1$
        
        // Create expected results
        List[] expected = new List[] { 
            Arrays.asList(new Object[] { "bb" }), //$NON-NLS-1$
        };    
    
        // Construct data manager with data
        FakeDataManager dataManager = new FakeDataManager();
        sampleData1(dataManager);
        
        // Plan query
        ProcessorPlan plan = helpGetPlan(sql, RealMetadataFactory.example1Cached());

        // Run query
        helpProcess(plan, dataManager, expected);
    }

    @Test public void testVariableInNestedExecParamInAssignment() { 
        // Create query 
        String sql = "EXEC pm1.vsp52()"; //$NON-NLS-1$
        
        // Create expected results
        List[] expected = new List[] { 
            Arrays.asList(new Object[] { "c" }), //$NON-NLS-1$
        };    
    
        // Construct data manager with data
        FakeDataManager dataManager = new FakeDataManager();
        sampleData1(dataManager);
        
        // Plan query
        ProcessorPlan plan = helpGetPlan(sql, RealMetadataFactory.example1Cached());

        // Run query
        helpProcess(plan, dataManager, expected);
    }

    @Test public void testInputParamInNestedExecParamInLoop() { 
        // Create query 
        String sql = "EXEC pm1.vsp53('b')"; //$NON-NLS-1$
        
        // Create expected results
        List[] expected = new List[] { 
            Arrays.asList(new Object[] { "bb" }), //$NON-NLS-1$
        };    
    
        // Construct data manager with data
        FakeDataManager dataManager = new FakeDataManager();
        sampleData1(dataManager);
        
        // Plan query
        ProcessorPlan plan = helpGetPlan(sql, RealMetadataFactory.example1Cached());

        // Run query
        helpProcess(plan, dataManager, expected);
    }

    @Test public void testInputParamInNestedExecParamInAssignment() { 
        // Create query 
        String sql = "EXEC pm1.vsp54('c')"; //$NON-NLS-1$
        
        // Create expected results
        List[] expected = new List[] { 
            Arrays.asList(new Object[] { "c" }), //$NON-NLS-1$
        };    
    
        // Construct data manager with data
        FakeDataManager dataManager = new FakeDataManager();
        sampleData1(dataManager);
        
        // Plan query
        ProcessorPlan plan = helpGetPlan(sql, RealMetadataFactory.example1Cached());

        // Run query
        helpProcess(plan, dataManager, expected);
    }
    
    @Test public void testBitwiseAggregateProc() throws Exception { 
        // Create query 
        String sql = "EXEC virt.agg()"; //$NON-NLS-1$
        
        // Create expected results
        List[] expected = new List[] { 
            Arrays.asList(new Object[] { new Integer(0), "a", new Integer(19) }), //$NON-NLS-1$
            Arrays.asList(new Object[] { new Integer(1), "b", new Integer(4) }), //$NON-NLS-1$
            Arrays.asList(new Object[] { new Integer(2), "c", new Integer(3) }), //$NON-NLS-1$
        };    
    
        // Plan query
        QueryMetadataInterface metadata = RealMetadataFactory.exampleBitwise();
        ProcessorPlan plan = helpGetPlan(sql, metadata);

        // Construct data manager with data
        FakeDataManager dataManager = new FakeDataManager();
        sampleDataBitwise(dataManager, metadata);
        
        // Run query
        helpProcess(plan, dataManager, expected);
    }
    
    private void sampleDataBitwise(FakeDataManager dataMgr, QueryMetadataInterface metadata) throws Exception {    
        dataMgr.registerTuples(
            metadata,
            "phys.t", new List[] { 
				    Arrays.asList(new Object[] { new Integer(0), "a", new Integer(1) }), //$NON-NLS-1$
				    Arrays.asList(new Object[] { new Integer(0), "a", new Integer(3) }), //$NON-NLS-1$
				    Arrays.asList(new Object[] { new Integer(0), "a", new Integer(16) }), //$NON-NLS-1$
				    Arrays.asList(new Object[] { new Integer(1), "b", new Integer(4) }), //$NON-NLS-1$
				    Arrays.asList(new Object[] { new Integer(2), "c", new Integer(2) }), //$NON-NLS-1$
				    Arrays.asList(new Object[] { new Integer(2), "c", new Integer(1) }), //$NON-NLS-1$
				    } );    
    }  
    
    @Test public void testFunctionGroupByInJoinCriteria() {  
        // Create query  
        String sql = "SELECT lower(vm1.g1.e1) from vm1.g1, vm1.g2a where vm1.g1.e1 = vm1.g2a.e1 group by lower(vm1.g1.e1)"; //$NON-NLS-1$ 
         
        // Create expected results 
        List[] expected = new List[] {  
                        Arrays.asList(new Object[] { "a" }), //$NON-NLS-1$ 
                Arrays.asList(new Object[] { "b" }), //$NON-NLS-1$ 
                Arrays.asList(new Object[] { "c" }) //$NON-NLS-1$ 
                };     

        // Construct data manager with data 
        FakeDataManager dataManager = new FakeDataManager(); 
        sampleData1(dataManager); 
         
        // Plan query 
        ProcessorPlan plan = helpGetPlan(sql, RealMetadataFactory.example1Cached()); 
 
        // Run query 
        helpProcess(plan, dataManager, expected); 
    }

    private TransformationMetadata createProjectErrorMetadata() {
    	MetadataStore metadataStore = new MetadataStore();
        Schema p1 = RealMetadataFactory.createPhysicalModel("p1", metadataStore); //$NON-NLS-1$
        Table t1 = RealMetadataFactory.createPhysicalGroup("t", p1); //$NON-NLS-1$
        RealMetadataFactory.createElements(t1, new String[] {"a", "b" }, new String[] { "string", "string" }); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
        
        Schema v1 = RealMetadataFactory.createVirtualModel("v1", metadataStore); //$NON-NLS-1$
        QueryNode n1 = new QueryNode("SELECT convert(a, integer) as c, b FROM p1.t"); //$NON-NLS-1$ //$NON-NLS-2$
        Table vt1 = RealMetadataFactory.createVirtualGroup("t1", v1, n1); //$NON-NLS-1$
        RealMetadataFactory.createElements(vt1, new String[] {"c", "b" }, new String[] { "integer", "string" }); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$

        QueryNode n3 = new QueryNode("SELECT c, b FROM v1.t1 UNION ALL SELECT c, b FROM v1.t1"); //$NON-NLS-1$ //$NON-NLS-2$
        Table vu1 = RealMetadataFactory.createVirtualGroup("u1", v1, n3); //$NON-NLS-1$
        RealMetadataFactory.createElements(vu1, new String[] {"c", "b" }, new String[] { "integer", "string" }); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
        
        return RealMetadataFactory.createTransformationMetadata(metadataStore, "projectError");
    }
    
    @Test public void testProjectionErrorOverUnionWithConvert() {  
        // Create query  
        QueryMetadataInterface metadata = createProjectErrorMetadata();
        String sql = "SELECT COUNT(*) FROM v1.u1"; //$NON-NLS-1$ 
         
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = TestOptimizer.getTypicalCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_UNION, true);
        caps.setCapabilitySupport(Capability.QUERY_SELECT_EXPRESSION, true);
        capFinder.addCapabilities("p1", caps); //$NON-NLS-1$

        Command command = helpParse(sql);
        ProcessorPlan plan = helpGetPlan(command, metadata, capFinder);
         
        // Run query 
        // Create expected results 
        List[] expected = new List[] {  
                        Arrays.asList(new Object[] { new Integer(2) }), 
                };     

        // Construct data manager with data 
        HardcodedDataManager dataManager = new HardcodedDataManager(); 
        dataManager.addData("SELECT 1 AS c_0 FROM p1.t AS g_1 UNION ALL SELECT 1 AS c_0 FROM p1.t AS g_0",  //$NON-NLS-1$
                            new List[] { Arrays.asList(1), Arrays.asList(1)});
        helpProcess(plan, dataManager, expected); 
    }
    
    @Test public void testUpdatesInLoop() { 
        String sql = "update vm1.g39 set e2=3"; //$NON-NLS-1$ 
 
        // Plan query 
        ProcessorPlan plan = helpGetPlan(sql, RealMetadataFactory.example1Cached(), TestOptimizer.getGenericFinder());        

        // Construct data manager with data 
        HardcodedDataManager dataManager = new HardcodedDataManager(); 
        dataManager.addData("SELECT g_0.e2 FROM pm1.g1 AS g_0 WHERE g_0.e2 = 3", //$NON-NLS-1$ 
                            new List[] { Arrays.asList(new Object[] { new Integer(3) } )});
        dataManager.addData("UPDATE pm1.g1 SET e2 = 3 WHERE pm1.g1.e2 = 3", //$NON-NLS-1$ 
                            new List[] { Arrays.asList(new Object[] { new Integer(1) } )});
        
        // Create expected results 
        List[] expected = new List[] { Arrays.asList(0)};        
        
        // Run query 
        helpProcess(plan, dataManager, expected);  
         
    }
    
    @Test public void testRand() { 
        // Create query 
        String sql = "SELECT RAND(E2) FROM pm1.g1 where pm1.g1.e2=3"; //$NON-NLS-1$
        
        // Create expected results
        List[] expected = new List[] { 
            Arrays.asList(new Object[] {new Double(0.731057369148862)}),
        };    
    
        // Construct data manager with data
        FakeDataManager dataManager = new FakeDataManager();
        sampleData1(dataManager);
        
        // Plan query
        ProcessorPlan plan = helpGetPlan(sql, RealMetadataFactory.example1Cached());
        
        // Run query
        helpProcess(plan, dataManager, expected);
    }
    
    /*
     *  Prior to case 3994 testInsertTempTableCreation1 worked, but testInsertTempTableCreation did not.
     *  Now they should both pass
     * 
     */    
    @Test public void testInsertTempTableCreation() {
    	MetadataStore metadataStore = new MetadataStore();
        Schema v1 = RealMetadataFactory.createVirtualModel("v1", metadataStore); //$NON-NLS-1$
        QueryNode n1 = new QueryNode("CREATE VIRTUAL PROCEDURE BEGIN insert into #temp (var1) values (1); select #temp.var1 from #temp; END"); //$NON-NLS-1$ //$NON-NLS-2$
        ColumnSet<Procedure> rs = RealMetadataFactory.createResultSet("rs", new String[] { "var1" }, new String[] { DataTypeManager.DefaultDataTypes.INTEGER}); //$NON-NLS-1$ //$NON-NLS-2$
        Procedure vp = RealMetadataFactory.createVirtualProcedure("vp", v1, null, n1); //$NON-NLS-1$
        vp.setResultSet(rs);
        QueryMetadataInterface metadata = RealMetadataFactory.createTransformationMetadata(metadataStore, "foo");
        
        ProcessorPlan plan = helpGetPlan("exec v1.vp()", metadata); //$NON-NLS-1$
        
        List[] expected = new List[] {
            Arrays.asList(new Object[] { new Integer(1) })
        };
        helpProcess(plan, new FakeDataManager(), expected);
    }
    
    @Test public void testInsertTempTableCreation1() {
    	MetadataStore metadataStore = new MetadataStore();
        Schema v1 = RealMetadataFactory.createVirtualModel("v1", metadataStore); //$NON-NLS-1$
        QueryNode n1 = new QueryNode("CREATE VIRTUAL PROCEDURE BEGIN insert into #temp (var1) values (1); select 2 as var1 into #temp; select #temp.var1 from #temp; END"); //$NON-NLS-1$ //$NON-NLS-2$
        ColumnSet<Procedure> rs = RealMetadataFactory.createResultSet("rs", new String[] { "var1" }, new String[] { DataTypeManager.DefaultDataTypes.INTEGER}); //$NON-NLS-1$ //$NON-NLS-2$
        Procedure vp = RealMetadataFactory.createVirtualProcedure("vp", v1, null, n1); //$NON-NLS-1$
        vp.setResultSet(rs);
        QueryMetadataInterface metadata = RealMetadataFactory.createTransformationMetadata(metadataStore, "foo");
        
        ProcessorPlan plan = helpGetPlan("exec v1.vp()", metadata); //$NON-NLS-1$
        
        List[] expected = new List[] {
            Arrays.asList(new Object[] { new Integer(1) }),
            Arrays.asList(new Object[] { new Integer(2) })
        };
        helpProcess(plan, new FakeDataManager(), expected);
    }    
        
    @Test public void testCase4531() { 
        String sql = "select intkey, intnum from (select intnum as intkey, 1 as intnum from bqt1.smalla union all select intkey, intnum from bqt1.smalla union all select intkey, intnum from bqt2.smalla) x"; //$NON-NLS-1$ 
 
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder(); 
        BasicSourceCapabilities caps = TestOptimizer.getTypicalCapabilities(); 
        caps.setCapabilitySupport(Capability.QUERY_UNION, true); 
        caps.setCapabilitySupport(Capability.QUERY_SELECT_EXPRESSION, true); 
        capFinder.addCapabilities("BQT1", caps); //$NON-NLS-1$ 
        capFinder.addCapabilities("BQT2", caps); //$NON-NLS-1$ 
 
        Command command = helpParse(sql); 
        ProcessorPlan plan = helpGetPlan(command, RealMetadataFactory.exampleBQTCached(), capFinder); 
 
        // Run query  
        // Create expected results  
        List[] expected = new List[] {   
                        Arrays.asList(new Object[] { new Integer(1), new Integer(1) }), 
                        Arrays.asList(new Object[] { new Integer(1), new Integer(1) }), 
                        Arrays.asList(new Object[] { new Integer(1), new Integer(1) }), 
                };      
         
        // Construct data manager with data  
        HardcodedDataManager dataManager = new HardcodedDataManager();  
        dataManager.addData("SELECT g_0.intkey, g_0.intnum FROM bqt2.smalla AS g_0",  //$NON-NLS-1$ 
                            new List[] { Arrays.asList(new Object[] { new Integer(1), new Integer(1) })}); 
        dataManager.addData("SELECT g_1.intnum AS c_0, 1 AS c_1 FROM bqt1.smalla AS g_1 UNION ALL SELECT g_0.IntKey AS c_0, g_0.IntNum AS c_1 FROM bqt1.smalla AS g_0",  //$NON-NLS-1$ 
                new List[] { Arrays.asList(new Object[] { new Integer(1), new Integer(1) }), 
                                 Arrays.asList(new Object[] { new Integer(1), new Integer(1) })}); 
        helpProcess(plan, dataManager, expected);  
    }
        
    private void sampleDataBQT2a(FakeDataManager dataMgr) throws Exception {
        QueryMetadataInterface metadata = RealMetadataFactory.exampleBQTCached();
    
        String[] groups = new String[] {"bqt1.smalla", "bqt2.smalla", "bqt3.smalla" }; //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    
        for(int groupIndex=0; groupIndex<groups.length; groupIndex++) {
            String groupName = groups[groupIndex];

            List[] tuples = new List[3];
            for(int row=0; row<tuples.length; row++) {
                tuples[row] = new ArrayList(17);
                tuples[row].add(new Integer(row)); //IntKey
                tuples[row].add(String.valueOf(row)); //StringKey
                tuples[row].add(new Integer(row));  //IntNum
                tuples[row].add(String.valueOf(row)); //StringNum
                for(int col=0; col<10; col++) { //FloatNum, LongNum, DoubleNum, ByteNum, DateValue, TimeValue, TimestampValue, BooleanValue, CharValue, ShortValue
                    tuples[row].add(null);    
                }    
                tuples[row].add(new BigInteger(String.valueOf(row))); //BigIntegerValue
                tuples[row].add(new BigDecimal(row)); //BigDecimalValue
                tuples[row].add(null);    //ObjectValue
            }
            dataMgr.registerTuples(metadata, groupName, tuples);
        }
    }    
    
    @Test public void testDefect15355() throws Exception {
        
        String sql = "SELECT e1, e1 FROM pm1.g1 "   //$NON-NLS-1$
        +"UNION ALL "   //$NON-NLS-1$
        +"SELECT e1, (SELECT e1 FROM pm2.g1 WHERE pm2.g1.e2 = pm1.g2.e2) FROM pm1.g2"; //$NON-NLS-1$
        
        
        // Create expected results
        List[] expected = new List[] { 
            Arrays.asList(new Object[] { "a", "a" }), //$NON-NLS-1$  //$NON-NLS-2$
            Arrays.asList(new Object[] { "b", "b" }), //$NON-NLS-1$  //$NON-NLS-2$
            Arrays.asList(new Object[] { "c", "c" }), //$NON-NLS-1$  //$NON-NLS-2$
            Arrays.asList(new Object[] { "a", "e" }), //$NON-NLS-1$  //$NON-NLS-2$
            Arrays.asList(new Object[] { "b", "b" }), //$NON-NLS-1$  //$NON-NLS-2$
            Arrays.asList(new Object[] { "b", null }), //$NON-NLS-1$  
            Arrays.asList(new Object[] { "b", null }), //$NON-NLS-1$  
            Arrays.asList(new Object[] { "d", null }), //$NON-NLS-1$  
        };    
        
        // Construct data manager with data
        FakeDataManager dataManager = new FakeDataManager();
        sampleData2(dataManager);
        
        // Plan query
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$
        capFinder.addCapabilities("pm2", caps); //$NON-NLS-1$
        
        QueryMetadataInterface metadata = RealMetadataFactory.example1Cached();
        
        Command command = helpParse(sql);   
        ProcessorPlan plan = helpGetPlan(command, metadata, capFinder);
        
        // Run query
        helpProcess(plan, dataManager, expected);
    }
    
    @Test public void testDefect15355b() throws Exception {
        
        String sql = "SELECT StringKey, BigIntegerValue FROM BQT1.SmallA "   //$NON-NLS-1$
        +"UNION ALL "   //$NON-NLS-1$
        +"SELECT StringKey, (SELECT BigIntegerValue FROM BQT3.SmallA WHERE BQT3.SmallA.BigIntegerValue = BQT2.SmallA.StringNum) FROM BQT2.SmallA"; //$NON-NLS-1$
        
        
        // Create expected results
        List[] expected = new List[] { 
            Arrays.asList(new Object[] { "0", new BigInteger("0") }), //$NON-NLS-1$  //$NON-NLS-2$
            Arrays.asList(new Object[] { "1", new BigInteger("1") }), //$NON-NLS-1$  //$NON-NLS-2$
            Arrays.asList(new Object[] { "2", new BigInteger("2") }), //$NON-NLS-1$  //$NON-NLS-2$
            Arrays.asList(new Object[] { "0", new BigInteger("0") }), //$NON-NLS-1$  //$NON-NLS-2$
            Arrays.asList(new Object[] { "1", new BigInteger("1") }), //$NON-NLS-1$  //$NON-NLS-2$
            Arrays.asList(new Object[] { "2", new BigInteger("2") }), //$NON-NLS-1$  //$NON-NLS-2$
        };    
        
        // Construct data manager with data
        FakeDataManager dataManager = new FakeDataManager();
        sampleDataBQT2a(dataManager);
        
        // Plan query
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        capFinder.addCapabilities("BQT1", caps); //$NON-NLS-1$
        capFinder.addCapabilities("BQT2", caps); //$NON-NLS-1$
        capFinder.addCapabilities("BQT3", caps); //$NON-NLS-1$
        
        QueryMetadataInterface metadata = RealMetadataFactory.exampleBQTCached();
        
        Command command = helpParse(sql);   
        ProcessorPlan plan = helpGetPlan(command, metadata, capFinder);
        
        // Run query
        helpProcess(plan, dataManager, expected);           
        
    }    

    @Test public void testDefect15355c() throws Exception {
        
        String sql = "SELECT StringKey, BigIntegerValue FROM VQT.Defect15355 WHERE StringKey = '0'";  //$NON-NLS-1$
        
        // Create expected results
        List[] expected = new List[] { 
            Arrays.asList(new Object[] { "0", new BigInteger("0") }), //$NON-NLS-1$  //$NON-NLS-2$
        };    
        
        // Construct data manager with data
        FakeDataManager dataManager = new FakeDataManager();
        sampleDataBQT2a(dataManager);
        
        // Plan query
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        capFinder.addCapabilities("BQT1", caps); //$NON-NLS-1$
        capFinder.addCapabilities("BQT2", caps); //$NON-NLS-1$
        capFinder.addCapabilities("BQT3", caps); //$NON-NLS-1$
        
        QueryMetadataInterface metadata = RealMetadataFactory.exampleBQTCached();
        
        Command command = helpParse(sql);   
        ProcessorPlan plan = helpGetPlan(command, metadata, capFinder);
        
        // Run query
        helpProcess(plan, dataManager, expected);           
        
    }       
    
    @Test public void testDefect15355d() throws Exception {
        
        String sql = "SELECT StringKey, BigIntegerValue FROM VQT.Defect15355a WHERE StringKey = '0'";  //$NON-NLS-1$
        
        // Create expected results
        List[] expected = new List[] { 
            Arrays.asList(new Object[] { "0", new BigInteger("0") }), //$NON-NLS-1$  //$NON-NLS-2$
        };    
        
        // Construct data manager with data
        FakeDataManager dataManager = new FakeDataManager();
        sampleDataBQT2a(dataManager);
        
        // Plan query
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        capFinder.addCapabilities("BQT1", caps); //$NON-NLS-1$
        capFinder.addCapabilities("BQT2", caps); //$NON-NLS-1$
        capFinder.addCapabilities("BQT3", caps); //$NON-NLS-1$
        
        QueryMetadataInterface metadata = RealMetadataFactory.exampleBQTCached();
        
        Command command = helpParse(sql);   
        ProcessorPlan plan = helpGetPlan(command, metadata, capFinder);
        
        // Run query
        helpProcess(plan, dataManager, expected);           
        
    }     

    @Test public void testDefect15355e() throws Exception {
        
        String sql = "SELECT BigIntegerValue, StringKey FROM VQT.Defect15355 WHERE StringKey = '0'";  //$NON-NLS-1$
        
        // Create expected results
        List[] expected = new List[] { 
            Arrays.asList(new Object[] { new BigInteger("0"), "0" }), //$NON-NLS-1$  //$NON-NLS-2$
        };    
        
        // Construct data manager with data
        FakeDataManager dataManager = new FakeDataManager();
        sampleDataBQT2a(dataManager);
        
        // Plan query
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        capFinder.addCapabilities("BQT1", caps); //$NON-NLS-1$
        capFinder.addCapabilities("BQT2", caps); //$NON-NLS-1$
        capFinder.addCapabilities("BQT3", caps); //$NON-NLS-1$
        
        QueryMetadataInterface metadata = RealMetadataFactory.exampleBQTCached();
        
        Command command = helpParse(sql);   
        ProcessorPlan plan = helpGetPlan(command, metadata, capFinder);
        
        // Run query
        helpProcess(plan, dataManager, expected);           
        
    }    

    @Test public void testDefect15355f() throws Exception {
        
        String sql = "SELECT BigIntegerValue FROM VQT.Defect15355 WHERE StringKey = '0'";  //$NON-NLS-1$
        
        // Create expected results
        List[] expected = new List[] { 
            Arrays.asList(new Object[] { new BigInteger("0") }), //$NON-NLS-1$  
        };    
        
        // Construct data manager with data
        FakeDataManager dataManager = new FakeDataManager();
        sampleDataBQT2a(dataManager);
        
        // Plan query
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        capFinder.addCapabilities("BQT1", caps); //$NON-NLS-1$
        capFinder.addCapabilities("BQT2", caps); //$NON-NLS-1$
        capFinder.addCapabilities("BQT3", caps); //$NON-NLS-1$
        
        QueryMetadataInterface metadata = RealMetadataFactory.exampleBQTCached();
        
        Command command = helpParse(sql);   
        ProcessorPlan plan = helpGetPlan(command, metadata, capFinder);
        
        // Run query
        helpProcess(plan, dataManager, expected);           
        
    }     

    @Test public void testDefect15355f2() throws Exception {
        
        String sql = "SELECT BigIntegerValue FROM VQT.Defect15355 WHERE StringKey LIKE '%0' AND StringKey LIKE '0%'";  //$NON-NLS-1$
        
        // Create expected results
        List[] expected = new List[] { 
            Arrays.asList(new Object[] { new BigInteger("0") }), //$NON-NLS-1$  
        };    
        
        // Construct data manager with data
        FakeDataManager dataManager = new FakeDataManager();
        sampleDataBQT2a(dataManager);
        
        // Plan query
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        capFinder.addCapabilities("BQT1", caps); //$NON-NLS-1$
        capFinder.addCapabilities("BQT2", caps); //$NON-NLS-1$
        capFinder.addCapabilities("BQT3", caps); //$NON-NLS-1$
        
        QueryMetadataInterface metadata = RealMetadataFactory.exampleBQTCached();
        
        Command command = helpParse(sql);   
        ProcessorPlan plan = helpGetPlan(command, metadata, capFinder);
        
        // Run query
        helpProcess(plan, dataManager, expected);           
        
    }      
    
    @Test public void testDefect15355g() throws Exception {
        
        String sql = "SELECT BigIntegerValue AS a, BigIntegerValue AS b FROM VQT.Defect15355 WHERE StringKey = '0'";  //$NON-NLS-1$
        
        // Create expected results
        List[] expected = new List[] { 
            Arrays.asList(new Object[] { new BigInteger("0"), new BigInteger("0") }), //$NON-NLS-1$  //$NON-NLS-2$
        };    
        
        // Construct data manager with data
        FakeDataManager dataManager = new FakeDataManager();
        sampleDataBQT2a(dataManager);
        
        // Plan query
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        capFinder.addCapabilities("BQT1", caps); //$NON-NLS-1$
        capFinder.addCapabilities("BQT2", caps); //$NON-NLS-1$
        capFinder.addCapabilities("BQT3", caps); //$NON-NLS-1$
        
        QueryMetadataInterface metadata = RealMetadataFactory.exampleBQTCached();
        
        Command command = helpParse(sql);   
        ProcessorPlan plan = helpGetPlan(command, metadata, capFinder);
        
        // Run query
        helpProcess(plan, dataManager, expected);           
        
    }     

    @Test public void testDefect15355h() throws Exception {
        
        String sql = "SELECT BigIntegerValue FROM VQT.Defect15355 WHERE BigIntegerValue = '0'";  //$NON-NLS-1$
                      
        // Create expected results
        List[] expected = new List[] { 
            Arrays.asList(new Object[] { new BigInteger("0") }), //$NON-NLS-1$  
        };    
        
        // Construct data manager with data
        FakeDataManager dataManager = new FakeDataManager();
        sampleDataBQT2a(dataManager);
        
        // Plan query
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        capFinder.addCapabilities("BQT1", caps); //$NON-NLS-1$
        capFinder.addCapabilities("BQT2", caps); //$NON-NLS-1$
        capFinder.addCapabilities("BQT3", caps); //$NON-NLS-1$
        
        QueryMetadataInterface metadata = RealMetadataFactory.exampleBQTCached();
        
        Command command = helpParse(sql);   
        ProcessorPlan plan = helpGetPlan(command, metadata, capFinder);
        
        // Run query
        helpProcess(plan, dataManager, expected);           
        
    }     

    @Test public void testDefect15355i() throws Exception {
        
        String sql = "SELECT BigIntegerValue FROM VQT.Defect15355b WHERE BigIntegerValue = '0'";  //$NON-NLS-1$
        
        // Create expected results
        List[] expected = new List[] { 
            Arrays.asList(new Object[] { new BigInteger("0") }), //$NON-NLS-1$  
        };    
        
        // Construct data manager with data
        FakeDataManager dataManager = new FakeDataManager();
        sampleDataBQT2a(dataManager);
        
        // Plan query
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        capFinder.addCapabilities("BQT1", caps); //$NON-NLS-1$
        capFinder.addCapabilities("BQT2", caps); //$NON-NLS-1$
        capFinder.addCapabilities("BQT3", caps); //$NON-NLS-1$
        
        QueryMetadataInterface metadata = RealMetadataFactory.exampleBQTCached();
        
        Command command = helpParse(sql);   
        ProcessorPlan plan = helpGetPlan(command, metadata, capFinder);
        
        // Run query
        helpProcess(plan, dataManager, expected);           
        
    }  
    
    /**
     * The inner most A.e1 was mistakenly getting transformed into pm1.g3.e1 
     */
    @Test public void testInnerCorrelatedReference() throws Exception {
        
        String sql = "SELECT DISTINCT A.e1 FROM pm1.g3 AS A WHERE (A.e1 IN (SELECT A.e1 FROM pm1.g3))";  //$NON-NLS-1$
        
        // Create expected results
        List[] expected = new List[] { 
        };    
        
        // Construct data manager with data
        HardcodedDataManager dataManager = new HardcodedDataManager();
        dataManager.addData("SELECT DISTINCT g_0.e1 FROM pm1.g3 AS g_0 WHERE g_0.e1 IN (SELECT g_0.e1 FROM pm1.g3 AS g_1)", expected); //$NON-NLS-1$
        
        QueryMetadataInterface metadata = RealMetadataFactory.example1Cached();
        
        // Plan query
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = TestOptimizer.getTypicalCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_FROM_INLINE_VIEWS, true);
        caps.setCapabilitySupport(Capability.QUERY_SELECT_EXPRESSION, true);
        caps.setCapabilitySupport(Capability.QUERY_SUBQUERIES_CORRELATED, true);
        caps.setCapabilitySupport(Capability.CRITERIA_IN_SUBQUERY, true);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$
        
        Command command = helpParse(sql);   
        ProcessorPlan plan = helpGetPlan(command, metadata, capFinder);
        
        // Run query
        helpProcess(plan, dataManager, expected);           
        
    }
    
    @Test public void testCase5413() throws Exception {

        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.CRITERIA_COMPARE_EQ, true);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$

        // Plan query
        String sql = "SELECT e1 FROM pm1.g2 WHERE LOOKUP('pm1.g1','e1', 'e2', 0) = e1";//$NON-NLS-1$
        QueryMetadataInterface metadata = RealMetadataFactory.example1Cached();
        Command command = TestProcessor.helpParse(sql);   
        CommandContext context = createCommandContext();
        ProcessorPlan plan = helpGetPlan(command, metadata, capFinder, context);
        
        // Run query
        List[] expected = new List[] {
            Arrays.asList(new Object[] { "a"}), //$NON-NLS-1$
        };
        
        FakeDataManager dataManager = new FakeDataManager();
        FakeDataStore.sampleData2(dataManager);
        
        helpProcess(plan, context, dataManager, expected);
    } 
    
    @Test public void testRaiseNullWithSelectInto() {
        String sql = "select pm1.g1.e1, pm1.g1.e2, pm1.g1.e3, pm1.g1.e4 into pm1.g2 from pm1.g1 where (1=0)"; //$NON-NLS-1$
        ProcessorPlan plan = helpGetPlan(helpParse(sql), RealMetadataFactory.example1Cached(), TestOptimizer.getGenericFinder()); 
        
        TestOptimizer.checkNodeTypes(plan, new int[] {
                0,      // Access
                0,      // DependentAccess
                0,      // DependentSelect
                0,      // DependentProject
                0,      // DupRemove
                0,      // Grouping
                0,      // NestedLoopJoinStrategy
                0,      // MergeJoinStrategy
                1,      // Null
                0,      // PlanExecution
                1,      // Project
                0,      // Select
                0,      // Sort
                0       // UnionAll
            });
        
        List[] expected = new List[] {
                Arrays.asList(new Object[] { new Integer(0)}),
        };

        helpProcess(plan, new FakeDataManager(), expected);
        
    }    
    
    /*
     * Test for Case6219
     */
    @Test public void testCase6219() {
        String sql = "SELECT e1 FROM pm1.g1, (SELECT 'ACT' AS StateCode,'A' AS StateAbbrv UNION ALL SELECT 'NSW' AS StateCode, 'N' AS StateAbbrv) AS StateNames_Abbrvs WHERE (pm1.g1.e1 = StateCode) AND ((StateNames_Abbrvs.StateAbbrv || pm1.g1.e1) = 'VVIC')"; //$NON-NLS-1$
        ProcessorPlan plan = helpGetPlan(helpParse(sql), RealMetadataFactory.example1Cached(), TestOptimizer.getGenericFinder()); 
        
        TestOptimizer.checkNodeTypes(plan, new int[] {
                0,      // Access
                1,      // DependentAccess
                0,      // DependentSelect
                0,      // DependentProject
                0,      // DupRemove
                0,      // Grouping
                0,      // NestedLoopJoinStrategy
                1,      // MergeJoinStrategy
                0,      // Null
                0,      // PlanExecution
                3,      // Project
                0,      // Select
                0,      // Sort
                1       // UnionAll
            });
        
        List[] expected = new List[] {
        };
        
        FakeDataManager manager = new FakeDataManager();
        sampleData1(manager);
        helpProcess(plan, manager, expected);
        
    }
    
    @Test public void testSortWithLimit() {
        String sql = "select e1 from (select pm1.g1.e1, pm1.g1.e2 from pm1.g1 order by pm1.g1.e1, pm1.g1.e2 limit 1) x"; //$NON-NLS-1$
        
        QueryMetadataInterface metadata = RealMetadataFactory.example1Cached();
        
        ProcessorPlan plan = helpGetPlan(helpParse(sql), metadata);
        
        List[] expected = new List[] {
                Arrays.asList(new Object[] { null}),
        };

        FakeDataManager manager = new FakeDataManager();
        sampleData1(manager);
        helpProcess(plan, manager, expected);
    }
    
    @Test public void testSortWithLimit1() {
        String sql = "select c from (select pm1.g1.e1 a, pm1.g1.e2 b, pm1.g1.e3 c from pm1.g1 order by b limit 1) x"; //$NON-NLS-1$
        
        QueryMetadataInterface metadata = RealMetadataFactory.example1Cached();
        
        ProcessorPlan plan = helpGetPlan(helpParse(sql), metadata);
        
        List[] expected = new List[] {
                Arrays.asList(new Object[] { Boolean.FALSE}),
        };

        FakeDataManager manager = new FakeDataManager();
        sampleData1(manager);
        helpProcess(plan, manager, expected);
        //note that the e1 column is not used in the source query
        assertEquals("SELECT pm1.g1.e3, pm1.g1.e2 FROM pm1.g1", manager.getQueries().iterator().next()); //$NON-NLS-1$
    }
    
    @Test public void testSortWithLimit2() {
        String sql = "select a from (select max(e2) a from pm1.g1 group by e2 order by a limit 1) x where a = 0"; //$NON-NLS-1$
        
        QueryMetadataInterface metadata = RealMetadataFactory.example1Cached();
        
        ProcessorPlan plan = helpGetPlan(helpParse(sql), metadata, TestOptimizer.getGenericFinder());
        
        List[] expected = new List[] {
                Arrays.asList(new Object[] { new Integer(0) }),
        };

        FakeDataManager manager = new FakeDataManager();
        sampleData1(manager);
        helpProcess(plan, manager, expected);
    }
    
    /**
     * A more direct test of 1, where the nested order by is unrelated
     */
    @Test public void testSortWithLimit3() {
        String sql = "select c from (select pm1.g1.e3 c from pm1.g1 order by pm1.g1.e2 limit 1) x"; //$NON-NLS-1$
        
        QueryMetadataInterface metadata = RealMetadataFactory.example1Cached();
        
        ProcessorPlan plan = helpGetPlan(helpParse(sql), metadata, TestOptimizer.getGenericFinder());
        
        List[] expected = new List[] {
                Arrays.asList(new Object[] { Boolean.FALSE }),
        };

        FakeDataManager manager = new FakeDataManager();
        sampleData1(manager);
        helpProcess(plan, manager, expected);
    }
    
    @Test public void testSortWithLimit4() {
        String sql = "select c from (select pm1.g1.e1 a, pm1.g1.e2 b, pm1.g1.e3 c from pm1.g1 order by b limit 1) x"; //$NON-NLS-1$
        
        QueryMetadataInterface metadata = RealMetadataFactory.example1Cached();
        
        BasicSourceCapabilities caps = TestOptimizer.getTypicalCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_ORDERBY_UNRELATED, false);
        
        ProcessorPlan plan = helpGetPlan(helpParse(sql), metadata, new DefaultCapabilitiesFinder(caps));
        
        List[] expected = new List[] {
                Arrays.asList(new Object[] { Boolean.FALSE }),
        };

        FakeDataManager manager = new FakeDataManager();
        sampleData1(manager);
        helpProcess(plan, manager, expected);
        assertEquals("SELECT g_0.e3 AS c_0, g_0.e2 AS c_1 FROM pm1.g1 AS g_0 ORDER BY c_1", manager.getQueries().iterator().next()); //$NON-NLS-1$
    }
    
    @Test public void testSortWithOffset() {
        String sql = "select e1 from (select pm1.g1.e1, pm1.g1.e2 from pm1.g1 order by pm1.g1.e1, pm1.g1.e2 offset 4 rows) x"; //$NON-NLS-1$
        
        QueryMetadataInterface metadata = RealMetadataFactory.example1Cached();
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.ROW_OFFSET, true);
        caps.setCapabilitySupport(Capability.QUERY_ORDERBY, true);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$
        
        ProcessorPlan plan = helpGetPlan(helpParse(sql), metadata, capFinder);
        
        List[] expected = new List[] {
                Arrays.asList(new Object[] { "a"}),
        };

        HardcodedDataManager manager = new HardcodedDataManager(RealMetadataFactory.example1Cached());
        manager.addData("SELECT g1.e1, g1.e2 FROM g1 ORDER BY g1.e1, g1.e2 LIMIT 4, 2147483647", new List[] {Arrays.asList("a", 1)});
        helpProcess(plan, manager, expected);
    }

    @Test public void testCountWithHaving() {
        String sql = "select e1, count(*) from pm1.g1 group by e1 having count(*) > 1"; //$NON-NLS-1$
        
        QueryMetadataInterface metadata = RealMetadataFactory.example1Cached();
        
        ProcessorPlan plan = helpGetPlan(helpParse(sql), metadata, TestOptimizer.getGenericFinder());
        
        List[] expected = new List[] {
                Arrays.asList(new Object[] { "a" , new Integer(3)}), //$NON-NLS-1$
        };

        FakeDataManager manager = new FakeDataManager();
        sampleData1(manager);
        helpProcess(plan, manager, expected);
    }
    
    @Test public void testLimitZero() {
        String sql = "select e1 from pm1.g1 limit 0"; //$NON-NLS-1$
        
        QueryMetadataInterface metadata = RealMetadataFactory.example1Cached();
        
        ProcessorPlan plan = helpGetPlan(helpParse(sql), metadata, TestOptimizer.getGenericFinder());
        
        TestOptimizer.checkNodeTypes(plan, TestRuleRaiseNull.FULLY_NULL);     
        
        List[] expected = new List[] {
        };

        FakeDataManager manager = new FakeDataManager();
        sampleData1(manager);
        helpProcess(plan, manager, expected);
    }
    
    @Test public void testLimitZero1() {
        String sql = "select distinct vm1.g1.e1, y.e1 from vm1.g1 left outer join (select 1 x, e1 from vm1.g2 limit 0) y on vm1.g1.e1 = y.e1 where vm1.g1.e1 = 'a'"; //$NON-NLS-1$
        
        QueryMetadataInterface metadata = RealMetadataFactory.example1Cached();
        
        ProcessorPlan plan = helpGetPlan(helpParse(sql), metadata, TestOptimizer.getGenericFinder());
        
        TestOptimizer.checkNodeTypes(plan, new int[] {
            1,      // Access
            0,      // DependentAccess
            0,      // DependentSelect
            0,      // DependentProject
            1,      // DupRemove
            0,      // Grouping
            0,      // NestedLoopJoinStrategy
            0,      // MergeJoinStrategy
            0,      // Null
            0,      // PlanExecution
            1,      // Project
            0,      // Select
            0,      // Sort
            0       // UnionAll
       });     
        
        List[] expected = new List[] {
            Arrays.asList(new Object[] { "a", null }), //$NON-NLS-1$
        };

        FakeDataManager manager = new FakeDataManager();
        sampleData1(manager);
        helpProcess(plan, manager, expected);
    }
    
    @Test public void testLimitZero2() {
        String sql = "select vm1.g1.e1 from vm1.g1 union select e1 from pm1.g2 limit 0"; //$NON-NLS-1$
        
        QueryMetadataInterface metadata = RealMetadataFactory.example1Cached();
        
        ProcessorPlan plan = helpGetPlan(helpParse(sql), metadata, TestOptimizer.getGenericFinder());
        
        List[] expected = new List[] {
        };

        FakeDataManager manager = new FakeDataManager();
        sampleData1(manager);
        helpProcess(plan, manager, expected);
    }
    
    @Test public void testLimitZero3() {
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.ROW_LIMIT, true);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$
        
        String sql = "select e1 from pm1.g2 limit 0"; //$NON-NLS-1$
        
        QueryMetadataInterface metadata = RealMetadataFactory.example1Cached();
        
        ProcessorPlan plan = helpGetPlan(helpParse(sql), metadata, capFinder);
        
        List[] expected = new List[] {
        };

        FakeDataManager manager = new FakeDataManager();
        sampleData1(manager);
        helpProcess(plan, manager, expected);
    }
    
    @Test public void testUnionWithTypeConversion() {
        String sql = "select pm1.g1.e1, pm1.g1.e2 from pm1.g1 where e1 = 'b' union select e2, e1 from pm1.g2 where e1 = 'b' order by e1, e2"; //$NON-NLS-1$
        
        QueryMetadataInterface metadata = RealMetadataFactory.example1Cached();
        
        ProcessorPlan plan = helpGetPlan(helpParse(sql), metadata, TestOptimizer.getGenericFinder());
        
        List[] expected = new List[] {
            Arrays.asList(new Object[] { "2", "b" }), //$NON-NLS-1$ //$NON-NLS-2$
            Arrays.asList(new Object[] { "b", "2" }), //$NON-NLS-1$ //$NON-NLS-2$
        };

        FakeDataManager manager = new FakeDataManager();
        sampleData1(manager);
        helpProcess(plan, manager, expected);
    }
    
    /**
     * Tests non-deterministic evaluation in the select clause.  
     * 
     * The evaluation of the rand function is delayed until processing time (which actually has predictable
     * values since the test initializes the command context with the same seed)
     * 
     * If this function were deterministic, it would be evaluated during rewrite to a single value.
     */
    @Test public void testNonDeterministicEvaluation() throws Exception {
        String sql = "select e1, convert(rand()*1000, integer) as x from pm1.g1 where e1 = 'a'"; //$NON-NLS-1$
        
        QueryMetadataInterface metadata = RealMetadataFactory.example1Cached();
        
        ProcessorPlan plan = helpGetPlan(helpParse(sql), metadata, TestOptimizer.getGenericFinder());
        
        List[] expected = new List[] {
            Arrays.asList(new Object[] { "a", new Integer(240) }), //$NON-NLS-1$
            Arrays.asList(new Object[] { "a", new Integer(637) }), //$NON-NLS-1$ 
            Arrays.asList(new Object[] { "a", new Integer(550) }), //$NON-NLS-1$ 
        };

        FakeDataManager manager = new FakeDataManager();
        sampleData1(manager);
        helpProcess(plan, manager, expected);
    }
    
    /**
     * here the rand function is deterministic and should yield a single value
     */
    @Test public void testDeterministicEvaluation() throws Exception {
        String sql = "select e1, convert(rand(0)*1000, integer) as x from pm1.g1 where e1 = 'a'"; //$NON-NLS-1$
        
        QueryMetadataInterface metadata = RealMetadataFactory.example1Cached();
        
        ProcessorPlan plan = helpGetPlan(helpParse(sql), metadata, TestOptimizer.getGenericFinder());
        
        List[] expected = new List[] {
            Arrays.asList(new Object[] { "a", new Integer(730) }), //$NON-NLS-1$
            Arrays.asList(new Object[] { "a", new Integer(730) }), //$NON-NLS-1$ 
            Arrays.asList(new Object[] { "a", new Integer(730) }), //$NON-NLS-1$ 
        };

        FakeDataManager manager = new FakeDataManager();
        sampleData1(manager);
        helpProcess(plan, manager, expected);
    }
    
    @Test public void testEmptyAggregate() throws Exception {
        String sql = "select count(e1) from pm1.g1 where 1 = 0"; //$NON-NLS-1$
        
        QueryMetadataInterface metadata = RealMetadataFactory.example1Cached();
        
        ProcessorPlan plan = helpGetPlan(helpParse(sql), metadata, TestOptimizer.getGenericFinder());
        
        List[] expected = new List[] {
            Arrays.asList(new Object[] { new Integer(0) })
        };

        FakeDataManager manager = new FakeDataManager();
        sampleData1(manager);
        helpProcess(plan, manager, expected);
    }
    
    @Test public void testNullAggregate() throws Exception {
        String sql = "select count(*), count(e1), sum(convert(e1, integer)) from pm1.g1 where e1 is null"; //$NON-NLS-1$
        
        QueryMetadataInterface metadata = RealMetadataFactory.example1Cached();
        
        ProcessorPlan plan = helpGetPlan(helpParse(sql), metadata, TestOptimizer.getGenericFinder());
        
        List[] expected = new List[] {
            Arrays.asList(new Object[] { new Integer(1), new Integer(0), null })
        };

        FakeDataManager manager = new FakeDataManager();
        sampleData1(manager);
        helpProcess(plan, manager, expected);
    }
    
    /**
     * here the presence of a group by causes no rows to be returned 
     */
    @Test public void testNullAggregate1() throws Exception {
        String sql = "select e1 from pm1.g1 where 1 = 0 group by e1"; //$NON-NLS-1$
        
        QueryMetadataInterface metadata = RealMetadataFactory.example1Cached();
        
        ProcessorPlan plan = helpGetPlan(helpParse(sql), metadata, TestOptimizer.getGenericFinder());
        
        List[] expected = new List[] {
        };

        FakeDataManager manager = new FakeDataManager();
        sampleData1(manager);
        helpProcess(plan, manager, expected);
    }
    
    @Test public void testReferenceReplacementWithExpression() throws Exception {
        String sql = "select e1, e2 from (select e1, convert(e2, string) e2 from pm1.g1) x where exists (select e3 from pm1.g2 where x.e2 = e1)"; //$NON-NLS-1$
        
        QueryMetadataInterface metadata = RealMetadataFactory.example1Cached();
        
        ProcessorPlan plan = helpGetPlan(helpParse(sql), metadata, TestOptimizer.getGenericFinder());
        
        List[] expected = new List[] {};

        FakeDataManager manager = new FakeDataManager();
        sampleData1(manager);
        helpProcess(plan, manager, expected);
    }

    /**
     * Here a merge join will be used since there is at least one equi join predicate.
     * TODO: this can be optimized further
     */
    @Test public void testCase6193_1() throws Exception { 
        // Create query 
        String sql = "select a.INTKEY, b.intkey from bqt1.smalla a LEFT OUTER JOIN bqt2.SMALLA b on a.intkey=b.intkey and a.intkey=5 where a.intkey <10 "; //$NON-NLS-1$
        
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        capFinder.addCapabilities("BQT2", caps);

        // Create expected results
        List[] expected = new List[] { 
            Arrays.asList(new Object[] { new Integer(0), null }),
            Arrays.asList(new Object[] { new Integer(1), null }),
            Arrays.asList(new Object[] { new Integer(2), null }),
            Arrays.asList(new Object[] { new Integer(3), null }),
            Arrays.asList(new Object[] { new Integer(4), null }),
            Arrays.asList(new Object[] { new Integer(5), new Integer(5) }),
            Arrays.asList(new Object[] { new Integer(6), null }),
            Arrays.asList(new Object[] { new Integer(7), null }),
            Arrays.asList(new Object[] { new Integer(8), null }),
            Arrays.asList(new Object[] { new Integer(9), null })
        };
        
        // Construct data manager with data
        FakeDataManager dataManager = new FakeDataManager();
        sampleDataBQT2(dataManager);
        
        // Plan query
        ProcessorPlan plan = TestOptimizer.helpPlan(sql, RealMetadataFactory.exampleBQTCached(), 
                                                    new String[] {"SELECT b.intkey FROM bqt2.SMALLA AS b", "SELECT a.intkey FROM bqt1.smalla AS a"}, new DefaultCapabilitiesFinder(), ComparisonMode.CORRECTED_COMMAND_STRING); //$NON-NLS-1$ //$NON-NLS-2$

        TestOptimizer.checkNodeTypes(plan, new int[] {
            2,      // Access
            0,      // DependentAccess
            0,      // DependentSelect
            0,      // DependentProject
            0,      // DupRemove
            0,      // Grouping
            0,      // NestedLoopJoinStrategy
            1,      // MergeJoinStrategy
            0,      // Null
            0,      // PlanExecution
            1,      // Project
            1,      // Select
            0,      // Sort
            0       // UnionAll
        });
        
        // Run query
        helpProcess(plan, dataManager, expected);
    }

    /**
     * Here a merge join will be used since there is at least one equi join predicate.
     */
    @Test public void testCase6193_2() throws Exception { 
        // Create query 
        String sql = "select a.e2, b.e2 from pm1.g1 a LEFT OUTER JOIN pm1.g2 b on a.e4=b.e4 and (a.e2+b.e2)=4 order by a.e2"; //$NON-NLS-1$
        
        // Create expected results
        List[] expected = new List[] { 
            Arrays.asList(new Object[] { new Integer(0), null }),
            Arrays.asList(new Object[] { new Integer(0), null }),
            Arrays.asList(new Object[] { new Integer(1), null }),
            Arrays.asList(new Object[] { new Integer(1), null }),
            Arrays.asList(new Object[] { new Integer(2), new Integer(2) }),
            Arrays.asList(new Object[] { new Integer(3), null }),
        };    
        
        // Construct data manager with data
        FakeDataManager dataManager = new FakeDataManager();
        sampleData1(dataManager);
        
        // Plan query
        ProcessorPlan plan = TestOptimizer.helpPlan(sql, RealMetadataFactory.example1Cached(), 
                                                    new String[] {"SELECT a.e4, a.e2 FROM pm1.g1 AS a", "SELECT b.e4, b.e2 FROM pm1.g2 AS b"}, new DefaultCapabilitiesFinder(), ComparisonMode.CORRECTED_COMMAND_STRING); //$NON-NLS-1$ //$NON-NLS-2$
        
        TestOptimizer.checkNodeTypes(plan, new int[] {
            2,      // Access
            0,      // DependentAccess
            0,      // DependentSelect
            0,      // DependentProject
            0,      // DupRemove
            0,      // Grouping
            0,      // NestedLoopJoinStrategy
            1,      // MergeJoinStrategy
            0,      // Null
            0,      // PlanExecution
            1,      // Project
            0,      // Select
            1,      // Sort
            0       // UnionAll
        });

        // Run query
        helpProcess(plan, dataManager, expected);
    }
    
    /**
     * Here a merge join will be used since there is at least one equi join predicate.
     * The inner merge join is also a dependent join
     */
    @Test public void testCase6193_3() throws Exception { 
        // Create query 
        String sql = "select a.x, b.y from (select 4 x union select 1) a LEFT OUTER JOIN (select (a.e2 + b.e2) y from pm1.g1 a LEFT OUTER JOIN pm1.g2 b on a.e4=b.e4) b on (a.x = b.y)"; //$NON-NLS-1$
        
        // Create expected results
        List[] expected = new List[] { 
            Arrays.asList(new Object[] { new Integer(1), null }),
            Arrays.asList(new Object[] { new Integer(4), new Integer(4) }),
        };    
        
        // Construct data manager with data
        FakeDataManager dataManager = new FakeDataManager();
        sampleData1(dataManager);
        
        // Plan query
        ProcessorPlan plan = TestOptimizer.helpPlan(sql, RealMetadataFactory.example1Cached(), 
                                                    new String[] {"SELECT a.e4, a.e2 FROM pm1.g1 AS a", "SELECT b.e4, b.e2 FROM pm1.g2 AS b"}, new DefaultCapabilitiesFinder(), ComparisonMode.CORRECTED_COMMAND_STRING); //$NON-NLS-1$ //$NON-NLS-2$
        
        TestOptimizer.checkNodeTypes(plan, new int[] {
            2,      // Access
            0,      // DependentAccess
            0,      // DependentSelect
            0,      // DependentProject
            0,      // DupRemove
            0,      // Grouping
            0,      // NestedLoopJoinStrategy
            2,      // MergeJoinStrategy
            0,      // Null
            0,      // PlanExecution
            4,      // Project
            0,      // Select
            0,      // Sort
            1       // UnionAll
        });
        
        TestOptimizer.checkDependentJoinCount(plan, 1);
        // Run query
        helpProcess(plan, dataManager, expected);
    }           
    
    /**
     * This is a control test.  It should work regardless of whether the reference is aliased
     * since accessnodes are now fully positional
     */
    @Test public void testPushdownNonAliasedSelectLiteral() throws Exception {
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_SELECT_EXPRESSION, true);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$

        QueryMetadataInterface metadata = RealMetadataFactory.example1Cached();
        
        List[] expected = new List[] {
            Arrays.asList("a", "b"), //$NON-NLS-1$ //$NON-NLS-2$
            Arrays.asList("a", "c") //$NON-NLS-1$ //$NON-NLS-2$
        };

        HardcodedDataManager manager = new HardcodedDataManager();
        manager.addData("SELECT pm1.g1.e1 FROM pm1.g1", new List[] {
                Arrays.asList("b"), //$NON-NLS-1$ //$NON-NLS-2$
                Arrays.asList("c") //$NON-NLS-1$ //$NON-NLS-2$
            }); //$NON-NLS-1$ 
        
        processPreparedStatement("select ?, e1 from pm1.g1", expected, manager, capFinder,
				metadata, Arrays.asList("a")); 
    }
    
    @Test public void testCase6486() { 
        // Create query 
        String sql = "select pm2.g1.e1 from pm1.g2, pm2.g1 where pm1.g2.e1=pm2.g1.e1 group by pm2.g1.e1"; //$NON-NLS-1$
        
        // Create expected results
        List[] expected = new List[] { 
            Arrays.asList(new Object[] { "b"}), //$NON-NLS-1$
            Arrays.asList(new Object[] { "d"}) //$NON-NLS-1$
        };    
        
        // Construct data manager with data
        FakeDataManager dataManager = new FakeDataManager();
        sampleData2(dataManager);
        
        // Plan query
        ProcessorPlan plan = helpGetPlan(sql, RealMetadataFactory.example1Cached());

        // Run query
        helpProcess(plan, dataManager, expected);
    }
    
    @Test public void testNonPushedOffset() throws Exception {
        String sql = "SELECT e1 FROM pm1.g1 LIMIT 1, 5"; //$NON-NLS-1$
        
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.ROW_LIMIT, true);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$

        List[] expected = new List[] { 
            Arrays.asList(new Object[] { "b"}), //$NON-NLS-1$
            Arrays.asList(new Object[] { "c"}), //$NON-NLS-1$
        };    
    
        FakeDataManager dataManager = new FakeDataManager();
        sampleData2(dataManager);
        
        ProcessorPlan plan = TestOptimizer.helpPlan(sql, RealMetadataFactory.example1Cached(), null, capFinder, new String[] {"SELECT pm1.g1.e1 FROM pm1.g1 LIMIT 6"}, TestOptimizer.ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$
        helpProcess(plan, dataManager, expected);          
    }
    
    @Test public void testNonCorrelatedSubQueryExecution() throws Exception {
        String sql = "SELECT e1 FROM pm1.g1 WHERE e2 IN (SELECT e2 FROM pm2.g1)"; //$NON-NLS-1$

        // Construct data manager with data
        HardcodedDataManager dataManager = new HardcodedDataManager();
        dataManager.setBlockOnce(true);
        dataManager.addData("SELECT pm1.g1.e2, pm1.g1.e1 FROM pm1.g1", new List[] { //$NON-NLS-1$
        		Arrays.asList(Integer.valueOf(1), "a"), //$NON-NLS-1$
        		Arrays.asList(Integer.valueOf(2), "b") //$NON-NLS-1$
        });
        dataManager.addData("SELECT pm2.g1.e2 FROM pm2.g1", new List[] { //$NON-NLS-1$
        		Arrays.asList(Integer.valueOf(2))
        });
        
        ProcessorPlan plan = helpGetPlan(sql, RealMetadataFactory.example1Cached());

        List[] expected = new List[] {
                Arrays.asList(new Object[] { "b" }), //$NON-NLS-1$
            };

        doProcess(plan, dataManager, expected, createCommandContext());
        
        //we expect 2 queries, 1 for the outer and 1 for the subquery
        assertEquals(2, dataManager.getCommandHistory().size());
    }
    
    /**
     * Test a query that uses ambiguous alias names in the top level query and 
     * its sub-query and uses columns belonging to the alias as a parameter to a 
     * function.
     * <p>
     * For example, <code>SELECT CONVERT(A.e2, biginteger) AS e2 FROM (SELECT 
     * CONVERT(e2, long) AS e2 FROM pm1.g1 AS A) AS A</code>
     * <p>
     * The test is to ensure that A.e2 from the top level is not confused with 
     * e2 in the second level.
     * <p>
     * Related Defects: JBEDSP-1137
     */
    @Test public void testAliasReuseInFunctionInSubQuery() throws Exception {
        // Create query
    	String sql = "SELECT CONVERT(A.e2, biginteger) AS e2 FROM (" + //$NON-NLS-1$
    	"   SELECT CONVERT(e2, long) AS e2 FROM pm1.g1 AS A WHERE e1 = 'a'" + //$NON-NLS-1$
    	") AS A"; //$NON-NLS-1$
        
        QueryMetadataInterface metadata = RealMetadataFactory.example1Cached();
        
        ProcessorPlan plan = helpGetPlan(helpParse(sql), metadata, TestOptimizer.getGenericFinder());
        
        List[] expected = new List[] {
            Arrays.asList(new Object[] { new BigInteger("0") }), //$NON-NLS-1$
            Arrays.asList(new Object[] { new BigInteger("3") }), //$NON-NLS-1$ 
            Arrays.asList(new Object[] { new BigInteger("0") }), //$NON-NLS-1$ 
        };

        FakeDataManager manager = new FakeDataManager();
        sampleData1(manager);
        helpProcess(plan, manager, expected);
    }
    
    @Test public void testImplicitAggregateWithInlineView() {
        String sql = "SELECT * FROM (SELECT b.count, enterprise_id FROM (SELECT COUNT(*), 2 AS enterprise_id FROM (SELECT 'A Name' AS Name, 1 AS enterprise_id) c ) b ) a WHERE enterprise_id = 1"; //$NON-NLS-1$
        
        List[] expected = new List[] {};    
    
        FakeDataManager dataManager = new FakeDataManager();
        sampleData1(dataManager);
        
        ProcessorPlan plan = helpGetPlan(helpParse(sql), RealMetadataFactory.example1Cached());
        
        helpProcess(plan, dataManager, expected);
    }
    
    @Test public void testCorrelatedNestedTable() {
        String sql = "select y.e2, x.e1, x.e2 from (select * from pm1.g1) y, table (select * from pm1.g3 where e2 = y.e2) x"; //$NON-NLS-1$
        
        List[] expected = new List[] {
        		Arrays.asList(0, "a", 0),
        		Arrays.asList(0, "a", 0),
        		Arrays.asList(1, null, 1),
        		Arrays.asList(1, "c", 1),
        		Arrays.asList(3, "a", 3),
        		Arrays.asList(1, null, 1),
        		Arrays.asList(1, "c", 1),
        		Arrays.asList(2, "b", 2),
        		Arrays.asList(0, "a", 0),
        		Arrays.asList(0, "a", 0),
        };    
    
        FakeDataManager dataManager = new FakeDataManager();
        sampleData1(dataManager);
        
        ProcessorPlan plan = helpGetPlan(helpParse(sql), RealMetadataFactory.example1Cached());
        
        helpProcess(plan, dataManager, expected);
    }
    
    @Test public void testCorrelatedNestedTable1() {
        String sql = "select y.e2, z.e2, x.e1, x.e2 from (select * from pm1.g1 order by e2 desc limit 2) y inner join pm1.g2 z on y.e1 = z.e1, table (select * from pm1.g3 where e2 = y.e2 + z.e2) x"; //$NON-NLS-1$
        
        List[] expected = new List[] {
        		Arrays.asList(3, 0, "a", 3),
        		Arrays.asList(3, 0, "a", 3),
        };    
    
        FakeDataManager dataManager = new FakeDataManager();
        sampleData1(dataManager);
        
        ProcessorPlan plan = helpGetPlan(helpParse(sql), RealMetadataFactory.example1Cached());
        
        helpProcess(plan, dataManager, expected);
    }
    
    @Test public void testCorrelatedNestedTable2() {
        String sql = "select y.e1, x.e1 from (select distinct e1 from pm1.g1 where e1 is not null) y, table (call pm1.sq3b(\"in\" = e1, in3 = 'something')) x"; //$NON-NLS-1$
        
        List[] expected = new List[] {
        		Arrays.asList("a", "a"),
        		Arrays.asList("a", "a"),
        		Arrays.asList("a", "a"),
        		Arrays.asList("b", "b"),
        		Arrays.asList("c", "c"),
        };    
    
        FakeDataManager dataManager = new FakeDataManager();
        sampleData1(dataManager);
        
        ProcessorPlan plan = helpGetPlan(helpParse(sql), RealMetadataFactory.example1Cached());
        
        helpProcess(plan, dataManager, expected);
    }
    
    @Test public void testCorrelatedNestedTable3() {
        String sql = "select y.e1, x.e1 from (select * from pm1.g1) y left outer join table (call pm1.sq3b(\"in\" = e2, in3 = 'something')) x on (1=1)"; //$NON-NLS-1$
        
        List[] expected = new List[] {
        		Arrays.asList("a", null),
        		Arrays.asList(null, null),
        		Arrays.asList("a", null),
        		Arrays.asList("c", null),
        		Arrays.asList("b", null),
        		Arrays.asList("a", null),
        };    
    
        FakeDataManager dataManager = new FakeDataManager();
        sampleData1(dataManager);
        
        ProcessorPlan plan = helpGetPlan(helpParse(sql), RealMetadataFactory.example1Cached());
        
        helpProcess(plan, dataManager, expected);
    }
    
    @Test public void testCorrelatedNestedTable4() {
    	String sql = "select y.e1, y.e2, z.e2 from (select * from pm1.g1) y inner join table (select * from pm1.g3 where e2 = y.e2) x left outer join (select null as e1, e2 from pm1.g2) z on (x.e1 = z.e1) on (x.e1 = y.e1)"; //$NON-NLS-1$
    	
        List[] expected = new List[] {
        		Arrays.asList("a", 0, null),
        		Arrays.asList("a", 0, null),
        		Arrays.asList("a", 3, null),
        		Arrays.asList("c", 1, null),
        		Arrays.asList("b", 2, null),
        		Arrays.asList("a", 0, null),
        		Arrays.asList("a", 0, null),
        };    
    
        FakeDataManager dataManager = new FakeDataManager();
        sampleData1(dataManager);
        
        ProcessorPlan plan = helpGetPlan(helpParse(sql), RealMetadataFactory.example1Cached());
        
        helpProcess(plan, dataManager, expected);
    }
    
    @Test public void testCorrelatedNestedTable5() {
    	String sql = "select y.e1, y.e2, z.e2 from (exec pm1.sq1()) y, table (exec pm1.sq2(y.e1)) x, table (exec pm1.sq2(x.e1)) z where y.e2 = 2"; //$NON-NLS-1$
    	
        List[] expected = new List[] {
        		Arrays.asList("b", 2, 2),
        };    
    
        FakeDataManager dataManager = new FakeDataManager();
        sampleData1(dataManager);
        
        ProcessorPlan plan = helpGetPlan(helpParse(sql), RealMetadataFactory.example1Cached());
        
        helpProcess(plan, dataManager, expected);
    }
    
    @Test public void testUncorrelatedScalarSubqueryPushdown() throws Exception {
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        QueryMetadataInterface metadata = RealMetadataFactory.example1Cached();
        
        BasicSourceCapabilities caps = getTypicalCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_SUBQUERIES_SCALAR, false);
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES, true);
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES_MAX, true);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$
        
        ProcessorPlan plan = helpPlan("select pm1.g1.e1 from pm1.g1 where e1 < (select max(vm1.g1.e1) from vm1.g1)", metadata,  //$NON-NLS-1$
                                      null, capFinder,
            new String[] { "SELECT g_0.e1 FROM pm1.g1 AS g_0 WHERE g_0.e1 < (SELECT MAX(g_0.e1) FROM pm1.g1 AS g_0)" }, ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$
        checkNodeTypes(plan, FULL_PUSHDOWN);
        
        HardcodedDataManager hdm = new HardcodedDataManager();
        hdm.addData("SELECT MAX(g_0.e1) FROM pm1.g1 AS g_0", new List[] {Arrays.asList("c")});
        hdm.addData("SELECT g_0.e1 FROM pm1.g1 AS g_0 WHERE g_0.e1 < 'c'", new List[] {Arrays.asList("a")});
        hdm.setBlockOnce(true);
        List[] expected = new List[] {
        		Arrays.asList("a"),
        };    

        helpProcess(plan, hdm, expected);
    }
    
    @Test public void testNonDeterministicScalarSubquery() throws Exception {
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        QueryMetadataInterface metadata = RealMetadataFactory.example1Cached();
        
        BasicSourceCapabilities caps = getTypicalCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_SUBQUERIES_SCALAR, false);
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES, true);
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES_MAX, true);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$
        
        ProcessorPlan plan = helpPlan("select count(distinct x) from (select (select uuid()) as x from pm1.g1) as v", metadata,  //$NON-NLS-1
                                      null, capFinder,
            new String[] { "SELECT 1 FROM pm1.g1 AS g_0" }, ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$
        
        HardcodedDataManager hdm = new HardcodedDataManager();
        hdm.addData("SELECT 1 FROM pm1.g1 AS g_0", new List[] {Arrays.asList(1), Arrays.asList(1)});
        hdm.setBlockOnce(true);
        List[] expected = new List[] {
        		Arrays.asList(2),
        };    

        helpProcess(plan, hdm, expected);
    }
    
    @Test public void testUncorrelatedScalarSubqueryPushdown1() throws Exception {
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        QueryMetadataInterface metadata = RealMetadataFactory.example1Cached();
        
        BasicSourceCapabilities caps = getTypicalCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_SUBQUERIES_SCALAR, false);
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES, true);
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES_MAX, true);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$
        
        ProcessorPlan plan = helpPlan("select pm1.g1.e1 from pm1.g1 where e1 < (select e1 from (EXEC pm1.sq1()) x order by e2 limit 1)", metadata,  //$NON-NLS-1$
                                      null, capFinder,
            new String[] { "SELECT g_0.e1 FROM pm1.g1 AS g_0 WHERE g_0.e1 < (SELECT e1 FROM (EXEC pm1.sq1()) AS x ORDER BY e2 LIMIT 1)" }, ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$
        checkNodeTypes(plan, FULL_PUSHDOWN);
        
        HardcodedDataManager hdm = new HardcodedDataManager();
        hdm.addData("SELECT g_0.e1 FROM pm1.g1 AS g_0 WHERE g_0.e1 < 'z'", new List[] {Arrays.asList("c")});
        hdm.addData("SELECT g_0.e1, g_0.e2 FROM pm1.g1 AS g_0", new List[] {Arrays.asList("z", 1), Arrays.asList("b", 2)});
        hdm.setBlockOnce(true);
        List[] expected = new List[] {
        		Arrays.asList("c"),
        };    

        helpProcess(plan, hdm, expected);
    }
    
    @Test public void testStoredProcedureSubqueryInput() {
    	String sql = "exec pm1.sp2((select e2 from pm1.g1 order by e1 limit 1))"; //$NON-NLS-1$
    	
        List[] expected = new List[] {
        		Arrays.asList("b", 2),
        };    
    
        HardcodedDataManager dataManager = new HardcodedDataManager();
        dataManager.addData("SELECT pm1.g1.e2, pm1.g1.e1 FROM pm1.g1", new List[] {Arrays.asList(1, "2"), Arrays.asList(3, "4")});
        dataManager.addData("EXEC pm1.sp2(1)", new List[] {Arrays.asList("b", 2)});
        ProcessorPlan plan = helpGetPlan(helpParse(sql), RealMetadataFactory.example1Cached());
        
        helpProcess(plan, dataManager, expected);
    }
    
    @Test public void testInlineViewWith() {
    	String sql = "select * from (with x as (select e1 from pm1.g1) select x.e1 from x order by e1 nulls last limit 1) y"; //$NON-NLS-1$
    	
        List[] expected = new List[] {
        		Arrays.asList("a"),
        };    
    
        FakeDataManager dataManager = new FakeDataManager();
        sampleData1(dataManager);
        
        ProcessorPlan plan = helpGetPlan(helpParse(sql), RealMetadataFactory.example1Cached());
        
        helpProcess(plan, dataManager, expected);
    }
    
    @Test public void testDeleteCompensation() {
    	String sql = "delete from pm1.g1 where e1 = 'a' and e2 in (select e2 from pm1.g2)"; //$NON-NLS-1$
    	
        List[] expected = new List[] {
        		Arrays.asList(3),
        };    
    
        FakeDataManager dataManager = new FakeDataManager();
        sampleData1(dataManager);
        
        ProcessorPlan plan = helpGetPlan(sql, RealMetadataFactory.example4(), TestOptimizer.getGenericFinder());
        
        helpProcess(plan, dataManager, expected);
    }
    
    @Test public void testUpdateCompensation() {
    	String sql = "update pm1.g1 set e4 = null where e1 = 'a' and exists (select 1 from pm1.g2 where e2 = pm1.g1.e2)"; //$NON-NLS-1$
    	
        List[] expected = new List[] {
        		Arrays.asList(3),
        };    
    
        FakeDataManager dataManager = new FakeDataManager();
        sampleData1(dataManager);
        
        BasicSourceCapabilities caps = getTypicalCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_SELECT_EXPRESSION, false);
        ProcessorPlan plan = helpGetPlan(sql, RealMetadataFactory.example4(), new DefaultCapabilitiesFinder(caps));
        
        helpProcess(plan, dataManager, expected);
    }
    
    @Test public void testDupSelect() throws Exception {
    	String sql = "select e1, e1 from pm1.g1";
        
    	HardcodedDataManager dataManager = new HardcodedDataManager();
    	
    	dataManager.addData("SELECT g_0.e1 FROM pm1.g1 AS g_0", new List[] {Arrays.asList(1)});
        
        ProcessorPlan plan = helpGetPlan(sql, RealMetadataFactory.example1Cached(), TestOptimizer.getGenericFinder());
        
        helpProcess(plan, dataManager, new List[] {Arrays.asList(1, 1)});
    }
    
    @Test public void testDupSelect1() throws Exception {
    	String sql = "select 1, 2 from pm1.g1";
        
    	HardcodedDataManager dataManager = new HardcodedDataManager();
    	
    	dataManager.addData("SELECT 2 FROM pm1.g1 AS g_0", new List[] {Arrays.asList(2)});
        
        ProcessorPlan plan = helpGetPlan(sql, RealMetadataFactory.example1Cached(), TestOptimizer.getGenericFinder());
        
        helpProcess(plan, dataManager, new List[] {Arrays.asList(1, 2)});
    }
    
    @Test public void testDupSelectWithOrderBy() throws Exception {
    	String sql = "select e1 as a, e1 as b from pm1.g1 order by b";
        
    	HardcodedDataManager dataManager = new HardcodedDataManager();
    	
    	//note that the command is referencing c_0
    	dataManager.addData("SELECT g_0.e1 AS c_0 FROM pm1.g1 AS g_0 ORDER BY c_0", new List[] {Arrays.asList(1)});
        
        ProcessorPlan plan = helpGetPlan(sql, RealMetadataFactory.example1Cached(), TestOptimizer.getGenericFinder());
        
        helpProcess(plan, dataManager, new List[] {Arrays.asList(1, 1)});
    }
    
    @Test public void testOrderByExpression() throws Exception {
    	String sql = "SELECT pm1.g1.e2 as y FROM pm1.g1 ORDER BY e3 || e1";
        
    	List[] expected = new List[] {
        		Arrays.asList(1),
        		Arrays.asList(0),
        		Arrays.asList(0),
        		Arrays.asList(2),
        		Arrays.asList(3),
        		Arrays.asList(1),
        };
    	
    	FakeDataManager dataManager = new FakeDataManager();
    	sampleData1(dataManager);
        ProcessorPlan plan = helpGetPlan(sql, RealMetadataFactory.example1Cached(), TestOptimizer.getGenericFinder());
        
        helpProcess(plan, dataManager, expected);
    }
    
    private static final boolean DEBUG = false;
}
