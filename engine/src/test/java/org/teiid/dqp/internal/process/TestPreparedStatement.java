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

package org.teiid.dqp.internal.process;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import org.junit.Test;
import org.teiid.dqp.internal.process.TestRequest.FakeApplicationEnvironment;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.api.exception.query.QueryParserException;
import com.metamatrix.api.exception.query.QueryPlannerException;
import com.metamatrix.api.exception.query.QueryResolverException;
import com.metamatrix.api.exception.query.QueryValidatorException;
import com.metamatrix.common.buffer.BufferManagerFactory;
import com.metamatrix.dqp.message.RequestMessage;
import com.metamatrix.platform.security.api.MetaMatrixSessionID;
import com.metamatrix.platform.security.api.SessionToken;
import com.metamatrix.query.metadata.QueryMetadataInterface;
import com.metamatrix.query.optimizer.TestOptimizer;
import com.metamatrix.query.optimizer.capabilities.BasicSourceCapabilities;
import com.metamatrix.query.optimizer.capabilities.CapabilitiesFinder;
import com.metamatrix.query.optimizer.capabilities.DefaultCapabilitiesFinder;
import com.metamatrix.query.optimizer.capabilities.FakeCapabilitiesFinder;
import com.metamatrix.query.optimizer.capabilities.SourceCapabilities.Capability;
import com.metamatrix.query.processor.FakeDataManager;
import com.metamatrix.query.processor.ProcessorDataManager;
import com.metamatrix.query.processor.TestProcessor;
import com.metamatrix.query.sql.lang.Command;
import com.metamatrix.query.unittest.FakeMetadataFacade;
import com.metamatrix.query.unittest.FakeMetadataFactory;

public class TestPreparedStatement {
	
	private static final int SESSION_ID = 6;
	
	private static boolean DEBUG = false;
	
	static class TestablePreparedPlanCache extends PreparedPlanCache {
		
		int hitCount;
		
		@Override
		public PreparedPlan getPreparedPlan(CacheID id) {
			PreparedPlan result = super.getPreparedPlan(id);
			if (result != null) {
				hitCount++;
			}
			return result;
		}
		
	}
		
    static void helpTestProcessing(String preparedSql, List values, List[] expected, ProcessorDataManager dataManager, QueryMetadataInterface metadata, boolean callableStatement) throws Exception { 
    	helpTestProcessing(preparedSql, values, expected, dataManager, metadata, callableStatement, false);
    }

    static void helpTestProcessing(String preparedSql, List values, List[] expected, ProcessorDataManager dataManager, QueryMetadataInterface metadata, boolean callableStatement, boolean isSessionSpecific) throws Exception { 
        helpTestProcessing(preparedSql, values, expected, dataManager, (CapabilitiesFinder)null, metadata, (TestablePreparedPlanCache)null, callableStatement, isSessionSpecific, /* isAlreadyCached */false); 
    }
    
    static public void helpTestProcessing(String preparedSql, List values, List[] expected, ProcessorDataManager dataManager, CapabilitiesFinder capFinder, QueryMetadataInterface metadata, TestablePreparedPlanCache prepPlanCache, boolean callableStatement, boolean isSessionSpecific, boolean isAlreadyCached) throws Exception { 
        TestablePreparedPlanCache pPlanCache = null;
        CapabilitiesFinder cFinder = null;
        ProcessorDataManager dManager = null;
        
        if ( dataManager == null ) {
            // Construct data manager with data
            dManager = new FakeDataManager();
            TestProcessor.sampleData1((FakeDataManager)dManager);    
        } else dManager = dataManager;
        
        if ( capFinder == null ) cFinder = new DefaultCapabilitiesFinder();
        else cFinder = capFinder;
        
        if ( prepPlanCache == null ) pPlanCache = new TestablePreparedPlanCache();
        else pPlanCache = prepPlanCache;
        
		// expected cache hit count
        int exHitCount = -1;
        
		/*
		 * If the plan is already cached we want our expected hit
		 * count of the cache to be at least 2 because we will 
		 * get the plan twice.  Otherwise, we want it to be 1.
		 */
        if ( isAlreadyCached ) {
        	exHitCount = pPlanCache.hitCount + 2;
        } else {
        	exHitCount = pPlanCache.hitCount + 1;
        }
        
        //Create plan or used cache plan if isPlanCached
        PreparedStatementRequest plan = TestPreparedStatement.helpGetProcessorPlan(preparedSql, values, cFinder, metadata, pPlanCache, SESSION_ID, callableStatement, false);

        // Run query
        TestProcessor.doProcess(plan.processPlan, dataManager, expected, plan.context);
        
        //test cached plan
    	plan = TestPreparedStatement.helpGetProcessorPlan(preparedSql, values, cFinder, metadata, pPlanCache, SESSION_ID, callableStatement, false);
    	
        //make sure the plan is only created once
        assertEquals("should reuse the plan", exHitCount, pPlanCache.hitCount); //$NON-NLS-1$
                
        // Run query again
        TestProcessor.doProcess(plan.processPlan, dataManager, expected, plan.context);
        
        //get the plan again with a new connection
        assertNotNull(TestPreparedStatement.helpGetProcessorPlan(preparedSql, values, cFinder, metadata, pPlanCache, 7, callableStatement, false));

        /*
         * If the command is not specific to a session we expect
         * another hit against the cache because we will use the 
         * cached plan, otherwise, a new plan would have been 
         * created and the hit count will be unchanged.
         */
        if ( !isSessionSpecific ) exHitCount++;
        assertEquals(exHitCount, pPlanCache.hitCount); 
	}
    	    
    @Test public void testWhere() throws Exception { 
        // Create query 
        String preparedSql = "SELECT pm1.g1.e1, e2, pm1.g1.e3 as a, e4 as b FROM pm1.g1 WHERE e2=?"; //$NON-NLS-1$
        
        // Create expected results
        List[] expected = new List[] { 
            Arrays.asList(new Object[] { "a",   new Integer(0),     Boolean.FALSE,  new Double(2.0) }), //$NON-NLS-1$
            Arrays.asList(new Object[] { "a",   new Integer(0),     Boolean.FALSE,  new Double(2.0) }) //$NON-NLS-1$
        };    
    
		List values = new ArrayList();
		values.add(new Short((short)0));
		FakeDataManager dataManager = new FakeDataManager();
        TestProcessor.sampleData1(dataManager);
		helpTestProcessing(preparedSql, values, expected, dataManager, FakeMetadataFactory.example1Cached(), false);
	}
    
    @Test public void testSessionSpecificFunction() throws Exception { 
        // Create query 
        String preparedSql = "SELECT user(), e2, pm1.g1.e3 as a, e4 as b FROM pm1.g1 WHERE e2=?"; //$NON-NLS-1$
        
        // Create expected results
        List[] expected = new List[] { 
            Arrays.asList(new Object[] { "foo",   new Integer(0),     Boolean.FALSE,  new Double(2.0) }), //$NON-NLS-1$
            Arrays.asList(new Object[] { "foo",   new Integer(0),     Boolean.FALSE,  new Double(2.0) }) //$NON-NLS-1$
        };    
    
		List values = new ArrayList();
		values.add(new Short((short)0));
        FakeDataManager dataManager = new FakeDataManager();
        TestProcessor.sampleData1(dataManager);
		helpTestProcessing(preparedSql, values, expected, dataManager, FakeMetadataFactory.example1Cached(), false, true);
	}
    
    @Test public void testFunctionWithReferencePushDown() throws Exception { 
        // Create query 
        String preparedSql = "SELECT pm1.g1.e1 FROM pm1.g1, pm1.g2 WHERE pm1.g1.e1 = pm1.g2.e1 and pm1.g1.e2+2=?"; //$NON-NLS-1$
        
        //Create plan
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.CRITERIA_COMPARE_EQ, true);
        caps.setCapabilitySupport(Capability.CRITERIA_COMPARE_ORDERED, true);        
        caps.setCapabilitySupport(Capability.CRITERIA_NOT, true);    
        caps.setCapabilitySupport(Capability.CRITERIA_IN, true);
        caps.setCapabilitySupport(Capability.QUERY_FROM_JOIN_INNER, true);
        caps.setCapabilitySupport(Capability.QUERY_FROM_JOIN_OUTER, true);
        caps.setCapabilitySupport(Capability.QUERY_FROM_JOIN_OUTER_FULL, true);
        caps.setCapabilitySupport(Capability.QUERY_FROM_JOIN_SELFJOIN, true);
        caps.setCapabilitySupport(Capability.QUERY_FROM_GROUP_ALIAS, true);
        caps.setCapabilitySupport(Capability.QUERY_ORDERBY, true);
        caps.setFunctionSupport("+", false); //$NON-NLS-1$
        caps.setFunctionSupport("convert", true); //$NON-NLS-1$
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$
        
        FakeMetadataFacade metadata = FakeMetadataFactory.example1Cached();
        
        List values = Arrays.asList(0);

        PreparedStatementRequest plan = helpGetProcessorPlan(preparedSql, values, capFinder, metadata, new PreparedPlanCache(), SESSION_ID, false, false);
        
        TestOptimizer.checkNodeTypes(plan.processPlan, TestOptimizer.FULL_PUSHDOWN);  
    }
    
	static public PreparedStatementRequest helpGetProcessorPlan(String preparedSql, List values, PreparedPlanCache prepPlanCache)
			throws MetaMatrixComponentException, QueryParserException,
			QueryResolverException, QueryValidatorException,
			QueryPlannerException {    	
		return helpGetProcessorPlan(preparedSql, values, new DefaultCapabilitiesFinder(), FakeMetadataFactory.example1Cached(), prepPlanCache, SESSION_ID, false, false);
    }
	
	static public PreparedStatementRequest helpGetProcessorPlan(String preparedSql, List values,
			PreparedPlanCache prepPlanCache, int conn)
			throws MetaMatrixComponentException, QueryParserException,
			QueryResolverException, QueryValidatorException,
			QueryPlannerException {
		return helpGetProcessorPlan(preparedSql, values,
				new DefaultCapabilitiesFinder(), FakeMetadataFactory
						.example1Cached(), prepPlanCache, conn, false, false);
	}

	static PreparedStatementRequest helpGetProcessorPlan(String preparedSql, List values,
			CapabilitiesFinder capFinder, QueryMetadataInterface metadata, PreparedPlanCache prepPlanCache, int conn, boolean callableStatement, boolean limitResults)
			throws MetaMatrixComponentException, QueryParserException,
			QueryResolverException, QueryValidatorException,
			QueryPlannerException {
        
        //Create Request
        RequestMessage request = new RequestMessage(preparedSql);
        request.setPreparedStatement(true);
        request.setCallableStatement(callableStatement);
        request.setParameterValues(values);
		if (values != null && values.size() > 0 && values.get(0) instanceof List) {
			request.setPreparedBatchUpdate(true);
		}
        if (limitResults) {
        	request.setRowLimit(1);
        }

        DQPWorkContext workContext = new DQPWorkContext();
        workContext.setVdbName("example1"); //$NON-NLS-1$
        workContext.setVdbVersion("1"); //$NON-NLS-1$
        workContext.setSessionToken(new SessionToken(new MetaMatrixSessionID(conn), "foo")); //$NON-NLS-1$        
        PreparedStatementRequest serverRequest = new PreparedStatementRequest(prepPlanCache) {
        	@Override
        	protected void createProcessor(Command processingCommand)
        			throws MetaMatrixComponentException {
        		
        	}
        };
        FakeApplicationEnvironment env = new FakeApplicationEnvironment(metadata, "example1", "1", "pm1", "1", "BINDING"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$ //$NON-NLS-5$
        serverRequest.initialize(request, env, BufferManagerFactory.getStandaloneBufferManager(), null, new HashMap(), null, DEBUG, null, workContext, 101024);
        serverRequest.setMetadata(capFinder, metadata, null);
        serverRequest.processRequest();
        
        assertNotNull(serverRequest.processPlan);
		return serverRequest;
	}
	
	@Test public void testValidateCorrectValues() throws Exception {
        // Create query 
        String preparedSql = "SELECT pm1.g1.e1, e2, pm1.g1.e3 as a, e4 as b FROM pm1.g1 WHERE pm1.g1.e1=?"; //$NON-NLS-1$
        
		List values = new ArrayList();
		values.add("a"); //$NON-NLS-1$
		
        //Create plan
        helpGetProcessorPlan(preparedSql, values, new PreparedPlanCache());
	}	

	/** SELECT pm1.g1.e1 FROM pm1.g1 WHERE pm1.g1.e2 IN (SELECT pm1.g2.e2 FROM pm1.g2 WHERE pm1.g2.e1 = ?)*/
	@Test public void testWithSubquery() throws Exception {
		// Create query 
		String preparedSql = "SELECT pm1.g1.e1 FROM pm1.g1 WHERE pm1.g1.e2 IN (SELECT pm1.g2.e2 FROM pm1.g2 WHERE pm1.g2.e1 = ?)"; //$NON-NLS-1$
        
		List values = Arrays.asList("a"); //$NON-NLS-1$
		
        //Create plan
        helpGetProcessorPlan(preparedSql, values, new PreparedPlanCache());
	}	

	/** SELECT pm1.g1.e1 FROM pm1.g1 WHERE pm1.g1.e1 = ? AND pm1.g1.e2 IN (SELECT pm1.g2.e2 FROM pm1.g2 WHERE pm1.g2.e1 = ?) */
	@Test public void testWithSubquery2() throws Exception {
		// Create query 
		String preparedSql = "SELECT pm1.g1.e1 FROM pm1.g1 WHERE pm1.g1.e1 = ? AND pm1.g1.e2 IN (SELECT pm1.g2.e2 FROM pm1.g2 WHERE pm1.g2.e1 = ?)"; //$NON-NLS-1$
                
		List values = Arrays.asList("d", "c"); //$NON-NLS-1$ //$NON-NLS-2$
				
        //Create plan
        helpGetProcessorPlan(preparedSql, values, new PreparedPlanCache());
	}	

	/** SELECT X.e1 FROM (SELECT pm1.g2.e1 FROM pm1.g2 WHERE pm1.g2.e1 = ?) as X */
	@Test public void testWithSubquery3() throws Exception {
		// Create query 
		String preparedSql = "SELECT X.e1 FROM (SELECT pm1.g2.e1 FROM pm1.g2 WHERE pm1.g2.e1 = ?) as X"; //$NON-NLS-1$
        
		//Create Request
		List values = new ArrayList();
		values.add("d"); //$NON-NLS-1$
		
        //Create plan
        helpGetProcessorPlan(preparedSql, values, new PreparedPlanCache());
	}	
	
	@Test public void testValidateWrongValues() throws Exception {
		// Create query 
	    String preparedSql = "SELECT pm1.g1.e1, e2, pm1.g1.e3 as a, e4 as b FROM pm1.g1 WHERE pm1.g1.e2=?"; //$NON-NLS-1$
	    TestablePreparedPlanCache prepCache = new TestablePreparedPlanCache();

	    //wrong type
		try{         	        
			List values = new ArrayList();
			values.add("x"); //$NON-NLS-1$
			
	        //Create plan
	        helpGetProcessorPlan(preparedSql, values, prepCache, SESSION_ID);
	        fail();
		}catch(QueryResolverException qe){
            assertEquals("Error converting parameter number 1 with value \"x\" to expected type integer.", qe.getMessage()); //$NON-NLS-1$
    	}    	
    	assertEquals(0, prepCache.hitCount);
    	
    	//test cached plan
    	try{	        
			List values = new ArrayList();
			values.add("a"); //$NON-NLS-1$
			values.add("b"); //$NON-NLS-1$			
			helpGetProcessorPlan(preparedSql, values, prepCache, SESSION_ID);
			fail();
	   	}catch(QueryResolverException qe){
	   	    assertEquals("The number of bound values '2' does not match the number of parameters '1' in the prepared statement.", qe.getMessage()); //$NON-NLS-1$
    	}    	
    	
    	assertEquals(1, prepCache.hitCount);  
    	
    	//wrong number of values
		try{         
			List values = new ArrayList();
			values.add("a"); //$NON-NLS-1$
			values.add(new Integer(0));
			helpGetProcessorPlan(preparedSql, values, prepCache);
			fail();
		}catch(QueryResolverException qe){
			assertEquals("The number of bound values '2' does not match the number of parameters '1' in the prepared statement.", qe.getMessage()); //$NON-NLS-1$
    	}    	
    	
	}	
    
    @Test public void testResolveParameterValues() throws Exception {
        // Create query 
        String preparedSql = "SELECT pm1.g1.e1, e2, pm1.g1.e3 as a, e4 as b FROM pm1.g1 WHERE pm1.g1.e2=?"; //$NON-NLS-1$
        
        List values = Arrays.asList("0"); //$NON-NLS-1$
        
		helpGetProcessorPlan(preparedSql, values, new PreparedPlanCache());
    }
    
    /**
     * TODO: there may be other ways of handling this situation in the future
     */
    @Test public void testLimitNoCache() throws Exception {
        // Create query 
        String preparedSql = "SELECT pm1.g1.e1, e2, pm1.g1.e3 as a, e4 as b FROM pm1.g1 WHERE pm1.g1.e2=?"; //$NON-NLS-1$
        
        List values = Arrays.asList("0"); //$NON-NLS-1$
        
        TestablePreparedPlanCache planCache = new TestablePreparedPlanCache();
        
		helpGetProcessorPlan(preparedSql, values, new DefaultCapabilitiesFinder(), FakeMetadataFactory.example1Cached(), planCache, SESSION_ID, false, true);

		helpGetProcessorPlan(preparedSql, values, new DefaultCapabilitiesFinder(), FakeMetadataFactory.example1Cached(), planCache, SESSION_ID, false, true);
		//make sure the plan wasn't reused
		assertEquals(0, planCache.hitCount);
    }
    
    @Test public void testUpdateProcedureCriteria() throws Exception {
        String preparedSql = "delete from vm1.g37 where e1=?"; //$NON-NLS-1$
        
        List[] expected = new List[] { 
            Arrays.asList(1),
        };    
    
		List values = Arrays.asList("aa "); //$NON-NLS-1$
        FakeDataManager dataManager = new FakeDataManager();
        TestProcessor.sampleData2b(dataManager);
		helpTestProcessing(preparedSql, values, expected, dataManager, FakeMetadataFactory.example1Cached(), false, false);
    }

}
