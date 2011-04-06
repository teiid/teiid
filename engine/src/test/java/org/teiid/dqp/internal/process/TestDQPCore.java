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

import java.sql.ResultSet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.Mockito;
import org.teiid.CommandContext;
import org.teiid.api.exception.query.QueryResolverException;
import org.teiid.cache.DefaultCacheFactory;
import org.teiid.client.RequestMessage;
import org.teiid.client.ResultsMessage;
import org.teiid.common.buffer.impl.BufferManagerImpl;
import org.teiid.dqp.internal.datamgr.ConnectorManagerRepository;
import org.teiid.dqp.internal.datamgr.FakeTransactionService;
import org.teiid.dqp.internal.process.AbstractWorkItem.ThreadState;
import org.teiid.dqp.service.AutoGenDataService;
import org.teiid.dqp.service.FakeBufferService;
import org.teiid.metadata.MetadataProvider;
import org.teiid.query.optimizer.TestOptimizer;
import org.teiid.query.optimizer.capabilities.BasicSourceCapabilities;
import org.teiid.query.optimizer.capabilities.SourceCapabilities.Capability;
import org.teiid.query.unittest.FakeMetadataFactory;
import org.teiid.query.unittest.RealMetadataFactory;

@SuppressWarnings("nls")
public class TestDQPCore {

    private DQPCore core;
    private DQPConfiguration config;
    private AutoGenDataService agds;

    @Before public void setUp() throws Exception {
    	agds = new AutoGenDataService();
        DQPWorkContext context = FakeMetadataFactory.buildWorkContext(RealMetadataFactory.exampleBQTCached());
        context.getVDB().getModel("BQT3").setVisible(false); //$NON-NLS-1$
        context.getVDB().getModel("VQT").setVisible(false); //$NON-NLS-1$

        ConnectorManagerRepository repo = Mockito.mock(ConnectorManagerRepository.class);
        context.getVDB().addAttchment(ConnectorManagerRepository.class, repo);
        Mockito.stub(repo.getConnectorManager(Mockito.anyString())).toReturn(agds);
        
        core = new DQPCore();
        core.setBufferService(new FakeBufferService());
        core.setCacheFactory(new DefaultCacheFactory());
        core.setTransactionService(new FakeTransactionService());
        
        config = new DQPConfiguration();
        config.setMaxActivePlans(1);
        config.setUserRequestSourceConcurrency(2);
        core.start(config);
    }
    
    @After public void tearDown() throws Exception {
    	DQPWorkContext.setWorkContext(new DQPWorkContext());
    	core.stop();
    }

    public RequestMessage exampleRequestMessage(String sql) {
        RequestMessage msg = new RequestMessage(sql);
        msg.setCursorType(ResultSet.TYPE_SCROLL_INSENSITIVE);
        msg.setFetchSize(10);
        msg.setPartialResults(false);
        msg.setExecutionId(100);
        return msg;
    }
    
    @Test public void testConfigurationSets() {
    	assertEquals(1, core.getMaxActivePlans());
    	assertEquals(2, core.getUserRequestSourceConcurrency());
    }

    @Test public void testRequest1() throws Exception {
    	helpExecute("SELECT IntKey FROM BQT1.SmallA", "a"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testUser1() throws Exception {
        String sql = "SELECT IntKey FROM BQT1.SmallA WHERE user() = 'logon'"; //$NON-NLS-1$
        String userName = "logon"; //$NON-NLS-1$
        helpExecute(sql, userName);
    }

    @Test public void testUser2() throws Exception {
        String sql = "SELECT IntKey FROM BQT1.SmallA WHERE user() LIKE 'logon'"; //$NON-NLS-1$
        String userName = "logon"; //$NON-NLS-1$
        helpExecute(sql, userName);
    }

    @Test public void testUser3() throws Exception {
        String sql = "SELECT IntKey FROM BQT1.SmallA WHERE user() IN ('logon3') AND StringKey LIKE '1'"; //$NON-NLS-1$
        String userName = "logon3"; //$NON-NLS-1$
        helpExecute(sql, userName);
    }

    @Test public void testUser4() throws Exception {
        String sql = "SELECT IntKey FROM BQT1.SmallA WHERE 'logon4' = user() AND StringKey = '1'"; //$NON-NLS-1$
        String userName = "logon4"; //$NON-NLS-1$
        helpExecute(sql, userName);
    }

    @Test public void testUser5() throws Exception {
        String sql = "SELECT IntKey FROM BQT1.SmallA WHERE user() IS NULL "; //$NON-NLS-1$
        String userName = "logon"; //$NON-NLS-1$
        helpExecute(sql, userName);
    }

    @Test public void testUser6() throws Exception {
        String sql = "SELECT IntKey FROM BQT1.SmallA WHERE user() = 'logon33' "; //$NON-NLS-1$
        String userName = "logon"; //$NON-NLS-1$
        helpExecute(sql, userName);
    }

    @Test public void testUser7() throws Exception {
        String sql = "UPDATE BQT1.SmallA SET IntKey = 2 WHERE user() = 'logon' AND StringKey = '1' "; //$NON-NLS-1$
        String userName = "logon"; //$NON-NLS-1$
        helpExecute(sql, userName);
    }

    @Test public void testUser8() throws Exception {
        String sql = "SELECT user(), StringKey FROM BQT1.SmallA WHERE IntKey = 1 "; //$NON-NLS-1$
        String userName = "logon"; //$NON-NLS-1$
        helpExecute(sql, userName);
    }

    @Test public void testUser9() throws Exception {
        String sql = "SELECT IntKey FROM BQT1.SmallA WHERE user() = StringKey AND StringKey = '1' "; //$NON-NLS-1$
        String userName = "1"; //$NON-NLS-1$
        helpExecute(sql, userName);
    }

    @Test public void testEnvSessionId() throws Exception {
        String sql = "SELECT env('sessionid') as SessionID"; //$NON-NLS-1$
        String userName = "1"; //$NON-NLS-1$
        ResultsMessage rm = helpExecute(sql, userName);
        assertEquals("1", rm.getResults()[0].get(0)); //$NON-NLS-1$
    }
    
    @Test public void testEnvSessionIdMixedCase() throws Exception {
        String sql = "SELECT env('sEsSIonId') as SessionID"; //$NON-NLS-1$
        String userName = "1"; //$NON-NLS-1$
        ResultsMessage rm = helpExecute(sql, userName);
        assertEquals("1", rm.getResults()[0].get(0)); //$NON-NLS-1$
    }
    
    @Test public void testTxnAutoWrap() throws Exception {
    	String sql = "SELECT * FROM BQT1.SmallA"; //$NON-NLS-1$
    	helpExecute(sql, "a", 1, true); //$NON-NLS-1$
    }
    
    /**
     * Ensures that VQT visibility does not affect the view query
     */
    @Test public void testViewVisibility() throws Exception {
    	String sql = "SELECT * FROM VQT.SmallA_2589g"; //$NON-NLS-1$
    	helpExecute(sql, "a"); //$NON-NLS-1$
    }
    
    @Test public void testLimitCompensation() throws Exception {
    	String sql = "SELECT * FROM VQT.SmallA_2589g LIMIT 1, 1"; //$NON-NLS-1$
    	BasicSourceCapabilities caps = TestOptimizer.getTypicalCapabilities();
    	caps.setCapabilitySupport(Capability.ROW_LIMIT, true);
    	agds.setCaps(caps);
    	ResultsMessage rm = helpExecute(sql, "a"); //$NON-NLS-1$
    	//we test for > 0 here because the autogen service doesn't obey the limit
    	assertTrue(rm.getResults().length > 0);
    }
    
    @Test public void testLimitCompensation1() throws Exception {
    	String sql = "SELECT * FROM VQT.SmallA_2589g LIMIT 1, 1"; //$NON-NLS-1$
    	ResultsMessage rm = helpExecute(sql, "a"); //$NON-NLS-1$
    	assertEquals(1, rm.getResults().length);
    }

    
    /**
     * Tests whether an exception result is sent when an exception occurs
     * @since 4.3
     */
    @Test public void testPlanningException() throws Exception {
        String sql = "SELECT IntKey FROM BQT1.BadIdea "; //$NON-NLS-1$
        
        RequestMessage reqMsg = exampleRequestMessage(sql);

        Future<ResultsMessage> message = core.executeRequest(reqMsg.getExecutionId(), reqMsg);
        try {
        	message.get(5000, TimeUnit.MILLISECONDS);
        } catch (ExecutionException e) {
        	assertTrue(e.getCause() instanceof QueryResolverException);
        }
    }
    
    @Ignore("visibility no longer ristricts access")
	@Test public void testLookupVisibility() throws Exception {
		helpTestVisibilityFails("select lookup('bqt3.smalla', 'intkey', 'stringkey', '?')"); //$NON-NLS-1$
	}
	
	@Test public void testCancel() throws Exception {
		assertFalse(this.core.cancelRequest(1L));
	}
	
    @Test public void testBufferLimit() throws Exception {
    	//the sql should return 100 rows
        String sql = "SELECT A.IntKey FROM BQT1.SmallA as A, BQT1.SmallA as B"; //$NON-NLS-1$
        String userName = "1"; //$NON-NLS-1$
        String sessionid = "1"; //$NON-NLS-1$
        
        RequestMessage reqMsg = exampleRequestMessage(sql);
        reqMsg.setCursorType(ResultSet.TYPE_FORWARD_ONLY);
        DQPWorkContext.getWorkContext().getSession().setSessionId(sessionid);
        DQPWorkContext.getWorkContext().getSession().setUserName(userName);
        ((BufferManagerImpl)core.getBufferManager()).setProcessorBatchSize(2);
        Future<ResultsMessage> message = core.executeRequest(reqMsg.getExecutionId(), reqMsg);
        ResultsMessage rm = message.get(5000, TimeUnit.MILLISECONDS);
        assertNull(rm.getException());
        assertEquals(2, rm.getResults().length);
        RequestWorkItem item = core.getRequestWorkItem(DQPWorkContext.getWorkContext().getRequestID(reqMsg.getExecutionId()));

        message = core.processCursorRequest(reqMsg.getExecutionId(), 3, 2);
        rm = message.get(5000, TimeUnit.MILLISECONDS);
        assertNull(rm.getException());
        assertEquals(2, rm.getResults().length);
        //ensure that we are idle
        for (int i = 0; i < 10 && item.getThreadState() != ThreadState.IDLE; i++) {
        	Thread.sleep(100);
        }
        assertEquals(ThreadState.IDLE, item.getThreadState());
        assertTrue(item.resultsBuffer.getManagedRowCount() <= 46);
        //pull the rest of the results
        for (int j = 0; j < 48; j++) {
            item = core.getRequestWorkItem(DQPWorkContext.getWorkContext().getRequestID(reqMsg.getExecutionId()));

	        message = core.processCursorRequest(reqMsg.getExecutionId(), j * 2 + 5, 2);
	        rm = message.get(5000, TimeUnit.MILLISECONDS);
	        assertNull(rm.getException());
	        assertEquals(2, rm.getResults().length);
        }
    }
    
    @Test public void testBufferReuse() throws Exception {
    	//the sql should return 100 rows
        String sql = "SELECT A.IntKey FROM BQT1.SmallA as A, BQT1.SmallA as B ORDER BY A.IntKey"; //$NON-NLS-1$
        String userName = "1"; //$NON-NLS-1$
        String sessionid = "1"; //$NON-NLS-1$
        
        RequestMessage reqMsg = exampleRequestMessage(sql);
        reqMsg.setCursorType(ResultSet.TYPE_FORWARD_ONLY);
        DQPWorkContext.getWorkContext().getSession().setSessionId(sessionid);
        DQPWorkContext.getWorkContext().getSession().setUserName(userName);
        ((BufferManagerImpl)core.getBufferManager()).setProcessorBatchSize(2);
        Future<ResultsMessage> message = core.executeRequest(reqMsg.getExecutionId(), reqMsg);
        ResultsMessage rm = message.get(5000, TimeUnit.MILLISECONDS);
        assertNull(rm.getException());
        assertEquals(2, rm.getResults().length);
        RequestWorkItem item = core.getRequestWorkItem(DQPWorkContext.getWorkContext().getRequestID(reqMsg.getExecutionId()));
        assertEquals(100, item.resultsBuffer.getRowCount());
    }
    
    @Test public void testSourceConcurrency() throws Exception {
    	//setup default of 2
    	agds.setSleep(100);
    	StringBuffer sql = new StringBuffer();
    	int branches = 20;
    	for (int i = 0; i < branches; i++) {
    		if (i > 0) {
    			sql.append(" union all ");
    		}
    		sql.append("select intkey || " + i + " from bqt1.smalla");
    	}
    	sql.append(" limit 2");
    	helpExecute(sql.toString(), "a");
    	//there's isn't a hard guarantee that only two requests will get started
    	assertTrue(agds.getExecuteCount().get() <= 6);
    	
    	//20 concurrent
    	core.setUserRequestSourceConcurrency(20);
    	agds.getExecuteCount().set(0);
    	helpExecute(sql.toString(), "a");
    	assertEquals(20, agds.getExecuteCount().get());
    	
    	//serial
    	core.setUserRequestSourceConcurrency(1);
    	agds.getExecuteCount().set(0);
    	helpExecute(sql.toString(), "a");
    	assertEquals(1, agds.getExecuteCount().get());
    }
    
    @Test public void testMetadataProvider() throws Exception {
    	this.config.setMetadataProvider(new MetadataProvider() {
    		int callCount;
    		@Override
    		public ViewDefinition getViewDefinition(String schema,
    				String viewName, CommandContext context) {
    			if (callCount++ > 0) {
        			ViewDefinition vd = new ViewDefinition("SELECT 'something else'", Scope.USER);
        			return vd;
    			}	
    			ViewDefinition vd = new ViewDefinition("SELECT 'hello world'", Scope.USER);
    			return vd;
    		}
    	});
    	//the sql should normally return 10 rows
        String sql = "SELECT * FROM vqt.SmallB UNION SELECT * FROM vqt.SmallB"; //$NON-NLS-1$
        String userName = "1"; //$NON-NLS-1$
        
        ResultsMessage rm = helpExecute(sql, userName);
        assertEquals(1, rm.getResults().length); //$NON-NLS-1$
        assertEquals("hello world", rm.getResults()[0].get(0)); //$NON-NLS-1$
        
        rm = helpExecute(sql, userName);
        assertEquals(1, rm.getResults().length); //$NON-NLS-1$
        assertEquals("something else", rm.getResults()[0].get(0)); //$NON-NLS-1$
    }
    
	public void helpTestVisibilityFails(String sql) throws Exception {
        RequestMessage reqMsg = exampleRequestMessage(sql); 
        reqMsg.setTxnAutoWrapMode(RequestMessage.TXN_WRAP_OFF);
        Future<ResultsMessage> message = core.executeRequest(reqMsg.getExecutionId(), reqMsg);
        ResultsMessage results = message.get(5000, TimeUnit.MILLISECONDS);
        assertEquals("[QueryValidatorException]Group does not exist: BQT3.SmallA", results.getException().toString()); //$NON-NLS-1$
	}

    ///////////////////////////Helper method///////////////////////////////////
    private ResultsMessage helpExecute(String sql, String userName) throws Exception {
    	return helpExecute(sql, userName, 1, false);
    }

    private ResultsMessage helpExecute(String sql, String userName, int sessionid, boolean txnAutoWrap) throws Exception {
        RequestMessage reqMsg = exampleRequestMessage(sql);
        DQPWorkContext.getWorkContext().getSession().setSessionId(String.valueOf(sessionid));
        DQPWorkContext.getWorkContext().getSession().setUserName(userName);
        if (txnAutoWrap) {
        	reqMsg.setTxnAutoWrapMode(RequestMessage.TXN_WRAP_ON);
        }

        Future<ResultsMessage> message = core.executeRequest(reqMsg.getExecutionId(), reqMsg);
        assertNotNull(core.getClientState(String.valueOf(sessionid), false));
        ResultsMessage results = message.get(5000, TimeUnit.MILLISECONDS);
        core.terminateSession(String.valueOf(sessionid));
        assertNull(core.getClientState(String.valueOf(sessionid), false));
        if (results.getException() != null) {
        	throw results.getException();
        }
        return results;
    }
}
