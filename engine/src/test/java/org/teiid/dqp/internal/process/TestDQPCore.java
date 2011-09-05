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
import java.util.concurrent.TimeoutException;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.Mockito;
import org.teiid.api.exception.query.QueryResolverException;
import org.teiid.cache.CacheConfiguration;
import org.teiid.cache.DefaultCacheFactory;
import org.teiid.client.RequestMessage;
import org.teiid.client.ResultsMessage;
import org.teiid.client.RequestMessage.StatementType;
import org.teiid.client.lob.LobChunk;
import org.teiid.client.util.ResultsFuture;
import org.teiid.common.buffer.BufferManager;
import org.teiid.common.buffer.BufferManagerFactory;
import org.teiid.common.buffer.impl.BufferManagerImpl;
import org.teiid.core.TeiidProcessingException;
import org.teiid.core.types.BlobType;
import org.teiid.dqp.internal.datamgr.ConnectorManagerRepository;
import org.teiid.dqp.internal.datamgr.FakeTransactionService;
import org.teiid.dqp.internal.process.AbstractWorkItem.ThreadState;
import org.teiid.dqp.service.AutoGenDataService;
import org.teiid.dqp.service.BufferService;
import org.teiid.query.optimizer.TestOptimizer;
import org.teiid.query.optimizer.capabilities.BasicSourceCapabilities;
import org.teiid.query.optimizer.capabilities.SourceCapabilities.Capability;
import org.teiid.query.unittest.RealMetadataFactory;

@SuppressWarnings("nls")
public class TestDQPCore {

    private final class LobThread extends Thread {
		BlobType bt;
		private final RequestMessage reqMsg;
		volatile ResultsFuture<LobChunk> chunkFuture;
		protected DQPWorkContext workContext;

		private LobThread(RequestMessage reqMsg) {
			this.reqMsg = reqMsg;
		}
		
		@Override
		public void run() {
			synchronized (this) {
				while (workContext == null) {
					try {
						this.wait();
					} catch (InterruptedException e) {
					}
				}
			}
			workContext.runInContext(new Runnable() {
				
				@Override
				public void run() {
					try {
						chunkFuture = core.requestNextLobChunk(1, reqMsg.getExecutionId(), bt.getReferenceStreamId());
					} catch (TeiidProcessingException e) {
						e.printStackTrace();
					}
				}
			});
		}
	}

	private DQPCore core;
    private DQPConfiguration config;
    private AutoGenDataService agds;

    @Before public void setUp() throws Exception {
    	agds = new AutoGenDataService();
        DQPWorkContext context = RealMetadataFactory.buildWorkContext(RealMetadataFactory.createTransformationMetadata(RealMetadataFactory.exampleBQTCached().getMetadataStore(), "bqt"));
        context.getVDB().getModel("BQT3").setVisible(false); //$NON-NLS-1$
        context.getVDB().getModel("VQT").setVisible(false); //$NON-NLS-1$

        ConnectorManagerRepository repo = Mockito.mock(ConnectorManagerRepository.class);
        context.getVDB().addAttchment(ConnectorManagerRepository.class, repo);
        Mockito.stub(repo.getConnectorManager(Mockito.anyString())).toReturn(agds);
        
        core = new DQPCore();
        core.setBufferService(new BufferService() {
			
			@Override
			public BufferManager getBufferManager() {
				return BufferManagerFactory.createBufferManager();
			}
		});
        core.setCacheFactory(new DefaultCacheFactory());
        core.setTransactionService(new FakeTransactionService());
        
        config = new DQPConfiguration();
        config.setMaxActivePlans(1);
        config.setUserRequestSourceConcurrency(2);
        config.setResultsetCacheConfig(new CacheConfiguration());
        core.start(config);
        core.getPrepPlanCache().setModTime(1);
        core.getRsCache().setModTime(1);
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
        ResultsMessage rm = message.get(500000, TimeUnit.MILLISECONDS);
        assertNull(rm.getException());
        assertEquals(2, rm.getResults().length);
        RequestWorkItem item = core.getRequestWorkItem(DQPWorkContext.getWorkContext().getRequestID(reqMsg.getExecutionId()));

        message = core.processCursorRequest(reqMsg.getExecutionId(), 3, 2);
        rm = message.get(500000, TimeUnit.MILLISECONDS);
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
        ResultsMessage rm = message.get(500000, TimeUnit.MILLISECONDS);
        assertNull(rm.getException());
        assertEquals(2, rm.getResults().length);
        RequestWorkItem item = core.getRequestWorkItem(DQPWorkContext.getWorkContext().getRequestID(reqMsg.getExecutionId()));
        assertEquals(100, item.resultsBuffer.getRowCount());
    }
    
    @Test public void testBufferReuse1() throws Exception {
    	//the sql should return 100 rows
        String sql = "SELECT IntKey FROM texttable('1112131415' columns intkey integer width 2 no row delimiter) t " +
        		"union " +
        		"SELECT IntKey FROM bqt1.smalla"; //$NON-NLS-1$
        String userName = "1"; //$NON-NLS-1$
        String sessionid = "1"; //$NON-NLS-1$
        agds.sleep = 500;
        agds.setUseIntCounter(true);
        RequestMessage reqMsg = exampleRequestMessage(sql);
        reqMsg.setCursorType(ResultSet.TYPE_FORWARD_ONLY);
        DQPWorkContext.getWorkContext().getSession().setSessionId(sessionid);
        DQPWorkContext.getWorkContext().getSession().setUserName(userName);
        BufferManagerImpl bufferManager = (BufferManagerImpl)core.getBufferManager();
		bufferManager.setProcessorBatchSize(20);
        Future<ResultsMessage> message = core.executeRequest(reqMsg.getExecutionId(), reqMsg);
        ResultsMessage rm = message.get(500000, TimeUnit.MILLISECONDS);
        assertNull(rm.getException());
        assertEquals(5, rm.getResults().length);
        
        message = core.processCursorRequest(reqMsg.getExecutionId(), 6, 5);
        rm = message.get(500000, TimeUnit.MILLISECONDS);
        assertNull(rm.getException());
        assertEquals(5, rm.getResults().length);
        
        message = core.processCursorRequest(reqMsg.getExecutionId(), 11, 5);
        rm = message.get(500000, TimeUnit.MILLISECONDS);
        assertNull(rm.getException());
        assertEquals(5, rm.getResults().length);
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
    	assertTrue(agds.getExecuteCount().get() <= 20);
    	assertTrue(agds.getExecuteCount().get() > 10);
    	
    	//serial
    	core.setUserRequestSourceConcurrency(1);
    	agds.getExecuteCount().set(0);
    	helpExecute(sql.toString(), "a");
    	assertEquals(1, agds.getExecuteCount().get());
    }
    
    @Test public void testUsingFinalBuffer() throws Exception {
    	String sql = "select intkey from bqt1.smalla union select 1";
    	((BufferManagerImpl)core.getBufferManager()).setProcessorBatchSize(2);
    	agds.sleep = 500;
        RequestMessage reqMsg = exampleRequestMessage(sql);
        Future<ResultsMessage> message = core.executeRequest(reqMsg.getExecutionId(), reqMsg);
        ResultsMessage rm = message.get(500000, TimeUnit.MILLISECONDS);
        assertNull(rm.getException());
        assertEquals(1, rm.getResults().length);

        message = core.processCursorRequest(reqMsg.getExecutionId(), 3, 2);
        rm = message.get(500000, TimeUnit.MILLISECONDS);
        assertNull(rm.getException());
        assertEquals(1, rm.getResults().length);
        
        message = core.processCursorRequest(reqMsg.getExecutionId(), 3, 2);
        rm = message.get(500000, TimeUnit.MILLISECONDS);
        assertNull(rm.getException());
        assertEquals(0, rm.getResults().length);
    }
    
    @Test public void testPreparedPlanInvalidation() throws Exception {
        String sql = "insert into #temp select * FROM vqt.SmallB"; //$NON-NLS-1$
        String userName = "1"; //$NON-NLS-1$
        int sessionid = 1; //$NON-NLS-1$
        RequestMessage reqMsg = exampleRequestMessage(sql);
        ResultsMessage rm = execute(userName, sessionid, reqMsg);
        assertEquals(1, rm.getResults().length); //$NON-NLS-1$
        
        sql = "select * from #temp"; //$NON-NLS-1$
        reqMsg = exampleRequestMessage(sql);
        reqMsg.setStatementType(StatementType.PREPARED);
        rm = execute(userName, sessionid, reqMsg);
        assertEquals(10, rm.getResults().length); //$NON-NLS-1$
        
        sql = "select * from #temp"; //$NON-NLS-1$
        reqMsg = exampleRequestMessage(sql);
        reqMsg.setStatementType(StatementType.PREPARED);
        rm = execute(userName, sessionid, reqMsg);
        assertEquals(10, rm.getResults().length); //$NON-NLS-1$
        
        assertEquals(1, this.core.getPrepPlanCache().getCacheHitCount());

        Thread.sleep(100);

        //perform a minor update, we should still use the cache
        sql = "delete from #temp where a12345 = '11'"; //$NON-NLS-1$
        reqMsg = exampleRequestMessage(sql);
        rm = execute(userName, sessionid, reqMsg);
        assertEquals(1, rm.getResults().length); //$NON-NLS-1$

        sql = "select * from #temp"; //$NON-NLS-1$
        reqMsg = exampleRequestMessage(sql);
        reqMsg.setStatementType(StatementType.PREPARED);
        rm = execute(userName, sessionid, reqMsg);
        assertEquals(10, rm.getResults().length); //$NON-NLS-1$
        
        assertEquals(2, this.core.getPrepPlanCache().getCacheHitCount());

        //perform a major update, we will purge the plan
        sql = "delete from #temp"; //$NON-NLS-1$
        reqMsg = exampleRequestMessage(sql);
        rm = execute(userName, sessionid, reqMsg);
        assertEquals(1, rm.getResults().length); //$NON-NLS-1$
        
        sql = "select * from #temp"; //$NON-NLS-1$
        reqMsg = exampleRequestMessage(sql);
        reqMsg.setStatementType(StatementType.PREPARED);
        rm = execute(userName, sessionid, reqMsg);
        assertEquals(0, rm.getResults().length); //$NON-NLS-1$
        
        assertEquals(2, this.core.getPrepPlanCache().getCacheHitCount());
    }
    
    @Test public void testRsCacheInvalidation() throws Exception {
        String sql = "select * FROM vqt.SmallB"; //$NON-NLS-1$
        String userName = "1"; //$NON-NLS-1$
        int sessionid = 1; //$NON-NLS-1$
        RequestMessage reqMsg = exampleRequestMessage(sql);
        reqMsg.setUseResultSetCache(true);
        ResultsMessage rm = execute(userName, sessionid, reqMsg);
        assertEquals(10, rm.getResults().length); //$NON-NLS-1$
                
        sql = "select * FROM vqt.SmallB"; //$NON-NLS-1$
        reqMsg = exampleRequestMessage(sql);
        reqMsg.setUseResultSetCache(true);
        rm = execute(userName, sessionid, reqMsg);
        assertEquals(10, rm.getResults().length); //$NON-NLS-1$
        
        assertEquals(1, this.core.getRsCache().getCacheHitCount());

        Thread.sleep(100);

        sql = "delete from bqt1.smalla"; //$NON-NLS-1$
        reqMsg = exampleRequestMessage(sql);
        rm = execute(userName, sessionid, reqMsg);
        assertEquals(1, rm.getResults().length); //$NON-NLS-1$
        
        sql = "select * FROM vqt.SmallB"; //$NON-NLS-1$
        reqMsg = exampleRequestMessage(sql);
        reqMsg.setUseResultSetCache(true);
        rm = execute(userName, sessionid, reqMsg);
        assertEquals(10, rm.getResults().length); //$NON-NLS-1$
        
        assertEquals(1, this.core.getRsCache().getCacheHitCount());
    }
    
    @Test public void testLobConcurrency() throws Exception {
    	RequestMessage reqMsg = exampleRequestMessage("select to_bytes(stringkey, 'utf-8') FROM BQT1.SmallA"); 
        reqMsg.setTxnAutoWrapMode(RequestMessage.TXN_WRAP_OFF);
        agds.setSleep(100);
        ResultsFuture<ResultsMessage> message = core.executeRequest(reqMsg.getExecutionId(), reqMsg);
        final LobThread t = new LobThread(reqMsg);
        t.start();
        message.addCompletionListener(new ResultsFuture.CompletionListener<ResultsMessage>() {
        	@Override
        	public void onCompletion(ResultsFuture<ResultsMessage> future) {
        		try {
        			final BlobType bt = (BlobType)future.get().getResults()[0].get(0);
        			t.bt = bt;
        			t.workContext = DQPWorkContext.getWorkContext();
        			synchronized (t) {
            			t.notify();
					}
        			Thread.sleep(100); //give the Thread a chance to run
				} catch (Exception e) {
					throw new RuntimeException(e);
				}
        	}
		});
        message.get();
        t.join();
        assertNotNull(t.chunkFuture.get().getBytes());
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
        if (txnAutoWrap) {
        	reqMsg.setTxnAutoWrapMode(RequestMessage.TXN_WRAP_ON);
        }
        ResultsMessage results = execute(userName, sessionid, reqMsg);
        core.terminateSession(String.valueOf(sessionid));
        assertNull(core.getClientState(String.valueOf(sessionid), false));
        if (results.getException() != null) {
        	throw results.getException();
        }
        return results;
    }

	private ResultsMessage execute(String userName, int sessionid, RequestMessage reqMsg)
			throws InterruptedException, ExecutionException, TimeoutException {
		DQPWorkContext.getWorkContext().getSession().setSessionId(String.valueOf(sessionid));
        DQPWorkContext.getWorkContext().getSession().setUserName(userName);

        Future<ResultsMessage> message = core.executeRequest(reqMsg.getExecutionId(), reqMsg);
        assertNotNull(core.getClientState(String.valueOf(sessionid), false));
        ResultsMessage results = message.get(500000, TimeUnit.MILLISECONDS);
		return results;
	}
}
