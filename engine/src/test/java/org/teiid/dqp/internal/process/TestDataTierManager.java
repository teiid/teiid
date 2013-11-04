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

import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.teiid.adminapi.impl.VDBMetaData;
import org.teiid.cache.CacheConfiguration;
import org.teiid.cache.DefaultCacheFactory;
import org.teiid.client.RequestMessage;
import org.teiid.client.SourceWarning;
import org.teiid.common.buffer.BlockedException;
import org.teiid.common.buffer.TupleSource;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidProcessingException;
import org.teiid.dqp.internal.datamgr.ConnectorManagerRepository;
import org.teiid.dqp.internal.datamgr.FakeTransactionService;
import org.teiid.dqp.message.AtomicRequestMessage;
import org.teiid.dqp.message.RequestID;
import org.teiid.dqp.service.AutoGenDataService;
import org.teiid.dqp.service.FakeBufferService;
import org.teiid.query.metadata.QueryMetadataInterface;
import org.teiid.query.optimizer.capabilities.DefaultCapabilitiesFinder;
import org.teiid.query.parser.QueryParser;
import org.teiid.query.processor.RegisterRequestParameter;
import org.teiid.query.resolver.QueryResolver;
import org.teiid.query.sql.lang.Command;
import org.teiid.query.unittest.RealMetadataFactory;
import org.teiid.query.util.CommandContext;
import org.teiid.translator.CacheDirective;

@SuppressWarnings("nls")
public class TestDataTierManager {
    
	private VDBMetaData vdb = RealMetadataFactory.exampleBQTVDB();
    private DQPCore rm;
    private DataTierManagerImpl dtm;
    private CommandContext context;
    private AutoGenDataService connectorManager = new AutoGenDataService();
    private RequestWorkItem workItem;
    private int limit = -1;
    private boolean serial = false;
    
    @Before public void setUp() {
    	limit = -1;
    	connectorManager = new AutoGenDataService();
    	serial = false;
    }
    
    private static Command helpGetCommand(String sql, QueryMetadataInterface metadata) throws Exception {
        Command command = QueryParser.getQueryParser().parseCommand(sql);
        QueryResolver.resolveCommand(command, metadata);
        return command;
    }
    
    private DataTierTupleSource helpSetup(int nodeId) throws Exception {
        return helpSetup("SELECT * FROM BQT1.SmallA", nodeId); //$NON-NLS-1$
    }
    
    private DataTierTupleSource helpSetup(String sql, int nodeId) throws Exception {
        helpSetupDataTierManager();
        AtomicRequestMessage request = helpSetupRequest(sql, nodeId, RealMetadataFactory.exampleBQTCached());
        request.setSerial(serial);
        return new DataTierTupleSource(request, workItem, connectorManager.registerRequest(request), dtm, limit);
    }

	private AtomicRequestMessage helpSetupRequest(String sql, int nodeId, QueryMetadataInterface metadata) throws Exception {
		DQPWorkContext workContext = RealMetadataFactory.buildWorkContext(metadata, vdb);
        
        Command command = helpGetCommand(sql, metadata);
        
        RequestMessage original = new RequestMessage();
        original.setExecutionId(1);
        original.setPartialResults(true);
        RequestID requestID = workContext.getRequestID(original.getExecutionId());
        
        context = new CommandContext();
        context.setSession(workContext.getSession());
        context.setVdbName("test"); //$NON-NLS-1$
        context.setVdbVersion(1);
        context.setQueryProcessorFactory(new QueryProcessorFactoryImpl(dtm.getBufferManager(), dtm, new DefaultCapabilitiesFinder(), null, metadata));
        workItem = TestDQPCoreRequestHandling.addRequest(rm, original, requestID, null, workContext);
        context.setWorkItem(workItem);
        AtomicRequestMessage request = new AtomicRequestMessage(original, workContext, nodeId);
        request.setCommand(command);
        request.setConnectorName("FakeConnectorID"); //$NON-NLS-1$
        request.setCommandContext(context);
        return request;
	}

	private void helpSetupDataTierManager() {
        FakeBufferService bs = new FakeBufferService();
		rm = new DQPCore();
        rm.setTransactionService(new FakeTransactionService());
        rm.setBufferManager(bs.getBufferManager());
        CacheConfiguration config = new CacheConfiguration();
        config.setMaxAgeInSeconds(-1);
		rm.setResultsetCache(new SessionAwareCache<CachedResults>("resultset", new DefaultCacheFactory(config), SessionAwareCache.Type.RESULTSET, 0));
        rm.setPreparedPlanCache(new SessionAwareCache<PreparedPlan>("preparedplan", new DefaultCacheFactory(config), SessionAwareCache.Type.PREPAREDPLAN, 0));
        rm.start(new DQPConfiguration());

        ConnectorManagerRepository repo = Mockito.mock(ConnectorManagerRepository.class);
        Mockito.stub(repo.getConnectorManager(Mockito.anyString())).toReturn(connectorManager);
        vdb.addAttchment(ConnectorManagerRepository.class, repo);
        
        dtm = new DataTierManagerImpl(rm,bs.getBufferManager(), true);
	}
    
    @Test public void testDataTierTupleSource() throws Exception {
    	DataTierTupleSource info = helpSetup(1);
    	assertEquals(10, pullTuples(info, 10));
        assertNotNull(workItem.getConnectorRequest(info.getAtomicRequestMessage().getAtomicRequestID()));
        assertNull(info.nextTuple());
        info.closeSource();
        assertNull(workItem.getConnectorRequest(info.getAtomicRequestMessage().getAtomicRequestID()));
    }
    
    @Test public void testDataTierTupleSourceWarnings() throws Exception {
    	DataTierTupleSource info = helpSetup(1);
    	connectorManager.addWarning = true;
    	assertEquals(10, pullTuples(info, 10));
        assertNotNull(workItem.getConnectorRequest(info.getAtomicRequestMessage().getAtomicRequestID()));
        assertNull(info.nextTuple());
        List<Exception> warnings = context.getAndClearWarnings();
		assertEquals(1, warnings.size());
        SourceWarning warning = (SourceWarning) warnings.get(0);
		assertFalse(warning.isPartialResultsError());
        info.closeSource();
        assertNull(workItem.getConnectorRequest(info.getAtomicRequestMessage().getAtomicRequestID()));
    }
    
    @Test public void testDataTierTupleSourceLimit() throws Exception {
    	limit = 1;
    	DataTierTupleSource info = helpSetup(1);
    	assertEquals(1, pullTuples(info, 1));
        assertNotNull(workItem.getConnectorRequest(info.getAtomicRequestMessage().getAtomicRequestID()));
        assertNull(info.nextTuple());
        info.closeSource();
        assertNull(workItem.getConnectorRequest(info.getAtomicRequestMessage().getAtomicRequestID()));
    }

	private int pullTuples(TupleSource info, int l)
			throws TeiidComponentException, TeiidProcessingException,
			InterruptedException {
		int i = 0;
		while (true) {
	    	try {
	    		if (info.nextTuple() == null) {
	    			break;
	    		}
	    		if (++i == l) {
	    			break;
	    		}
	    	} catch (BlockedException e) {
	    		Thread.sleep(50);
	    	}
    	}
		return i;
	}
    
    @Test public void testPartialResults() throws Exception {
    	DataTierTupleSource info = helpSetup(1);
    	connectorManager.throwExceptionOnExecute = true;
    	for (int i = 0; i < 10; i++) {
	    	try {
	    		assertNull(info.nextTuple());
	    		SourceWarning warning = (SourceWarning) context.getAndClearWarnings().get(0);
	    		assertTrue(warning.isPartialResultsError());
	    		return;
	    	} catch (BlockedException e) {
	    		Thread.sleep(50);
	    	}
    	}
    	fail();
    }
    
    @Test public void testNoRowsException() throws Exception {
    	this.connectorManager.setRows(0);
    	DataTierTupleSource info = helpSetup(3);
    	while (true) {
	    	try {
	        	assertNull(info.nextTuple());
	    		break;
	    	} catch (BlockedException e) {
	    		Thread.sleep(50);
	    	}
    	}
    }
    
    @Test public void testAsynch() throws Exception {
    	this.connectorManager.dataNotAvailable = 10;
    	this.serial = true;
    	this.connectorManager.setRows(0);
    	DataTierTupleSource info = helpSetup(3);
    	boolean blocked = false;
    	while (true) {
	    	try {
	        	assertNull(info.nextTuple());
	    		break;
	    	} catch (BlockedException e) {
	    		blocked = true;
	    		try {
		    		info.nextTuple();
	    		} catch (BlockedException be) {
		    		fail();
	    		}
	    		Thread.sleep(50);
	    	}
    	}
    	assertTrue(blocked);
    }
    
    @Test public void testAsynchStrict() throws Exception {
    	this.connectorManager.dataNotAvailable = 1000;
    	this.serial = true;
    	this.connectorManager.strict = true;
    	this.connectorManager.setRows(0);
    	DataTierTupleSource info = helpSetup(3);
    	boolean blocked = false;
    	while (true) {
	    	try {
	        	assertNull(info.nextTuple());
	    		break;
	    	} catch (BlockedException e) {
	    		blocked = true;
	    		try {
		    		info.nextTuple();
		    		fail();
	    		} catch (BlockedException be) {
	    			//we won't bother to wait the full second
	    		}
	    		break;
	    	}
    	}
    	assertTrue(blocked);
    }
    
    @Test public void testCaching() throws Exception {
    	assertEquals(0, connectorManager.getExecuteCount().get());

    	QueryMetadataInterface metadata = RealMetadataFactory.exampleBQTCached();
    	
    	CacheDirective cd = new CacheDirective();
    	this.connectorManager.cacheDirective = cd;
    	helpSetupDataTierManager();
    	Command command = helpSetupRequest("SELECT stringkey from bqt1.smalla", 1, metadata).getCommand();
    	RegisterRequestParameter rrp = new RegisterRequestParameter();
    	rrp.connectorBindingId = "x";
    	TupleSource ts = dtm.registerRequest(context, command, "foo", rrp);
    	assertTrue(ts instanceof CachingTupleSource);
    	assertEquals(10, pullTuples(ts, -1));
    	assertEquals(1, connectorManager.getExecuteCount().get());
    	assertFalse(rrp.doNotCache);
    	assertFalse(((CachingTupleSource)ts).dtts.errored);
    	assertNull(((CachingTupleSource)ts).dtts.scope);
    	ts.closeSource();
    	assertEquals(1, this.rm.getRsCache().getCachePutCount());
    	assertEquals(1, this.rm.getRsCache().getTotalCacheEntries());
    	
    	//same session, should be cached
    	command = helpSetupRequest("SELECT stringkey from bqt1.smalla", 1, metadata).getCommand();
    	rrp = new RegisterRequestParameter();
    	rrp.connectorBindingId = "x";
    	ts = dtm.registerRequest(context, command, "foo", rrp);
    	assertFalse(ts instanceof CachingTupleSource);
    	assertEquals(10, pullTuples(ts, -1));
    	assertEquals(1, connectorManager.getExecuteCount().get());
    	assertTrue(rrp.doNotCache);
    	
    	//switch sessions
    	command = helpSetupRequest("SELECT stringkey from bqt1.smalla", 1, metadata).getCommand();
    	this.context.getSession().setSessionId("different");
    	rrp = new RegisterRequestParameter();
    	rrp.connectorBindingId = "x";
    	ts = dtm.registerRequest(context, command, "foo", rrp);
    	assertTrue(ts instanceof CachingTupleSource);
    	assertEquals(9, pullTuples(ts, 9));
    	assertEquals(2, connectorManager.getExecuteCount().get());
    	assertFalse(rrp.doNotCache);
    	ts.closeSource(); //should force read all
    	assertFalse(((CachingTupleSource)ts).dtts.errored);
    	assertNull(((CachingTupleSource)ts).dtts.scope);
    	
    	assertEquals(2, this.rm.getRsCache().getCachePutCount());
    	assertEquals(2, this.rm.getRsCache().getTotalCacheEntries());
    }
    
}
