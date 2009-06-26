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

import java.sql.ResultSet;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.teiid.dqp.internal.datamgr.impl.FakeTransactionService;
import org.teiid.dqp.internal.process.DQPCore;
import org.teiid.dqp.internal.process.DQPWorkContext;
import org.teiid.dqp.internal.process.DQPCore.ConnectorCapabilitiesCache;

import junit.framework.TestCase;

import com.metamatrix.api.exception.query.QueryResolverException;
import com.metamatrix.common.application.ApplicationEnvironment;
import com.metamatrix.common.vdb.api.ModelInfo;
import com.metamatrix.dqp.message.RequestID;
import com.metamatrix.dqp.message.RequestMessage;
import com.metamatrix.dqp.message.ResultsMessage;
import com.metamatrix.dqp.service.AutoGenDataService;
import com.metamatrix.dqp.service.DQPServiceNames;
import com.metamatrix.dqp.service.FakeBufferService;
import com.metamatrix.dqp.service.FakeMetadataService;
import com.metamatrix.dqp.service.FakeVDBService;
import com.metamatrix.jdbc.api.ExecutionProperties;
import com.metamatrix.platform.security.api.MetaMatrixSessionID;
import com.metamatrix.platform.security.api.SessionToken;
import com.metamatrix.query.optimizer.capabilities.BasicSourceCapabilities;
import com.metamatrix.query.optimizer.capabilities.SourceCapabilities;
import com.metamatrix.query.unittest.FakeMetadataFactory;


public class TestDQPCore extends TestCase {

    public TestDQPCore(String name) {
        super(name);
    }
    
    private DQPCore core;

    @Override
    protected void setUp() throws Exception {
        DQPWorkContext workContext = new DQPWorkContext();
        workContext.setVdbName("bqt"); //$NON-NLS-1$
        workContext.setVdbVersion("1"); //$NON-NLS-1$
        workContext.setSessionToken(new SessionToken(new MetaMatrixSessionID(1), "foo")); //$NON-NLS-1$
        DQPWorkContext.setWorkContext(workContext);
        
        String vdbName = "bqt"; //$NON-NLS-1$
		String vdbVersion = "1"; //$NON-NLS-1$
    	
    	final ApplicationEnvironment env = new ApplicationEnvironment();
        env.bindService(DQPServiceNames.BUFFER_SERVICE, new FakeBufferService());
        FakeMetadataService mdSvc = new FakeMetadataService();
		mdSvc.addVdb(vdbName, vdbVersion, FakeMetadataFactory.exampleBQTCached()); 
        env.bindService(DQPServiceNames.METADATA_SERVICE, mdSvc);
        env.bindService(DQPServiceNames.DATA_SERVICE, new AutoGenDataService());
        env.bindService(DQPServiceNames.TRANSACTION_SERVICE, new FakeTransactionService());
        FakeVDBService vdbService = new FakeVDBService();
        vdbService.addBinding(vdbName, vdbVersion, "BQT1", "mmuuid:blah", "BQT"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ 
        vdbService.addBinding(vdbName, vdbVersion, "BQT2", "mmuuid:blah", "BQT"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ 
        vdbService.addBinding(vdbName, vdbVersion, "BQT3", "mmuuid:blah", "BQT"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ 
        vdbService.addModel(vdbName, vdbVersion, "BQT3", ModelInfo.PRIVATE, false); //$NON-NLS-1$
        env.bindService(DQPServiceNames.VDB_SERVICE, vdbService);

        core = new DQPCore() {
            public ApplicationEnvironment getEnvironment() {
                return env; 
            }
        };
        core.start(new Properties());
    }
    
    @Override
    protected void tearDown() throws Exception {
    	DQPWorkContext.setWorkContext(new DQPWorkContext());
    	core.stop();
    }

    public RequestMessage exampleRequestMessage(String sql) {
        RequestMessage msg = new RequestMessage(sql);
        msg.setCallableStatement(false);
        msg.setCursorType(ResultSet.TYPE_SCROLL_INSENSITIVE);
        msg.setFetchSize(10);
        msg.setPartialResults(false);
        msg.setExecutionId(100);
        return msg;
    }

    public void testRequest1() throws Exception {
    	helpExecute("SELECT IntKey FROM BQT1.SmallA", "a"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testUser1() throws Exception {
        String sql = "SELECT IntKey FROM BQT1.SmallA WHERE user() = 'logon'"; //$NON-NLS-1$
        String userName = "logon"; //$NON-NLS-1$
        helpExecute(sql, userName);
    }

    public void testUser2() throws Exception {
        String sql = "SELECT IntKey FROM BQT1.SmallA WHERE user() LIKE 'logon'"; //$NON-NLS-1$
        String userName = "logon"; //$NON-NLS-1$
        helpExecute(sql, userName);
    }

    public void testUser3() throws Exception {
        String sql = "SELECT IntKey FROM BQT1.SmallA WHERE user() IN ('logon3') AND StringKey LIKE '1'"; //$NON-NLS-1$
        String userName = "logon3"; //$NON-NLS-1$
        helpExecute(sql, userName);
    }

    public void testUser4() throws Exception {
        String sql = "SELECT IntKey FROM BQT1.SmallA WHERE 'logon4' = user() AND StringKey = '1'"; //$NON-NLS-1$
        String userName = "logon4"; //$NON-NLS-1$
        helpExecute(sql, userName);
    }

    public void testUser5() throws Exception {
        String sql = "SELECT IntKey FROM BQT1.SmallA WHERE user() IS NULL "; //$NON-NLS-1$
        String userName = "logon"; //$NON-NLS-1$
        helpExecute(sql, userName);
    }

    public void testUser6() throws Exception {
        String sql = "SELECT IntKey FROM BQT1.SmallA WHERE user() = 'logon33' "; //$NON-NLS-1$
        String userName = "logon"; //$NON-NLS-1$
        helpExecute(sql, userName);
    }

    public void testUser7() throws Exception {
        String sql = "UPDATE BQT1.SmallA SET IntKey = 2 WHERE user() = 'logon' AND StringKey = '1' "; //$NON-NLS-1$
        String userName = "logon"; //$NON-NLS-1$
        helpExecute(sql, userName);
    }

    public void testUser8() throws Exception {
        String sql = "SELECT user(), StringKey FROM BQT1.SmallA WHERE IntKey = 1 "; //$NON-NLS-1$
        String userName = "logon"; //$NON-NLS-1$
        helpExecute(sql, userName);
    }

    public void testUser9() throws Exception {
        String sql = "SELECT IntKey FROM BQT1.SmallA WHERE user() = StringKey AND StringKey = '1' "; //$NON-NLS-1$
        String userName = "1"; //$NON-NLS-1$
        helpExecute(sql, userName);
    }

    public void testEnvSessionId() throws Exception {
        String sql = "SELECT env('sessionid') as SessionID"; //$NON-NLS-1$
        String userName = "1"; //$NON-NLS-1$
        ResultsMessage rm = helpExecute(sql, userName);
        assertEquals("1", rm.getResults()[0].get(0)); //$NON-NLS-1$
    }
    
    public void testEnvSessionIdMixedCase() throws Exception {
        String sql = "SELECT env('sEsSIonId') as SessionID"; //$NON-NLS-1$
        String userName = "1"; //$NON-NLS-1$
        ResultsMessage rm = helpExecute(sql, userName);
        assertEquals("1", rm.getResults()[0].get(0)); //$NON-NLS-1$
    }
    
    public void testTxnAutoWrap() throws Exception {
    	String sql = "SELECT * FROM BQT1.SmallA"; //$NON-NLS-1$
    	helpExecute(sql, "a", 1, true); //$NON-NLS-1$
    }
    
    public void testPlanOnly() throws Exception {
    	String sql = "SELECT * FROM BQT1.SmallA option planonly"; //$NON-NLS-1$
    	helpExecute(sql,"a"); //$NON-NLS-1$
    }
    
    /**
     * Tests whether an exception result is sent when an exception occurs
     * @since 4.3
     */
    public void testPlanningException() throws Exception {
        String sql = "SELECT IntKey FROM BQT1.BadIdea "; //$NON-NLS-1$
        
        RequestMessage reqMsg = exampleRequestMessage(sql);

        Future<ResultsMessage> message = core.executeRequest(reqMsg.getExecutionId(), reqMsg);
        try {
        	message.get(5000, TimeUnit.MILLISECONDS);
        } catch (ExecutionException e) {
        	assertTrue(e.getCause() instanceof QueryResolverException);
        }
    }
    
    public void testCapabilitesCache() {
    	ConnectorCapabilitiesCache cache = new ConnectorCapabilitiesCache();
    	DQPWorkContext workContext = new DQPWorkContext();
    	workContext.setVdbName("foo"); //$NON-NLS-1$
    	workContext.setVdbVersion("1"); //$NON-NLS-1$
    	Map<String, SourceCapabilities> vdbCapabilites = cache.getVDBConnectorCapabilities(workContext);
    	assertNull(vdbCapabilites.get("model1")); //$NON-NLS-1$
    	vdbCapabilites.put("model1", new BasicSourceCapabilities()); //$NON-NLS-1$
    	vdbCapabilites = cache.getVDBConnectorCapabilities(workContext);
    	assertNotNull(vdbCapabilites.get("model1")); //$NON-NLS-1$
    	workContext.setVdbName("bar"); //$NON-NLS-1$
    	vdbCapabilites = cache.getVDBConnectorCapabilities(workContext);
    	assertNull(vdbCapabilites.get("model1")); //$NON-NLS-1$
    }
    
	public void testLookupVisibility() throws Exception {
		helpTestVisibilityFails("select lookup('bqt3.smalla', 'intkey', 'stringkey', '?')"); //$NON-NLS-1$
	}
	
	public void testCancel() throws Exception {
		assertFalse(this.core.cancelRequest(new RequestID(1)));
	}
    
	public void helpTestVisibilityFails(String sql) throws Exception {
        RequestMessage reqMsg = exampleRequestMessage(sql); 
        reqMsg.setTxnAutoWrapMode(ExecutionProperties.AUTO_WRAP_OFF);
        Future<ResultsMessage> message = core.executeRequest(reqMsg.getExecutionId(), reqMsg);
        ResultsMessage results = message.get(50000, TimeUnit.MILLISECONDS);
        assertEquals("[QueryValidatorException]Group does not exist: BQT3.SmallA", results.getException().toString()); //$NON-NLS-1$
	}

	public void testXQueryVisibility() throws Exception {
        String xquery = "<Items>\r\n" + //$NON-NLS-1$
				"{\r\n" + //$NON-NLS-1$
				"for $x in doc(\"select * from bqt3.smalla\")//Item\r\n" + //$NON-NLS-1$
				"return  <Item>{$x/intkey/text()}</Item>\r\n" + //$NON-NLS-1$
				"}\r\n" + //$NON-NLS-1$
				"</Items>\r\n"; //$NON-NLS-1$
		
		helpTestVisibilityFails(xquery);
	}
    

    ///////////////////////////Helper method///////////////////////////////////
    private ResultsMessage helpExecute(String sql, String userName) throws Exception {
    	return helpExecute(sql, userName, 1, false);
    }

    private ResultsMessage helpExecute(String sql, String userName, int sessionid, boolean txnAutoWrap) throws Exception {
        RequestMessage reqMsg = exampleRequestMessage(sql);
        DQPWorkContext.getWorkContext().setSessionToken(new SessionToken(new MetaMatrixSessionID(sessionid), userName));
        if (txnAutoWrap) {
        	reqMsg.setTxnAutoWrapMode(ExecutionProperties.AUTO_WRAP_ON);
        }

        Future<ResultsMessage> message = core.executeRequest(reqMsg.getExecutionId(), reqMsg);
        ResultsMessage results = message.get(50000, TimeUnit.MILLISECONDS);
        assertNull(results.getException());
        return results;
    }
}
