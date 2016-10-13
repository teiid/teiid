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

import org.junit.Test;
import org.mockito.Mockito;
import org.teiid.cache.DefaultCacheFactory;
import org.teiid.client.RequestMessage;
import org.teiid.client.RequestMessage.StatementType;
import org.teiid.common.buffer.BufferManager;
import org.teiid.common.buffer.BufferManagerFactory;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidProcessingException;
import org.teiid.dqp.internal.datamgr.ConnectorManagerRepository;
import org.teiid.dqp.internal.datamgr.FakeTransactionService;
import org.teiid.dqp.internal.process.AuthorizationValidator.CommandType;
import org.teiid.dqp.service.AutoGenDataService;
import org.teiid.query.metadata.QueryMetadataInterface;
import org.teiid.query.parser.QueryParser;
import org.teiid.query.processor.FakeDataManager;
import org.teiid.query.resolver.QueryResolver;
import org.teiid.query.sql.lang.Command;
import org.teiid.query.tempdata.TempTableStore;
import org.teiid.query.tempdata.TempTableStore.TransactionMode;
import org.teiid.query.unittest.RealMetadataFactory;

@SuppressWarnings("nls")
public class TestRequest {

    private static final TempTableStore TEMP_TABLE_STORE = new TempTableStore("1", TransactionMode.ISOLATE_WRITES); //$NON-NLS-1$
	private final static String QUERY = "SELECT * FROM pm1.g1";  //$NON-NLS-1$
    
    /**
     * Test Request.validateEntitlement().  
     * Make sure that this can be called both before and after metadata is initialized. 
     * See defect 17209.
     * @throws Exception
     * @since 4.2
     */
    @Test public void testValidateEntitlement() throws Exception {
        QueryMetadataInterface metadata = RealMetadataFactory.example1Cached();
        
        Request request = new Request();
        Command command = QueryParser.getQueryParser().parseCommand(QUERY);
        QueryResolver.resolveCommand(command, metadata);
        
        RequestMessage message = new RequestMessage();
        DQPWorkContext workContext = RealMetadataFactory.buildWorkContext(metadata, RealMetadataFactory.example1VDB());
        
		request.initialize(message, BufferManagerFactory.getStandaloneBufferManager(), null,
				new FakeTransactionService(), TEMP_TABLE_STORE, workContext, null, null); 
        request.initMetadata();
        DefaultAuthorizationValidator drav = new DefaultAuthorizationValidator();
        DataRolePolicyDecider drpd = new DataRolePolicyDecider();
        drpd.setAllowCreateTemporaryTablesByDefault(true);
        drpd.setAllowFunctionCallsByDefault(true);
        drav.setPolicyDecider(drpd);
        request.setAuthorizationValidator(drav);
        request.validateAccess(new String[] {QUERY}, command, CommandType.USER);
    }
    
    /**
     * Test Request.processRequest().
     * Test processing the same query twice, and make sure that doesn't cause problems.  
     * See defect 17209.
     * @throws Exception
     * @since 4.2
     */
    @Test public void testProcessRequest() throws Exception {
        QueryMetadataInterface metadata = RealMetadataFactory.example1Cached();
        
        //Try before plan is cached.
        //If this doesn't throw an exception, assume it was successful.
        RequestMessage message = new RequestMessage(QUERY);
        DQPWorkContext workContext = RealMetadataFactory.buildWorkContext(metadata, RealMetadataFactory.example1VDB());

        helpProcessMessage(message, null, workContext);
        
        //Try again, now that plan is already cached.
        //If this doesn't throw an exception, assume it was successful.        
        message = new RequestMessage(QUERY);
        helpProcessMessage(message, null, workContext);
    }
    
    @Test public void testCommandContext() throws Exception {
        QueryMetadataInterface metadata = RealMetadataFactory.example1Cached();
        
        //Try before plan is cached.
        //If this doesn't throw an exception, assume it was successful.
        RequestMessage message = new RequestMessage(QUERY);
        DQPWorkContext workContext = RealMetadataFactory.buildWorkContext(metadata, RealMetadataFactory.example1VDB());
        
        Request request = helpProcessMessage(message, null, workContext);
        assertEquals("1", request.context.getConnectionId()); //$NON-NLS-1$
        assertNotNull(request.context.getTransactionContext());
    }

    private Request helpProcessMessage(RequestMessage message, SessionAwareCache<PreparedPlan> cache, DQPWorkContext workContext) throws TeiidComponentException,
                                                           TeiidProcessingException {
        Request request = null;
        if (cache != null) {
        	request = new PreparedStatementRequest(cache);
        } else {
        	request = new Request();
        }
        ConnectorManagerRepository repo = Mockito.mock(ConnectorManagerRepository.class);
        workContext.getVDB().addAttchment(ConnectorManagerRepository.class, repo);
        Mockito.stub(repo.getConnectorManager(Mockito.anyString())).toReturn(new AutoGenDataService());
        
        request.initialize(message, Mockito.mock(BufferManager.class),
				new FakeDataManager(), new FakeTransactionService(), TEMP_TABLE_STORE, workContext, null, null);
        DefaultAuthorizationValidator drav = new DefaultAuthorizationValidator();
        request.setAuthorizationValidator(drav);
        request.processRequest();
        return request;
    }
    
    /**
     * Test PreparedStatementRequest.processRequest().  
     * Test processing the same query twice, and make sure that doesn't cause problems.  
     * @throws Exception
     * @since 4.2
     */
    @Test public void testProcessRequestPreparedStatement() throws Exception {
        QueryMetadataInterface metadata = RealMetadataFactory.example1Cached();
        SessionAwareCache<PreparedPlan> cache = new SessionAwareCache<PreparedPlan>("preparedplan", DefaultCacheFactory.INSTANCE, SessionAwareCache.Type.PREPAREDPLAN, 0);
        

        //Try before plan is cached.
        //If this doesn't throw an exception, assume it was successful.
        RequestMessage message = new RequestMessage(QUERY);
        DQPWorkContext workContext = RealMetadataFactory.buildWorkContext(metadata, RealMetadataFactory.example1VDB());
        
        message.setStatementType(StatementType.PREPARED);
        message.setParameterValues(new ArrayList<Object>());
        
        helpProcessMessage(message, cache, workContext);
        
        //Try again, now that plan is already cached.
        //If this doesn't throw an exception, assume it was successful.
        message = new RequestMessage(QUERY);
        message.setStatementType(StatementType.PREPARED);
        message.setParameterValues(new ArrayList<Object>());

        helpProcessMessage(message, cache, workContext);
    }
}
