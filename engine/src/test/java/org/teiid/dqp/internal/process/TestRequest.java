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

import java.util.ArrayList;
import java.util.Collections;

import junit.framework.TestCase;

import org.mockito.Mockito;
import org.teiid.dqp.internal.datamgr.impl.ConnectorManagerRepository;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.api.exception.query.QueryParserException;
import com.metamatrix.api.exception.query.QueryPlannerException;
import com.metamatrix.api.exception.query.QueryResolverException;
import com.metamatrix.api.exception.query.QueryValidatorException;
import com.metamatrix.common.buffer.BufferManager;
import com.metamatrix.dqp.message.RequestMessage;
import com.metamatrix.dqp.message.RequestMessage.StatementType;
import com.metamatrix.dqp.service.AutoGenDataService;
import com.metamatrix.query.analysis.AnalysisRecord;
import com.metamatrix.query.metadata.QueryMetadataInterface;
import com.metamatrix.query.parser.QueryParser;
import com.metamatrix.query.processor.FakeDataManager;
import com.metamatrix.query.resolver.QueryResolver;
import com.metamatrix.query.sql.lang.Command;
import com.metamatrix.query.unittest.FakeMetadataFactory;
import com.metamatrix.query.util.ContextProperties;


/** 
 * @since 4.2
 */
public class TestRequest extends TestCase {

    private final static String QUERY = "SELECT * FROM pm1.g1";  //$NON-NLS-1$
    
    /**
     * Constructor for TestRequest.
     * @param name
     */
    public TestRequest(String name) {
        super(name);
    }
    
    /**
     * Test Request.validateEntitlement().  
     * Make sure that this can be called both before and after metadata is initialized. 
     * See defect 17209.
     * @throws Exception
     * @since 4.2
     */
    public void testValidateEntitlement() throws Exception {
        QueryMetadataInterface metadata = FakeMetadataFactory.example1Cached();
        
        
        Request request = new Request();
        Command command = QueryParser.getQueryParser().parseCommand(QUERY);
        QueryResolver.resolveCommand(command, Collections.EMPTY_MAP, metadata, AnalysisRecord.createNonRecordingRecord());
        
        ConnectorManagerRepository repo = Mockito.mock(ConnectorManagerRepository.class);
        Mockito.stub(repo.getConnectorManager(Mockito.anyString())).toReturn(new AutoGenDataService());
        
        
        RequestMessage message = new RequestMessage();
        DQPWorkContext workContext = FakeMetadataFactory.buildWorkContext(metadata, FakeMetadataFactory.example1VDB());
        
        request.initialize(message, null, null,null,false, null, workContext, 101024, repo, false);
        request.initMetadata();
        request.validateAccess(command);
    }
    
    
    /**
     * Test Request.processRequest().
     * Test processing the same query twice, and make sure that doesn't cause problems.  
     * See defect 17209.
     * @throws Exception
     * @since 4.2
     */
    public void testProcessRequest() throws Exception {
        QueryMetadataInterface metadata = FakeMetadataFactory.example1Cached();
        
        //Try before plan is cached.
        //If this doesn't throw an exception, assume it was successful.
        RequestMessage message = new RequestMessage(QUERY);
        DQPWorkContext workContext = FakeMetadataFactory.buildWorkContext(metadata, FakeMetadataFactory.example1VDB());

        helpProcessMessage(message, null, workContext);
        
        //Try again, now that plan is already cached.
        //If this doesn't throw an exception, assume it was successful.        
        message = new RequestMessage(QUERY);
        helpProcessMessage(message, null, workContext);
    }
    
    public void testCommandContext() throws Exception {
        QueryMetadataInterface metadata = FakeMetadataFactory.example1Cached();
        
        //Try before plan is cached.
        //If this doesn't throw an exception, assume it was successful.
        RequestMessage message = new RequestMessage(QUERY);
        DQPWorkContext workContext = FakeMetadataFactory.buildWorkContext(metadata, FakeMetadataFactory.example1VDB());
        
        Request request = helpProcessMessage(message, null, workContext);
        assertEquals("1", request.context.getEnvironmentProperties().get(ContextProperties.SESSION_ID)); //$NON-NLS-1$
    }

    private Request helpProcessMessage(RequestMessage message, SessionAwareCache<PreparedPlan> cache, DQPWorkContext workContext) throws QueryValidatorException,
                                                           QueryParserException,
                                                           QueryResolverException,
                                                           MetaMatrixComponentException,
                                                           QueryPlannerException {
        Request request = null;
        if (cache != null) {
        	request = new PreparedStatementRequest(cache);
        } else {
        	request = new Request();
        }
        
        ConnectorManagerRepository repo = Mockito.mock(ConnectorManagerRepository.class);
        Mockito.stub(repo.getConnectorManager(Mockito.anyString())).toReturn(new AutoGenDataService());
        
        request.initialize(message, Mockito.mock(BufferManager.class),
				new FakeDataManager(),  null, false, null, workContext,
				101024, repo, false);
        
        request.processRequest();
        return request;
    }
    
    

    
    /**
     * Test PreparedStatementRequest.processRequest().  
     * Test processing the same query twice, and make sure that doesn't cause problems.  
     * @throws Exception
     * @since 4.2
     */
    public void testProcessRequestPreparedStatement() throws Exception {
        QueryMetadataInterface metadata = FakeMetadataFactory.example1Cached();
        SessionAwareCache<PreparedPlan> cache = new SessionAwareCache<PreparedPlan>();
        

        //Try before plan is cached.
        //If this doesn't throw an exception, assume it was successful.
        RequestMessage message = new RequestMessage(QUERY);
        DQPWorkContext workContext = FakeMetadataFactory.buildWorkContext(metadata, FakeMetadataFactory.example1VDB());
        
        message.setStatementType(StatementType.PREPARED);
        message.setParameterValues(new ArrayList());
        
        helpProcessMessage(message, cache, workContext);
        
        //Try again, now that plan is already cached.
        //If this doesn't throw an exception, assume it was successful.
        message = new RequestMessage(QUERY);
        message.setStatementType(StatementType.PREPARED);
        message.setParameterValues(new ArrayList());

        helpProcessMessage(message, cache, workContext);
    }
}
