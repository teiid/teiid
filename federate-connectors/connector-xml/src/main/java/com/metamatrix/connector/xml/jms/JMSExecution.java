/*
 * JBoss, Home of Professional Open Source.
 * Copyright (C) 2008 Red Hat, Inc.
 * Copyright (C) 2000-2007 MetaMatrix, Inc.
 * Licensed to Red Hat, Inc. under one or more contributor 
 * license agreements.  See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
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



package com.metamatrix.connector.xml.jms;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Iterator;
import java.util.List;

import javax.jms.JMSException;
import javax.jms.Session;

import com.metamatrix.connector.xml.SOAPConnectorState;
import com.metamatrix.connector.xml.XMLConnection;
import com.metamatrix.connector.xml.XMLExecution;
import com.metamatrix.connector.xml.base.BaseBatchProducer;
import com.metamatrix.connector.xml.base.BaseResultsProducer;
import com.metamatrix.connector.xml.base.CriteriaDesc;
import com.metamatrix.connector.xml.base.ExecutionInfo;
import com.metamatrix.connector.xml.base.QueryAnalyzer;
import com.metamatrix.connector.xml.base.Response;
import com.metamatrix.connector.xml.cache.IDocumentCache;
import com.metamatrix.connector.xml.soap.SOAPDocBuilder;
import com.metamatrix.data.api.AsynchQueryExecution;
import com.metamatrix.data.api.Batch;
import com.metamatrix.data.api.ConnectorEnvironment;
import com.metamatrix.data.api.ConnectorLogger;
import com.metamatrix.data.api.ExecutionContext;
import com.metamatrix.data.basic.BasicBatch;
import com.metamatrix.data.exception.ConnectorException;
import com.metamatrix.data.language.IQuery;
import com.metamatrix.data.metadata.runtime.RuntimeMetadata;

public class JMSExecution implements AsynchQueryExecution, XMLExecution {

	JMSConnection connection; 
	RuntimeMetadata metadata;
	ExecutionContext exeCtx;
	ConnectorEnvironment connectorEnv;
	Session session;
	ConnectorLogger logger;
	List requestExecutors;
	private int requestNumber = 0;
	List allResultsList;
	private int m_maxBatch;
	
	//Timout variables;
	long executeStartTime;
	long receiveTimeoutDuration;
	private ExecutionInfo executionInfo;
	
	public JMSExecution(JMSConnection connection, RuntimeMetadata metadata, ExecutionContext exeCtx, 
			ConnectorEnvironment connectorEnv, ConnectorLogger logger) throws ConnectorException {
		super();
		this.connection = connection;
		this.metadata = metadata;
		this.exeCtx = exeCtx;
		this.connectorEnv = connectorEnv;
		this.logger = logger;
		this.receiveTimeoutDuration = ((JMSConnectorState)connection.getState()).getReceiveTimeout();
		allResultsList = new ArrayList();
		try {
			session = connection.getJMSSession();
		} catch (JMSException e) {
			throw new ConnectorException(e);
		}
	}

	// Start AsynchQueryExecution implementation
	public void close() throws ConnectorException {
		try {
			if(session.getTransacted()) {
				session.commit();
			}
			session.close();
			connection.stop();
		} catch (Exception e) {
			logger.logError(e.getMessage());
		}
	}

	public void cancel() throws ConnectorException {
		try {
            if(session.getTransacted()) {
                session.commit();
            }
			session.close();
		} catch (JMSException e) {
			logger.logError(e.getMessage());
		}
	}

	public long getPollInterval() {
		long retval;
		if(timedOut()) {
			retval = 0;
		} else {
			// TODO : make a real implemetation;
			retval = 100; 
		}
		return retval;
	}
	
    /**
     * Analyzes the Query and breaks it into multipl requests if needed.  Creates a 
     * JMSRequestExecutor for each request that creates and sends the request if
     * needed.  RequestExecutors are save in a List and accesses in next
     * Batch()
     */
	public void executeAsynch(IQuery query, int maxBatchSize) throws ConnectorException {
		setExecuteStartTime(Calendar.getInstance().getTimeInMillis());
		m_maxBatch = maxBatchSize;
		QueryAnalyzer analyzer = new QueryAnalyzer(query, metadata, connection.getQueryPreprocessor(),
				logger, exeCtx, connectorEnv);
        executionInfo = analyzer.getExecutionInfo();
		List requestPerms = analyzer.getRequestPerms();
        
        requestExecutors = new ArrayList(requestPerms.size());
        Iterator requests = requestPerms.iterator();
        while(requests.hasNext()) {
        	List criteriaListForOnePerm = Arrays.asList((CriteriaDesc[])requests.next());
            executionInfo.setParameters(criteriaListForOnePerm);
            requestExecutors.add(connection.getState().makeExecutor(this));
            incrementRequestNumber();
        }
	}

    
	public Batch nextBatch() throws ConnectorException {
		
		List unfinishedExecutors = new ArrayList();
		Iterator executors = requestExecutors.iterator();
		boolean haveResults = false;
        while(executors.hasNext()){
			JMSRequestExecutor executor = (JMSRequestExecutor) executors.next();
			int responseCount = executor.getDocumentCount();
			for(int i = 0; i < responseCount; i++){
				if(executor.hasResponse()) {
					Response response = executor.getXMLResponse(0);
	                if(connection.getState() instanceof SOAPConnectorState) {
	                	SOAPDocBuilder.removeEnvelope((SOAPConnectorState)connection.getState(), response);
	                }
					IDocumentCache cache = connection.getConnector().getStatementCache();
	        		BaseResultsProducer resProducer = new BaseResultsProducer(cache, logger);
	                List responseResultList = resProducer.getResult(executionInfo, response);
                    allResultsList = BaseResultsProducer.combineResults(responseResultList, allResultsList);
                    haveResults = true;
				} else {
					unfinishedExecutors.add(executor);
				}

			}
		}
		requestExecutors = unfinishedExecutors;
        
        Batch batch = new BasicBatch();
		if(haveResults) {
            batch = BaseBatchProducer.createBatch(allResultsList, 0,
                    m_maxBatch, executionInfo, exeCtx, connectorEnv);
            allResultsList.clear();
			if(requestExecutors.isEmpty()) {
                batch.setLast();
            }
		} else if( timedOut() ) {
        	logger.logInfo("Timed out");
			batch.setLast();
		}
		return batch;
	}
	// End AsynchQueryExecution implementation
	
	// start timout implementation
	private void setExecuteStartTime(long now) {
		this.executeStartTime = now;
	}
	
	private boolean timedOut() {
		return Calendar.getInstance().getTimeInMillis() - executeStartTime >= receiveTimeoutDuration;
	}
	// end timout implementation

	//Needed by the request executor to produce a cache key;
	public int getRequestNumber() {
		return requestNumber;
	}
	
	public void incrementRequestNumber() {
		++requestNumber;
	}

	public ExecutionInfo getInfo() {
		return executionInfo;
	}

	public XMLConnection getConnection() {
		return connection;
	}

	public IDocumentCache getCache() {
		return connection.getConnector().getCache();
	}

	public ExecutionContext getExeContext() {
		return exeCtx;
	}
}
