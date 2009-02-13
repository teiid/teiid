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



package com.metamatrix.connector.xml.jms;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Iterator;
import java.util.List;

import javax.jms.JMSException;
import javax.jms.Session;

import com.metamatrix.connector.api.ConnectorEnvironment;
import com.metamatrix.connector.api.ConnectorLogger;
import com.metamatrix.connector.api.DataNotAvailableException;
import com.metamatrix.connector.api.ExecutionContext;
import com.metamatrix.connector.api.ResultSetExecution;
import com.metamatrix.connector.basic.BasicExecution;
import com.metamatrix.connector.exception.ConnectorException;
import com.metamatrix.connector.language.IQuery;
import com.metamatrix.connector.metadata.runtime.RuntimeMetadata;
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

public class JMSExecution extends BasicExecution implements ResultSetExecution, XMLExecution {

	JMSConnection connection; 
	RuntimeMetadata metadata;
	ExecutionContext exeCtx;
	ConnectorEnvironment connectorEnv;
	Session session;
	ConnectorLogger logger;
	List requestExecutors;
	private int requestNumber = 0;
	
	//Timout variables;
	long executeStartTime;
	long receiveTimeoutDuration;
	private ExecutionInfo executionInfo;
	private IQuery query;
    private BaseBatchProducer batchProducer;
	
	public JMSExecution(IQuery query, JMSConnection connection, RuntimeMetadata metadata, ExecutionContext exeCtx, 
			ConnectorEnvironment connectorEnv, ConnectorLogger logger) throws ConnectorException {
		super();
		this.connection = connection;
		this.metadata = metadata;
		this.exeCtx = exeCtx;
		this.connectorEnv = connectorEnv;
		this.logger = logger;
		this.receiveTimeoutDuration = ((JMSConnectorState)connection.getState()).getReceiveTimeout();
		this.query = query;
		try {
			session = connection.getJMSSession();
		} catch (JMSException e) {
			throw new ConnectorException(e);
		}
	}

	// Start AsynchQueryExecution implementation
	@Override
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

	@Override
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

    /**
     * Analyzes the Query and breaks it into multipl requests if needed.  Creates a 
     * JMSRequestExecutor for each request that creates and sends the request if
     * needed.  RequestExecutors are save in a List and accesses in next
     * Batch()
     */
	public void execute() throws ConnectorException {
		setExecuteStartTime(Calendar.getInstance().getTimeInMillis());
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
	
	@Override
	public List next() throws ConnectorException, DataNotAvailableException {
		while (true) {
			if (this.batchProducer == null) {
				Iterator executors = requestExecutors.iterator();
				while(this.batchProducer == null && executors.hasNext()){
					JMSRequestExecutor executor = (JMSRequestExecutor) executors.next();
					if(executor.hasResponse()) {
						executors.remove();
						Response response = executor.getXMLResponse(0);
		                if(connection.getState() instanceof SOAPConnectorState) {
		                	SOAPDocBuilder.removeEnvelope((SOAPConnectorState)connection.getState(), response);
		                }
						IDocumentCache cache = connection.getConnector().getStatementCache();
		        		BaseResultsProducer resProducer = new BaseResultsProducer(cache, logger);
		                List responseResultList = resProducer.getResult(executionInfo, response);
		                this.batchProducer = new BaseBatchProducer(responseResultList, executionInfo, exeCtx, connectorEnv);
					}
				}
			}
			
			if (this.batchProducer != null) {
				List row = batchProducer.createRow();
				if (row != null) {
					return row;
				}
				this.batchProducer = null;
			} else if( timedOut() ) {
	        	logger.logInfo("Timed out");
	        	return null;
			} else {
				throw new DataNotAvailableException(100);
			}
		}
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
