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


package com.metamatrix.connector.xml.base;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import com.metamatrix.connector.xml.DocumentProducer;
import com.metamatrix.connector.xml.XMLConnection;
import com.metamatrix.connector.xml.XMLConnectorState;
import com.metamatrix.connector.xml.XMLExecution;
import com.metamatrix.connector.xml.cache.IDocumentCache;
import com.metamatrix.data.api.Batch;
import com.metamatrix.data.api.ConnectorEnvironment;
import com.metamatrix.data.api.ConnectorLogger;
import com.metamatrix.data.api.ExecutionContext;
import com.metamatrix.data.api.SynchQueryExecution;
import com.metamatrix.data.basic.BasicBatch;
import com.metamatrix.data.exception.ConnectorException;
import com.metamatrix.data.language.IQuery;
import com.metamatrix.data.metadata.runtime.RuntimeMetadata;

public class XMLExecutionImpl implements SynchQueryExecution, XMLExecution {

    private XMLConnectionImpl m_conn;

    private ConnectorLogger m_logger;

    public ExecutionContext exeContext;

    private ConnectorEnvironment connectorEnv;

    private RuntimeMetadata m_metadata;

    private int m_maxBatch;
    
    private int resultSize = 0;

    private ExecutionInfo m_info;

    private List m_resultList;

	private int returnIndex;

    public XMLExecutionImpl(XMLConnectionImpl conn, RuntimeMetadata metadata,
            ExecutionContext exeContext, ConnectorEnvironment connectorEnv) {
        setConnection(conn);
        m_metadata = metadata;
        m_logger = m_conn.getState().getLogger();
        m_resultList = null;
        this.exeContext = exeContext;
        this.connectorEnv = connectorEnv;
        
        m_logger.logTrace("ConnectionIdentifier: " + exeContext.getConnectionIdentifier());
        m_logger.logTrace("ConnectorIdentifier: " + exeContext.getConnectorIdentifier());
        m_logger.logTrace("ExecutionCountIdentifier: " + exeContext.getExecutionCountIdentifier());
        m_logger.logTrace("PartIdentifier: " + exeContext.getPartIdentifier());
        m_logger.logTrace("RequestIdentifier: " + exeContext.getRequestIdentifier());
        exeContext.keepExecutionAlive(true);
    }

    /*
     * Beginning of SynchQueryExecution implementation.
     */

    public void execute(IQuery query, int maxBatchSize)
            throws ConnectorException {
        try {
        	XMLConnectorState state = m_conn.getState();
        	m_maxBatch = maxBatchSize;
            
            QueryAnalyzer analyzer = new QueryAnalyzer(query, m_metadata, state.getPreprocessor(), m_logger, exeContext, connectorEnv);
            m_info = analyzer.getExecutionInfo();
            List requestPerms = analyzer.getRequestPerms();
            
            //Initialize the result and execute each request. 
            m_resultList = new ArrayList();
            int invocationCount = 0;
            for (Iterator iter = requestPerms.iterator(); iter.hasNext(); ) {
                List criteriaListForOnePerm = Arrays.asList((CriteriaDesc[])iter.next());
                m_info.setParameters(criteriaListForOnePerm);
                DocumentProducer requestExecutor = (DocumentProducer) state.makeExecutor(this);
                Response response = requestExecutor.getXMLResponse(invocationCount);
                IDocumentCache cache = m_conn.getConnector().getStatementCache();
        		BaseResultsProducer resProducer = new BaseResultsProducer(cache, getLogger());
                List resultList = resProducer.getResult(m_info, response);
                // Who's idea was it to arrange the data by field and then by row, instead
                // of the other way round?
                m_resultList = BaseResultsProducer.combineResults(resultList, m_resultList);
                ++invocationCount;
            }
            if(!m_resultList.isEmpty()) {
                List columnResult = (List)m_resultList.get(0);
                if(!columnResult.isEmpty()) {
                    resultSize = columnResult.size();
                }
            }
        }
        catch (RuntimeException e) {
        	throw new ConnectorException(e);
        }
    }

    public Batch nextBatch() throws ConnectorException {
        try {
            Batch batch = new BasicBatch();
            if (m_resultList == null) {
                batch.setLast();
                return batch;
            }
            BaseBatchProducer batchProducer = new BaseBatchProducer();
            batch = batchProducer.createBatch(m_resultList, this.returnIndex, this.m_maxBatch,
                    this.m_info, this.exeContext, this.connectorEnv);
            batchProducer.getCurrentReturnIndex();
            returnIndex = batchProducer.getCurrentReturnIndex();
            testBatchForLast(returnIndex, resultSize, batch);
            return batch;
        }
        catch (RuntimeException e) {
            throw new ConnectorException(e);
        }
    }

	public void close() throws ConnectorException {
        try {
          m_conn.getConnector().deleteCacheItems(exeContext.getRequestIdentifier(),
            		exeContext.getPartIdentifier(),exeContext.getExecutionCountIdentifier());
          m_logger.logTrace("XMLExecution closed for ConnectionIdentifier " + 
            		exeContext.getConnectionIdentifier() + " and PartIdentifier " +
            		exeContext.getPartIdentifier());
        }
        catch (RuntimeException e) {
        	m_logger.logTrace("Exception while closing ExecutiontImpl: " + e.getMessage());
            throw new ConnectorException(e);
        }
    }

    public void cancel() throws ConnectorException {
        try {
            //no impl
        }
        catch (RuntimeException e) {
            throw new ConnectorException(e);
        }
    }
    
	/*
	 * End of SynchQueryExecution implementation.
	 */
    

	/*
	 * Beginning of Batch handling functions.
	 */
    
    /**
     * Flags the batch as complete if the current size of the result is as large
     * or larger than the expected size.
     * @param currentSize
     * @param totalSize
     * @param testBatch
     */
    private void testBatchForLast(int currentSize, int totalSize,
            Batch testBatch) {
        if (currentSize >= totalSize) {
            testBatch.setLast();
        }
    }
    
	/*
	 * End of Batch handling functions.
	 */
    
	/*
	 * Beginning of simple accessor functions.
	 */
    /**
     * Provides access to the XMLConnectionImpl to the Executors.
     * @return
     */
    public XMLConnection getConnection() {
        return m_conn;
    }

    /**
     * Accessor to allow derived implementations to override the XMLConnectionImpl.
     * @param conn
     */
    void setConnection(XMLConnectionImpl conn) {
        m_conn = conn;
    }

    /**
     * Provides access to the ExecutionInfo to the Executors.
     * @return
     */
    public ExecutionInfo getInfo() {
        return m_info;
    }

    /**
     * Provides access to the ConnectorLogger to the Executors.
     * @return
     */
    public ConnectorLogger getLogger()
    {
    	return m_logger;
    }

    /**
     * Provides access to the connector binding name to the Executors.
     * @return
     */    
    public String getSystemName() {
    	return connectorEnv.getConnectorName();
    }

	public IDocumentCache getCache() {
		return m_conn.getConnector().getCache();
	}

	public ExecutionContext getExeContext() {
		return exeContext;
	}
    
	/*
	 * End of simple accessor functions.
	 */
}
