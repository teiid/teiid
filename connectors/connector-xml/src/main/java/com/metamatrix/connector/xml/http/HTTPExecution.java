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


package com.metamatrix.connector.xml.http;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.teiid.connector.api.ConnectorEnvironment;
import org.teiid.connector.api.ConnectorException;
import org.teiid.connector.api.ExecutionContext;
import org.teiid.connector.language.IQuery;
import org.teiid.connector.metadata.runtime.RuntimeMetadata;

import com.metamatrix.connector.xml.ResultProducer;
import com.metamatrix.connector.xml.XMLConnectorState;
import com.metamatrix.connector.xml.base.CriteriaDesc;
import com.metamatrix.connector.xml.base.QueryAnalyzer;
import com.metamatrix.connector.xml.base.XMLConnectionImpl;
import com.metamatrix.connector.xml.streaming.BaseStreamingExecution;
import com.metamatrix.connector.xml.streaming.InvalidPathException;
import com.metamatrix.connector.xml.streaming.StreamingResultsProducer;
import com.metamatrix.connector.xml.streaming.XPathSplitter;

public class HTTPExecution extends BaseStreamingExecution {

    public HTTPExecution(IQuery query, XMLConnectionImpl conn, RuntimeMetadata metadata,
            ExecutionContext exeContext, ConnectorEnvironment connectorEnv) {
    	super(query, conn, metadata, exeContext, connectorEnv);
    }
    
    /**
     * HTTP execution can have multiple permutations from a single SQL
     * query, but each will have only one response.
     */
    public void execute()
            throws ConnectorException {

    	XMLConnectorState state = connection.getState();
        
        QueryAnalyzer analyzer = new QueryAnalyzer(query, metadata, state.getPreprocessor(), logger, exeContext, connEnv);
        exeInfo = analyzer.getExecutionInfo();
        List requestPerms = analyzer.getRequestPerms();
        
        for (Iterator iter = requestPerms.iterator(); iter.hasNext(); ) {
        	List<CriteriaDesc> criteriaList = Arrays.asList((CriteriaDesc[]) iter.next());
        	exeInfo.setParameters(criteriaList);
        
        	XPathSplitter splitter = new XPathSplitter();
        	try {
        		xpaths = splitter.split(exeInfo.getTableXPath());
        	} catch (InvalidPathException e) {
        		e.printStackTrace();
        	}
		
        	rowProducer = new StreamingResultsProducer(exeInfo, state);
        	resultProducers.add(getStreamProducer());
        }
    }
    
	@Override
	public ResultProducer getStreamProducer() throws ConnectorException {
		return new HTTPExecutor(connection.getState(), this, exeInfo);
	}
}
