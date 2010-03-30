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

package org.teiid.connector.xml.soap;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.teiid.connector.api.ConnectorException;
import org.teiid.connector.api.ExecutionContext;
import org.teiid.connector.language.Select;
import org.teiid.connector.metadata.runtime.RuntimeMetadata;

import com.metamatrix.connector.xml.Document;
import com.metamatrix.connector.xml.ResultProducer;
import com.metamatrix.connector.xml.base.CriteriaDesc;
import com.metamatrix.connector.xml.base.ExecutionInfo;
import com.metamatrix.connector.xml.base.QueryAnalyzer;
import com.metamatrix.connector.xml.streaming.BaseStreamingExecution;

public class SOAPResultSetExecution extends BaseStreamingExecution implements ResultProducer{

	private ExecutionInfo executionInfo;
    private ArrayList<SOAPRequest> requests = new ArrayList<SOAPRequest>();
	
	public SOAPResultSetExecution(Select query, RuntimeMetadata metadata, ExecutionContext exeContext, SoapManagedConnectionFactory config) {
		super(query, config, metadata, exeContext);
	}

	@Override
	public ResultProducer getStreamProducer() throws ConnectorException {
		return this;
	}

	@Override
	public void execute() throws ConnectorException {
		QueryAnalyzer analyzer = new QueryAnalyzer(query);
        
		this.executionInfo = analyzer.getExecutionInfo();		
        List<CriteriaDesc[]> requestPerms = analyzer.getRequestPerms();

        int requestNumber = 0;
		for (CriteriaDesc[] criteria : requestPerms) {
        	List<CriteriaDesc> criteriaList = Arrays.asList(criteria);
        	SOAPRequest request = new SOAPRequest((SoapManagedConnectionFactory)this.config, this.context, this.executionInfo, criteriaList, requestNumber++);
        	this.requests.add(request);
        }
	}

	@Override
	public ExecutionInfo getExecutionInfo() {
		return this.executionInfo;
	}

	@Override
	public void close() throws ConnectorException {
		for (SOAPRequest request : requests) {
			request.release();
		}
	}

	@Override
	public Iterator<Document> getXMLDocuments() throws ConnectorException {
		ArrayList<com.metamatrix.connector.xml.Document> result = new ArrayList<com.metamatrix.connector.xml.Document>();
		
        for(SOAPRequest request : this.requests) {
        	com.metamatrix.connector.xml.Document doc = request.getDocumentStream();
        	result.add(doc);
        }
        return result.iterator();
    }
}
