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


package com.metamatrix.connector.xml.soap;


import java.util.Arrays;
import java.util.List;

import org.teiid.connector.api.ConnectorException;

import com.metamatrix.connector.xml.SOAPConnectorState;
import com.metamatrix.connector.xml.XMLExecution;
import com.metamatrix.connector.xml.base.CriteriaDesc;
import com.metamatrix.connector.xml.base.ExecutionInfo;
import com.metamatrix.connector.xml.base.QueryAnalyzer;
import com.metamatrix.connector.xml.http.HTTPConnectorState;
import com.metamatrix.connector.xml.http.HTTPExecutor;

public class SOAPExecutor extends HTTPExecutor {
	
	public SOAPExecutor(SOAPConnectorState state, XMLExecution execution, ExecutionInfo exeInfo, QueryAnalyzer analyzer) throws ConnectorException {
        super((HTTPConnectorState) state, execution, exeInfo, analyzer);
    }
    
	@Override
	protected void createRequests(XMLExecution execution,
			ExecutionInfo exeInfo, List<CriteriaDesc[]> requestPerms)
			throws ConnectorException {
		int requestNumber = 0;
		for (CriteriaDesc[] criteria : requestPerms) {
        	List<CriteriaDesc> criteriaList = Arrays.asList(criteria);
        	SOAPRequest request = new SOAPRequest((SOAPConnectorState) state, execution, exeInfo, criteriaList, requestNumber ++);
        	requests.add(request);
        }
	}
}
