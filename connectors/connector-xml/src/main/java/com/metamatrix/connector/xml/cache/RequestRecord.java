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



package com.metamatrix.connector.xml.cache;

import java.util.HashMap;
import java.util.Map;

import com.metamatrix.connector.api.ConnectorLogger;
import com.metamatrix.connector.exception.ConnectorException;
import com.metamatrix.connector.xml.CachingConnector;

/**
 * 
 * The RequestRecord contains references to each of the request parts sent to
 * the Connector for a single request. When the request is closed these records
 * are used to clean items out of the cache if performace caching is
 * deactivated.
 * 
 */
public class RequestRecord  implements Record {

	/**
	 * The Connector
	 */
	CachingConnector connector;

	/**
	 * The RequestIdentifier from the Connector API.
	 */
	String requestID;

	/**
	 * The child RequestPartRecords of this request.
	 */
	Map requestParts;

	public RequestRecord(String requestID, CachingConnector connector) throws ConnectorException
   {
      if (null == requestID) {
    	  throw new ConnectorException("RequestRecord: RequestIDs cannot be null");
      }
       if(null == connector) {
    	   throw new ConnectorException("RequestRecord: The CachingConnector parameter cannot be null");
       }
	  this.requestID = requestID;
	  this.connector = connector;
      requestParts = new HashMap();
   }

	public void addRequestPart(String partID, String executionID, String sourceRequestID,
			String cacheKey, ConnectorLogger logger) {
		RequestPartRecord partRecord = (RequestPartRecord) requestParts
				.get(partID);
		if (null == partRecord) {
			logger
					.logTrace("Creating new RequestPartRecord for Request Part Identifier "
							+ partID
							+ " and Invocation Number "
							+ sourceRequestID);
			partRecord = new RequestPartRecord(this, partID, executionID, sourceRequestID,
					cacheKey, logger);
			requestParts.put(partID, partRecord);
		} else {
			logger
					.logTrace("Adding new Source Request ID to existing RequestPartRecord: Invocation Number "
							+ sourceRequestID);
			partRecord.addExecutionRecord(executionID, sourceRequestID, cacheKey, logger);
		}

	}

	public void deleteRequestPart(String partID, String executionID, ConnectorLogger logger) {
		RequestPartRecord partRecord = (RequestPartRecord) requestParts
				.get(partID);
		if (null != partRecord) {
			logger.logTrace("Deleting RequestPartRecord for Part Identifier "
					+ partID);
			partRecord.deleteExecutionRecords(executionID, logger);
			requestParts.remove(partID);
		}
	}

	public boolean isEmpty() {
		return requestParts.isEmpty();
	}

	public IDocumentCache getCache() {
		return connector.getCache();
	}

	public String getID() {
		return requestID;
	}

}
