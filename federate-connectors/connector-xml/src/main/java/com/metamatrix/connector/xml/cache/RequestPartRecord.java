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
 
package com.metamatrix.connector.xml.cache;

import java.util.HashMap;
import java.util.Map;

import com.metamatrix.data.api.ConnectorLogger;

public class RequestPartRecord implements Record {
	
	/**
	 * The parent request of this RequestPart.
	 */
	Record parent;
	
	/**
	 * The ID of this RequestPart
	 */
	String partID;
		
	Map executionRecords;

	public RequestPartRecord(Record parent, String partID, String executionID, String sourceRequestID, String cacheKey, ConnectorLogger logger) {
		executionRecords = new HashMap();
		this.partID = partID;
        this.parent = parent;
		addExecutionRecord(executionID, sourceRequestID, cacheKey, logger);
	}

	public void addExecutionRecord(String executionID, String sourceRequestID, String cacheKey, ConnectorLogger logger) {
		ExecutionRecord execution = (ExecutionRecord) executionRecords.get(executionID);
		if(null == execution) {
			logger.logTrace("Creating new ExecutionRecord for executionID " + executionID);
			execution = new ExecutionRecord(this, executionID, sourceRequestID, cacheKey);
			executionRecords.put(executionID, execution);
		} else {
			logger.logTrace("Adding CacheRecord for executionID " + executionID);
			execution.addCacheRecord(sourceRequestID, cacheKey);
		}

	}

	public IDocumentCache getCache() {
		return parent.getCache();
	}

	public String getID() {
		return parent.getID() + partID;
	}

	public void deleteExecutionRecords(String executionID, ConnectorLogger logger) {
		ExecutionRecord execution = (ExecutionRecord) executionRecords.get(executionID);
		if(null != execution) {
			logger.logTrace("Deleting cache items for ExecutionRecord " + executionID);
			execution.deleteCacheItems(logger);
		} else {
			
		}
	}
}
