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

import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import com.metamatrix.data.api.ConnectorLogger;

public class ExecutionRecord implements Record {

	Record parent;
	
	String executionID;
	
	/**
	 * The CachedObjectRecords created by this part.
	 */
	Map cacheRecords;

	public ExecutionRecord(Record parent, String executionID, String sourceRequestID, String cacheKey) {
		cacheRecords = new HashMap();
		this.executionID = executionID;
		this.parent = parent;
		addCacheRecord(sourceRequestID, cacheKey);
	}

	public void addCacheRecord(String sourceRequestID, String cacheKey) {
		CachedObjectRecord record = (CachedObjectRecord)cacheRecords.get(sourceRequestID);
		if(null == record) {
			record = new CachedObjectRecord(this, sourceRequestID, cacheKey);
			cacheRecords.put(sourceRequestID, record);
		} else {
			throw new RuntimeException("Error - Cannot add a CacheRecord to a RequestPartRecord with an existing key");
		}
	}
	
	public void deleteCacheItems(ConnectorLogger logger) {
		Collection records = cacheRecords.values();
		for(Iterator iter = records.iterator(); iter.hasNext(); ) {
			CachedObjectRecord record = (CachedObjectRecord) iter.next();
			String cacheKey = record.getCacheKey();
			((IDocumentCache)parent.getCache()).release(cacheKey , record.getID());
            logger.logTrace("Releasing cache item with key " + cacheKey + " and id " + record.getID());
		}
		cacheRecords.clear();
	}

	public String getID() {
		return parent.getID() + executionID;
	}

	public IDocumentCache getCache() {
		return parent.getCache();
	}

}
