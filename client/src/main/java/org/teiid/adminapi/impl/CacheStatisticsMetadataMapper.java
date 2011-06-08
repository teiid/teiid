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
package org.teiid.adminapi.impl;

import org.jboss.dmr.ModelNode;
import org.jboss.dmr.ModelType;

public class CacheStatisticsMetadataMapper {
	private static final String HITRATIO = "hitRatio"; //$NON-NLS-1$
	private static final String TOTAL_ENTRIES = "totalEntries"; //$NON-NLS-1$
	private static final String REQUEST_COUNT = "requestCount"; //$NON-NLS-1$

	
	public static ModelNode wrap(CacheStatisticsMetadata object) {
		if (object == null)
			return null;
		
		ModelNode cache = new ModelNode();
		cache.get(ModelNodeConstants.TYPE).set(ModelType.OBJECT);
		
		cache.get(TOTAL_ENTRIES).set(object.getTotalEntries());
		cache.get(HITRATIO).set(object.getHitRatio());
		cache.get(REQUEST_COUNT).set(object.getRequestCount());
		
		return cache;
	}

	public static CacheStatisticsMetadata unwrap(ModelNode node) {
		if (node == null)
			return null;
			
		CacheStatisticsMetadata cache = new CacheStatisticsMetadata();
		cache.setTotalEntries(node.get(TOTAL_ENTRIES).asInt());
		cache.setHitRatio(node.get(HITRATIO).asDouble());
		cache.setRequestCount(node.get(REQUEST_COUNT).asInt());
		return cache;
	}

}
