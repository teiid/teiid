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

public class WorkerPoolStatisticsMetadataMapper {
	private static final String MAX_THREADS = "maxThreads"; //$NON-NLS-1$
	private static final String HIGHEST_QUEUED = "highestQueued"; //$NON-NLS-1$
	private static final String QUEUED = "queued"; //$NON-NLS-1$
	private static final String QUEUE_NAME = "queueName"; //$NON-NLS-1$
	private static final String TOTAL_SUBMITTED = "totalSubmitted"; //$NON-NLS-1$
	private static final String TOTAL_COMPLETED = "totalCompleted"; //$NON-NLS-1$
	private static final String HIGHEST_ACTIVE_THREADS = "highestActiveThreads"; //$NON-NLS-1$
	private static final String ACTIVE_THREADS = "activeThreads"; //$NON-NLS-1$
	
	
	public static ModelNode wrap(WorkerPoolStatisticsMetadata object) {
		if (object == null)
			return null;
		ModelNode transaction = new ModelNode();
		transaction.get(ModelNodeConstants.TYPE).set(ModelType.OBJECT);
		
		transaction.get(ACTIVE_THREADS).set(object.getActiveThreads());
		transaction.get(HIGHEST_ACTIVE_THREADS).set(object.getHighestActiveThreads());
		transaction.get(TOTAL_COMPLETED).set(object.getTotalCompleted());
		transaction.get(TOTAL_SUBMITTED).set(object.getTotalSubmitted());
		transaction.get(QUEUE_NAME).set(object.getQueueName());
		transaction.get(QUEUED).set(object.getQueued());
		transaction.get(HIGHEST_QUEUED).set(object.getHighestQueued());
		transaction.get(MAX_THREADS).set(object.getMaxThreads());
		
		return transaction;
	}

	public static WorkerPoolStatisticsMetadata unwrapMetaValue(ModelNode node) {
		if (node == null)
			return null;

		WorkerPoolStatisticsMetadata stats = new WorkerPoolStatisticsMetadata();
		stats.setActiveThreads(node.get(ACTIVE_THREADS).asInt());
		stats.setHighestActiveThreads(node.get(HIGHEST_ACTIVE_THREADS).asInt());
		stats.setTotalCompleted(node.get(TOTAL_COMPLETED).asLong());
		stats.setTotalSubmitted(node.get(TOTAL_SUBMITTED).asLong());
		stats.setQueueName(node.get(QUEUE_NAME).asString());
		stats.setQueued(node.get(QUEUED).asInt());
		stats.setHighestQueued(node.get(HIGHEST_QUEUED).asInt());
		stats.setMaxThreads(node.get(MAX_THREADS).asInt());			
		return stats;
	}
}
