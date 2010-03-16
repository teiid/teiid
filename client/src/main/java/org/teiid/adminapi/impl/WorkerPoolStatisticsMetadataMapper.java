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

import java.lang.reflect.Type;

import org.jboss.metatype.api.types.CompositeMetaType;
import org.jboss.metatype.api.types.MetaType;
import org.jboss.metatype.api.types.SimpleMetaType;
import org.jboss.metatype.api.values.CompositeValue;
import org.jboss.metatype.api.values.CompositeValueSupport;
import org.jboss.metatype.api.values.MetaValue;
import org.jboss.metatype.api.values.MetaValueFactory;
import org.jboss.metatype.api.values.SimpleValueSupport;
import org.jboss.metatype.plugins.types.MutableCompositeMetaType;
import org.jboss.metatype.spi.values.MetaMapper;

public class WorkerPoolStatisticsMetadataMapper extends MetaMapper<WorkerPoolStatisticsMetadata> {
	private static final String MAX_THREADS = "maxThreads"; //$NON-NLS-1$
	private static final String HIGHEST_QUEUED = "highestQueued"; //$NON-NLS-1$
	private static final String QUEUED = "queued"; //$NON-NLS-1$
	private static final String QUEUE_NAME = "queueName"; //$NON-NLS-1$
	private static final String TOTAL_SUBMITTED = "totalSubmitted"; //$NON-NLS-1$
	private static final String TOTAL_COMPLETED = "totalCompleted"; //$NON-NLS-1$
	private static final String HIGHEST_ACTIVE_THREADS = "highestActiveThreads"; //$NON-NLS-1$
	private static final String ACTIVE_THREADS = "activeThreads"; //$NON-NLS-1$
	private static final MutableCompositeMetaType metaType;
	private static final MetaValueFactory metaValueFactory = MetaValueFactory.getInstance();
	
	static {
		metaType = new MutableCompositeMetaType(WorkerPoolStatisticsMetadata.class.getName(), "The Worker Pool statistics"); //$NON-NLS-1$
		metaType.addItem(ACTIVE_THREADS, "ActiveThreads", SimpleMetaType.INTEGER_PRIMITIVE); //$NON-NLS-1$
		metaType.addItem(HIGHEST_ACTIVE_THREADS, "HighestActiveThreads", SimpleMetaType.INTEGER_PRIMITIVE); //$NON-NLS-1$
		metaType.addItem(TOTAL_COMPLETED, "TotalCompleted", SimpleMetaType.LONG_PRIMITIVE); //$NON-NLS-1$
		metaType.addItem(TOTAL_SUBMITTED, "TotalSubmitted", SimpleMetaType.LONG_PRIMITIVE); //$NON-NLS-1$
		metaType.addItem(QUEUE_NAME, "QueueName", SimpleMetaType.STRING); //$NON-NLS-1$
		metaType.addItem(QUEUED, "Queued", SimpleMetaType.INTEGER_PRIMITIVE); //$NON-NLS-1$
		metaType.addItem(HIGHEST_QUEUED, "HighestQueued", SimpleMetaType.INTEGER_PRIMITIVE); //$NON-NLS-1$
		metaType.addItem(MAX_THREADS, "MaxThreads", SimpleMetaType.INTEGER_PRIMITIVE); //$NON-NLS-1$
		metaType.freeze();
	}
	
	@Override
	public Type mapToType() {
		return WorkerPoolStatisticsMetadata.class;
	}
	
	@Override
	public MetaType getMetaType() {
		return metaType;
	}
	
	@Override
	public MetaValue createMetaValue(MetaType metaType, WorkerPoolStatisticsMetadata object) {
		if (object == null)
			return null;
		if (metaType instanceof CompositeMetaType) {
			CompositeMetaType composite = (CompositeMetaType) metaType;
			CompositeValueSupport transaction = new CompositeValueSupport(composite);
			
			transaction.set(ACTIVE_THREADS, SimpleValueSupport.wrap(object.getActiveThreads()));
			transaction.set(HIGHEST_ACTIVE_THREADS, SimpleValueSupport.wrap(object.getHighestActiveThreads()));
			transaction.set(TOTAL_COMPLETED, SimpleValueSupport.wrap(object.getTotalCompleted()));
			transaction.set(TOTAL_SUBMITTED, SimpleValueSupport.wrap(object.getTotalSubmitted()));
			transaction.set(QUEUE_NAME, SimpleValueSupport.wrap(object.getQueueName()));
			transaction.set(QUEUED, SimpleValueSupport.wrap(object.getQueued()));
			transaction.set(HIGHEST_QUEUED, SimpleValueSupport.wrap(object.getHighestQueued()));
			transaction.set(MAX_THREADS, SimpleValueSupport.wrap(object.getMaxThreads()));
			
			return transaction;
		}
		throw new IllegalArgumentException("Cannot convert Worker Pool Statistics " + object); //$NON-NLS-1$
	}

	@Override
	public WorkerPoolStatisticsMetadata unwrapMetaValue(MetaValue metaValue) {
		if (metaValue == null)
			return null;

		if (metaValue instanceof CompositeValue) {
			CompositeValue compositeValue = (CompositeValue) metaValue;
			
			WorkerPoolStatisticsMetadata stats = new WorkerPoolStatisticsMetadata();
			stats.setActiveThreads((Integer) metaValueFactory.unwrap(compositeValue.get(ACTIVE_THREADS)));
			stats.setHighestActiveThreads((Integer) metaValueFactory.unwrap(compositeValue.get(HIGHEST_ACTIVE_THREADS)));
			stats.setTotalCompleted((Long) metaValueFactory.unwrap(compositeValue.get(TOTAL_COMPLETED)));
			stats.setTotalSubmitted((Long) metaValueFactory.unwrap(compositeValue.get(TOTAL_SUBMITTED)));
			stats.setQueueName((String) metaValueFactory.unwrap(compositeValue.get(QUEUE_NAME)));
			stats.setQueued((Integer) metaValueFactory.unwrap(compositeValue.get(QUEUED)));
			stats.setHighestQueued((Integer) metaValueFactory.unwrap(compositeValue.get(HIGHEST_QUEUED)));
			stats.setMaxThreads((Integer) metaValueFactory.unwrap(compositeValue.get(MAX_THREADS)));			
			return stats;
		}
		throw new IllegalStateException("Unable to unwrap transaction " + metaValue); //$NON-NLS-1$
	}
}
