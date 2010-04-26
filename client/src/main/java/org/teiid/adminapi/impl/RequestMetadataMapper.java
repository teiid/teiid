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
import org.jboss.metatype.api.types.EnumMetaType;
import org.jboss.metatype.api.types.MetaType;
import org.jboss.metatype.api.types.SimpleMetaType;
import org.jboss.metatype.api.values.CompositeValue;
import org.jboss.metatype.api.values.CompositeValueSupport;
import org.jboss.metatype.api.values.EnumValueSupport;
import org.jboss.metatype.api.values.MetaValue;
import org.jboss.metatype.api.values.MetaValueFactory;
import org.jboss.metatype.api.values.SimpleValueSupport;
import org.jboss.metatype.plugins.types.MutableCompositeMetaType;
import org.jboss.metatype.spi.values.MetaMapper;
import org.teiid.adminapi.Request;
import org.teiid.adminapi.Request.ProcessingState;

public class RequestMetadataMapper extends MetaMapper<RequestMetadata> {
	private static final String TRANSACTION_ID = "transactionId"; //$NON-NLS-1$
	private static final String NODE_ID = "nodeId"; //$NON-NLS-1$
	private static final String SOURCE_REQUEST = "sourceRequest"; //$NON-NLS-1$
	private static final String COMMAND = "command"; //$NON-NLS-1$
	private static final String START_TIME = "startTime"; //$NON-NLS-1$
	private static final String SESSION_ID = "sessionId"; //$NON-NLS-1$
	private static final String EXECUTION_ID = "executionId"; //$NON-NLS-1$
	private static final String STATE = "processingState"; //$NON-NLS-1$
	private static final String THREAD_STATE = "threadState"; //$NON-NLS-1$
	private static final MutableCompositeMetaType metaType;
	private static final MetaValueFactory metaValueFactory = MetaValueFactory.getInstance();
	
	static {
		metaType = new MutableCompositeMetaType(RequestMetadata.class.getName(), "The Request meta data"); //$NON-NLS-1$
		metaType.addItem(EXECUTION_ID, EXECUTION_ID, SimpleMetaType.LONG_PRIMITIVE);
		metaType.addItem(SESSION_ID, SESSION_ID, SimpleMetaType.STRING);
		metaType.addItem(START_TIME, START_TIME, SimpleMetaType.LONG_PRIMITIVE);
		metaType.addItem(COMMAND, COMMAND, SimpleMetaType.STRING);
		metaType.addItem(SOURCE_REQUEST, SOURCE_REQUEST, SimpleMetaType.BOOLEAN_PRIMITIVE);
		metaType.addItem(NODE_ID, NODE_ID, SimpleMetaType.INTEGER);
		metaType.addItem(TRANSACTION_ID, TRANSACTION_ID, SimpleMetaType.STRING);
		metaType.addItem(STATE, STATE, new EnumMetaType(Request.ProcessingState.values()));
		metaType.addItem(THREAD_STATE, THREAD_STATE, new EnumMetaType(Request.ThreadState.values()));
		metaType.freeze();
	}
	
	@Override
	public Type mapToType() {
		return RequestMetadata.class;
	}
	
	@Override
	public MetaType getMetaType() {
		return metaType;
	}
	
	@Override
	public MetaValue createMetaValue(MetaType metaType, RequestMetadata object) {
		if (object == null)
			return null;
		if (metaType instanceof CompositeMetaType) {
			CompositeMetaType composite = (CompositeMetaType) metaType;
			CompositeValueSupport request = new CompositeValueSupport(composite);
			
			request.set(EXECUTION_ID, SimpleValueSupport.wrap(object.getExecutionId()));
			request.set(SESSION_ID, SimpleValueSupport.wrap(object.getSessionId()));
			request.set(START_TIME, SimpleValueSupport.wrap(object.getStartTime()));
			request.set(COMMAND, SimpleValueSupport.wrap(object.getCommand()));
			request.set(SOURCE_REQUEST, SimpleValueSupport.wrap(object.sourceRequest()));
			request.set(NODE_ID, SimpleValueSupport.wrap(object.getNodeId()));
			request.set(TRANSACTION_ID,SimpleValueSupport.wrap(object.getTransactionId()));
			EnumMetaType emt = (EnumMetaType)composite.getType(STATE);
			request.set(STATE, new EnumValueSupport(emt, object.getState()));
			request.set(THREAD_STATE, new EnumValueSupport((EnumMetaType)composite.getType(THREAD_STATE), object.getThreadState()));
			return request;
		}
		throw new IllegalArgumentException("Cannot convert RequestMetadata " + object); //$NON-NLS-1$
	}

	@Override
	public RequestMetadata unwrapMetaValue(MetaValue metaValue) {
		if (metaValue == null)
			return null;

		if (metaValue instanceof CompositeValue) {
			CompositeValue compositeValue = (CompositeValue) metaValue;
			
			RequestMetadata request = new RequestMetadata();
			request.setExecutionId((Long) metaValueFactory.unwrap(compositeValue.get(EXECUTION_ID)));
			request.setSessionId((String) metaValueFactory.unwrap(compositeValue.get(SESSION_ID)));
			request.setStartTime((Long) metaValueFactory.unwrap(compositeValue.get(START_TIME)));
			request.setCommand((String) metaValueFactory.unwrap(compositeValue.get(COMMAND)));
			request.setSourceRequest((Boolean) metaValueFactory.unwrap(compositeValue.get(SOURCE_REQUEST)));
			request.setNodeId((Integer) metaValueFactory.unwrap(compositeValue.get(NODE_ID)));
			request.setTransactionId((String) metaValueFactory.unwrap(compositeValue.get(TRANSACTION_ID)));
			request.setState((ProcessingState) metaValueFactory.unwrap(compositeValue.get(STATE)));
			return request;
		}
		throw new IllegalStateException("Unable to unwrap RequestMetadata " + metaValue); //$NON-NLS-1$
	}

}
