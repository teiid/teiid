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

public class RequestMetadataMapper extends MetaMapper<RequestMetadata> {
	private static final MutableCompositeMetaType metaType;
	private static final MetaValueFactory metaValueFactory = MetaValueFactory.getInstance();
	
	static {
		metaType = new MutableCompositeMetaType(RequestMetadata.class.getName(), "The Session domain meta data");
		metaType.addItem("executionId", "executionId", SimpleMetaType.LONG_PRIMITIVE);
		metaType.addItem("sessionId", "sessionId", SimpleMetaType.LONG_PRIMITIVE);
		metaType.addItem("createdTime", "createdTime", SimpleMetaType.LONG_PRIMITIVE);
		metaType.addItem("processingTime", "processingTime", SimpleMetaType.LONG_PRIMITIVE);
		metaType.addItem("command", "command", SimpleMetaType.STRING);
		metaType.addItem("sourceRequest", "sourceRequest", SimpleMetaType.BOOLEAN_PRIMITIVE);
		metaType.addItem("nodeId", "nodeId", SimpleMetaType.INTEGER_PRIMITIVE);
		metaType.addItem("transactionId", "transactionId", SimpleMetaType.STRING);
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
			
			request.set("executionId", SimpleValueSupport.wrap(object.getExecutionId()));
			request.set("sessionId", SimpleValueSupport.wrap(object.getSessionId()));
			request.set("processingTime", SimpleValueSupport.wrap(object.getProcessingTime()));
			request.set("command", SimpleValueSupport.wrap(object.getCommand()));
			request.set("sourceRequest", SimpleValueSupport.wrap(object.sourceRequest()));
			request.set("nodeId", SimpleValueSupport.wrap(object.getNodeId()));
			request.set("transactionId",SimpleValueSupport.wrap(object.getTransactionId()));
			
			return request;
		}
		throw new IllegalArgumentException("Cannot convert request " + object);
	}

	@Override
	public RequestMetadata unwrapMetaValue(MetaValue metaValue) {
		if (metaValue == null)
			return null;

		if (metaValue instanceof CompositeValue) {
			CompositeValue compositeValue = (CompositeValue) metaValue;
			
			RequestMetadata request = new RequestMetadata();
			request.setExecutionId((Long) metaValueFactory.unwrap(compositeValue.get("executionId")));
			request.setSessionId((Long) metaValueFactory.unwrap(compositeValue.get("sessionId")));
			request.setProcessingTime((Long) metaValueFactory.unwrap(compositeValue.get("processingTime")));
			request.setCommand((String) metaValueFactory.unwrap(compositeValue.get("command")));
			request.setSourceRequest((Boolean) metaValueFactory.unwrap(compositeValue.get("sourceRequest")));
			request.setNodeId((Integer) metaValueFactory.unwrap(compositeValue.get("nodeId")));
			request.setTransactionId((String) metaValueFactory.unwrap(compositeValue.get("transactionId")));
			return request;
		}
		throw new IllegalStateException("Unable to unwrap request " + metaValue);
	}

}
