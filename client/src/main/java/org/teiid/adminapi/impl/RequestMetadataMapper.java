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
import org.teiid.adminapi.Request.ProcessingState;
import org.teiid.adminapi.Request.ThreadState;

public class RequestMetadataMapper {
	private static final String TRANSACTION_ID = "transactionId"; //$NON-NLS-1$
	private static final String NODE_ID = "nodeId"; //$NON-NLS-1$
	private static final String SOURCE_REQUEST = "sourceRequest"; //$NON-NLS-1$
	private static final String COMMAND = "command"; //$NON-NLS-1$
	private static final String START_TIME = "startTime"; //$NON-NLS-1$
	private static final String SESSION_ID = "sessionId"; //$NON-NLS-1$
	private static final String EXECUTION_ID = "executionId"; //$NON-NLS-1$
	private static final String STATE = "processingState"; //$NON-NLS-1$
	private static final String THREAD_STATE = "threadState"; //$NON-NLS-1$
	
	
	public static ModelNode wrap(RequestMetadata object) {
		
		if (object == null) {
			return null;
		}
		
		ModelNode request = new ModelNode();
		request.get(ModelNodeConstants.TYPE).set(ModelType.OBJECT);
		
		request.get(EXECUTION_ID).set(object.getExecutionId());
		request.get(SESSION_ID).set(object.getSessionId());
		request.get(START_TIME).set(object.getStartTime());
		request.get(COMMAND).set(object.getCommand());
		request.get(SOURCE_REQUEST).set(object.sourceRequest());
		request.get(NODE_ID).set(object.getNodeId());
		request.get(TRANSACTION_ID).set(object.getTransactionId());
		request.get(STATE).set(object.getState().name());
		request.get(THREAD_STATE).set(object.getThreadState().name());
		return request;
	}

	public static RequestMetadata unwrap(ModelNode node) {
		if (node == null)
			return null;

		RequestMetadata request = new RequestMetadata();
		request.setExecutionId(node.get(EXECUTION_ID).asLong());
		request.setSessionId(node.get(SESSION_ID).asString());
		request.setStartTime(node.get(START_TIME).asLong());
		request.setCommand(node.get(COMMAND).asString());
		request.setSourceRequest(node.get(SOURCE_REQUEST).asBoolean());
		request.setNodeId(node.get(NODE_ID).asInt());
		request.setTransactionId(node.get(TRANSACTION_ID).asString());
		request.setState(ProcessingState.valueOf(node.get(STATE).asString()));
		request.setThreadState(ThreadState.valueOf(node.get(THREAD_STATE).asString()));
		return request;
	}

}
