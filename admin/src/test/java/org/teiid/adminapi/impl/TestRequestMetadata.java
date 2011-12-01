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

import static org.junit.Assert.assertEquals;

import org.jboss.dmr.ModelNode;
import org.junit.Test;
import org.teiid.adminapi.Request.ProcessingState;
import org.teiid.adminapi.Request.ThreadState;
@SuppressWarnings("nls")
public class TestRequestMetadata {
	
	@Test public void testMapping() {
		RequestMetadata request = buildRequest();
		
		ModelNode node = VDBMetadataMapper.RequestMetadataMapper.INSTANCE.wrap(request, new ModelNode());
		
		RequestMetadata actual = VDBMetadataMapper.RequestMetadataMapper.INSTANCE.unwrap(node);
		
		assertEquals(request, actual);
		assertEquals(request.getState(), actual.getState());
	}

	private RequestMetadata buildRequest() {
		RequestMetadata request = new RequestMetadata();
		request.setState(ProcessingState.PROCESSING);
		request.setCommand("select * from foo"); //$NON-NLS-1$
		request.setExecutionId(1234);
		request.setName("request-name"); //$NON-NLS-1$
		request.setSessionId("session-id");//$NON-NLS-1$
		request.setSourceRequest(false);
		request.setStartTime(12345L);
		request.setTransactionId("transaction-id");//$NON-NLS-1$
		request.setThreadState(ThreadState.RUNNING);
		//request.setNodeId(1);
		return request;
	}
	
	public static final String desc = "{\n" + 
			"    \"execution-id\" : {\n" + 
			"        \"type\" : {\n" + 
			"            \"TYPE_MODEL_VALUE\" : \"LONG\"\n" + 
			"        },\n" + 
			"        \"description\" : \"Unique Identifier for Request\",\n" + 
			"        \"required\" : true\n" + 
			"    },\n" + 
			"    \"session-id\" : {\n" + 
			"        \"type\" : {\n" + 
			"            \"TYPE_MODEL_VALUE\" : \"STRING\"\n" + 
			"        },\n" + 
			"        \"description\" : \"Session Identifier\",\n" + 
			"        \"required\" : true\n" + 
			"    },\n" + 
			"    \"start-time\" : {\n" + 
			"        \"type\" : {\n" + 
			"            \"TYPE_MODEL_VALUE\" : \"LONG\"\n" + 
			"        },\n" + 
			"        \"description\" : \"Start time for the request\",\n" + 
			"        \"required\" : true\n" + 
			"    },\n" + 
			"    \"command\" : {\n" + 
			"        \"type\" : {\n" + 
			"            \"TYPE_MODEL_VALUE\" : \"STRING\"\n" + 
			"        },\n" + 
			"        \"description\" : \"Executing Command\",\n" + 
			"        \"required\" : true\n" + 
			"    },\n" + 
			"    \"source-request\" : {\n" + 
			"        \"type\" : {\n" + 
			"            \"TYPE_MODEL_VALUE\" : \"BOOLEAN\"\n" + 
			"        },\n" + 
			"        \"description\" : \"Is this Connector level request\",\n" + 
			"        \"required\" : true\n" + 
			"    },\n" + 
			"    \"node-id\" : {\n" + 
			"        \"type\" : {\n" + 
			"            \"TYPE_MODEL_VALUE\" : \"INT\"\n" + 
			"        },\n" + 
			"        \"description\" : \"Node Identifier\",\n" + 
			"        \"required\" : false\n" + 
			"    },\n" + 
			"    \"transaction-id\" : {\n" + 
			"        \"type\" : {\n" + 
			"            \"TYPE_MODEL_VALUE\" : \"STRING\"\n" + 
			"        },\n" + 
			"        \"description\" : \"Get Transaction XID if transaction involved\",\n" + 
			"        \"required\" : false\n" + 
			"    },\n" + 
			"    \"processing-state\" : {\n" + 
			"        \"type\" : {\n" + 
			"            \"TYPE_MODEL_VALUE\" : \"STRING\"\n" + 
			"        },\n" + 
			"        \"description\" : \"State of the Request\",\n" + 
			"        \"required\" : true\n" + 
			"    },\n" + 
			"    \"thread-state\" : {\n" + 
			"        \"type\" : {\n" + 
			"            \"TYPE_MODEL_VALUE\" : \"STRING\"\n" + 
			"        },\n" + 
			"        \"description\" : \"Thread state\",\n" + 
			"        \"required\" : true\n" + 
			"    }\n" + 
			"}";
	@Test public void testDescribe() {
		assertEquals(desc, VDBMetadataMapper.RequestMetadataMapper.INSTANCE.describe(new ModelNode()).toJSONString(false));
	}

}
