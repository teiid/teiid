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

@SuppressWarnings("nls")
public class TestSessionMetadata {
	
	@Test public void testMapping() {
		SessionMetadata session = new SessionMetadata();
		session.setSessionId("test");
		session.setApplicationName("foo");
		session.setClientHostName("localhost");
		session.setCreatedTime(1234);
		session.setIPAddress("127.0.0.1");
		session.setVDBName("vdb-name");
		session.setVDBVersion(2);
		session.setSecurityContext("auth-domain");
		session.setUserName("user");

		ModelNode node = MetadataMapper.SessionMetadataMapper.wrap(session);
		
		SessionMetadata session1 = MetadataMapper.SessionMetadataMapper.unwrap(node);
		
		assertEquals(session.getSessionId(), session1.getSessionId());
		assertEquals(session.getApplicationName(), session1.getApplicationName());
		
	}
	
	private static final String describe = "{\n" + 
			"    \"type\" : {\n" + 
			"        \"TYPE_MODEL_VALUE\" : \"OBJECT\"\n" + 
			"    },\n" + 
			"    \"attributes\" : {\n" + 
			"        \"application-name\" : {\n" + 
			"            \"type\" : {\n" + 
			"                \"TYPE_MODEL_VALUE\" : \"STRING\"\n" + 
			"            },\n" + 
			"            \"description\" : \"Application assosiated with Session\",\n" + 
			"            \"required\" : false\n" + 
			"        },\n" + 
			"        \"created-time\" : {\n" + 
			"            \"type\" : {\n" + 
			"                \"TYPE_MODEL_VALUE\" : \"LONG\"\n" + 
			"            },\n" + 
			"            \"description\" : \"When session created\",\n" + 
			"            \"required\" : true\n" + 
			"        },\n" + 
			"        \"client-host-address\" : {\n" + 
			"            \"type\" : {\n" + 
			"                \"TYPE_MODEL_VALUE\" : \"LONG\"\n" + 
			"            },\n" + 
			"            \"description\" : \"Host name from where the session created\",\n" + 
			"            \"required\" : true\n" + 
			"        },\n" + 
			"        \"ip-address\" : {\n" + 
			"            \"type\" : {\n" + 
			"                \"TYPE_MODEL_VALUE\" : \"STRING\"\n" + 
			"            },\n" + 
			"            \"description\" : \"IP address from where session is created\",\n" + 
			"            \"required\" : true\n" + 
			"        },\n" + 
			"        \"last-ping-time\" : {\n" + 
			"            \"type\" : {\n" + 
			"                \"TYPE_MODEL_VALUE\" : \"LONG\"\n" + 
			"            },\n" + 
			"            \"description\" : \"Last ping time\",\n" + 
			"            \"required\" : true\n" + 
			"        },\n" + 
			"        \"session-id\" : {\n" + 
			"            \"type\" : {\n" + 
			"                \"TYPE_MODEL_VALUE\" : \"STRING\"\n" + 
			"            },\n" + 
			"            \"description\" : \"Session Identifier\",\n" + 
			"            \"required\" : true\n" + 
			"        },\n" + 
			"        \"user-name\" : {\n" + 
			"            \"type\" : {\n" + 
			"                \"TYPE_MODEL_VALUE\" : \"STRING\"\n" + 
			"            },\n" + 
			"            \"description\" : \"User name associated with session\",\n" + 
			"            \"required\" : true\n" + 
			"        },\n" + 
			"        \"vdb-name\" : {\n" + 
			"            \"type\" : {\n" + 
			"                \"TYPE_MODEL_VALUE\" : \"STRING\"\n" + 
			"            },\n" + 
			"            \"description\" : \"The Virtual Database Name\",\n" + 
			"            \"required\" : true\n" + 
			"        },\n" + 
			"        \"vdb-version\" : {\n" + 
			"            \"type\" : {\n" + 
			"                \"TYPE_MODEL_VALUE\" : \"INT\"\n" + 
			"            },\n" + 
			"            \"description\" : \"The Virtual Database Version\",\n" + 
			"            \"required\" : true\n" + 
			"        },\n" + 
			"        \"security-domain\" : {\n" + 
			"            \"type\" : {\n" + 
			"                \"TYPE_MODEL_VALUE\" : \"STRING\"\n" + 
			"            },\n" + 
			"            \"description\" : \"Security domain that session used for login\",\n" + 
			"            \"required\" : false\n" + 
			"        }\n" + 
			"    }\n" + 
			"}"; 
	
	@Test public void testDescribe() {
		ModelNode n = MetadataMapper.SessionMetadataMapper.describe(new ModelNode());
		//System.out.println(n.toJSONString(false));
		assertEquals(describe, n.toJSONString(false));
	}
}
