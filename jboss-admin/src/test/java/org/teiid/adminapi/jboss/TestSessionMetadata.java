/*
 * Copyright Red Hat, Inc. and/or its affiliates
 * and other contributors as indicated by the @author tags and
 * the COPYRIGHT.txt file distributed with this work.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.teiid.adminapi.jboss;

import static org.junit.Assert.assertEquals;

import org.jboss.dmr.ModelNode;
import org.junit.Test;
import org.teiid.adminapi.impl.SessionMetadata;
import org.teiid.adminapi.jboss.VDBMetadataMapper;

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

		ModelNode node = VDBMetadataMapper.SessionMetadataMapper.INSTANCE.wrap(session, new ModelNode());
		
		SessionMetadata session1 = VDBMetadataMapper.SessionMetadataMapper.INSTANCE.unwrap(node);
		
		assertEquals(session.getSessionId(), session1.getSessionId());
		assertEquals(session.getApplicationName(), session1.getApplicationName());
		
	}
	
	private static final String describe = "{\n" + 
			"    \"application-name\" : {\n" + 
			"        \"type\" : {\n" + 
			"            \"TYPE_MODEL_VALUE\" : \"STRING\"\n" + 
			"        },\n" + 
			"        \"description\" : \"Application assosiated with Session\",\n" + 
			"        \"required\" : false\n" + 
			"    },\n" + 
			"    \"created-time\" : {\n" + 
			"        \"type\" : {\n" + 
			"            \"TYPE_MODEL_VALUE\" : \"LONG\"\n" + 
			"        },\n" + 
			"        \"description\" : \"When session created\",\n" + 
			"        \"required\" : true\n" + 
			"    },\n" + 
			"    \"client-host-address\" : {\n" + 
			"        \"type\" : {\n" + 
			"            \"TYPE_MODEL_VALUE\" : \"LONG\"\n" + 
			"        },\n" + 
			"        \"description\" : \"Host name from where the session created\",\n" + 
			"        \"required\" : true\n" + 
			"    },\n" + 
			"    \"ip-address\" : {\n" + 
			"        \"type\" : {\n" + 
			"            \"TYPE_MODEL_VALUE\" : \"STRING\"\n" + 
			"        },\n" + 
			"        \"description\" : \"IP address from where session is created\",\n" + 
			"        \"required\" : true\n" + 
			"    },\n" + 
			"    \"last-ping-time\" : {\n" + 
			"        \"type\" : {\n" + 
			"            \"TYPE_MODEL_VALUE\" : \"LONG\"\n" + 
			"        },\n" + 
			"        \"description\" : \"Last ping time\",\n" + 
			"        \"required\" : true\n" + 
			"    },\n" + 
			"    \"session-id\" : {\n" + 
			"        \"type\" : {\n" + 
			"            \"TYPE_MODEL_VALUE\" : \"STRING\"\n" + 
			"        },\n" + 
			"        \"description\" : \"Session Identifier\",\n" + 
			"        \"required\" : true\n" + 
			"    },\n" + 
			"    \"user-name\" : {\n" + 
			"        \"type\" : {\n" + 
			"            \"TYPE_MODEL_VALUE\" : \"STRING\"\n" + 
			"        },\n" + 
			"        \"description\" : \"User name associated with session\",\n" + 
			"        \"required\" : true\n" + 
			"    },\n" + 
			"    \"vdb-name\" : {\n" + 
			"        \"type\" : {\n" + 
			"            \"TYPE_MODEL_VALUE\" : \"STRING\"\n" + 
			"        },\n" + 
			"        \"description\" : \"The Virtual Database Name\",\n" + 
			"        \"required\" : true\n" + 
			"    },\n" + 
			"    \"vdb-version\" : {\n" + 
			"        \"type\" : {\n" + 
			"            \"TYPE_MODEL_VALUE\" : \"INT\"\n" + 
			"        },\n" + 
			"        \"description\" : \"The Virtual Database Version\",\n" + 
			"        \"required\" : true\n" + 
			"    },\n" + 
			"    \"security-domain\" : {\n" + 
			"        \"type\" : {\n" + 
			"            \"TYPE_MODEL_VALUE\" : \"STRING\"\n" + 
			"        },\n" + 
			"        \"description\" : \"Security domain that session used for login\",\n" + 
			"        \"required\" : false\n" + 
			"    }\n" + 
			"}"; 
	
	@Test public void testDescribe() {
		ModelNode n = VDBMetadataMapper.SessionMetadataMapper.INSTANCE.describe(new ModelNode());
		//System.out.println(n.toJSONString(false));
		assertEquals(describe, n.toJSONString(false));
	}
}
