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

import static org.junit.Assert.*;

import org.jboss.dmr.ModelNode;
import org.junit.Test;
import org.teiid.adminapi.impl.SessionMetadata;

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

    private static final String describe = "{\"attributes\" : {\n" +
            "    \"application-name\" : {\n" +
            "        \"type\" : {\n" +
            "            \"TYPE_MODEL_VALUE\" : \"STRING\"\n" +
            "        },\n" +
            "        \"description\" : \"application-name\",\n" +
            "        \"expressions-allowed\" : false,\n" +
            "        \"required\" : false,\n" +
            "        \"nillable\" : true,\n" +
            "        \"min-length\" : 1,\n" +
            "        \"max-length\" : 2147483647\n" +
            "    },\n" +
            "    \"created-time\" : {\n" +
            "        \"type\" : {\n" +
            "            \"TYPE_MODEL_VALUE\" : \"LONG\"\n" +
            "        },\n" +
            "        \"description\" : \"created-time\",\n" +
            "        \"expressions-allowed\" : false,\n" +
            "        \"required\" : true,\n" +
            "        \"nillable\" : false\n" +
            "    },\n" +
            "    \"client-host-address\" : {\n" +
            "        \"type\" : {\n" +
            "            \"TYPE_MODEL_VALUE\" : \"STRING\"\n" +
            "        },\n" +
            "        \"description\" : \"client-host-address\",\n" +
            "        \"expressions-allowed\" : false,\n" +
            "        \"required\" : false,\n" +
            "        \"nillable\" : true,\n" +
            "        \"min-length\" : 1,\n" +
            "        \"max-length\" : 2147483647\n" +
            "    },\n" +
            "    \"client-hardware-address\" : {\n" +
            "        \"type\" : {\n" +
            "            \"TYPE_MODEL_VALUE\" : \"STRING\"\n" +
            "        },\n" +
            "        \"description\" : \"client-hardware-address\",\n" +
            "        \"expressions-allowed\" : false,\n" +
            "        \"required\" : false,\n" +
            "        \"nillable\" : true,\n" +
            "        \"min-length\" : 1,\n" +
            "        \"max-length\" : 2147483647\n" +
            "    },\n" +
            "    \"ip-address\" : {\n" +
            "        \"type\" : {\n" +
            "            \"TYPE_MODEL_VALUE\" : \"STRING\"\n" +
            "        },\n" +
            "        \"description\" : \"ip-address\",\n" +
            "        \"expressions-allowed\" : false,\n" +
            "        \"required\" : false,\n" +
            "        \"nillable\" : true,\n" +
            "        \"min-length\" : 1,\n" +
            "        \"max-length\" : 2147483647\n" +
            "    },\n" +
            "    \"last-ping-time\" : {\n" +
            "        \"type\" : {\n" +
            "            \"TYPE_MODEL_VALUE\" : \"LONG\"\n" +
            "        },\n" +
            "        \"description\" : \"last-ping-time\",\n" +
            "        \"expressions-allowed\" : false,\n" +
            "        \"required\" : true,\n" +
            "        \"nillable\" : false\n" +
            "    },\n" +
            "    \"session-id\" : {\n" +
            "        \"type\" : {\n" +
            "            \"TYPE_MODEL_VALUE\" : \"STRING\"\n" +
            "        },\n" +
            "        \"description\" : \"session-id\",\n" +
            "        \"expressions-allowed\" : false,\n" +
            "        \"required\" : true,\n" +
            "        \"nillable\" : false,\n" +
            "        \"min-length\" : 1,\n" +
            "        \"max-length\" : 2147483647\n" +
            "    },\n" +
            "    \"user-name\" : {\n" +
            "        \"type\" : {\n" +
            "            \"TYPE_MODEL_VALUE\" : \"STRING\"\n" +
            "        },\n" +
            "        \"description\" : \"user-name\",\n" +
            "        \"expressions-allowed\" : false,\n" +
            "        \"required\" : true,\n" +
            "        \"nillable\" : false,\n" +
            "        \"min-length\" : 1,\n" +
            "        \"max-length\" : 2147483647\n" +
            "    },\n" +
            "    \"vdb-name\" : {\n" +
            "        \"type\" : {\n" +
            "            \"TYPE_MODEL_VALUE\" : \"STRING\"\n" +
            "        },\n" +
            "        \"description\" : \"vdb-name\",\n" +
            "        \"expressions-allowed\" : false,\n" +
            "        \"required\" : true,\n" +
            "        \"nillable\" : false,\n" +
            "        \"min-length\" : 1,\n" +
            "        \"max-length\" : 2147483647\n" +
            "    },\n" +
            "    \"vdb-version\" : {\n" +
            "        \"type\" : {\n" +
            "            \"TYPE_MODEL_VALUE\" : \"STRING\"\n" +
            "        },\n" +
            "        \"description\" : \"vdb-version\",\n" +
            "        \"expressions-allowed\" : false,\n" +
            "        \"required\" : true,\n" +
            "        \"nillable\" : false,\n" +
            "        \"min-length\" : 1,\n" +
            "        \"max-length\" : 2147483647\n" +
            "    },\n" +
            "    \"security-domain\" : {\n" +
            "        \"type\" : {\n" +
            "            \"TYPE_MODEL_VALUE\" : \"STRING\"\n" +
            "        },\n" +
            "        \"description\" : \"security-domain\",\n" +
            "        \"expressions-allowed\" : false,\n" +
            "        \"required\" : false,\n" +
            "        \"nillable\" : true,\n" +
            "        \"min-length\" : 1,\n" +
            "        \"max-length\" : 2147483647\n" +
            "    }\n" +
            "}}";

    @Test public void testDescribe() {
        ModelNode n = TestVDBMetaData.describe(new ModelNode(), VDBMetadataMapper.SessionMetadataMapper.INSTANCE.getAttributeDefinitions());
        //System.out.println(n.toJSONString(false));
        assertEquals(describe, n.toJSONString(false));
    }
}
