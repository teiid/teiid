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
import org.teiid.adminapi.impl.TransactionMetadata;

@SuppressWarnings("nls")
public class TestTransactionMetadata {

    @Test public void testMapping() {
        TransactionMetadata tm = new TransactionMetadata();
        tm.setAssociatedSession("x");
        tm.setCreatedTime(1234);
        tm.setId("tnx-id");
        tm.setScope("scope");

        ModelNode node = VDBMetadataMapper.TransactionMetadataMapper.INSTANCE.wrap(tm, new ModelNode());

        TransactionMetadata tm1 = VDBMetadataMapper.TransactionMetadataMapper.INSTANCE.unwrap(node);

        assertEquals(tm.getAssociatedSession(), tm1.getAssociatedSession());

        assertEquals(tm.getCreatedTime(), tm1.getCreatedTime());
        assertEquals(tm.getId(), tm1.getId());
        assertEquals(tm.getScope(), tm1.getScope());
    }

    private static final String describe = "{\"attributes\" : {\n" +
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
            "    \"txn-created-time\" : {\n" +
            "        \"type\" : {\n" +
            "            \"TYPE_MODEL_VALUE\" : \"LONG\"\n" +
            "        },\n" +
            "        \"description\" : \"txn-created-time\",\n" +
            "        \"expressions-allowed\" : false,\n" +
            "        \"required\" : true,\n" +
            "        \"nillable\" : false\n" +
            "    },\n" +
            "    \"txn-scope\" : {\n" +
            "        \"type\" : {\n" +
            "            \"TYPE_MODEL_VALUE\" : \"LONG\"\n" +
            "        },\n" +
            "        \"description\" : \"txn-scope\",\n" +
            "        \"expressions-allowed\" : false,\n" +
            "        \"required\" : true,\n" +
            "        \"nillable\" : false\n" +
            "    },\n" +
            "    \"txn-id\" : {\n" +
            "        \"type\" : {\n" +
            "            \"TYPE_MODEL_VALUE\" : \"STRING\"\n" +
            "        },\n" +
            "        \"description\" : \"txn-id\",\n" +
            "        \"expressions-allowed\" : false,\n" +
            "        \"required\" : true,\n" +
            "        \"nillable\" : false,\n" +
            "        \"min-length\" : 1,\n" +
            "        \"max-length\" : 2147483647\n" +
            "    }\n" +
            "}}";

    @Test
    public void testDescribe() {
        ModelNode n = TestVDBMetaData.describe(new ModelNode(), VDBMetadataMapper.TransactionMetadataMapper.INSTANCE.getAttributeDefinitions());
        //System.out.println(n.toJSONString(false));
        assertEquals(describe, n.toJSONString(false));
    }
}
