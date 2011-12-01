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

	private static final String describe = "{\n" + 
			"    \"session-id\" : {\n" + 
			"        \"type\" : {\n" + 
			"            \"TYPE_MODEL_VALUE\" : \"STRING\"\n" + 
			"        },\n" + 
			"        \"description\" : \"Session Identifier\",\n" + 
			"        \"required\" : true\n" + 
			"    },\n" + 
			"    \"txn-created-time\" : {\n" + 
			"        \"type\" : {\n" + 
			"            \"TYPE_MODEL_VALUE\" : \"LONG\"\n" + 
			"        },\n" + 
			"        \"description\" : \"Transaction created time\",\n" + 
			"        \"required\" : true\n" + 
			"    },\n" + 
			"    \"txn-scope\" : {\n" + 
			"        \"type\" : {\n" + 
			"            \"TYPE_MODEL_VALUE\" : \"LONG\"\n" + 
			"        },\n" + 
			"        \"description\" : \"Transaction scope (Request, Local, Global)\",\n" + 
			"        \"required\" : true\n" + 
			"    },\n" + 
			"    \"txn-id\" : {\n" + 
			"        \"type\" : {\n" + 
			"            \"TYPE_MODEL_VALUE\" : \"STRING\"\n" + 
			"        },\n" + 
			"        \"description\" : \"Transaction Identifier (XID)\",\n" + 
			"        \"required\" : true\n" + 
			"    }\n" + 
			"}";
	@Test
	public void testDescribe() {
		ModelNode n = VDBMetadataMapper.TransactionMetadataMapper.INSTANCE.describe(new ModelNode());
		//System.out.println(n.toJSONString(false));
		assertEquals(describe, n.toJSONString(false));
	}
}
