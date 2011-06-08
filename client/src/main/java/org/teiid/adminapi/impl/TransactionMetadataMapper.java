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

public class TransactionMetadataMapper {
	private static final String ID = "id"; //$NON-NLS-1$
	private static final String SCOPE = "scope"; //$NON-NLS-1$
	private static final String CREATED_TIME = "createdTime"; //$NON-NLS-1$
	private static final String ASSOCIATED_SESSION = "associatedSession"; //$NON-NLS-1$
	
	public static ModelNode wrap(TransactionMetadata object) {
		if (object == null)
			return null;
		
		ModelNode transaction = new ModelNode();
		transaction.get(ModelNodeConstants.TYPE).set(ModelType.OBJECT);
		
		transaction.get(ASSOCIATED_SESSION).set(object.getAssociatedSession());
		transaction.get(CREATED_TIME).set(object.getCreatedTime());
		transaction.get(SCOPE).set(object.getScope());
		transaction.get(ID).set(object.getId());
		
		return transaction;
	}

	public static TransactionMetadata unwrap(ModelNode node) {
		if (node == null)
			return null;

		TransactionMetadata transaction = new TransactionMetadata();
		transaction.setAssociatedSession(node.get(ASSOCIATED_SESSION).asString());
		transaction.setCreatedTime(node.get(CREATED_TIME).asLong());
		transaction.setScope(node.get(SCOPE).asString());
		transaction.setId(node.get(ID).asString());
		return transaction;
	}
}
