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
package org.teiid.jboss;

import org.jboss.as.controller.AbstractRemoveStepHandler;
import org.jboss.as.controller.OperationContext;
import org.jboss.dmr.ModelNode;

class TeiidRemove extends AbstractRemoveStepHandler {
	public static TeiidRemove INSTANCE = new TeiidRemove();
	
	@Override
    protected void performRuntime(OperationContext context, ModelNode operation, ModelNode model) {
		
		context.removeService(TeiidServiceNames.PREPAREDPLAN_CACHE_FACTORY);
		context.removeService(TeiidServiceNames.PREPAREDPLAN_CACHE_FACTORY);
		
		context.removeService(TeiidServiceNames.PREPAREDPLAN_CACHE_FACTORY);
		context.removeService(TeiidServiceNames.RESULTSET_CACHE_FACTORY);
		context.removeService(TeiidServiceNames.AUTHORIZATION_VALIDATOR);
		context.removeService(TeiidServiceNames.EVENT_DISTRIBUTOR_FACTORY);
		
		context.removeService(TeiidServiceNames.ENGINE);
		context.removeService(TeiidServiceNames.CACHE_PREPAREDPLAN);
		context.removeService(TeiidServiceNames.CACHE_RESULTSET);
		context.removeService(TeiidServiceNames.TUPLE_BUFFER);
		context.removeService(TeiidServiceNames.BUFFER_MGR);
		context.removeService(TeiidServiceNames.BUFFER_DIR);
		context.removeService(TeiidServiceNames.OBJECT_SERIALIZER);
		context.removeService(TeiidServiceNames.VDB_STATUS_CHECKER);
		context.removeService(TeiidServiceNames.VDB_REPO);
		context.removeService(TeiidServiceNames.TRANSLATOR_REPO);
    }
}
