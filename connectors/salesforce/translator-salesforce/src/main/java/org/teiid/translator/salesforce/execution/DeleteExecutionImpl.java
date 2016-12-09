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
package org.teiid.translator.salesforce.execution;

import javax.resource.ResourceException;

import org.teiid.language.Command;
import org.teiid.language.Delete;
import org.teiid.metadata.RuntimeMetadata;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.salesforce.SalesForceExecutionFactory;
import org.teiid.translator.salesforce.SalesforceConnection;
import org.teiid.translator.salesforce.execution.visitors.DeleteVisitor;
import org.teiid.translator.salesforce.execution.visitors.IQueryProvidingVisitor;


public class DeleteExecutionImpl extends AbstractUpdateExecution {


	public DeleteExecutionImpl(SalesForceExecutionFactory ef, Command command,
			SalesforceConnection salesforceConnection,
			RuntimeMetadata metadata, ExecutionContext context) {
		super(ef, command, salesforceConnection, metadata, context);
	}

	@Override
	public void execute() throws TranslatorException {
		DeleteVisitor dVisitor = new DeleteVisitor(getMetadata());
		dVisitor.visitNode(command);
		execute(((Delete)command).getWhere(), dVisitor);
	}
	
	@Override
	protected int processIds(String[] ids, IQueryProvidingVisitor visitor)
	        throws ResourceException {
	    return getConnection().delete(ids);
	}
}
