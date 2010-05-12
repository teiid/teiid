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
package org.teiid.resource.adapter.salesforce.execution;

import java.util.ArrayList;
import java.util.List;

import javax.xml.bind.JAXBElement;
import javax.xml.namespace.QName;

import org.teiid.language.ColumnReference;
import org.teiid.language.Command;
import org.teiid.language.Literal;
import org.teiid.language.SetClause;
import org.teiid.language.Update;
import org.teiid.metadata.Column;
import org.teiid.metadata.RuntimeMetadata;
import org.teiid.resource.adapter.salesforce.SalesforceConnection;
import org.teiid.resource.adapter.salesforce.Util;
import org.teiid.resource.adapter.salesforce.execution.visitors.UpdateVisitor;
import org.teiid.translator.ConnectorException;
import org.teiid.translator.ExecutionContext;


public class UpdateExecutionImpl extends AbstractUpdateExecution {

	@SuppressWarnings("unchecked")
	private static final Class stringClazz = new String().getClass();
	
	public UpdateExecutionImpl(Command command,
			SalesforceConnection salesforceConnection,
			RuntimeMetadata metadata, ExecutionContext context) {
		super(command, salesforceConnection, metadata, context);
	}

	@SuppressWarnings("unchecked")
	@Override
	public void execute() throws ConnectorException {
		UpdateVisitor visitor = new UpdateVisitor(getMetadata());
		visitor.visit((Update)command);
		String[] Ids = getIDs(((Update)command).getWhere(), visitor);

		if (null != Ids && Ids.length > 0) {
			List<JAXBElement> elements = new ArrayList<JAXBElement>();
			for (SetClause clause : ((Update)command).getChanges()) {
				ColumnReference element = clause.getSymbol();
				Column column = element.getMetadataObject();
				String val = ((Literal) clause.getValue()).toString();
				JAXBElement messageElem = new JAXBElement(new QName(column.getNameInSource()), stringClazz, Util.stripQutes(val));
				elements.add(messageElem);
			}

			List<DataPayload> updateDataList = new ArrayList<DataPayload>();
			for (int i = 0; i < Ids.length; i++) {
				DataPayload data = new DataPayload();
				data.setType(visitor.getTableName());
				data.setID(Ids[i]);
				data.setMessageElements(elements);
				updateDataList.add(data);
			}

			result = getConnection().update(updateDataList);
		}
	}
}
