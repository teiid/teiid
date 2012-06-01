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
package org.teiid.query.metadata;

import javax.resource.ResourceException;

import org.teiid.metadata.MetadataFactory;
import org.teiid.metadata.Schema;
import org.teiid.metadata.Table;
import org.teiid.query.QueryPlugin;
import org.teiid.query.function.metadata.FunctionMetadataValidator;
import org.teiid.query.report.ActivityReport;
import org.teiid.query.report.ReportItem;
import org.teiid.resource.spi.WrappedConnection;
import org.teiid.translator.ExecutionFactory;
import org.teiid.translator.TranslatorException;

public class NativeMetadataRepository extends BaseMetadataRepository {

	@Override
	public void loadMetadata(MetadataFactory factory, ExecutionFactory executionFactory, Object connectionFactory) throws TranslatorException {
		
		if (executionFactory == null ) {
			throw new TranslatorException(QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30591, factory.getName()));
		}
		
		if (connectionFactory == null) {
			throw new TranslatorException(QueryPlugin.Event.TEIID31097, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID31097));
		}
		
		Object connection = executionFactory.getConnection(connectionFactory, null);
		Object unwrapped = null;
		
		if (connection instanceof WrappedConnection) {
			try {
				unwrapped = ((WrappedConnection)connection).unwrap();
			} catch (ResourceException e) {
				 throw new TranslatorException(QueryPlugin.Event.TEIID30477, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30477));
			}	
		}
		
		try {
			executionFactory.getMetadata(factory, (unwrapped == null) ? connection:unwrapped);
		} finally {
			executionFactory.closeConnection(connection, connectionFactory);
		}
		validateMetadata(factory.getSchema());
		
		super.loadMetadata(factory, executionFactory, connectionFactory);		
	}
	
    private void validateMetadata(Schema schema) throws TranslatorException {
    	for (Table t : schema.getTables().values()) {
			if (t.getColumns() == null || t.getColumns().size() == 0) {
				throw new TranslatorException(QueryPlugin.Event.TEIID30580, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30580, t.getFullName())); 
			}
		}
    	ActivityReport<ReportItem> report = new ActivityReport<ReportItem>("Translator metadata load " + schema.getName()); //$NON-NLS-1$
		FunctionMetadataValidator.validateFunctionMethods(schema.getFunctions().values(),report);
		if(report.hasItems()) {
		    throw new TranslatorException(QueryPlugin.Util.getString("ERR.015.001.0005", report)); //$NON-NLS-1$
		}
	}	
}
