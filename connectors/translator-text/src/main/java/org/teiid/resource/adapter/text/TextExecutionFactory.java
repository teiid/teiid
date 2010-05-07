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

package org.teiid.resource.adapter.text;

import java.util.Map;
import java.util.Properties;

import javax.resource.ResourceException;
import javax.resource.cci.ConnectionFactory;

import org.teiid.connector.language.QueryExpression;
import org.teiid.connector.language.Select;
import org.teiid.connector.metadata.runtime.Column;
import org.teiid.connector.metadata.runtime.MetadataFactory;
import org.teiid.connector.metadata.runtime.RuntimeMetadata;
import org.teiid.connector.metadata.runtime.Table;
import org.teiid.resource.ConnectorException;
import org.teiid.resource.adapter.BasicExecutionFactory;
import org.teiid.resource.cci.ExecutionContext;
import org.teiid.resource.cci.MetadataProvider;
import org.teiid.resource.cci.ResultSetExecution;
import org.teiid.resource.cci.TranslatorProperty;
import org.teiid.resource.cci.TypeFacility;


/**
 * Implementation of text connector.
 */
public class TextExecutionFactory extends BasicExecutionFactory implements MetadataProvider{

	private boolean enforceColumnCount = false;
	private String dateResultFormatsDelimiter;
	private String dateResultFormats;
	
	@Override
	public void start() throws ConnectorException {
		super.start();
    }

	@TranslatorProperty(name="EnforceColumnCount", display="Enforce Column Count",description="This forces the number of columns in text file to match what was modeled", defaultValue="false")
	public boolean isEnforceColumnCount() {
		return enforceColumnCount;
	}

	public void setEnforceColumnCount(Boolean enforceColumnCount) {
		this.enforceColumnCount = enforceColumnCount.booleanValue();
	}
	
	@TranslatorProperty(name="DateResultFormatsDelimiter", display="Date Result Formats Delimiter", advanced=true)
	public String getDateResultFormatsDelimiter() {
		return dateResultFormatsDelimiter;
	}

	public void setDateResultFormatsDelimiter(String dateResultFormatsDelimiter) {
		this.dateResultFormatsDelimiter = dateResultFormatsDelimiter;
	}

	@TranslatorProperty(name="DateResultFormats", display="Date Result Formats",advanced=true)
	public String getDateResultFormats() {
		return dateResultFormats;
	}

	public void setDateResultFormats(String dateResultFormats) {
		this.dateResultFormats = dateResultFormats;
	}	
	
    @Override
    public ResultSetExecution createResultSetExecution(QueryExpression command, ExecutionContext executionContext, RuntimeMetadata metadata, Object connectionFactory)
    		throws ConnectorException {
    	try {
			ConnectionFactory cf = (ConnectionFactory)connectionFactory;
			
			return new TextSynchExecution(this, (Select)command, (TextConnection)cf.getConnection());
		} catch (ResourceException e) {
			throw new ConnectorException(e);
		}
    }

	@Override
	public void getConnectorMetadata(MetadataFactory metadataFactory, Object connectionFactory) throws ConnectorException {
		TextConnection conn;
		try {
			ConnectionFactory cf = (ConnectionFactory)connectionFactory;
			conn = (TextConnection)cf.getConnection();
		} catch (ResourceException e) {
			throw new ConnectorException(e);
		}
		
		for (Map.Entry<String, Properties> entry : conn.getMetadataProperties().entrySet()) {
			Properties p = entry.getValue();
			String columns = p.getProperty(TextDescriptorPropertyNames.COLUMNS);
			if (columns == null) {
				continue;
			}
			String types = p.getProperty(TextDescriptorPropertyNames.TYPES);
			String[] columnNames = columns.trim().split(","); //$NON-NLS-1$
			String[] typeNames = null; 
			if (types != null) {
				typeNames = types.trim().split(","); //$NON-NLS-1$
				if (typeNames.length != columnNames.length) {
					throw new ConnectorException(TextPlugin.Util.getString("TextConnector.column_mismatch", entry.getKey())); //$NON-NLS-1$
				}
			}
			Table table = metadataFactory.addTable(entry.getKey().substring(entry.getKey().indexOf('.') + 1));
			for (int i = 0; i < columnNames.length; i++) {
				String type = typeNames == null?TypeFacility.RUNTIME_NAMES.STRING:typeNames[i].trim().toLowerCase();
				Column column = metadataFactory.addColumn(columnNames[i].trim(), type, table);
				column.setNameInSource(String.valueOf(i));
				column.setNativeType(TypeFacility.RUNTIME_NAMES.STRING);
			}
		}
	} 

}
