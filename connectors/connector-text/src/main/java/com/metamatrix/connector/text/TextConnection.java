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

package com.metamatrix.connector.text;

import java.util.Map;
import java.util.Properties;

import org.teiid.connector.api.ConnectorException;
import org.teiid.connector.api.ExecutionContext;
import org.teiid.connector.api.MetadataProvider;
import org.teiid.connector.api.ResultSetExecution;
import org.teiid.connector.api.TypeFacility;
import org.teiid.connector.basic.BasicConnection;
import org.teiid.connector.language.Select;
import org.teiid.connector.language.QueryExpression;
import org.teiid.connector.metadata.runtime.Column;
import org.teiid.connector.metadata.runtime.MetadataFactory;
import org.teiid.connector.metadata.runtime.RuntimeMetadata;
import org.teiid.connector.metadata.runtime.Table;


/**
 * Implementation of Connection interface for text connection.
 */
public class TextConnection extends BasicConnection implements MetadataProvider {

    // metadata props -- Map<groupName --> Map<propName, propValue>
	Map <String, Properties> metadataProps;

    // connector props
	private TextManagedConnectionFactory config;

    /**
     * Constructor.
     * @param env
     */
    TextConnection(TextManagedConnectionFactory env, Map metadataProps) {
    	this.config = env;
        this.metadataProps = metadataProps;
    }

    @Override
    public ResultSetExecution createResultSetExecution(QueryExpression command, ExecutionContext executionContext, RuntimeMetadata metadata)
    		throws ConnectorException {
    	return new TextSynchExecution(this.config, (Select)command, this.metadataProps);
    }

    @Override
    public void close() {
        metadataProps = null;
    }
    
	@Override
	public void getConnectorMetadata(MetadataFactory metadataFactory) throws ConnectorException {
		for (Map.Entry<String, Properties> entry : this.metadataProps.entrySet()) {
			Properties p = entry.getValue();
			String columns = p.getProperty(TextPropertyNames.COLUMNS);
			if (columns == null) {
				continue;
			}
			String types = p.getProperty(TextPropertyNames.TYPES);
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
