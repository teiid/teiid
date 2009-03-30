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

import java.util.HashMap;
import java.util.Map;

import org.teiid.connector.api.ConnectorEnvironment;
import org.teiid.connector.api.ConnectorException;
import org.teiid.connector.api.ExecutionContext;
import org.teiid.connector.api.ResultSetExecution;
import org.teiid.connector.basic.BasicConnection;
import org.teiid.connector.language.IQuery;
import org.teiid.connector.language.IQueryCommand;
import org.teiid.connector.metadata.runtime.RuntimeMetadata;


/**
 * Implementation of Connection interface for text connection.
 */
public class TextConnection extends BasicConnection {

    // metadata props -- Map<groupName --> Map<propName, propValue>
    Map metadataProps = new HashMap();

    // connector props
    ConnectorEnvironment env;

    /**
     * Constructor.
     * @param env
     */
    TextConnection(ConnectorEnvironment env, Map metadataProps) throws ConnectorException {
    	this.env = env;
        this.metadataProps = metadataProps;
    }

    @Override
    public ResultSetExecution createResultSetExecution(IQueryCommand command,
    		ExecutionContext executionContext, RuntimeMetadata metadata)
    		throws ConnectorException {
    	return new TextSynchExecution((IQuery)command, this, metadata);
    }

    @Override
    public void close() {
        metadataProps = null;
        env.getLogger().logDetail("Text Connection is successfully closed."); //$NON-NLS-1$
    }
}
