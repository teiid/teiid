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

package org.teiid.resource.adapter.yahoo;

import org.teiid.connector.language.QueryExpression;
import org.teiid.connector.language.Select;
import org.teiid.connector.metadata.runtime.RuntimeMetadata;
import org.teiid.resource.ConnectorException;
import org.teiid.resource.adapter.BasicExecutionFactory;
import org.teiid.resource.cci.ConnectorCapabilities;
import org.teiid.resource.cci.ExecutionContext;
import org.teiid.resource.cci.ResultSetExecution;

public class YahooExecutionFactory extends BasicExecutionFactory {

    @Override
    public void start() throws ConnectorException {
    }

    @Override
    public Class<? extends ConnectorCapabilities> getDefaultCapabilities() {
    	return YahooCapabilities.class;
    }
    
    @Override
    public ResultSetExecution createResultSetExecution(QueryExpression command, ExecutionContext executionContext, RuntimeMetadata metadata, Object connectionFactory)
    		throws ConnectorException {
    	return new YahooExecution((Select)command, metadata);
    }    
}
