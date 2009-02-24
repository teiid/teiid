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

package com.metamatrix.connector.exec;
import java.util.List;

import com.metamatrix.connector.api.ConnectorEnvironment;
import com.metamatrix.connector.api.ConnectorException;
import com.metamatrix.connector.api.ConnectorLogger;
import com.metamatrix.connector.api.ExecutionContext;
import com.metamatrix.connector.api.ResultSetExecution;
import com.metamatrix.connector.basic.BasicConnection;
import com.metamatrix.connector.language.IQuery;
import com.metamatrix.connector.language.IQueryCommand;
import com.metamatrix.connector.metadata.runtime.RuntimeMetadata;

public class ExecConnection extends BasicConnection {
    
    ConnectorEnvironment env;
    
    // Connector logger
    ConnectorLogger logger;
    private List exclusionList;
    
    /**
     * Constructor.
     * @param env
     */    
    ExecConnection(ConnectorEnvironment env, List exclusionThese) throws ConnectorException {
        this.env = env;
        this.logger = env.getLogger();
        this.exclusionList = exclusionThese;
    }
    
    @Override
    public ResultSetExecution createResultSetExecution(IQueryCommand command,
    		ExecutionContext executionContext, RuntimeMetadata metadata)
    		throws ConnectorException {
        return new ExecAntExecution((IQuery)command, env, metadata, logger, exclusionList);
    }

    @Override
    public void close() {
        logger.logDetail("Exec Connection is successfully closed."); //$NON-NLS-1$
    }

}
