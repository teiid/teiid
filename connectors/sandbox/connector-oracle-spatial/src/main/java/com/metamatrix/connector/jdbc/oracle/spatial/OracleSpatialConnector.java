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

/*
 * Created on Jul 13, 2005
 *
 * TODO To change the template for this generated file go to
 * Window - Preferences - Java - Code Style - Code Templates
 */
package com.metamatrix.connector.jdbc.oracle.spatial;

import org.teiid.connector.api.ConnectorCapabilities;
import org.teiid.connector.api.ConnectorEnvironment;
import org.teiid.connector.api.ConnectorException;
import org.teiid.connector.jdbc.JDBCConnector;


public class OracleSpatialConnector extends JDBCConnector {
    
	@Override
	public void start(ConnectorEnvironment environment)
			throws ConnectorException {
		super.start(environment);
        
        environment.getLogger().logInfo(Messages.getString("OracleSpatialConnector.Connector_initialized")); //$NON-NLS-1$
        environment.getLogger().logTrace("Connector init properties: " + environment.getProperties()); //$NON-NLS-1$
    }
    
    @Override
    public ConnectorCapabilities getCapabilities() {
    	return new OracleSpatialCapabilities();
    }

}
