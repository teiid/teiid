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

package com.metamatrix.connector.xml.base;

import org.teiid.connector.api.ConnectorEnvironment;
import org.teiid.connector.api.ConnectorException;
import org.teiid.connector.api.ConnectorLogger;
import org.teiid.connector.basic.BasicConnector;

import com.metamatrix.connector.xml.StatefulConnector;
import com.metamatrix.connector.xml.XMLConnectorState;

public abstract class LoggingConnector extends BasicConnector implements StatefulConnector {

	protected ConnectorLogger m_logger;
    protected ConnectorEnvironment m_environment;
    protected XMLConnectorState m_state;
    
    /* (non-Javadoc)
	 * @see com.metamatrix.connector.xml.base.StatefulConnector#initialize(com.metamatrix.data.api.ConnectorEnvironment)
	 */
    @Override
	public void start(ConnectorEnvironment environment) throws ConnectorException {
    	m_logger = environment.getLogger();
    	m_environment = environment;
    	m_state = createState(m_environment);
    }
    
    /* (non-Javadoc)
	 * @see com.metamatrix.connector.xml.base.StatefulConnector#getLogger()
	 */
    public ConnectorLogger getLogger() {
        return m_logger;
    }
    
    /* (non-Javadoc)
	 * @see com.metamatrix.connector.xml.base.StatefulConnector#getEnvironment()
	 */
    public ConnectorEnvironment getEnvironment() {
        return m_environment;
    }
    
    /* (non-Javadoc)
	 * @see com.metamatrix.connector.xml.base.StatefulConnector#getState()
	 */
    public XMLConnectorState getState() {
		return m_state;
	}
    
	private XMLConnectorState createState(ConnectorEnvironment env) throws ConnectorException {		
		String stateClassName = env.getProperties().getProperty(XMLConnectorState.STATE_CLASS_PROP);
		XMLConnectorState state = null;
		try {
			Class clazz = Thread.currentThread().getContextClassLoader().loadClass(stateClassName);
			state = (XMLConnectorState) clazz.newInstance();
			state.setLogger(this.getLogger());
			state.setState(env);
		} catch (Exception e) {
			ConnectorException ce = new ConnectorException(Messages.getString("XMLConnector.could.not.create.state") //$NON-NLS-1$ 
					+ stateClassName); 
			ce.initCause(e);
			throw ce;
		}
		return state;
	}
	
}
