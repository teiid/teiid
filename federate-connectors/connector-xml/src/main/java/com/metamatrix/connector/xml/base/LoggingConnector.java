/*
 * JBoss, Home of Professional Open Source.
 * Copyright (C) 2008 Red Hat, Inc.
 * Copyright (C) 2000-2007 MetaMatrix, Inc.
 * Licensed to Red Hat, Inc. under one or more contributor 
 * license agreements.  See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
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

import com.metamatrix.connector.xml.XMLConnectorState;
import com.metamatrix.data.api.ConnectorEnvironment;
import com.metamatrix.data.api.ConnectorLogger;
import com.metamatrix.data.exception.ConnectorException;

/**
 * @author JChoate
 *
 * TODO To change the template for this generated type comment go to
 * Window - Preferences - Java - Code Style - Code Templates
 */
public abstract class LoggingConnector implements StatefulConnector {

    private static ConnectorLogger m_logger;
    private ConnectorEnvironment m_environment;
    protected XMLConnectorState m_state;
    
    /* (non-Javadoc)
	 * @see com.metamatrix.connector.xml.base.StatefulConnector#initialize(com.metamatrix.data.api.ConnectorEnvironment)
	 */
    public void initialize(ConnectorEnvironment environment) throws ConnectorException {
        try {
        	m_logger = environment.getLogger();
        	m_environment = environment;
        	m_state = createState(m_environment);
        }
        catch (RuntimeException e) {
        	throw new ConnectorException(e);
        }
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
