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
package org.teiid.rhq.comm.impl;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.teiid.rhq.comm.Component;
import org.teiid.rhq.comm.Connection;
import org.teiid.rhq.comm.ConnectionException;
import org.teiid.rhq.comm.ConnectionPool;
import org.teiid.rhq.comm.ExecutedResult;



/** 
 */
public class InvalidConnectionImpl implements
                                    Connection,
                                    TeiidConnectionConstants {


	private static final Log LOG = LogFactory.getLog(InvalidConnectionImpl.class);

    private static final Object METRIC=new Double(0);
    private static final String EMPTY_STRING="";  //$NON-NLS-1$

    
    private String key = EMPTY_STRING;
    private Properties environmentProps = null;
    private ConnectionPool connectionPool = null;
    
    public InvalidConnectionImpl (final String key,
                                final Properties envProps,
                                final ConnectionPool pool) {
        this.key = key;
        this.environmentProps = envProps;       
        this.connectionPool = pool;
    }
    
    public boolean isValid() {
        return false;
    }
    /** 
     * @see org.teiid.rhq.comm.Connection#isAlive()
     * @since 1.0
     */
    public boolean isAlive() {
        return false;
    }

    /** 
     * @see org.teiid.rhq.comm.Connection#close()
     * @since 1.0
     */
    public void close() {
        try {
            if (connectionPool != null) {
                 connectionPool.close(this);
             }
                
        } catch (Exception e) {
            LOG.error("Error returning connection to the connection pool", e); //$NON-NLS-1$
        } 
    }
    
    /** 
     * @see org.teiid.rhq.comm.Connection#getMetric(java.lang.String, java.lang.String, java.util.Map)
     * @since 1.0
     */
    public Object getMetric(String componentType,
    						String identifier,
                            String metric,
                            Map valueMap) throws ConnectionException {
        return METRIC;
    }

    /** 
     * @see org.teiid.rhq.comm.Connection#executeOperation(java.lang.String, java.lang.String, java.util.Map)
     * @since 1.0
     */
    public void executeOperation(ExecutedResult operationResult,
    							 Map argumentMap) throws ConnectionException {       
    }

    /** 
     * @see org.teiid.rhq.comm.Connection#isAvailable(java.lang.String, java.lang.String)
     * @since 1.0
     */
    public Boolean isAvailable(String componentType,
                               String identifier) throws ConnectionException {
        return false;
    }
    
    

    /** 
     * @see org.teiid.rhq.comm.Connection#getProperties(java.lang.String, java.lang.String)
     * @since 1.0
     */
    public Properties getProperties(String resourceType,
                                    String identifier) throws ConnectionException {
        return new Properties();
    }

    /** 
     * @see org.teiid.rhq.comm.Connection#getProperty(java.lang.String, java.lang.String)
     * @since 1.0
     */
    public String getProperty(String identifier,
                              String property) throws ConnectionException {
        return EMPTY_STRING;
    }

    /** 
     * @see org.teiid.rhq.comm.Connection#getKey()
     * @since 1.0
     */
    public String getKey() throws Exception {
        return key;
    }

    /** 
     * @see org.teiid.rhq.comm.Connection#getConnectors(java.lang.String)
     * @since 1.0
     */
    public Collection<Component> getConnectors(String vmname) throws ConnectionException {
        return Collections.EMPTY_LIST;
    }

    
    public Collection<Component> getVMs(String hostname) throws ConnectionException {
        return Collections.EMPTY_LIST;
    }    
    
    public Collection<Component> getVDBs(List fieldNameList) throws ConnectionException {
		return Collections.EMPTY_LIST;
	}

    
    public Collection getAllHosts() throws ConnectionException {
        return Collections.EMPTY_LIST;
    }

	public Component getHost(String identifier) throws ConnectionException {
		// TODO Auto-generated method stub
		return null;
	}
    
    public Collection<Component> getConnectorsForConfig(String vmidentifier)
	throws ConnectionException {
// TODO Auto-generated method stub
return Collections.EMPTY_LIST;
}

public Collection<Component> getServices(String vmIdentifier)
	throws ConnectionException {
// TODO Auto-generated method stub
return Collections.EMPTY_LIST;
}

public Collection<Component> getServicesForConfig(String identifier)
	throws ConnectionException {
// TODO Auto-generated method stub
return Collections.EMPTY_LIST;
}

public Collection<Component> discoverComponents(String componentType,
		String identifier) throws ConnectionException {
	// TODO Auto-generated method stub
	return Collections.EMPTY_LIST;
}  



}
