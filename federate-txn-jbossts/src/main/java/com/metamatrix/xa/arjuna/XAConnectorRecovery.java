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

package com.metamatrix.xa.arjuna;

import java.sql.SQLException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import javax.transaction.xa.XAResource;

import com.arjuna.ats.jta.recovery.XAResourceRecovery;

import com.metamatrix.data.api.SecurityContext;
import com.metamatrix.data.exception.ConnectorException;
import com.metamatrix.data.xa.api.XAConnection;
import com.metamatrix.data.xa.api.XAConnector;
import com.metamatrix.dqp.internal.datamgr.impl.ExecutionContextImpl;


/** 
 * Recovery module responsible for finding and supplying the all XAResources
 * in the MetaMatrix system for recovery use.
 */
public class XAConnectorRecovery implements XAResourceRecovery {
    private static Map resourceRegistry = Collections.synchronizedMap(new HashMap());

    Iterator connectorIter = null;
    Map connectionsHeld = new HashMap();
    
    /** 
     * @see com.arjuna.ats.jta.recovery.XAResourceRecovery#getXAResource()
     */
    public XAResource getXAResource() throws SQLException {
        synchronized (resourceRegistry) {
            String connectorName = (String)connectorIter.next();
            
            XAConnection conn = null;
            try {
                 conn = (XAConnection)this.connectionsHeld.get(connectorName);
                if (conn == null) {
                    conn = getNewConnection(connectorName);
                    this.connectionsHeld.put(connectorName, conn);
                }
                
                return conn.getXAResource();
            } catch (ConnectorException e) {
                if (conn != null) {
                    conn.release();
                    this.connectionsHeld.remove(connectorName);
                }
                // todo: log exception
                throw new SQLException(e.getMessage());
            }
        }
    }

    /** 
     * @see com.arjuna.ats.jta.recovery.XAResourceRecovery#hasMoreResources()
     */
    public boolean hasMoreResources() {
        boolean hasNext = false;
        
        synchronized (resourceRegistry) {
            if (this.connectorIter != null) {
                hasNext = this.connectorIter.hasNext();
            }
            
            // we are done with current iteration; get ready for next next scan
            if (!hasNext) {
                Map map = new HashMap(resourceRegistry);
                this.connectorIter = map.keySet().iterator();
            }
        }
        return hasNext;
    }

    /** 
     * @see com.arjuna.ats.jta.recovery.XAResourceRecovery#initialise(java.lang.String)
     */
    public boolean initialise(String p) throws SQLException {
        return true;
    }
    
    public static void addConnector(String name, XAConnector connector) {
        resourceRegistry.put(name, connector);
    }
    
    public static void removeConnector(String name) {
        resourceRegistry.remove(name);
    }
        
    private XAConnection getNewConnection(String connectorName) throws ConnectorException {
        XAConnector connector = (XAConnector)resourceRegistry.get(connectorName);

        SecurityContext context = new ExecutionContextImpl("none", "1", "internal", null, null, "internal", connectorName,"12.0.1","internal", "0", false); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$ //$NON-NLS-5$ //$NON-NLS-6$ //$NON-NLS-7$
        return (XAConnection)connector.getXAConnection(context, null);
    }
}
