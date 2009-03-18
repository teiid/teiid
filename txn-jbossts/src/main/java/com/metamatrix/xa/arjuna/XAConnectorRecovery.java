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

package com.metamatrix.xa.arjuna;

import java.sql.SQLException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import javax.transaction.xa.XAResource;

import org.teiid.dqp.internal.transaction.TransactionProvider.XAConnectionSource;

import com.arjuna.ats.jta.recovery.XAResourceRecovery;


/** 
 * Recovery module responsible for finding and supplying the all XAResources
 * in the MetaMatrix system for recovery use.
 */
public class XAConnectorRecovery implements XAResourceRecovery {
    private static Map<String, XAConnectionSource> resourceRegistry = Collections.synchronizedMap(new HashMap<String, XAConnectionSource>());

    private Iterator connectorIter = null;
    private Map<String, XAConnectionSource> connectionsHeld = new HashMap<String, XAConnectionSource>();
    
    /** 
     * @see com.arjuna.ats.jta.recovery.XAResourceRecovery#getXAResource()
     */
    public XAResource getXAResource() throws SQLException {
        synchronized (resourceRegistry) {
            String connectorName = (String)connectorIter.next();
            
            XAConnectionSource conn = null;
            try {
                 conn = this.connectionsHeld.get(connectorName);
                if (conn == null) {
                	conn = resourceRegistry.get(connectorName);
                    this.connectionsHeld.put(connectorName, conn);
                }
                
                return conn.getXAResource();
            } catch (SQLException e) {
                if (conn != null) {
                    conn.close();
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
    
    public static void addConnector(String name, XAConnectionSource connector) {
        resourceRegistry.put(name, connector);
    }
    
    public static void removeConnector(String name) {
        resourceRegistry.remove(name);
    }
        
}
