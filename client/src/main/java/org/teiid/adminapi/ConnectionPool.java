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

package org.teiid.adminapi;


/** 
 * This object holds the statisics for a ConnectionPool that is being utilized by a Connector.
 * As per how many available connections
 * processed etc.
 * <p>An identifier for QueueWorkerPool, is nothing but the modules it self, like "DQP", 
 * "QueryService" or Connector Binding names etc.</p> 
 * 
 * @since 4.3
 */
public interface ConnectionPool extends AdminObject {
    /** 
     * @return Returns total number of current connections in the Connection Pool 
     * @since 6.1
     */
    public int getTotalConnections();
    
    /** 
     * @return Returns the number of connections waiting for use in the connection pool. 
     * @since 6.1
     */
    public int getConnectionsWaiting();
    
    /** 
     * @return Returns the number of Connections currently in use by clients. 
     * 
     * @since 6.1
     */
    public int getConnectionsInuse();
    
    /** 
     * @return Returns the number of Connections created since the Connection Pool was created. 
     * @since 6.1
     */
    long getConnectionsCreated();
    
    
    /**
     * @return The number of Connections destroyed since the Connection Pool was created. 
     */
    long getConnectionsDestroyed();
    


}
