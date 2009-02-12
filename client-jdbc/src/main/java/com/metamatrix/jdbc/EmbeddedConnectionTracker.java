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

package com.metamatrix.jdbc;

import java.net.URL;
import java.sql.Connection;
import java.util.HashSet;
import java.util.Set;


/** 
 * This object keeps track of connections for a given dqp instance which is
 * identified by the "dqpURL". The main responsibility of this listener is to
 * take down the DQP instance when the connection count to the DQP goes down
 * to zero.
 */
class EmbeddedConnectionTracker implements ConnectionListener {

    private URL dqpURL = null;
    private EmbeddedDriver dqpDriver = null;
    private Set connections = new HashSet();
    
    
    public EmbeddedConnectionTracker(URL dqpURL, EmbeddedDriver dqpDriver) {
        this.dqpURL = dqpURL;
        this.dqpDriver = dqpDriver;
    }

    public void connectionAdded(String id, Connection connection) {
        synchronized (connections) {
            connections.add(connection);            
        }
    }
    
    public void connectionRemoved(String id, Connection connection) {
        synchronized (connections) {
            connections.remove(connection);
            if(connections.size() == 0) {
                dqpDriver.shutdown(dqpURL);
            }
        }
    }
    
    public Set getConnections() {
        Set tempSet = null;
        synchronized (connections) {
            tempSet = new HashSet(connections);
        }
        return tempSet;
    }
    
}
