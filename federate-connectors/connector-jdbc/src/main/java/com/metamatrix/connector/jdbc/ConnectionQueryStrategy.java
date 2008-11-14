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

/*
 */
package com.metamatrix.connector.jdbc;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Connection strategy that tests a connection by running a test query. 
 */
public class ConnectionQueryStrategy implements ConnectionStrategy{
    private String connTestquery;
    private long prevTime;
    
    private boolean isFailed = false;
    private int queryInterval;
        
    public ConnectionQueryStrategy(String query, int queryInterval){
        this.connTestquery = query;
        prevTime = System.currentTimeMillis();
        this.queryInterval = queryInterval;
    }    
    
    /**
     * Set the interval after which to run another test query.
	 * @param interval in ms.
     */
    public void setQueryInterval(int queryInterval) {
        this.queryInterval = queryInterval;
    }
    
    
    /**
     * @see com.metamatrix.connector.jdbc.ConnectionStrategy#isConnectionAlive()
     */
    public boolean isConnectionAlive(Connection connection) {
        try {
            if(connection.isClosed()){
                return false;
            } 
            
            return executeTestQuery(connection);
        } catch(SQLException e) {
            isFailed = true;
            return false;
        }
    }
    
    
    public boolean isConnectionFailed(Connection connection) {
        try {
            if(connection.isClosed()){
                return isFailed;
            } 
            
            return (! executeTestQuery(connection));
        } catch(SQLException e) {
            isFailed = true;
            return true;
        }
    }

    
    /**
     * Executes a test query to see if the data source is available.
     * @param connection Should not be closed.
     * @return whether it succeeds
     */
    private boolean executeTestQuery(Connection connection) {
        Statement statement = null;
        try {
            long currentTime = System.currentTimeMillis();
            //execute a test query if the interval has passed since the last query
            if(currentTime - prevTime > queryInterval){
                prevTime = currentTime;          
                statement = connection.createStatement();
                statement.executeQuery(connTestquery);
            }
            isFailed = false;
            return true;
        } catch(SQLException e) {
           isFailed = true;
            return false;
        } finally {
            if ( statement != null ) {
                try {
                    statement.close();
                    statement=null;
                } catch ( SQLException e ) {
                }
            }
        }
    }

}
