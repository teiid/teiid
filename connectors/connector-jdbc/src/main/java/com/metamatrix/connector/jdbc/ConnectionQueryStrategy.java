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
        
    public ConnectionQueryStrategy(String query){
        this.connTestquery = query;
    }    
    
    /**
     * @see com.metamatrix.connector.jdbc.ConnectionStrategy#isConnectionAlive()
     */
    public boolean isConnectionAlive(Connection connection) {
        Statement statement = null;
    	try {
            if(connection.isClosed()){
                return false;
            } 
            statement = connection.createStatement();
            statement.executeQuery(connTestquery);
        } catch(SQLException e) {
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
        return true;
    }
    
}
