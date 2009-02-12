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

package com.metamatrix.common.jdbc.db;

import java.sql.Statement;
import java.sql.Connection;
import java.sql.SQLException;
import com.metamatrix.common.jdbc.JDBCPlatform;

public class InformixPlatform  extends JDBCPlatform {

	/**
	 * Constructor for InformixPlatform.
	 */
	public InformixPlatform() {
		super();
	}
	
    public boolean isInformix() {
        return true;
    }

    public boolean isDefault() {
        return false;
    }
    
    public boolean isClosed(Connection connection) {
    	if(!super.isClosed(connection)) {
            Statement statement = null;
            try {
                statement = connection.createStatement();
    			statement.executeQuery("select 'x' from informix.sysusers where 1=0"); //$NON-NLS-1$
    			return false;
    		} catch(SQLException e) {
    			return true;	
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
    	return true;
    }
}
