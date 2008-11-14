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

package com.metamatrix.common.jdbc.db;

import java.util.Map;
import java.sql.Statement;
import java.sql.Connection;
import java.sql.SQLException;

import com.metamatrix.common.jdbc.JDBCPlatform;
import com.metamatrix.common.jdbc.syntax.ExpressionOperator;

public class SybasePlatform extends JDBCPlatform {

    public SybasePlatform() {
        super();
    }

    public boolean isSybase() {
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
    			statement.executeQuery("Select 'x'"); //$NON-NLS-1$
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
    
     protected Map buildPlatformOperators() {
        Map operators = super.buildPlatformOperators();

        // override for Sybase specfic
        addOperator(ExpressionOperator.simpleFunction("toUpperCase","UPPER")); //$NON-NLS-1$ //$NON-NLS-2$

        return operators;
      }       
} 
