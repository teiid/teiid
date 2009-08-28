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

package com.metamatrix.jdbc.api;


import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Stack;

import org.teiid.jdbc.TeiidDriver;


/** 
 * This class can be used as the base class to write Query based tests using
 * the Teiid Driver for integration testing. Just like the scripted one this one should provide all
 * those required flexibility in testing.
 */
public class AbstractMMQueryTestCase extends AbstractQueryTest {
	
	static {
		new TeiidDriver();
	}
	
    private Stack<java.sql.Connection> contexts = new Stack<java.sql.Connection>();
    
    public void pushConnection() {
    	this.contexts.push(this.internalConnection);
    	this.internalConnection = null;
    }
    
    public void popConnection() {
    	this.internalConnection = this.contexts.pop();
    }
    
     public com.metamatrix.jdbc.api.Connection getConnection(String vdb, String propsFile){
        return getConnection(vdb, propsFile, "");  //$NON-NLS-1$
    }
    
    public com.metamatrix.jdbc.api.Connection getConnection(String vdb, String propsFile, String addtionalStuff){
    	closeResultSet();
    	closeStatement();
    	closeConnection();
        try {
			this.internalConnection = createConnection(vdb, propsFile, addtionalStuff);
		} catch (SQLException e) {
			throw new RuntimeException(e);
		} 
        return (com.metamatrix.jdbc.api.Connection) this.internalConnection;
    }
    
    public static com.metamatrix.jdbc.api.Connection createConnection(String vdb, String propsFile, String addtionalStuff) throws SQLException {
        String url = "jdbc:metamatrix:"+vdb+"@" + propsFile+addtionalStuff; //$NON-NLS-1$ //$NON-NLS-2$
        return (com.metamatrix.jdbc.api.Connection)DriverManager.getConnection(url); 
    }    
        
      
    protected void helpTest(String query, String[] expected, String vdb, String props, String urlProperties) {
        getConnection(vdb, props, urlProperties);
        executeAndAssertResults(query, expected);
        closeConnection();
    }
 
}