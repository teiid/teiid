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


import java.util.Properties;

import org.teiid.rhq.comm.Connection;
import org.teiid.rhq.comm.ConnectionException;
import org.teiid.rhq.comm.ConnectionFactory;
import org.teiid.rhq.comm.ConnectionPool;




public class FakeConnectionFactory implements
		ConnectionFactory {
    
    static boolean createdfactory = false;
	private static Properties connEnv;
    private ConnectionPool pool;
	
	public Connection createConnection(){

        return new FakeConnection(pool, connEnv);
           
	}
	
	


	@Override
	public String getURL() {
		// TODO Auto-generated method stub
		return null;
	}




	@Override
	public void initialize(Properties env, ConnectionPool connectionPool)
			throws ConnectionException {
			connEnv = env;
	        this.pool = connectionPool;
	        createdfactory = true;
		
	}


    public void closeConnection(Connection connection)  {

    }
    
  
    public String getProperty(String key) {
        return connEnv.getProperty(key);
    }
   

}
