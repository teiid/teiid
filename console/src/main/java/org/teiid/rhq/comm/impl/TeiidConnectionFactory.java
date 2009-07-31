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

/*
 * The connection factory implementation is located here so that a new version
 * of MetaMatrix that has different connection requirements can be dealt
 * with on a per MM Version.
 */

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.StringTokenizer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.teiid.adminapi.Admin;
import org.teiid.jdbc.TeiidDriver;
import org.teiid.rhq.comm.Connection;
import org.teiid.rhq.comm.ConnectionConstants;
import org.teiid.rhq.comm.ConnectionException;
import org.teiid.rhq.comm.ConnectionFactory;
import org.teiid.rhq.comm.ConnectionPool;

import com.metamatrix.common.comm.platform.client.ServerAdminFactory;
import com.metamatrix.common.jdbc.JDBCUtil;
import com.metamatrix.jdbc.MMConnection;



public class TeiidConnectionFactory implements
		ConnectionFactory {
    private static final Log LOG = LogFactory.getLog(TeiidConnectionFactory.class);

	private Properties connEnv;
    private ConnectionPool pool;
    private String username = null;
    private String password = null;
    private String url = null;
    
    public TeiidConnectionFactory() {
        
    }
    
    public String getURL() {
    	return url;
    }
	
	public Connection createConnection() throws ConnectionException {
		
		Thread currentThread = Thread.currentThread();
		ClassLoader threadContextLoader = currentThread.getContextClassLoader();
		try {
	        currentThread.setContextClassLoader(pool.getClassLoader());
	        
	        MMConnection mmconn = getConnection();
	
	        if (mmconn != null) {
	            ConnectionImpl conn  = new ConnectionImpl(url, connEnv, pool, mmconn);
	            if (conn.isAlive()) {
	                return conn;
	            }
	            
	            conn.closeSource();                       
	        } 
	        
	        return new InvalidConnectionImpl(url, connEnv, pool);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			throw new ConnectionException(e);
		} finally {
			currentThread.setContextClassLoader(threadContextLoader);
		}
            
		
	}


	public void initialize( final Properties env, final ConnectionPool connectionPool) throws ConnectionException {
		this.connEnv = (Properties) env.clone();
        this.pool = connectionPool;
       
        
        if (pool == null || connEnv == null || connEnv.isEmpty()) {
            throw new ConnectionException("ConnectionFactory has not been initialized properly"); //$NON-NLS-1$
        }
       
        
        url = connEnv.getProperty(ConnectionConstants.URL);
        username = connEnv
                .getProperty(ConnectionConstants.USERNAME);
        password = connEnv
                .getProperty(ConnectionConstants.PASSWORD);
        
        
        if (url == null ) {
            throw new ConnectionException("URL is not set"); //$NON-NLS-1$
            
        }
        if (username == null ) {
            throw new ConnectionException("USERNAME is not set"); //$NON-NLS-1$
            
        }
        if (password == null ) {
            throw new ConnectionException("PASSWORD is not set"); //$NON-NLS-1$
            
        }        
	}


    /** 
     * @see org.teiid.rhq.comm.ConnectionFactory#closeConnection(org.teiid.rhq.comm.Connection)
     */
    public void closeConnection(Connection connection)  {
        if (connection instanceof ConnectionImpl) {
            ConnectionImpl conn = (ConnectionImpl) connection;
            conn.closeSource();
        } else {
        
        	connection.close();
        }
    }
    
    private MMConnection getConnection()  {
    	MMConnection conn = null;
        Throwable firstexception = null;

            try {
            	Properties props = new Properties();
            	props.setProperty(JDBCUtil.DATABASE, url);
            	props.setProperty(JDBCUtil.DRIVER, TeiidDriver.class.getName());
            	props.setProperty(JDBCUtil.USERNAME, username);
            	props.setProperty(JDBCUtil.PASSWORD, password);
            	
            	conn = (MMConnection) JDBCUtil.createJDBCConnection(props);
            	

                return conn;
            } catch (Throwable ce) {
                if (firstexception == null) {
                    firstexception = ce;
                }
            }
        
        if (firstexception != null) {
        	firstexception.printStackTrace();
            LOG.error("Unable to connect to JBEDSP System: " + firstexception.getMessage()); //$NON-NLS-1$
        }

        return null;
    }    
       
    /**
     * @param userName
     * @param password
     * @param servers
     * @return Admin
     * @throws Exception,
     *             AdminException
     * @since 5.5.3
     */
//    private Admin getAdminAPI(String pusername, String ppassword, String purl)
//            throws Exception {
//      
//        String servers = null;
//                
//        com.metamatrix.common.comm.platform.client.ServerAdminFactory factory = null;
// 
//        Admin serverAdmin = null;
//        
//        try {
//            factory = ServerAdminFactory.getInstance();
//
//            serverAdmin = factory.createAdmin(pusername, ppassword.toCharArray(),                                                   
//                purl, "RHQ"); //$NON-NLS-1$
//             
//        } catch (Throwable ae) {
//                if (ae instanceof MetaMatrixRuntimeException) {
//                    Throwable child = ((MetaMatrixRuntimeException) ae).getChild();
//                    if (child.getMessage().indexOf("Read timed out") > 0) { //$NON-NLS-1$ 
//                        try {
//                            
//                            System.setProperty("com.metamatrix.ssl.trustStore", key + "/client/metamatrix.truststore"); //$NON-NLS-1$  //$NON-NLS-2$ 
//                            servers = MMConnectionConstants.SSL_PROTOCOL + iNetAddress.getHostName() + ":" + pport; //$NON-NLS-1$ 
//                            serverAdmin = factory.createAdmin(pusername, ppassword.toCharArray(),                                                   
//                                                          servers, "RHQ");//$NON-NLS-1$ 
//                        } catch (Exception ae2) {
//                            throw ae;
//                        }
//                    } else {
//                        throw ae;
//                    }
//                } else {
//                    throw new Exception(ae.getMessage());
//                }

//        }
//        LOG.info("Connected to JBEDSP Server using " + servers);  //$NON-NLS-1$
//
//        this.url = purl;
//
//        return serverAdmin;
//    }
    
    
    
    /*
     * Return the tokens in a string in a list. This is particularly
     * helpful if the tokens need to be processed in reverse order. In that case,
     * a list iterator can be acquired from the list for reverse order traversal.
     *
     * @param str String to be tokenized
     * @param delimiter Characters which are delimit tokens
     * @return List of string tokens contained in the tokenized string
     */
    private static List getTokens(String str, String delimiter) {
        ArrayList l = new ArrayList();
        StringTokenizer tokens = new StringTokenizer(str, delimiter);
        while(tokens.hasMoreTokens()) {
            l.add(tokens.nextToken());
        }
        return l;
    }      
  
   

}
