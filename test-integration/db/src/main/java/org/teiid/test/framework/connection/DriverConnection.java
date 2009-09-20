/*
 * Copyright (c) 2000-2007 MetaMatrix, Inc.
 * All rights reserved.
 */
package org.teiid.test.framework.connection;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

import org.teiid.test.framework.exception.QueryTestFailedException;
import org.teiid.test.framework.exception.TransactionRuntimeException;



/** 
 * The DriverConnection strategy that can get connections in standalone mode
 * or embedded mode. 
 */
public class DriverConnection extends ConnectionStrategy{
     
    private String url = null;
    private String driver = null;
    private String username = null;
    private String pwd = null;
    
    private Connection connection;

        
    public DriverConnection(Properties props) throws QueryTestFailedException {
    	   super(props);
    	   validate();
    }
    
	public void validate()  {

    	String urlProp = this.getEnvironment().getProperty(DS_URL);
    	if (urlProp == null || urlProp.length() == 0) {
    		throw new TransactionRuntimeException("Property " + DS_URL + " was not specified");
    	}
       StringBuffer urlSB = new StringBuffer(urlProp);
        
       String appl = this.getEnvironment().getProperty(DS_APPLICATION_NAME);
        if (appl != null) {
            urlSB.append(";");
            urlSB.append("ApplicationName").append("=").append(appl);
        }
        
        url = urlSB.toString();
        
        driver = this.getEnvironment().getProperty(DS_DRIVER);
    	if (driver == null || driver.length() == 0) {
    		throw new TransactionRuntimeException("Property " + DS_DRIVER + " was not specified");
    	}
    	
    	   // need both user variables because Teiid uses 'user' and connectors use 'username'

    	this.username = this.getEnvironment().getProperty(DS_USER);
    	if (username == null) {
    		this.username = this.getEnvironment().getProperty(DS_USERNAME);
    	}
    	this.pwd = this.getEnvironment().getProperty(DS_PASSWORD);
    	
        try {
            // Load jdbc driver
            Class.forName(driver);
        } catch (ClassNotFoundException e) {
        	throw new TransactionRuntimeException(e);
        }
       	
	}

	public synchronized Connection getConnection() throws QueryTestFailedException {
        if (this.connection != null) {
        	try {
				if (!this.connection.isClosed()) {
					return this.connection;
				}
			} catch (SQLException e) {

			}
        	
        }
		
        this.connection = getJDBCConnection(this.driver, this.url, this.username, this.pwd); 
        return this.connection;
    }    
   
    
    private Connection getJDBCConnection(String driver, String url, String user, String passwd) throws QueryTestFailedException {

        System.out.println("Creating Connection: \"" + url + "\""); //$NON-NLS-1$ //$NON-NLS-2$

        Connection conn;
        try {
            // Create a connection
        	if (user != null && user.length() > 0) {
        		conn = DriverManager.getConnection(url, user, passwd); 
        	} else {
        		conn = DriverManager.getConnection(url);
        	}
        	
      
        } catch (SQLException e) {
        	throw new QueryTestFailedException(e);
        }
        return conn;
      
    }
            
    public void shutdown() {
    	if (this.connection != null) {
    		try {
    			this.connection.close();
    		} catch (Exception e) {
				//ignore
			}
       	}
    	
    	this.connection = null;
           
    }
}
