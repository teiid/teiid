/*
 * Copyright (c) 2000-2007 MetaMatrix, Inc.
 * All rights reserved.
 */
package org.teiid.test.framework.connection;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

import org.teiid.connector.jdbc.JDBCPropertyNames;
import org.teiid.test.framework.exception.QueryTestFailedException;
import org.teiid.test.framework.exception.TransactionRuntimeException;

import com.metamatrix.jdbc.api.ExecutionProperties;



/** 
 * The DriverConnection strategy that can get connections in standalone mode
 * or embedded mode. 
 */
public class DriverConnection extends ConnectionStrategy{
	
	   public static final String DS_USER = "user"; //$NON-NLS-1$
	    
	    // need both user variables because Teiid uses 'user' and connectors use 'username'
	    public static final String DS_USERNAME = JDBCPropertyNames.USERNAME; //$NON-NLS-1$
	    public static final String DS_PASSWORD = JDBCPropertyNames.PASSWORD;     //$NON-NLS-1$
	    
	    // the driver is only used for making direct connections to the source, the 
	    // connector type will provide the JDBCPropertyNames.CONNECTION_SOURCE driver class
	    public static final String DS_DRIVER = "driver"; //$NON-NLS-1$
	 
	    public static final String DS_URL = JDBCPropertyNames.URL;     //$NON-NLS-1$
	    public static final String DS_APPLICATION_NAME = "application-name"; //$NON-NLS-1$
	    
     
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

        System.out.println("Creating Driver Connection: \"" + url + "\""); //$NON-NLS-1$ //$NON-NLS-2$

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
        } catch (Throwable t) {
        	t.printStackTrace();
        	throw new QueryTestFailedException(t.getMessage());
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
