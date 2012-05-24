/*
 * Copyright (c) 2000-2007 MetaMatrix, Inc.
 * All rights reserved.
 */
package org.teiid.test.framework.connection;

import java.util.Properties;

import org.teiid.test.framework.ConfigPropertyLoader;
import org.teiid.test.framework.ConfigPropertyNames;
import org.teiid.test.framework.TestLogger;
import org.teiid.test.framework.datasource.DataSourceFactory;
import org.teiid.test.framework.exception.TransactionRuntimeException;

/**
 * The ConnectionStrategyFactory is responsible for creating a connection strategy that is to be used to provide the type of 
 * connection.
 * @author vanhalbert
 *
 */
@SuppressWarnings("nls")
public class ConnectionStrategyFactory {
	
    
	    public static ConnectionStrategy createConnectionStrategy()  {

	    	ConfigPropertyLoader configLoader = ConfigPropertyLoader.getInstance();
		
			ConnectionStrategy strategy = null;
	     	Properties props = configLoader.getProperties();
           
	        String type = props.getProperty(ConfigPropertyNames.CONNECTION_TYPE, ConfigPropertyNames.CONNECTION_TYPES.DRIVER_CONNECTION);
	        if (type == null) {
	        	throw new TransactionRuntimeException("Property " + ConfigPropertyNames.CONNECTION_TYPE + " was specified");
	        }
	        
	        try {
	        
        	        if (type.equalsIgnoreCase(ConfigPropertyNames.CONNECTION_TYPES.DRIVER_CONNECTION)) {
        	        	// pass in null to create new strategy
        	                strategy = new DriverConnection(props);
        	                TestLogger.logDebug("Created Driver Strategy");
        	        }
        	        else if (type.equalsIgnoreCase(ConfigPropertyNames.CONNECTION_TYPES.DATASOURCE_CONNECTION)) {
        	            strategy = new DataSourceConnection(props);
        	            TestLogger.logDebug("Created DataSource Strategy");
        	        }
        	        else if (type.equalsIgnoreCase(ConfigPropertyNames.CONNECTION_TYPES.JNDI_CONNECTION)) {
        	            strategy = new JEEConnection(props);
        	            TestLogger.logDebug("Created JEE Strategy");
        	        }   
        	        
        	        if (strategy == null) {
        	        	new TransactionRuntimeException("Invalid property value for " + ConfigPropertyNames.CONNECTION_TYPE + " is " + type );
        	        }
        	        // call configure here because this is creating the connection to Teiid
        	        // direct connections to the datasource use the static call directly to create strategy and don't need to configure
        	    	strategy.configure();
        	        return strategy;
	        } catch (Exception e) {
	            throw new TransactionRuntimeException(e);
	        }

	    }	    
	    
		public static void main(String[] args) {
			//NOTE: to run this test to validate the DataSourceMgr, do the following:
			//   ---  need 3 datasources,   Oracle, SqlServer and 1 other
			
			ConfigPropertyLoader config = ConfigPropertyLoader.getInstance();
			
			new DataSourceFactory(config);

		}
}
