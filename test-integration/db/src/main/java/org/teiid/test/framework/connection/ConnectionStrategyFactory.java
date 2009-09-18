/*
 * Copyright (c) 2000-2007 MetaMatrix, Inc.
 * All rights reserved.
 */
package org.teiid.test.framework.connection;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

import org.teiid.test.framework.ConfigPropertyLoader;
import org.teiid.test.framework.exception.QueryTestFailedException;
import org.teiid.test.framework.exception.TransactionRuntimeException;

public class ConnectionStrategyFactory {
	
	/**
	 * Specify this property to set a specific configuration to use
	 */
		public static final String CONFIG_FILE="config";
		
		/**
		 * The default config file to use when #CONFIG_FILE system property isn't set
		 */
		private static final String DEFAULT_CONFIG_FILE_NAME="default-config.properties";
	
	    private static ConnectionStrategyFactory _instance = null;
	    private ConnectionStrategy strategy = null;
	    private static Map<String, ConnectionStrategy> sources = null;

    
	    
	    private ConnectionStrategyFactory(){
	    }
	    
	    public static synchronized ConnectionStrategyFactory getInstance()   {
	        if (_instance == null) {
	            _instance = new ConnectionStrategyFactory();
	            try {
					_instance.initialize();
				} catch (QueryTestFailedException e) {
					// TODO Auto-generated catch block
					throw new TransactionRuntimeException(e);
				}
	        }
	        return _instance;
	    }
	    
	    private static void init() {
	    	if (sources == null) {
	    		sources = new HashMap<String, ConnectionStrategy>();
	    	}
	    }
	    
    
	    public static synchronized void destroyInstance() {
	        if (_instance != null) {
	        	_instance.shutdown();
	        	
	            _instance = null;
	            

	        }
	    }    
	    
	    private void shutdown() {
            Properties p = System.getProperties();
            p.remove(CONFIG_FILE);

            
        	for (Iterator it=sources.keySet().iterator(); it.hasNext();  ){
        		
        		ConnectionStrategy cs = sources.get(it.next());
        		try {
        			cs.shutdown();
        		} catch (Exception e) {
        			
        		}
        		
        	}
        	sources.clear();
        	sources = null;
        	
        	strategy.shutdown();
        	strategy = null;
 
	    }
	    
	    public ConnectionStrategy getConnectionStrategy() {
	    	return this.strategy;
	    }
	    
	    private void initialize() throws QueryTestFailedException  {
	        init();
	        this.strategy = create(ConfigPropertyLoader.getProperties());	                     

	    }
	        
	    private ConnectionStrategy create(Properties props) throws QueryTestFailedException  {
	    	
	     	ConnectionStrategy strategy = null;
	                
	        String type = props.getProperty(ConnectionStrategy.CONNECTION_TYPE, ConnectionStrategy.DRIVER_CONNECTION);
	        if (type == null) {
	        	throw new RuntimeException("Property " + ConnectionStrategy.CONNECTION_TYPE + " was specified");
	        }
	        
	        if (type.equalsIgnoreCase(ConnectionStrategy.DRIVER_CONNECTION)) {
	                strategy = createDriverStrategy(null, props);
	                System.out.println("Created Driver Strategy");
	        }
	        else if (type.equalsIgnoreCase(ConnectionStrategy.DATASOURCE_CONNECTION)) {
	            strategy = createDataSourceStrategy(null, props);
	            System.out.println("Created DataSource Strategy");
	        }
	        else if (type.equalsIgnoreCase(ConnectionStrategy.JNDI_CONNECTION)) {
	            strategy = createJEEStrategy(null, props);
	            System.out.println("Created JEE Strategy");
	        }   
	        
	        if (strategy == null) {
	        	new TransactionRuntimeException("Invalid property value for " + ConnectionStrategy.CONNECTION_TYPE + " is " + type );
	        }
	        // call configure here because this is creating the connection to Teiid
	        // direct connections to the datasource use the static call directly to create strategy and don't need to configure
	    	strategy.configure();
	        return strategy;
	    }
	    
	    public synchronized static ConnectionStrategy createDriverStrategy(String identifier, Properties props) throws QueryTestFailedException  {
	     	if (identifier == null) {
	    		return new DriverConnection(props);
	     	}
	     	init();
	     	
	     	ConnectionStrategy strategy = null;
	     	if (sources.containsKey(identifier)) {
	     		strategy = sources.get(identifier);
	     	} else {
	     		strategy = new DriverConnection(props);
	     		sources.put(identifier, strategy);
	     	}	     	
       	
	       	return strategy;
	    
	    }
	    
	    public synchronized static ConnectionStrategy createDataSourceStrategy(String identifier, Properties props) throws QueryTestFailedException  {	     	
	     	if (identifier == null) {
	    		return new DataSourceConnection(props);
	     	}
	     	init();
	     	
	     	ConnectionStrategy strategy = null;
	     	if (sources.containsKey(identifier)) {
	     		strategy = sources.get(identifier);
	     	} else {
	     		strategy = new DataSourceConnection(props);
	     		sources.put(identifier, strategy);
	     	}
	       	
	       	return strategy;
	    
	    }
	    
	    public synchronized static ConnectionStrategy createJEEStrategy(String identifier, Properties props) throws QueryTestFailedException  {
	     	if (identifier == null) {
	    		return new JEEConnection(props);
	     	}
	     	
	     	init();
	     	ConnectionStrategy strategy = null;
	     	if (sources.containsKey(identifier)) {
	     		strategy = sources.get(identifier);
	     	} else {
	     		strategy = new JEEConnection(props);
	     		sources.put(identifier, strategy);
	     	}
	       	
	       	return strategy;
	    
	    }
	    
//		private final Properties loadProperties(String filename) {
//			Properties props = null;
//		    try {
//		        InputStream in = ConnectionStrategyFactory.class.getResourceAsStream("/"+filename);
//		        if (in != null) {
//		        	props = new Properties();
//		        	props.load(in);
//		        	return props;
//		        }
//		        else {
//		        	throw new RuntimeException("Failed to load properties from file '"+filename+ "' configuration file");
//		        }
//		    } catch (IOException e) {
//		        throw new RuntimeException("Error loading properties from file '"+filename+ "'" + e.getMessage());
//		    }
//		}
		
		public static void main(String[] args) {
			ConnectionStrategyFactory cf = ConnectionStrategyFactory.getInstance();

		}
	        
}
