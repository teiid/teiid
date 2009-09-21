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
import org.teiid.test.framework.ConfigPropertyNames;
import org.teiid.test.framework.exception.QueryTestFailedException;
import org.teiid.test.framework.exception.TransactionRuntimeException;

public class ConnectionStrategyFactory {
	
	    private static ConnectionStrategyFactory _instance = null;
	    private ConnectionStrategy strategy = null;
	    private Map<String, ConnectionStrategy> sources = null;

   	    
	    private ConnectionStrategyFactory(){
	    }
	    
	    public static synchronized ConnectionStrategyFactory getInstance()   {
	        if (_instance == null) {
	            _instance = new ConnectionStrategyFactory();

					_instance. init();
					// TODO Auto-generated catch block
//					_instance = null;
//					throw new TransactionRuntimeException(e);
//				}
	        }
	        return _instance;
	    }
	    
	    private void init() {
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
            p.remove(ConfigPropertyNames.CONFIG_FILE);

            
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
	    
	    public synchronized ConnectionStrategy getConnectionStrategy() throws QueryTestFailedException {
	    	if (strategy == null) {
	    		this.strategy = create(ConfigPropertyLoader.getProperties());
	    	}
	    	return this.strategy;
	    }
	    
	        
	    private ConnectionStrategy create(Properties props) throws QueryTestFailedException  {
	    	
	     	ConnectionStrategy strategy = null;
	                
	        String type = props.getProperty(ConfigPropertyNames.CONNECTION_TYPE, ConfigPropertyNames.CONNECTION_TYPES.DRIVER_CONNECTION);
	        if (type == null) {
	        	throw new RuntimeException("Property " + ConfigPropertyNames.CONNECTION_TYPE + " was specified");
	        }
	        
	        if (type.equalsIgnoreCase(ConfigPropertyNames.CONNECTION_TYPES.DRIVER_CONNECTION)) {
	                strategy = createDriverStrategy(null, props);
	                System.out.println("Created Driver Strategy");
	        }
	        else if (type.equalsIgnoreCase(ConfigPropertyNames.CONNECTION_TYPES.DATASOURCE_CONNECTION)) {
	            strategy = createDataSourceStrategy(null, props);
	            System.out.println("Created DataSource Strategy");
	        }
	        else if (type.equalsIgnoreCase(ConfigPropertyNames.CONNECTION_TYPES.JNDI_CONNECTION)) {
	            strategy = createJEEStrategy(null, props);
	            System.out.println("Created JEE Strategy");
	        }   
	        
	        if (strategy == null) {
	        	new TransactionRuntimeException("Invalid property value for " + ConfigPropertyNames.CONNECTION_TYPE + " is " + type );
	        }
	        // call configure here because this is creating the connection to Teiid
	        // direct connections to the datasource use the static call directly to create strategy and don't need to configure
	    	strategy.configure();
	        return strategy;
	    }
	    
	    public synchronized ConnectionStrategy createDriverStrategy(String identifier, Properties props) throws QueryTestFailedException  {
	     	if (identifier == null) {
	    		return new DriverConnection(props);
	     	}
	     	
	     	ConnectionStrategy strategy = null;
	     	if (sources.containsKey(identifier)) {
	     		strategy = sources.get(identifier);
	     	} else {
	     		strategy = new DriverConnection(props);
	     		sources.put(identifier, strategy);
	     	}	     	
       	
	       	return strategy;
	    
	    }
	    
	    public synchronized ConnectionStrategy createDataSourceStrategy(String identifier, Properties props) throws QueryTestFailedException  {	     	
	     	if (identifier == null) {
	    		return new DataSourceConnection(props);
	     	}
	     	
	     	ConnectionStrategy strategy = null;
	     	if (sources.containsKey(identifier)) {
	     		strategy = sources.get(identifier);
	     	} else {
	     		strategy = new DataSourceConnection(props);
	     		sources.put(identifier, strategy);
	     	}
	       	
	       	return strategy;
	    
	    }
	    
	    public synchronized ConnectionStrategy createJEEStrategy(String identifier, Properties props) throws QueryTestFailedException  {
	     	if (identifier == null) {
	    		return new JEEConnection(props);
	     	}
	     	
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
