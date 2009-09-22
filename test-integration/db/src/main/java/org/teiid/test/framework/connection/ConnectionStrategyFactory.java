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
	    /**
	     *  this strategy represents the connection strategy used to connect to Teiid
	     *  and is based on the properties loaded by the {@link ConfigPropertyLoader}
	     */
	    private ConnectionStrategy strategy = null;
	    private Map<String, ConnectionStrategy> driversources = null;
	    private Map<String, ConnectionStrategy> datasourcesources = null;
	    private Map<String, ConnectionStrategy> jeesources = null;

   	    
	    private ConnectionStrategyFactory(){
	    }
	    
	    public static synchronized ConnectionStrategyFactory getInstance()   {
	        if (_instance == null) {
	            _instance = new ConnectionStrategyFactory();
	        }
	        return _instance;
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

            
            if (driversources != null) {
            	shutDownSources(driversources);
            	driversources = null;
            }
            
            if (datasourcesources != null) {
            	shutDownSources(datasourcesources);
            	datasourcesources = null;
            }
            
            if (jeesources != null) {
            	shutDownSources(jeesources);
            	jeesources = null;
            }
        	
        	try {
        		strategy.shutdown();
        	} catch (Exception e) {
        		
        	} finally {
        		strategy = null;
        	}
 
	    }
	    
	    private void shutDownSources(Map<String, ConnectionStrategy> sources) {
	       	for (Iterator it=sources.keySet().iterator(); it.hasNext();  ){	        		
	        		ConnectionStrategy cs = sources.get(it.next());
	        		try {
	        			cs.shutdown();
	        		} catch (Exception e) {
	        			
	        		}
	        		
	        	}
	        	sources.clear();
	
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
	        	// pass in null to create new strategy
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
	    	if (driversources == null) {
	    		driversources = new HashMap<String, ConnectionStrategy>();
	    	}
	    	
	     	if (identifier == null) {
	     			return new DriverConnection(props);
	     	}
	     	
	     	ConnectionStrategy strategy = null;
	     	if (driversources.containsKey(identifier)) {
	     		strategy = driversources.get(identifier);
	     	} else {
	     		strategy = new DriverConnection(props);
	     		driversources.put(identifier, strategy);
	     	}	     	
       	
	       	return strategy;
	    
	    }
	    
	    public synchronized ConnectionStrategy createDataSourceStrategy(String identifier, Properties props) throws QueryTestFailedException  {	     	
	    	if (datasourcesources == null) {
	    		datasourcesources = new HashMap<String, ConnectionStrategy>();
	    	}
	    	
	     	if (identifier == null) {
	    		return new DataSourceConnection(props);
	     	}
	     	
	     	ConnectionStrategy strategy = null;
	     	if (datasourcesources.containsKey(identifier)) {
	     		strategy = datasourcesources.get(identifier);
	     	} else {
	     		strategy = new DataSourceConnection(props);
	     		datasourcesources.put(identifier, strategy);
	     	}
	       	
	       	return strategy;
	    
	    }
	    
	    public synchronized ConnectionStrategy createJEEStrategy(String identifier, Properties props) throws QueryTestFailedException  {
	    	if (jeesources == null) {
	    		jeesources = new HashMap<String, ConnectionStrategy>();
	    	}
	    	
	     	if (identifier == null) {
	    		return new JEEConnection(props);
	     	}
	     	
	     	ConnectionStrategy strategy = null;
	     	if (jeesources.containsKey(identifier)) {
	     		strategy = jeesources.get(identifier);
	     	} else {
	     		strategy = new JEEConnection(props);
	     		jeesources.put(identifier, strategy);
	     	}
	       	
	       	return strategy;
	    
	    }
	    
		
		public static void main(String[] args) {
			ConnectionStrategyFactory cf = ConnectionStrategyFactory.getInstance();

		}
	        
}
