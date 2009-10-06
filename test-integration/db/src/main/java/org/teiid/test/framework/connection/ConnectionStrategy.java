/*
 * Copyright (c) 2000-2007 MetaMatrix, Inc.
 * All rights reserved.
 */
package org.teiid.test.framework.connection;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

import javax.sql.XAConnection;

import org.teiid.adminapi.Admin;
import org.teiid.adminapi.AdminOptions;
import org.teiid.adminapi.Model;
import org.teiid.adminapi.VDB;
import org.teiid.test.framework.ConfigPropertyNames.CONNECTION_STRATEGY_PROPS;
import org.teiid.test.framework.datasource.DataSource;
import org.teiid.test.framework.datasource.DataSourceFactory;
import org.teiid.test.framework.exception.QueryTestFailedException;
import org.teiid.test.framework.exception.TransactionRuntimeException;


public abstract class ConnectionStrategy {
    
    private Map<String, ConnectionStrategy> driversources = null;
    private Map<String, ConnectionStrategy> datasourcesources = null;
    private Map<String, ConnectionStrategy> jeesources = null;

    
    private Map<String, DataSource> datasources = null;
    
    private DataSourceFactory dsFactory;
    
    
    public ConnectionStrategy(Properties props, DataSourceFactory dsFactory) throws QueryTestFailedException {
    	this.env = props;
    	this.dsFactory = dsFactory;
   	
    }
    
    /*
     * Lifecycle methods for managing the  connection
     */
    
    /**
     * Returns a connection
     * @return Connection
     */
    public abstract Connection getConnection() throws QueryTestFailedException;
    
    /**
     * Implement shutdown of your type of connecton
     * 
     *
     * @since
     */
    public void shutdown() {
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
    }
    
    private void shutDownSources(Map<String, ConnectionStrategy> sources) {
       	for (Iterator<String> it=sources.keySet().iterator(); it.hasNext();  ){	        		
        		ConnectionStrategy cs = sources.get(it.next());
        		try {
        			cs.shutdown();
        		} catch (Exception e) {
        			
        		}
        		
        	}
        	sources.clear();

    }

    public Connection getAdminConnection() throws QueryTestFailedException{
    	return null;
    }
    
    private boolean autoCommit;
    public boolean getAutocommit() {
    	return autoCommit;
    }

    
    public XAConnection getXAConnection() throws QueryTestFailedException {
        return null;
    }
    
    
    private Properties env = null;
    
    
    public Properties getEnvironment() {
    	return env;
    }
    
    public int getNumberAvailableDataSources() {
    	return this.dsFactory.getNumberAvailableDataSources();
    }
    
    public Map<String, DataSource> getDataSources() {
    	return this.datasources;
    }
    
    class CloseInterceptor implements InvocationHandler {

        Connection conn;

        CloseInterceptor(Connection conn) {
            this.conn = conn;
        }
        public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
            if (method.getName().equals("close")) { //$NON-NLS-1$
                return null;
            }
            try {
                return method.invoke(this.conn, args);
            } catch (InvocationTargetException e) {
                throw e.getTargetException();
            }
        }
    }    
    
   
    void configure() throws QueryTestFailedException  {
    	
    	datasources = new HashMap<String, DataSource>(3);
    	
    	String ac = this.env.getProperty(CONNECTION_STRATEGY_PROPS.AUTOCOMMIT, "true");
    	this.autoCommit = Boolean.getBoolean(ac);
    	
        com.metamatrix.jdbc.api.Connection c =null;
        try {
        	
        	// the the driver strategy is going to be used to connection directly to the connector binding
        	// source, then no administration can be done
        	java.sql.Connection conn = getConnection();
        	if ( conn instanceof com.metamatrix.jdbc.api.Connection) {
        		c = (com.metamatrix.jdbc.api.Connection) conn;
        	} else {
        		return;
        	}
            
            Admin admin = (Admin)c.getAdminAPI();
        
//            Properties p = new Properties();
//            if (this.env.getProperty(PROCESS_BATCH) != null) {
//                p.setProperty("metamatrix.buffer.processorBatchSize", this.env.getProperty(PROCESS_BATCH)); //$NON-NLS-1$
//            }
//            
//            if (this.env.getProperty(CONNECTOR_BATCH) != null) {
//                p.setProperty("metamatrix.buffer.connectorBatchSize", this.env.getProperty(CONNECTOR_BATCH)); //$NON-NLS-1$
//            }
            
            setupVDBConnectorBindings(admin);
            
            admin.restart();
 
            System.out.println("Bouncing the system..(wait 15 seconds)"); //$NON-NLS-1$
            Thread.sleep(1000*15);
        //    Thread.sleep(1000*60);
            System.out.println("done."); //$NON-NLS-1$

        } catch (Exception e) {
        	e.printStackTrace();

            throw new TransactionRuntimeException(e);
        }  finally {
        	// need to close and flush the connection after restarting
        	this.shutdown();
           	
        }
    }    
    
    protected void setupVDBConnectorBindings(Admin api) throws QueryTestFailedException {
         
    	try {

    		Collection<VDB> vdbs = api.getVDBs("*");
    		if (vdbs == null) {
    	  		throw new QueryTestFailedException("GetVDBS returned no vdbs available");
    	  		 
    		} else if (vdbs.size() != 1) {
    			throw new QueryTestFailedException("GetVDBS returned more than 1 vdb available");
    		}
    		VDB vdb = (VDB) vdbs.iterator().next();
    		Iterator<Model> modelIt = vdb.getModels().iterator();
    		while (modelIt.hasNext() ) {
    			Model m = modelIt.next();
    			
    			if (!m.isPhysical()) continue;
    			
    			// get the mapping, if defined
    			String mappedName = this.env.getProperty(m.getName());
    			
	        	String useName = m.getName();
	        	if(mappedName != null) {
	        		useName = mappedName;
	        	}

	        	org.teiid.test.framework.datasource.DataSource ds = this.dsFactory.getDatasource(useName, m.getName());
	        	
	        	if (ds != null) {
		        	datasources.put(m.getName(), ds);

	                System.out.println("Set up Connector Binding (model:mapping:type): " + m.getName() + ":" + useName + ":"  + ds.getConnectorType()); //$NON-NLS-1$

		        	AdminOptions ao = new AdminOptions(AdminOptions.OnConflict.OVERWRITE);
		        	ao.addOption(AdminOptions.BINDINGS_IGNORE_DECRYPT_ERROR);
		        	
		        	api.addConnectorBinding(ds.getName(), ds.getConnectorType(), ds.getProperties(), ao);
		        	
		        	api.assignBindingToModel(ds.getName(), vdb.getName(), vdb.getVDBVersion(), m.getName());
		        	
	        	} else {
	        		throw new QueryTestFailedException("Error: Unable to create binding to map to model : " + m.getName() + ", the mapped name " + useName + " had no datasource properties defined");
	        	}

    		}
    		
    	} catch (QueryTestFailedException qt) {
    		throw qt;
    	} catch (Exception t) {
    		t.printStackTrace();
    		throw new QueryTestFailedException(t);
    	}

    	
    }
    
    public synchronized ConnectionStrategy createDriverStrategy(String identifier, Properties props) throws QueryTestFailedException  {
    	if (driversources == null) {
    		driversources = new HashMap<String, ConnectionStrategy>();
    	}
    	
     	if (identifier == null) {
     			return new DriverConnection(props, dsFactory);
     	}
     	
     	ConnectionStrategy strategy = null;
     	if (driversources.containsKey(identifier)) {
     		strategy = driversources.get(identifier);
     	} else {
     		strategy = new DriverConnection(props, dsFactory);
     		driversources.put(identifier, strategy);
     	}	     	
   	
       	return strategy;
    
    }
    
    public synchronized ConnectionStrategy createDataSourceStrategy(String identifier, Properties props) throws QueryTestFailedException  {	     	
    	if (datasourcesources == null) {
    		datasourcesources = new HashMap<String, ConnectionStrategy>();
    	}
    	
     	if (identifier == null) {
    		return new DataSourceConnection(props, dsFactory);
     	}
     	
     	ConnectionStrategy strategy = null;
     	if (datasourcesources.containsKey(identifier)) {
     		strategy = datasourcesources.get(identifier);
     	} else {
     		strategy = new DataSourceConnection(props, dsFactory);
     		datasourcesources.put(identifier, strategy);
     	}
       	
       	return strategy;
    
    }
    
    public synchronized ConnectionStrategy createJEEStrategy(String identifier, Properties props) throws QueryTestFailedException  {
    	if (jeesources == null) {
    		jeesources = new HashMap<String, ConnectionStrategy>();
    	}
    	
     	if (identifier == null) {
    		return new JEEConnection(props, dsFactory);
     	}
     	
     	ConnectionStrategy strategy = null;
     	if (jeesources.containsKey(identifier)) {
     		strategy = jeesources.get(identifier);
     	} else {
     		strategy = new JEEConnection(props, dsFactory);
     		jeesources.put(identifier, strategy);
     	}
       	
       	return strategy;
    
    }
  
        
 
   
}
