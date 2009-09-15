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
import java.util.Iterator;
import java.util.Properties;

import javax.sql.XAConnection;

import org.teiid.adminapi.Admin;
import org.teiid.adminapi.AdminOptions;
import org.teiid.adminapi.Model;
import org.teiid.adminapi.VDB;
import org.teiid.connector.jdbc.JDBCPropertyNames;
import org.teiid.test.framework.datasource.DataSourceMgr;
import org.teiid.test.framework.exception.QueryTestFailedException;
import org.teiid.test.framework.exception.TransactionRuntimeException;



public abstract class ConnectionStrategy {
	/**
	 * Connection Type indicates the type of connection (strategy) to use
	 */
    public static final String CONNECTION_TYPE = "connection-type"; //$NON-NLS-1$
    
    /**
     * The connection types that map to connection strategies
     * ****************************************************************
     */
    // used to create the jdb driver
    public static final String DRIVER_CONNECTION = "driver"; //$NON-NLS-1$
    // used to create a datasource 
    public static final String DATASOURCE_CONNECTION = "datasource"; //$NON-NLS-1$
    // used for when embedded is running in an appserver
    public static final String JNDI_CONNECTION = "jndi"; //$NON-NLS-1$
    /*
     * ******************************************************************
     */
    
    public static final String DS_USER = "user"; //$NON-NLS-1$
    
    // need both user variables because Teiid uses 'user' and connectors use 'username'
    public static final String DS_USERNAME = JDBCPropertyNames.USERNAME; //$NON-NLS-1$
    public static final String DS_PASSWORD = JDBCPropertyNames.PASSWORD;     //$NON-NLS-1$
    
    // the driver is only used for making direct connections to the source, the 
    // connector type will provide the JDBCPropertyNames.CONNECTION_SOURCE driver class
    public static final String DS_DRIVER = "driver"; //$NON-NLS-1$
 
    public static final String DS_URL = JDBCPropertyNames.URL;     //$NON-NLS-1$
    public static final String DS_SERVERNAME = "servername"; //$NON-NLS-1$
    public static final String DS_SERVERPORT = "portnumber"; //$NON-NLS-1$
    public static final String DS_JNDINAME = "ds-jndiname"; //$NON-NLS-1$
    public static final String DS_DATABASENAME = "databasename"; //$NON-NLS-1$
    public static final String DS_APPLICATION_NAME = "application-name"; //$NON-NLS-1$
    
    public static final String PROCESS_BATCH = "process-batch"; //$NON-NLS-1$
    public static final String CONNECTOR_BATCH = "connector-batch"; //$NON-NLS-1$
    public static final String JNDINAME_USERTXN = "usertxn-jndiname"; //$NON-NLS-1$
    
    
    public static final String AUTOCOMMIT = "autocommit"; //$NON-NLS-1$
    
    public static final String EXEC_IN_BATCH = "execute.in.batch"; //$NON-NLS-1$

    
    public ConnectionStrategy(Properties props) throws QueryTestFailedException {
    	this.env = props;

    	
    }
    
    /*
     * Lifecycle methods for managing the  connection
     */
    
    /**
     * Returns a connection
     * @return Connection
     */
    public abstract Connection getConnection() throws QueryTestFailedException;
    
    public Connection getAdminConnection() throws QueryTestFailedException{
    	return null;
    }
    
    private boolean autoCommit;
    public boolean getAutocommit() {
    	return autoCommit;
    }

    public abstract void shutdown();
    
    public XAConnection getXAConnection() throws QueryTestFailedException {
        return null;
    }
    
    
    private Properties env = null;
    
    
    public Properties getEnvironment() {
    	return env;
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
    	
    	String ac = this.env.getProperty(AUTOCOMMIT, "true");
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
        
            boolean update = false;
            Properties p = new Properties();
            if (this.env.getProperty(PROCESS_BATCH) != null) {
                p.setProperty("metamatrix.buffer.processorBatchSize", this.env.getProperty(PROCESS_BATCH)); //$NON-NLS-1$
                update = true;
            }
            
            if (this.env.getProperty(CONNECTOR_BATCH) != null) {
                p.setProperty("metamatrix.buffer.connectorBatchSize", this.env.getProperty(CONNECTOR_BATCH)); //$NON-NLS-1$
                update = true;
            }
            
            // update the system.
//            if (update) {
//            	admin.s
//            	admin.updateSystemProperties(p);
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

	        	org.teiid.test.framework.datasource.DataSource ds = DataSourceMgr.getInstance().getDatasource(useName, m.getName());
	        	
	        	if (ds != null) {

		        	AdminOptions ao = new AdminOptions(AdminOptions.OnConflict.OVERWRITE);
		        	ao.addOption(AdminOptions.BINDINGS_IGNORE_DECRYPT_ERROR);
		        	
		        	api.addConnectorBinding(ds.getName(), ds.getType(), ds.getProperties(), ao);
		        	
		        	api.assignBindingToModel(ds.getName(), vdb.getName(), vdb.getVDBVersion(), m.getName());
		        	
	        	} else {
	        		throw new QueryTestFailedException("Error: Unable to create binding to map to model : " + m.getName() + ", the mapped name " + useName + " had no datasource properties defined");
	        	}

    		}
    		
    	} catch (QueryTestFailedException qt) {
    		throw qt;
    	} catch (Exception t) {
    		throw new QueryTestFailedException(t);
    	}

    	
    }
        
 
    /**
     *  The datasource_identifier must match one of the mappings in the file
     *  at {@see} src/main/resources/datasources/datasource_mapping.txt
     * @param datasource_identifier
     * @return
     *
     * @since
     */
//    public XAConnection getXASource(String datasource_identifier) {
//        return null;
//    }
//    
//    public Connection getSource(String datasource_identifier) {
//        return null;
//    }
    
   
}
