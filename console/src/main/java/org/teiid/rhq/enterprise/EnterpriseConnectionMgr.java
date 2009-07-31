package org.teiid.rhq.enterprise;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.rhq.core.pluginapi.inventory.InvalidPluginConfigurationException;
import org.teiid.rhq.admin.ConnectionMgr;
import org.teiid.rhq.comm.Connection;
import org.teiid.rhq.comm.ConnectionConstants;
import org.teiid.rhq.comm.ConnectionException;
import org.teiid.rhq.comm.ConnectionPool;
import org.teiid.rhq.enterprise.pool.ConnectionPoolImpl;


public class EnterpriseConnectionMgr implements ConnectionMgr {
	private static final Log log = LogFactory.getLog(EnterpriseConnectionMgr.class);
	
	
	public static final String INSTALL_SERVER_PROP="mmservers";  //$NON-NLS-1$

	private static Map poolMap =  Collections.synchronizedMap( new HashMap(5) );
	

	private Properties props = null;


	public Connection getConnection(String key) throws ConnectionException {
        Connection connection = null;
        
        log.info("Get Connection for  " + key); //$NON-NLS-1$
        
        ConnectionPool pool = (ConnectionPool) poolMap.get(key);
        connection = pool.getConnection();
                
        return connection;
	}
	

	public Set getInstallationSystemKeys() {
		// TODO Auto-generated method stub
		return poolMap.keySet();
	}


	public boolean hasServersDefined() {
        
        if (poolMap != null && poolMap.size() > 0) {
            return true;
        }
        return false;
        
    }	

	public Map getServerInstallations() {
        Map connectionList = new HashMap();
        Iterator installationIter = poolMap.keySet().iterator();

         while (installationIter.hasNext()) {
            String installDir = (String) installationIter.next();
            try {
                if (poolMap.get(installDir) != null) {
                    ConnectionPool pool = (ConnectionPool) poolMap.get(installDir);
                
                    Connection connection = pool.getConnection(); 
                    connectionList.put(installDir, connection);
                } else {
                    // this shouldn't happen
                    connectionList.put(installDir, null);
                }
            } catch (Exception e) {
                connectionList.put(installDir, null);
            }
        }
        

		return connectionList;
	}


	public void shutdown() {
        Iterator installationIter = poolMap.keySet().iterator();
        
        while (installationIter.hasNext()) {
           String installDir = (String) installationIter.next();
               ConnectionPool pool = (ConnectionPool) poolMap.get(installDir);
       		try {
    			pool.shutdown();
    		} catch (ConnectionException e) {
    			// TODO Auto-generated catch block
    			log.error("Error shutting down connection pool " + pool.getKey(), e);
    			e.printStackTrace();
    		}
        }
         
        poolMap.clear();
    }
	  public void initialize(Properties props, ClassLoader cl) {
			this.props = props;

	        String servers = this.props.getProperty(INSTALL_SERVER_PROP);

	        /**
	         * split the server installation properties by the delimiter
	         * to determine the number of servers installed on the local machine
	         */
	         Collection installationList = AdminUtil.getTokens(servers, ";");  //$NON-NLS-1$
	        Iterator installationIter = installationList.iterator();

	        while (installationIter.hasNext()) {
	            String serverInfoValues = (String) installationIter.next();
	            Collection serverInfoValuesList = new LinkedList();
	            serverInfoValuesList = AdminUtil.getTokens(serverInfoValues, ",");  //$NON-NLS-1$
	            Object[] serverInfoArray = serverInfoValuesList.toArray();
	            String installDir = (String) serverInfoArray[0];
	            String url = (String) serverInfoArray[1];
	            String username = (String) serverInfoArray[2];
	            String password = (String) serverInfoArray[3];

	            props.setProperty(ConnectionConstants.PASSWORD, password);
	            props.setProperty(ConnectionConstants.USERNAME, username);
	            props.setProperty(ConnectionConstants.URL, url);                             
	                        
	            // allow override of the factory class
	            // this was put in to allow testing to set the factory
	            String factoryclass = System.getProperty(ConnectionPool.CONNECTION_FACTORY);
	            if (factoryclass != null) {
	                props.setProperty(ConnectionPool.CONNECTION_FACTORY, factoryclass);
	            }
	            
	            try {	            
	                 
	                 Class clzz = Class.forName(ConnectionPoolImpl.class.getName(), true, cl);
	                ConnectionPool pool = (ConnectionPool) clzz.newInstance();
	                pool.initialize(props, cl); 
	                          
	                poolMap.put(pool.getKey(), pool);
	           
	                log.info("ConnectionPool created for key " + pool.getKey() + " at url " + url); //$NON-NLS-1$ //$NON-NLS-2$
	            } catch (Throwable t) {
	                throw new InvalidPluginConfigurationException(t);
	            }
	        }
	    }	
	

}
