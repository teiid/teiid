package org.teiid.rhq.embedded;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.rhq.core.pluginapi.inventory.InvalidPluginConfigurationException;
import org.teiid.rhq.admin.ConnectionMgr;
import org.teiid.rhq.comm.Connection;
import org.teiid.rhq.comm.ConnectionException;
import org.teiid.rhq.comm.ConnectionPool;
import org.teiid.rhq.embedded.pool.ConnectionPoolImpl;
import org.teiid.rhq.embedded.pool.EmbeddedConnectionConstants;



public class EmbeddedConnectionMgr implements ConnectionMgr {
	private static final Log log = LogFactory.getLog(EmbeddedConnectionMgr.class);
		
	private ConnectionPool pool;
	private Properties props;
	private ClassLoader loader;
	
	private Map<String, Connection> connectionList = new HashMap(1);
	 

	public Connection getConnection(String key) throws ConnectionException {
		return pool.getConnection();
	}
	
	

	public Set<String> getInstallationSystemKeys() {
		Set<String> keys = new HashSet<String>(1);
		keys.add(EmbeddedConnectionConstants.SYSTEM_KEY);
		return keys;
	}



	public Map getServerInstallations() {
		connectionList = new HashMap(1);
		try {
			connectionList.put(pool.getKey(), pool.getConnection());
		} catch (ConnectionException e) {
			// TODO Auto-generated catch block
			throw new InvalidPluginConfigurationException(e);
		}
		return connectionList;
	}

	public void shutdown() {

			try {
				pool.shutdown();
			} catch (ConnectionException e) {
				// TODO Auto-generated catch block
				log.error("Error shutting down connection pool", e);

			}
			pool = null;
			
			connectionList.clear();
		
	}

	   public boolean hasServersDefined() {
		   return (pool !=null);
	   }
	   
		  public void initialize(Properties props, ClassLoader cl) {
				this.props = props;
				this.loader = cl;
		
		            // allow override of the factory class
		            // this was put in to allow testing to set the factory
		            String factoryclass = System.getProperty(ConnectionPool.CONNECTION_FACTORY);
		            if (factoryclass != null) {
		                props.setProperty(ConnectionPool.CONNECTION_FACTORY, factoryclass);
		            }
		            
		            try {	            
		                 Class clzz = Class.forName(ConnectionPoolImpl.class.getName(), true, this.loader);
		                 this.pool = (ConnectionPoolImpl) clzz.newInstance(); 
		                	 //new ConnectionPoolImpl();
		                this.pool.initialize(props, cl); 
		           
		                log.info("ConnectionPool created for key " + pool.getKey()); //$NON-NLS-1$ //$NON-NLS-2$
		            } catch (Throwable t) {
		                throw new InvalidPluginConfigurationException(t);
		            }
		      }
}
