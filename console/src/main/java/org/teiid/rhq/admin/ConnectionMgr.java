package org.teiid.rhq.admin;

import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.teiid.rhq.comm.Connection;
import org.teiid.rhq.comm.ConnectionException;


public interface ConnectionMgr {

	/**
	 * Called to indicate that this manager should be initialized.
	 * @param props
	 * @param cl is the ClassLoader to use to instantiate any classes downstream
	 */
	void initialize(Properties props, ClassLoader cl);
	
	/**
	 * Called to reset the pool.   A subsequent call to @see #initialize(Properties, ClassLoader)
	 * would establish a new set of installations based on the properties.
	 */
	void shutdown();
	
	/**
	 * Returns <code>true</code> if server installations have been configured and will be returned
	 * in the {@link #getServerInstallations()} call.
	 * @return true if servers are defined.
	 */
	
	 boolean hasServersDefined();
	 
	 
	 /**
	  * Returns the unique set of keys for each installation
	  * @return Set of unique keys 
	  */
	 Set getInstallationSystemKeys();
	 

	
	/**
     * this is called only during discovery to obtain a connection for each 
     * system (or server installation) on the local machine.
     * 
     *  In cases where a connection cannot be obtained, an entry in the
     *  <code>Map</code> will be added with a null set for the value <code>Connection</code>
     * @return Map <key=installdir value=Connection>
     * @throws Exception
     * @since 1.0
     */
	 Map<String, Connection> getServerInstallations();

    /**
     * Called to get a {@link Connection} that will be used to
     * call the MetaMatrix Server.
     * @param key is the unique identifier for the system in
     * which to obtain the connection for.  
     * @return Connection for the system to communicate with.
     * @throws Exception
     * @since 1.0
     */	
    Connection getConnection(String key) throws ConnectionException;

}
