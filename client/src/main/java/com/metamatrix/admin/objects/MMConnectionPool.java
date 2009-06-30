package com.metamatrix.admin.objects;

import org.teiid.adminapi.ConnectionPool;

public class MMConnectionPool extends MMAdminObject implements ConnectionPool {
	
	/**
	 * @since 6.1
	 */
	private static final long serialVersionUID = -2341549955193216875L;
	
	/** 
	*  This will the name of the connector binding
	*/
	private String connectorBindingName;


	/**
	 * This will be identifier used in the registry to identify the connector
	 * binding and in which host and process that it's running in
	 */
	private String connectorBindingIdentifier;
	
	
	private int poolType;
	

	// current state
	/**
	 * Number of connections currently in use by a client
	 */
	private int connectionInUse;
	/**
	 * Number of connections waiting for use by a client
	 */
	private int connectionsWaiting;
	/**
	 * Total number of connections currently in the pool
	 */
	private int totalConnections;
	
	
	// total counts never reset
	/**
	 * Total number of connections that have been destroyed since the inception of the pool
	 */
	private long connectionsDestroyed;
	/**
	 * Total number of connections that have been created since the inception of the pool
	 */
	private long connectionsCreated;

	
	public MMConnectionPool() {
	}
	
	public boolean isXAPoolType() {
		return (this.poolType==1?true:false);
	}
	
	public int getPoolType() {
		return this.poolType;
	}
	
	public void setPoolType(int type) {
		this.poolType = type;
	}
		
	public String getConnectorBindingName() {
		return connectorBindingName;
	}
	
	public void setConnectorBindingName(String bindingName) {
		this.connectorBindingName = bindingName;
	}

	
	public String getConnectorBindingIdentifier() {
		return connectorBindingIdentifier;
	}

	public void setConnectorBindingIdentifier(String identifier) {
		this.connectorBindingIdentifier = identifier;
	}	


	public int getConnectionsInuse() {
		return this.connectionInUse;
	}

	public int getConnectionsWaiting() {
		return this.connectionsWaiting;
	}

	public long getConnectionsCreated() {
		return this.connectionsCreated;
	}

	public long getConnectionsDestroyed() {
		return this.connectionsDestroyed;
	}

	public int getTotalConnections() {
		return this.totalConnections;
	}
	
	public void setConnectionsInUse(int inUseConnections) {
		this.connectionInUse = inUseConnections;
	}

	public void setConnectionsWaiting(int waitingConnections) {
		this.connectionsWaiting = waitingConnections;
	}

	public void setTotalConnections(int totalConnections) {
		this.totalConnections = totalConnections;
	}

	public void setConnectionsDestroyed(long connectionsDestroyed) {
		this.connectionsDestroyed = connectionsDestroyed;
	}

	public void setConnectionsCreated(long connectionsCreated) {
		this.connectionsCreated = connectionsCreated;
	}


	@Override
    /**
     * Get string for display purposes 
     * @see java.lang.Object#toString()
     * @since 6.1
     */
    public String toString() {
        StringBuffer str = new StringBuffer();
        
        str.append(this.getIdentifier() + " ConnectionPoolStats:\n"); //$NON-NLS-1$
        str.append("\tisXAPoolType = " + isXAPoolType()); //$NON-NLS-1$
        str.append("\ttotalConnections = " + this.totalConnections); //$NON-NLS-1$
        str.append("\tinUseConnections = " + this.connectionInUse); //$NON-NLS-1$
        str.append("\twaitingConnections = " + connectionsWaiting);     //$NON-NLS-1$
        str.append("\tconnectionsCreated = " + connectionsCreated);     //$NON-NLS-1$
        str.append("\tconnectionsDestroyed = " + connectionsDestroyed);     //$NON-NLS-1$
        return str.toString();
    }


}
