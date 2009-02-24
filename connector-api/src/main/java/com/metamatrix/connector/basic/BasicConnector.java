package com.metamatrix.connector.basic;

import com.metamatrix.connector.DataPlugin;
import com.metamatrix.connector.api.Connector;
import com.metamatrix.connector.api.ConnectorException;
import com.metamatrix.connector.api.ConnectorIdentity;
import com.metamatrix.connector.api.CredentialMap;
import com.metamatrix.connector.api.ExecutionContext;
import com.metamatrix.connector.api.SingleIdentity;
import com.metamatrix.connector.api.MappedUserIdentity;

public abstract class BasicConnector implements Connector {

	private boolean useCredentialMap;
	private boolean adminConnectionsAllowed = true;
	private String connectorName;
		
    /* (non-Javadoc)
	 * @see com.metamatrix.connector.api.Connector#createIdentity(com.metamatrix.connector.api.ExecutionContext)
	 */
	public ConnectorIdentity createIdentity(ExecutionContext context)
			throws ConnectorException {
		if (context == null) {
			if (adminConnectionsAllowed) {
				return new SingleIdentity();
			}
			throw new ConnectorException(DataPlugin.Util.getString("UserIdentityFactory.single_identity_not_supported")); //$NON-NLS-1$
		}
		Object payload = context.getTrustedPayload();
		if (!(payload instanceof CredentialMap)) {
			if (useCredentialMap) {
				throw new ConnectorException(DataPlugin.Util.getString("UserIdentityFactory.extraction_error")); //$NON-NLS-1$
			}
			return new SingleIdentity();
		}
		CredentialMap credMap = (CredentialMap)payload;
		String user = credMap.getUser(connectorName);
		String password = credMap.getPassword(connectorName);
		if (user == null || password == null) {
			throw new ConnectorException("Payload missing credentials for " + connectorName); //$NON-NLS-1$
		}
		return new MappedUserIdentity(context.getUser(), user, password);
	}
	
	public void setConnectorName(String connectorName) {
		this.connectorName = connectorName;
	}
	
	public void setUseCredentialMap(boolean useCredentialMap) {
		this.useCredentialMap = useCredentialMap;
	}
	
	public boolean areAdminConnectionsAllowed() {
		return adminConnectionsAllowed;
	}
	
	public void setAdminConnectionsAllowed(boolean adminConnectionsAllowed) {
		this.adminConnectionsAllowed = adminConnectionsAllowed;
	}
	
}
