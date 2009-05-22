package org.teiid.connector.basic;

import org.teiid.connector.DataPlugin;
import org.teiid.connector.api.Connector;
import org.teiid.connector.api.ConnectorEnvironment;
import org.teiid.connector.api.ConnectorException;
import org.teiid.connector.api.ConnectorIdentity;
import org.teiid.connector.api.ConnectorPropertyNames;
import org.teiid.connector.api.CredentialMap;
import org.teiid.connector.api.ExecutionContext;
import org.teiid.connector.api.MappedUserIdentity;
import org.teiid.connector.api.SingleIdentity;

import com.metamatrix.common.util.PropertiesUtils;

public abstract class BasicConnector implements Connector {

	private boolean useCredentialMap;
	private boolean adminConnectionsAllowed = true;
	private String connectorName;
	
	@Override
	public void start(ConnectorEnvironment environment)
			throws ConnectorException {
		this.connectorName = environment.getConnectorName();
		this.adminConnectionsAllowed = PropertiesUtils.getBooleanProperty(environment.getProperties(), ConnectorPropertyNames.ADMIN_CONNECTIONS_ALLOWED, true);
		this.useCredentialMap = PropertiesUtils.getBooleanProperty(environment.getProperties(), ConnectorPropertyNames.USE_CREDENTIALS_MAP, false);
	}
		
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
			throw new ConnectorException(DataPlugin.Util.getString("UserIdentityFactory.missing_credentials", connectorName)); //$NON-NLS-1$
		}
		return new MappedUserIdentity(context.getUser(), user, password);
	}
	
	public String getConnectorName() {
		return connectorName;
	}
	
	public void setConnectorName(String connectorName) {
		this.connectorName = connectorName;
	}
	
	public boolean useCredentialMap() {
		return useCredentialMap;
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
