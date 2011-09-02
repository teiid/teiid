/*
 * JBoss, Home of Professional Open Source.
 * See the COPYRIGHT.txt file distributed with this work for information
 * regarding copyright ownership.  Some portions may be licensed
 * to Red Hat, Inc. under one or more contributor license agreements.
 * 
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 * 
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 * 
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301 USA.
 */

package org.teiid.adminapi;

import java.io.IOException;
import java.io.InputStream;
import java.net.UnknownHostException;
import java.util.Collection;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import javax.security.auth.callback.*;
import javax.security.sasl.RealmCallback;
import javax.security.sasl.RealmChoiceCallback;

import org.jboss.as.cli.Util;
import org.jboss.as.cli.operation.impl.DefaultOperationRequestAddress;
import org.jboss.as.controller.client.ModelControllerClient;
import org.teiid.adminapi.VDB.ConnectionType;


/** 
 * Singleton factory for class for creating Admin connections to the Teiid
 */
public class AdminFactory {
	private static AdminFactory INSTANCE = new AdminFactory();
	
	public static AdminFactory getInstance() {
		return INSTANCE;
	}
    /**
     * Creates a ServerAdmin with the specified connection properties. 
     * @param userName
     * @param password
     * @param serverURL
     * @param applicationName
     * @return
     * @throws AdminException
     */
    public Admin createAdmin(String host, int port, String userName, char[] password) throws AdminException {
        if(host == null) {
            host = "localhost"; //$NON-NLS-1$
        }

        if(port < 0) {
            port = 9990;
        }

        try {
            CallbackHandler cbh = new AuthenticationCallbackHandler(userName, password);
            ModelControllerClient newClient = ModelControllerClient.Factory.create(host, port, cbh);

            List<String> nodeTypes = Util.getNodeTypes(newClient, new DefaultOperationRequestAddress());
            if (!nodeTypes.isEmpty()) {
                boolean domainMode = nodeTypes.contains("server-group"); //$NON-NLS-1$ 
                System.out.println("Connected to " //$NON-NLS-1$ 
                        + (domainMode ? "domain controller at " : "standalone controller at ") //$NON-NLS-1$ //$NON-NLS-2$
                        + host + ":" + port); //$NON-NLS-1$ 
                return new AdminImpl(newClient);
            } 
            System.out.println("The controller is not available at " + host + ":" + port); //$NON-NLS-1$ //$NON-NLS-2$
        } catch (UnknownHostException e) {
        	System.out.println("Failed to resolve host '" + host + "': " + e.getLocalizedMessage()); //$NON-NLS-1$ //$NON-NLS-2$
        }
        return null;
    }
    
    private class AuthenticationCallbackHandler implements CallbackHandler {
        private boolean realmShown = false;
        private String userName = null;
        private char[] password = null;

        public AuthenticationCallbackHandler(String user, char[] password) {
        	this.userName = user;
        	this.password = password;
        }
        
        public void handle(Callback[] callbacks) throws IOException, UnsupportedCallbackException {
            // Special case for anonymous authentication to avoid prompting user for their name.
            if (callbacks.length == 1 && callbacks[0] instanceof NameCallback) {
                ((NameCallback)callbacks[0]).setName("anonymous CLI user"); //$NON-NLS-1$
                return;
            }

            for (Callback current : callbacks) {
                if (current instanceof RealmCallback) {
                    RealmCallback rcb = (RealmCallback) current;
                    String defaultText = rcb.getDefaultText();
                    rcb.setText(defaultText); // For now just use the realm suggested.
                    if (realmShown == false) {
                        realmShown = true;
                    }
                } else if (current instanceof RealmChoiceCallback) {
                    throw new UnsupportedCallbackException(current, "Realm choice not currently supported."); //$NON-NLS-1$
                } else if (current instanceof NameCallback) {
                    NameCallback ncb = (NameCallback) current;
                    ncb.setName(userName);
                } else if (current instanceof PasswordCallback) {
                    PasswordCallback pcb = (PasswordCallback) current;
                    pcb.setPassword(password);
                } else {
                    throw new UnsupportedCallbackException(current);
                }
            }
        }

    }    
    
    private class AdminImpl implements Admin{
    	public AdminImpl (ModelControllerClient connection) {
    		
    	}

		@Override
		public void addDataRoleMapping(String vdbName, int vdbVersion,
				String dataRole, String mappedRoleName) throws AdminException {
			// rameshTODO Auto-generated method stub
			
		}

		@Override
		public void assignToModel(String vdbName, int vdbVersion,
				String modelName, String sourceName, String translatorName,
				String dsName) throws AdminException {
			// rameshTODO Auto-generated method stub
			
		}

		@Override
		public void cancelRequest(String sessionId, long executionId)
				throws AdminException {
			// rameshTODO Auto-generated method stub
			
		}

		@Override
		public void changeVDBConnectionType(String vdbName, int vdbVersion,
				ConnectionType type) throws AdminException {
			// rameshTODO Auto-generated method stub
			
		}

		@Override
		public void clearCache(String cacheType) throws AdminException {
			// rameshTODO Auto-generated method stub
			
		}

		@Override
		public void clearCache(String cacheType, String vdbName, int vdbVersion)
				throws AdminException {
			// rameshTODO Auto-generated method stub
			
		}

		@Override
		public void close() {
			// rameshTODO Auto-generated method stub
			
		}

		@Override
		public void createDataSource(String deploymentName,
				String templateName, Properties properties)
				throws AdminException {
			// rameshTODO Auto-generated method stub
			
		}

		@Override
		public void deleteDataSource(String deployedName) throws AdminException {
			// rameshTODO Auto-generated method stub
			
		}

		@Override
		public void deleteVDB(String vdbName, int vdbVersion)
				throws AdminException {
			// rameshTODO Auto-generated method stub
			
		}

		@Override
		public void deployVDB(String fileName, InputStream vdb)
				throws AdminException {
			// rameshTODO Auto-generated method stub
			
		}

		@Override
		public CacheStatistics getCacheStats(String cacheType)
				throws AdminException {
			// rameshTODO Auto-generated method stub
			return null;
		}

		@Override
		public Collection<String> getCacheTypes() throws AdminException {
			// rameshTODO Auto-generated method stub
			return null;
		}

		@Override
		public Collection<String> getDataSourceNames() throws AdminException {
			// rameshTODO Auto-generated method stub
			return null;
		}

		@Override
		public Set<String> getDataSourceTemplateNames() throws AdminException {
			// rameshTODO Auto-generated method stub
			return null;
		}

		@Override
		public Collection<Request> getRequests() throws AdminException {
			// rameshTODO Auto-generated method stub
			return null;
		}

		@Override
		public Collection<Request> getRequestsForSession(String sessionId)
				throws AdminException {
			// rameshTODO Auto-generated method stub
			return null;
		}

		@Override
		public Collection<Session> getSessions() throws AdminException {
			// rameshTODO Auto-generated method stub
			return null;
		}

		@Override
		public Collection<PropertyDefinition> getTemplatePropertyDefinitions(
				String templateName) throws AdminException {
			// rameshTODO Auto-generated method stub
			return null;
		}

		@Override
		public Collection<Transaction> getTransactions() throws AdminException {
			// rameshTODO Auto-generated method stub
			return null;
		}

		@Override
		public Translator getTranslator(String deployedName)
				throws AdminException {
			// rameshTODO Auto-generated method stub
			return null;
		}

		@Override
		public Collection<Translator> getTranslators() throws AdminException {
			// rameshTODO Auto-generated method stub
			return null;
		}

		@Override
		public VDB getVDB(String vdbName, int vbdVersion) throws AdminException {
			// rameshTODO Auto-generated method stub
			return null;
		}

		@Override
		public Set<VDB> getVDBs() throws AdminException {
			// rameshTODO Auto-generated method stub
			return null;
		}

		@Override
		public WorkerPoolStatistics getWorkerPoolStats() throws AdminException {
			// rameshTODO Auto-generated method stub
			return null;
		}

		@Override
		public void markDataSourceAvailable(String jndiName)
				throws AdminException {
			// rameshTODO Auto-generated method stub
			
		}

		@Override
		public void mergeVDBs(String sourceVDBName, int sourceVDBVersion,
				String targetVDBName, int targetVDBVersion)
				throws AdminException {
			// rameshTODO Auto-generated method stub
			
		}

		@Override
		public void removeDataRoleMapping(String vdbName, int vdbVersion,
				String dataRole, String mappedRoleName) throws AdminException {
			// rameshTODO Auto-generated method stub
			
		}

		@Override
		public void setAnyAuthenticatedForDataRole(String vdbName,
				int vdbVersion, String dataRole, boolean anyAuthenticated)
				throws AdminException {
			// rameshTODO Auto-generated method stub
			
		}

		@Override
		public void terminateSession(String sessionId) throws AdminException {
			// rameshTODO Auto-generated method stub
			
		}

		@Override
		public void terminateTransaction(String transactionId)
				throws AdminException {
			// rameshTODO Auto-generated method stub
			
		}
    }
    
}
