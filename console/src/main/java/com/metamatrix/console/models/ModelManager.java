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

package com.metamatrix.console.models;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.metamatrix.common.log.LogManager;
import com.metamatrix.console.ConsolePlugin;
import com.metamatrix.console.connections.ConnectionInfo;
import com.metamatrix.console.security.UserCapabilities;
import com.metamatrix.console.ui.ViewManager;
import com.metamatrix.console.util.ExceptionUtility;
import com.metamatrix.console.util.ExternalException;
import com.metamatrix.console.util.LogContexts;
import com.metamatrix.console.util.StaticUtilities;
import com.metamatrix.platform.admin.api.AuthorizationAdminAPI;
import com.metamatrix.platform.admin.api.ConfigurationAdminAPI;
import com.metamatrix.platform.admin.api.ExtensionSourceAdminAPI;
import com.metamatrix.platform.admin.api.MembershipAdminAPI;
import com.metamatrix.platform.admin.api.RuntimeStateAdminAPI;
import com.metamatrix.platform.admin.api.SubSystemAdminAPI;
import com.metamatrix.platform.security.api.MetaMatrixPrincipalName;
import com.metamatrix.server.admin.api.RuntimeMetadataAdminAPI;
import com.metamatrix.server.admin.api.TransactionAdminAPI;

public class ModelManager {

	private static List /*<ConnectionInfo>*/ connectionsList =
			new ArrayList(5);

    private static Map /*<ConnectionInfo to Map<Class to SubSystemAdminAPI>>*/ connectionToAPIMap = new HashMap(5);
    
    private static Map /*<ConnectionInfo to Managers>*/ mgrsMap = new HashMap(5);

	public static ConnectionInfo[] getConnections() {
		ConnectionInfo[] connections =
				new ConnectionInfo[connectionsList.size()];
		Iterator it = connectionsList.iterator();
		for (int i = 0; it.hasNext(); i++) {
			connections[i] = (ConnectionInfo)it.next();
		}
		return connections;
	}
    
    public static int getNumberofConnections() {
        return connectionsList.size();
    }

	public static void removeConnection(ConnectionInfo connection) {
        removeConnectionLo(connection);    
	}

    /** 
     * @param connection
     * @since 4.3
     */
    public static void removeConnectionLo(ConnectionInfo connection) {
        
        HashMap capi = (HashMap) connectionToAPIMap.remove(connection);
        if (capi != null) {
            capi.clear();
        }
        
        mgrsMap.remove(connection);
        connectionsList.remove(connection);
    }

	public static boolean initViews(ConnectionInfo connection,
			boolean createFrame) {
		boolean initialized = false;
        boolean continuing = true;
        try {
            if (!UserCapabilities.getInstance().hasAnyRole(connection)) {
               	continuing = false;
				MetaMatrixPrincipalName name =
                        UserCapabilities.getLoggedInUser(connection);
				
				String title = ConsolePlugin.Util.getString("ModelManager.noRolesAssignedDialog.title"); //$NON-NLS-1$
				String msg =   ConsolePlugin.Util.getString("ModelManager.noRolesAssignedDialog.msg",name.getName()); //$NON-NLS-1$
				
                StaticUtilities.displayModalDialogWithOK(null,title,msg);
				//Must directly call ModelManager.getConnection().close()
				//instead of calling ConsoleMainFrame.getInstance().logoff(),
				//because ConsoleMainFrame instance has not been created.
            	connection.close();
            }
        } catch (Exception e) {
            continuing = false;
            
			String msg =   ConsolePlugin.Util.getString("ModelManager.roleVerifyError.msg"); //$NON-NLS-1$
            ExceptionUtility.showCannotInitializeMessage(msg,e);
        }
        if (continuing) {
        	if (createFrame) {
				ViewManager.init(connection);
			}
            initialized = true;
        }
        return initialized;
    }

    public static boolean init(ConnectionInfo conn)
    		throws ExternalException, Exception {
        removeConnection(conn);
        connectionsList.add(conn);
        
        connectionToAPIMap.put(conn, new HashMap(5));
        
        GroupsManager userManager = new GroupsManager(conn);
        
        SessionManager sessionManager = new SessionManager(conn);
        
        QueryManager queryManager = new QueryManager(conn); 
        
        EntitlementManager entitlementManager = new EntitlementManager(conn);
        
        ResourceManager resourceManager = new ResourceManager(conn);
        
        SummaryManager summaryManager = new SummaryManager(conn);
        
        ServerLogManager serverLogManager = new ServerLogManager(conn);
        
        PropertiesManager propertiesManager = new PropertiesManager(conn);
        
        ConnectorManager connectorManager = new ConnectorManager(conn);
        
        AuthenticationProviderManager authenticationProviderManager = new AuthenticationProviderManager(conn);

        ConfigurationManager configurationManager =
            new ConfigurationManager(conn);
        VdbManager vdbManager = new VdbManager(conn);
        
        RuntimeMgmtManager runtimeMgmtManager = new RuntimeMgmtManager(conn);
        ExtensionSourceManager extensionSourceManager =
            new ExtensionSourceManager(conn);
        
        Managers mgrs = new Managers(userManager, sessionManager,
                                     queryManager, entitlementManager, 
                                     resourceManager, summaryManager,
                                     serverLogManager, propertiesManager, vdbManager,
                                     configurationManager, connectorManager, authenticationProviderManager,
                                     runtimeMgmtManager, extensionSourceManager);
        mgrsMap.put(conn, mgrs);
        userManager.init();
        sessionManager.init();
        queryManager.init();
        entitlementManager.init();
        resourceManager.init();
        summaryManager.init();
        serverLogManager.init();
        propertiesManager.init();
        vdbManager.init();
        extensionSourceManager.init();
        configurationManager.init();
            
        
		return true;
    }

    public static ServerLogManager getServerLogManager(ConnectionInfo conn) {
    	ServerLogManager mgr = null;
    	Managers mgrs = (Managers)mgrsMap.get(conn);
    	if (mgrs != null) {
    		mgr = mgrs.getServerLogManager();
    	}
        return mgr;
    }

    public static GroupsManager getGroupsManager(ConnectionInfo conn) {
    	GroupsManager mgr = null;
    	Managers mgrs = (Managers)mgrsMap.get(conn);
    	if (mgrs != null) {
    		mgr = mgrs.getGroupsManager();
    	}
        return mgr;
    }

    public static SessionManager getSessionManager(ConnectionInfo conn) {
    	SessionManager mgr = null;
    	Managers mgrs = (Managers)mgrsMap.get(conn);
    	if (mgrs != null) {
    		mgr = mgrs.getSessionManager();
    	}
        return mgr;
    }

    public static QueryManager getQueryManager(ConnectionInfo conn) {
    	QueryManager mgr = null;
    	Managers mgrs = (Managers)mgrsMap.get(conn);
    	if (mgrs != null) {
    		mgr = mgrs.getQueryManager();
    	}
        return mgr;
	}

    public static EntitlementManager getEntitlementManager(ConnectionInfo conn) {
    	EntitlementManager mgr = null;
    	Managers mgrs = (Managers)mgrsMap.get(conn);
    	if (mgrs != null) {
    		mgr = mgrs.getEntitlementManager();
    	}
        return mgr;
	}

	public static ResourceManager getResourceManager(ConnectionInfo conn) {
    	ResourceManager mgr = null;
    	Managers mgrs = (Managers)mgrsMap.get(conn);
    	if (mgrs != null) {
    		mgr = mgrs.getPoolManager();
    	}
        return mgr;
	}

    public static SummaryManager getSummaryManager(ConnectionInfo conn) {
    	SummaryManager mgr = null;
    	Managers mgrs = (Managers)mgrsMap.get(conn);
    	if (mgrs != null) {
    		mgr = mgrs.getSummaryManager();
    	}
        return mgr;
	}

    public static PropertiesManager getPropertiesManager(ConnectionInfo conn) {
    	PropertiesManager mgr = null;
    	Managers mgrs = (Managers)mgrsMap.get(conn);
    	if (mgrs != null) {
    		mgr = mgrs.getPropertiesManager();
    	}
        return mgr;
	}

    public static VdbManager getVdbManager(ConnectionInfo conn) {
    	VdbManager mgr = null;
    	Managers mgrs = (Managers)mgrsMap.get(conn);
    	if (mgrs != null) {
    		mgr = mgrs.getVDBManager();
    	}
        return mgr;
	}

    public static ConfigurationManager getConfigurationManager(
    		ConnectionInfo conn) {
    	ConfigurationManager mgr = null;
    	Managers mgrs = (Managers)mgrsMap.get(conn);
    	if (mgrs != null) {
    		mgr = mgrs.getConfigurationManager();
    	}
        return mgr;
	}

    public static RuntimeMgmtManager getRuntimeMgmtManager(
    		ConnectionInfo conn) {
    	RuntimeMgmtManager mgr = null;
    	Managers mgrs = (Managers)mgrsMap.get(conn);
    	if (mgrs != null) {
    		mgr = mgrs.getRuntimeMgmtManager();
    	}
        return mgr;
	}

    public static ConnectorManager getConnectorManager(ConnectionInfo conn) {
    	ConnectorManager mgr = null;
    	Managers mgrs = (Managers)mgrsMap.get(conn);
    	if (mgrs != null) {
    		mgr = mgrs.getConnectorManager();
    	}
        return mgr;
	}
    
    public static AuthenticationProviderManager getAuthenticationProviderManager(ConnectionInfo conn) {
    	AuthenticationProviderManager mgr = null;
    	Managers mgrs = (Managers)mgrsMap.get(conn);
    	if (mgrs != null) {
    		mgr = mgrs.getAuthenticationProviderManager();
    	}
        return mgr;
	}

    public static ExtensionSourceManager getExtensionSourceManager(
    		ConnectionInfo conn) {
    	ExtensionSourceManager mgr = null;
    	Managers mgrs = (Managers)mgrsMap.get(conn);
    	if (mgrs != null) {
    		mgr = mgrs.getExtensionSourceManager();
    	}
        return mgr;
	}

    public static AuthorizationAdminAPI getAuthorizationAPI(ConnectionInfo conn) {

        return (AuthorizationAdminAPI) getSubSystemAdminAPI(AuthorizationAdminAPI.class, conn);
    }

    public static ConfigurationAdminAPI getConfigurationAPI(ConnectionInfo conn) {
        
        return (ConfigurationAdminAPI) getSubSystemAdminAPI(ConfigurationAdminAPI.class, conn);
    }

    public static MembershipAdminAPI getMembershipAPI(ConnectionInfo conn) {
        
        return (MembershipAdminAPI) getSubSystemAdminAPI(MembershipAdminAPI.class, conn);
	}

    public static RuntimeStateAdminAPI getRuntimeStateAPI(ConnectionInfo conn) {
        
        return (RuntimeStateAdminAPI) getSubSystemAdminAPI(RuntimeStateAdminAPI.class, conn);
	}

    public static RuntimeMetadataAdminAPI getRuntimeMetadataAPI(ConnectionInfo conn) {

        return (RuntimeMetadataAdminAPI) getSubSystemAdminAPI(RuntimeMetadataAdminAPI.class, conn);
	}

    public static TransactionAdminAPI getTransactionAPI(ConnectionInfo conn) {
        
        return (TransactionAdminAPI) getSubSystemAdminAPI(TransactionAdminAPI.class, conn);
    }

    public static ExtensionSourceAdminAPI getExtensionSourceAPI(ConnectionInfo conn) {

        return (ExtensionSourceAdminAPI) getSubSystemAdminAPI(ExtensionSourceAdminAPI.class, conn);
	}
    
    private static synchronized SubSystemAdminAPI getSubSystemAdminAPI(Class serviceInterface, ConnectionInfo conn) {
        SubSystemAdminAPI impl = null;
        Map apiToImplMap = (Map) connectionToAPIMap.get(conn);
        impl = (SubSystemAdminAPI) apiToImplMap.get(serviceInterface);
        if (impl == null) {
            try {
            	impl = (SubSystemAdminAPI)conn.getServerConnection().getService(serviceInterface);
            } catch (Exception e) {
                LogManager.logCritical(LogContexts.INITIALIZATION, e, e.getMessage());
                //todo: dialog?
            }
            if (impl != null) {
                apiToImplMap.put(serviceInterface, impl);
            }
        }
        
        return impl;
    }
    
    public static synchronized void clearServices(ConnectionInfo conn) {
    	Map apiToImplMap = (Map) connectionToAPIMap.get(conn);
    	if (apiToImplMap != null) {
    		apiToImplMap.clear();
    	}
    }
   
}
