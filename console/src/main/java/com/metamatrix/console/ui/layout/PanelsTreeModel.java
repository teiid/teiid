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

package com.metamatrix.console.ui.layout;

import javax.swing.tree.DefaultTreeModel;
import javax.swing.tree.TreePath;

import com.metamatrix.console.ConsolePlugin;
import com.metamatrix.console.connections.ConnectionAndPanel;
import com.metamatrix.console.connections.ConnectionInfo;
import com.metamatrix.console.util.StaticTreeUtilities;
import com.metamatrix.core.util.MetaMatrixProductVersion;

public class PanelsTreeModel extends DefaultTreeModel {

    ///////////////////////////////////////////////////////////////////////////
    // CONSTANTS
    ///////////////////////////////////////////////////////////////////////////

    public static final String RUNTIME = "Runtime Management"; //$NON-NLS-1$
    public static final String SUMMARY = "Summary"; //$NON-NLS-1$
    public static final Class SUMMARY_PANEL_CLASS =
        	com.metamatrix.console.ui.views.summary.SummaryPanel.class;
    public static final String SESSIONS = "Sessions"; //$NON-NLS-1$
    public static final Class SESSIONS_PANEL_CLASS =
        	com.metamatrix.console.ui.views.sessions.SessionPanel.class;
    public static final String SYS_STATE = "System State"; //$NON-NLS-1$
    public static final Class RUNTIME_MGMT_PANEL_CLASS =
        	com.metamatrix.console.ui.views.runtime.RuntimeMgmtPanel.class;
    public static final String SYS_LOG = "Log Viewer"; //$NON-NLS-1$
    public static final Class SYSTEM_LOG_PANEL_CLASS =
        	com.metamatrix.console.ui.views.syslog.SysLogPanel.class;
    public static final String MM_SERVER = MetaMatrixProductVersion.METAMATRIX_SERVER_TYPE_NAME;  
    public static final String QUERIES = "Queries"; //$NON-NLS-1$
    public static final Class QUERIES_PANEL_CLASS =
        	com.metamatrix.console.ui.views.queries.QueryPanel.class;
    public static final String TRANSACTIONS = "Transactions";
    public static final Class TRANSACTIONS_PANEL_CLASS =
        	com.metamatrix.console.ui.views.transactions.TransactionsPanel.class;
    public static final String CONFIGURATION = "Configuration"; //$NON-NLS-1$
    public static final String SYS_PROPS = "System Properties"; //$NON-NLS-1$
    public static final Class SYSTEM_PROPERTIES_PANEL_CLASS =
        	com.metamatrix.console.ui.views.properties.PropertiesMasterPanel.class;
    public static final String POOLS_CONFIG = "Connection Pools"; //$NON-NLS-1$
    public static final String RESOURCES = "Resources"; //$NON-NLS-1$
    public static final Class RESOURCES_PANEL_CLASS = 
    		com.metamatrix.console.ui.views.resources.ResourcesMainPanel.class;
    public static final String PSC_DEPLOY = "Deployment"; //$NON-NLS-1$
    public static final Class DEPLOY_PSC_DEFN_PANEL_CLASS =
        	com.metamatrix.console.ui.views.deploy.DeployMainPanel.class;
    public static final String VDB = "Virtual Databases"; //$NON-NLS-1$
    public static final Class VDB_PANEL_CLASS =
        	com.metamatrix.console.ui.views.vdb.VdbMainPanel.class;
    public static final String CONNECTORS = "Connector Types"; //$NON-NLS-1$
    public static final Class CONNECTOR_DEFINITIONS_PANEL_CLASS =
        	com.metamatrix.console.ui.views.connector.ConnectorPanel.class;
    public static final String CONNECTOR_BINDINGS = "Connector Bindings"; //$NON-NLS-1$
    public static final Class CONNECTOR_BINDINGS_PANEL_CLASS =
        	com.metamatrix.console.ui.views.connectorbinding.ConnectorBindingPanel.class;

    public static final String AUTHORIZATION = "Security"; //$NON-NLS-1$
    public static final String AUTHORIZATION_SUMMARY = "Summary"; //$NON-NLS-1$
    public static final Class AUTHORIZATION_SUMMARY_PANEL_CLASS =
            com.metamatrix.console.ui.views.authorization.SummaryMain.class;
    public static final String DOMAIN_PROVIDERS = "Membership Domain Providers"; //$NON-NLS-1$
    public static final Class AUTHORIZATION_PROVIDERS_PANEL_CLASS =
            com.metamatrix.console.ui.views.authorization.ProvidersMain.class;
    public static final String ADMIN_ROLES = "Admin Roles"; //$NON-NLS-1$
    public static final Class ADMIN_ROLES_PANEL_CLASS =
            com.metamatrix.console.ui.views.users.AdminRolesMain.class;
    public static final String DATA_ROLES = "Data Roles"; //$NON-NLS-1$
    public static final Class ENTITLEMENTS_VIEW_PANEL_CLASS =
            com.metamatrix.console.ui.views.entitlements.EntitlementsPanel.class;
    public static final String METADATA_ROLES = "Repository Roles"; //$NON-NLS-1$
    public static final String EXTENSION_SOURCES = "Extension Modules"; //$NON-NLS-1$
    public static final Class EXTENSION_SOURCES_CLASS =
            com.metamatrix.console.ui.views.extensionsource.ExtensionSourcesPanel.class;
    public static final String SYSTEMLOGSETUP = "Log Settings"; //$NON-NLS-1$
    public static final Class SYSTEMLOGSETUP_PANEL_CLASS =
            com.metamatrix.console.ui.views.logsetup.SystemLogSetUpPanel.class;

	public static final Object[] BLOCKED_ITEMS = new Object[] {RUNTIME, 
	    	MM_SERVER, CONFIGURATION, AUTHORIZATION};
	    	
    private static final String MM_SERV_TIP =
            "The " + MM_SERVER + " product provides integrated information " + //$NON-NLS-1$ //$NON-NLS-2$
            "access to disparate data sources"; //$NON-NLS-1$
//    private static final String MB_SERV_TIP =
//    		"The " + MB_SERVER + " product manages enterprise metadata"; //$NON-NLS-1$

    ///////////////////////////////////////////////////////////////////////////
    // FIELDS
    ///////////////////////////////////////////////////////////////////////////

    private static PanelsTreeModel theModel = null;
    
    ///////////////////////////////////////////////////////////////////////////
    // CONSTRUCTORS
    ///////////////////////////////////////////////////////////////////////////

    private PanelsTreeModel(PanelsTreeNode root, ConnectionInfo connection) {
        super(root);
		addConnection(connection, null);
	}

    ///////////////////////////////////////////////////////////////////////////
    // METHODS
    ///////////////////////////////////////////////////////////////////////////

    public static PanelsTreeModel createInstance(ConnectionInfo connection) {
		PanelsTreeNode root = new PanelsTreeNode("root", null, null, false); //$NON-NLS-1$
		theModel = new PanelsTreeModel(root, connection);
		return theModel;
	}
	
    public TreePath getPathForPanel(ConnectionAndPanel panel) {
    	TreePath tp = null;
    	TreePath[] paths = StaticTreeUtilities.allTreePathsToLeafNodes(this);
    	int i = 0;
    	while ((i < paths.length) && (tp == null)) {
    		PanelsTreeNode lastNode = 
    				(PanelsTreeNode)paths[i].getLastPathComponent();
    		ConnectionInfo lastNodeConnection = lastNode.getConnection();
    		Class lastNodePanelClass = lastNode.getPanelClass();
    		if (panel.getConnection().equals(lastNodeConnection) &&
    				panel.getPanelClass().equals(lastNodePanelClass)) {
    			tp = paths[i];
    		} else {
    			i++;
    		}
    	}
		return tp;
    }

	public void addConnection(ConnectionInfo connection,
    		PanelsTreeNode connectionNode) {
    	
		// --------------------------
		// nodeToAddTo - Root Node
		// --------------------------
    	PanelsTreeNode nodeToAddTo;
		PanelsTreeNode root = (PanelsTreeNode)this.getRoot();
    	if (connectionNode != null) {
    		root.add(connectionNode);
    		nodeToAddTo = connectionNode;
    	} else {
    		nodeToAddTo = root;
    	}
    	
    	String tipText;
    	
        // --------------------------------------------------------------------------------
        // set up Runtime Management header node
        // --------------------------------------------------------------------------------
        PanelsTreeNode runtime = new PanelsTreeNode(RUNTIME, null, connection,
        		false);
        tipText = ConsolePlugin.Util.getString("PanelsTreeModel.runtimePanel.tooltip"); //$NON-NLS-1$
        runtime.setToolTipText(tipText); 
        nodeToAddTo.add(runtime);

        // summary panel node
        PanelsTreeNode summary = new PanelsTreeNode(SUMMARY, 
        		SUMMARY_PANEL_CLASS, connection, false);
        tipText = ConsolePlugin.Util.getString("PanelsTreeModel.summaryPanel.tooltip"); //$NON-NLS-1$
        summary.setToolTipText(tipText); 
        runtime.add(summary);
        
        // session panel node
        PanelsTreeNode sessions = new PanelsTreeNode(SESSIONS,
        		SESSIONS_PANEL_CLASS, connection, false);
        tipText = ConsolePlugin.Util.getString("PanelsTreeModel.sessionsPanel.tooltip"); //$NON-NLS-1$
        sessions.setToolTipText(tipText); 
        runtime.add(sessions);

        // runtime services panel node
        PanelsTreeNode sysState = new PanelsTreeNode(SYS_STATE,
        		RUNTIME_MGMT_PANEL_CLASS, connection, false);
        tipText = ConsolePlugin.Util.getString("PanelsTreeModel.sysStatePanel.tooltip"); //$NON-NLS-1$
        sysState.setToolTipText(tipText);
        runtime.add(sysState);

        // system log panel node
        PanelsTreeNode log = new PanelsTreeNode(SYS_LOG,
        		SYSTEM_LOG_PANEL_CLASS, connection, false);
        tipText = ConsolePlugin.Util.getString("PanelsTreeModel.sysLogPanel.tooltip"); //$NON-NLS-1$
        log.setToolTipText(tipText); 
        runtime.add(log);

        // MetaMatrixServer header node
        PanelsTreeNode runtimeMMServer = new PanelsTreeNode(MM_SERVER, null,
        		connection, false);
        runtimeMMServer.setToolTipText(MM_SERV_TIP);
        runtime.add(runtimeMMServer);

        // queries panel node
        PanelsTreeNode queries = new PanelsTreeNode(QUERIES, 
        		QUERIES_PANEL_CLASS, connection, false);
        
        tipText = ConsolePlugin.Util.getString("PanelsTreeModel.queryPanel.tooltip"); //$NON-NLS-1$
        queries.setToolTipText(tipText); 
        runtimeMMServer.add(queries);

        // transactions panel node
        PanelsTreeNode transactions = new PanelsTreeNode(TRANSACTIONS,
        		TRANSACTIONS_PANEL_CLASS, connection, false);
        transactions.setToolTipText(
            	"View and manage transactions that are currently being executed " 
            	+ "in the MetaMatrix Server");
        runtimeMMServer.add(transactions);

        // --------------------------------------------------------------------------------
        // setup Configuration header node
        // --------------------------------------------------------------------------------
        PanelsTreeNode configuration = new PanelsTreeNode(CONFIGURATION, null,
        		connection, false);
        
        tipText = ConsolePlugin.Util.getString("PanelsTreeModel.configPanel.tooltip"); //$NON-NLS-1$
        configuration.setToolTipText(tipText); 
        nodeToAddTo.add(configuration);


        // PSC definition & deployments panel node
        PanelsTreeNode pscDeploy = new PanelsTreeNode(PSC_DEPLOY,
        		DEPLOY_PSC_DEFN_PANEL_CLASS, connection, false);
        tipText = ConsolePlugin.Util.getString("PanelsTreeModel.pscDeployPanel.tooltip"); //$NON-NLS-1$
        pscDeploy.setToolTipText(tipText);
        configuration.add(pscDeploy);

        // system properties panel node
        PanelsTreeNode props = new PanelsTreeNode(SYS_PROPS,
        		SYSTEM_PROPERTIES_PANEL_CLASS, connection, false);
        tipText = ConsolePlugin.Util.getString("PanelsTreeModel.systemPropsPanel.tooltip"); //$NON-NLS-1$
        props.setToolTipText(tipText); 
        configuration.add(props);

		// Resources panel node
		PanelsTreeNode resources = new PanelsTreeNode(RESOURCES,
				RESOURCES_PANEL_CLASS, connection, false);
        tipText = ConsolePlugin.Util.getString("PanelsTreeModel.resourcesPanel.tooltip"); //$NON-NLS-1$
		resources.setToolTipText(tipText);
		configuration.add(resources);
		
        // Extension Source panel node
        PanelsTreeNode extSrc = new PanelsTreeNode(EXTENSION_SOURCES,
                EXTENSION_SOURCES_CLASS, connection, false);
        tipText = ConsolePlugin.Util.getString("PanelsTreeModel.extSourcesPanel.tooltip"); //$NON-NLS-1$
        extSrc.setToolTipText(tipText); 
        configuration.add(extSrc);

         // system logging setup node
        PanelsTreeNode systeLogSetup = new PanelsTreeNode(SYSTEMLOGSETUP,
        		SYSTEMLOGSETUP_PANEL_CLASS, connection, false);
        tipText = ConsolePlugin.Util.getString("PanelsTreeModel.sysLoggingPanel.tooltip"); //$NON-NLS-1$
        systeLogSetup.setToolTipText(tipText); 
        configuration.add(systeLogSetup);

        // MetaMatrixServer header node
        PanelsTreeNode configMMServer = new PanelsTreeNode(MM_SERVER, null,
        		connection, false);
        configMMServer.setToolTipText(MM_SERV_TIP);
        configuration.add(configMMServer);

        // VDB panel node
        PanelsTreeNode vdb = new PanelsTreeNode(VDB, VDB_PANEL_CLASS,
        		connection, false);
        tipText = ConsolePlugin.Util.getString("PanelsTreeModel.vdbPanel.tooltip"); //$NON-NLS-1$
        vdb.setToolTipText(tipText);
        configMMServer.add(vdb);

        // connector bindings panel node
        PanelsTreeNode bindings = new PanelsTreeNode(CONNECTOR_BINDINGS,
        		CONNECTOR_BINDINGS_PANEL_CLASS, connection, false);
        tipText = ConsolePlugin.Util.getString("PanelsTreeModel.bindingsPanel.tooltip"); //$NON-NLS-1$
        bindings.setToolTipText(tipText);
        configMMServer.add(bindings);

		// connectors panel node
        PanelsTreeNode connectors = new PanelsTreeNode(CONNECTORS,
        		CONNECTOR_DEFINITIONS_PANEL_CLASS, connection, false);
        tipText = ConsolePlugin.Util.getString("PanelsTreeModel.connectorsPanel.tooltip"); //$NON-NLS-1$
        connectors.setToolTipText(tipText);
        configMMServer.add(connectors);

        // --------------------------------------------------------------------------------
        // Security Tree Node
        // --------------------------------------------------------------------------------
        PanelsTreeNode securityNode = new PanelsTreeNode(AUTHORIZATION, null,
        		connection, false);
        tipText = ConsolePlugin.Util.getString("PanelsTreeModel.securityPanel.tooltip"); //$NON-NLS-1$
        securityNode.setToolTipText(tipText);
        nodeToAddTo.add(securityNode);

	        // --------------------------------------------------------------------------------
	        // Security Summary Tree Node
	        // --------------------------------------------------------------------------------
	        PanelsTreeNode authSummary = new PanelsTreeNode(AUTHORIZATION_SUMMARY, AUTHORIZATION_SUMMARY_PANEL_CLASS,
	        		connection, false);
	        tipText = ConsolePlugin.Util.getString("PanelsTreeModel.authSummaryPanel.tooltip"); //$NON-NLS-1$
	        authSummary.setToolTipText(tipText);
	        securityNode.add(authSummary);

        	// --------------------------------------------------------------------------------
	        // Domain Providers Tree Node
	        // --------------------------------------------------------------------------------
	        PanelsTreeNode providers = new PanelsTreeNode(DOMAIN_PROVIDERS, AUTHORIZATION_PROVIDERS_PANEL_CLASS,
	        		connection, false);
	        tipText = ConsolePlugin.Util.getString("PanelsTreeModel.domainProvidersPanel.tooltip"); //$NON-NLS-1$
	        providers.setToolTipText(tipText);
	        securityNode.add(providers);

	        // --------------------------------------------------------------------------------
	        // Admin Roles Tree Node
	        // --------------------------------------------------------------------------------
	        PanelsTreeNode roles = new PanelsTreeNode(ADMIN_ROLES, ADMIN_ROLES_PANEL_CLASS,
	        		connection, false);
	        tipText = ConsolePlugin.Util.getString("PanelsTreeModel.adminRolesPanel.tooltip"); //$NON-NLS-1$
	        roles.setToolTipText(tipText);
	        securityNode.add(roles);

	        // --------------------------------------------------------------------------------
	        // Data Roles Node
	        // --------------------------------------------------------------------------------
	        // Data Node Entitlements panel node
	        PanelsTreeNode entitlements =
	            new PanelsTreeNode(DATA_ROLES,
	            		ENTITLEMENTS_VIEW_PANEL_CLASS, connection, false);
	        tipText = ConsolePlugin.Util.getString("PanelsTreeModel.dataRolesPanel.tooltip"); //$NON-NLS-1$
	        entitlements.setToolTipText(tipText);
	        securityNode.add(entitlements);    
	}

    public String getHeaderNodeText(PanelsTreeNode theNode) {
        PanelsTreeNode parent = (PanelsTreeNode)theNode.getParent();
        if (parent == null) {
            // root
            return null;
        }
        if (!parent.equals(getRoot())) {
            return getHeaderNodeText(parent);
        }
        return theNode.getName();
    }
}
