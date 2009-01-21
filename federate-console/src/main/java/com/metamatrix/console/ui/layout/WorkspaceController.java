/*
 * JBoss, Home of Professional Open Source.
 * Copyright (C) 2008 Red Hat, Inc.
 * Copyright (C) 2000-2007 MetaMatrix, Inc.
 * Licensed to Red Hat, Inc. under one or more contributor 
 * license agreements.  See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
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

import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import javax.swing.Action;
import javax.swing.JLabel;
import javax.swing.JPanel;

import com.metamatrix.common.log.LogManager;
import com.metamatrix.common.queue.WorkerPool;
import com.metamatrix.common.queue.WorkerPoolFactory;
import com.metamatrix.console.connections.ConnectionAndPanel;
import com.metamatrix.console.connections.ConnectionInfo;
import com.metamatrix.console.models.ExtensionSourceManager;
import com.metamatrix.console.models.GroupsManager;
import com.metamatrix.console.models.ModelManager;
import com.metamatrix.console.models.ResourceManager;
import com.metamatrix.console.models.ServerLogManager;
import com.metamatrix.console.notification.RuntimeUpdateNotification;
import com.metamatrix.console.security.UserCapabilities;
import com.metamatrix.console.ui.NotifyOnExitConsole;
import com.metamatrix.console.ui.views.authorization.ProvidersMain;
import com.metamatrix.console.ui.views.authorization.SummaryMain;
import com.metamatrix.console.ui.views.connector.ConnectorPanel;
import com.metamatrix.console.ui.views.connectorbinding.ConnectorBindingPanel;
import com.metamatrix.console.ui.views.deploy.DeployMainPanel;
import com.metamatrix.console.ui.views.entitlements.EntitlementsPanel;
import com.metamatrix.console.ui.views.extensionsource.ExtensionSourcesPanel;
import com.metamatrix.console.ui.views.logsetup.SystemLogSetUpPanel;
import com.metamatrix.console.ui.views.properties.PropertiesMasterPanel;
import com.metamatrix.console.ui.views.queries.QueryPanel;
import com.metamatrix.console.ui.views.resources.ResourcesMainPanel;
import com.metamatrix.console.ui.views.runtime.RuntimeMgmtPanel;
import com.metamatrix.console.ui.views.sessions.SessionPanel;
import com.metamatrix.console.ui.views.summary.SummaryPanel;
import com.metamatrix.console.ui.views.syslog.SysLogPanel;
import com.metamatrix.console.ui.views.users.AdminRolesMain;
import com.metamatrix.console.ui.views.vdb.VdbMainPanel;
import com.metamatrix.console.util.AutoRefreshable;
import com.metamatrix.console.util.ExceptionUtility;
import com.metamatrix.console.util.LogContexts;
import com.metamatrix.console.util.Refreshable;
import com.metamatrix.console.util.StaticProperties;

/**
 *
 */
public class WorkspaceController implements
                                NotifyOnExitConsole {

    private static WorkspaceController theController = null;

    // Always maintained in order of least recently given focus to
    // most recently given focus
    private List /* <ConnectionAndPanel> */panels = new ArrayList(50);

    private HashMap hmAutoRefreshables = null;
    private Workspace workspace;
    private ConnectionAndPanel currentlyDisplayedPanel = null;
    private boolean programmaticConnectionSelectionChange = false;
    private boolean changingConnections = false;

    /**Worker pool used for changing panels*/
    private WorkerPool workerPool; 

    
    private WorkspaceController(Workspace ws) {
        super();
        workspace = ws;
        
        workerPool = WorkerPoolFactory.newWorkerPool("WorkspaceControllerQueue", 1, 1000); //$NON-NLS-1$
    }

    public static void createInstance(Workspace workspace) {
        if (theController == null) {
            theController = new WorkspaceController(workspace);
        } else {
            String message = "Attempt to create duplicate WorkspaceController"; //$NON-NLS-1$
            LogManager.logError(LogContexts.INITIALIZATION, message);
            throw new RuntimeException(message);
        }
    }

    public static WorkspaceController getInstance() {
        return theController;
    }

    /**
     * Change to a different user connection. Does the work in a background thread, because this can be slow (especially if the
     * server becomes unavailable).
     * 
     * @param connection
     */
    public void connectionSelectionChanged(final ConnectionInfo connection) {

        if (!programmaticConnectionSelectionChange) {
            Thread thread = new Thread() {

                public void run() {
                    try {
                        changingConnections = true;
                        // Find most recently displayed panel for this connection
                        int loc = panels.size() - 1;
                        ConnectionAndPanel cp = null;
                        while ((cp == null) && (loc >= 0)) {
                            ConnectionAndPanel thisCP = (ConnectionAndPanel)panels.get(loc);
                            if (connection.equals(thisCP.getConnection())) {
                                cp = new ConnectionAndPanel(connection, thisCP.getPanelClass(), thisCP.getPanel());
                            } else {
                                loc--;
                            }
                        }
                        if (cp == null) {
                            // Must be first time this connection has been selected. No
                            // panels yet exist. Create the initial one.
                            Class panelClass = ConsoleMainFrame.INITIAL_PANEL_CLASS;
                            WorkspacePanel panel = createPanelOfClass(panelClass, connection);
                            cp = new ConnectionAndPanel(connection, panelClass, panel);
                            // panels.add(cp);
                        }
                        PanelsTree tree = PanelsTree.getInstance(connection);
                        ConsoleMainFrame.getInstance().displayTreeForConnection(connection);
                        tree.selectNodeForPanel(cp);
                        treeSelectionChangedToClass(cp.getPanelClass(), true, connection);
                        changingConnections = false;
                    } catch (Exception e) {
                        // todo: log
                    }
                }
            };

            thread.start();
        }
    }

    public boolean isChangingConnections() {
        return changingConnections;
    }

    /**
     * Change to a different panel. Does the work in a background WorkerPool, because this can be slow (especially if the server
     * becomes unavailable).  The WorkerPool is has maxSize 1, so that requests are processed in order.
     */
    public void treeSelectionChangedToClass(final Class clazz,
                                            final boolean showThePanel,
                                            final ConnectionInfo connection) {
        WorkspaceControllerWorkItem item = new WorkspaceControllerWorkItem(clazz, showThePanel, connection);
        workerPool.execute(item);
    }
    

    /**
     * Do the work of changing to a different panel. See <code>treeSelectionChangedToClass</code>.
     * @param item
     * @since 4.3
     */
    protected void doChangePanel(WorkspaceControllerWorkItem item) {
        Class clazz = item.clazz;
        boolean showThePanel = item.showThePanel;
        ConnectionInfo connection = item.connection;
        
        if (clazz != null) {
            try {
                ConsoleMenuBar.getInstance().emptyTheActionsMenu();
                WorkspacePanel panel = getPanelOfClass(clazz, connection);
                boolean panelAdded = false;
                if (panel == null) {
                    panel = createPanelOfClass(clazz, connection);
                    ConnectionAndPanel cp = new ConnectionAndPanel(connection, panel.getClass(), panel);
                    panels.add(cp);
                    panelAdded = true;
                }
                // If there is an exception on create, an error dialog will have been
                // displayed, and panel will still be null.
                if (panel != null) {
                    // Add this panel to our list of panels. If the panel was
                    // already in the list, first remove it from its current
                    // location in the list.
                    ConnectionAndPanel cp = new ConnectionAndPanel(connection, panel.getClass(), panel);
                    if (!panelAdded) {
                        int index = panelsIndex(cp);
                        if (index >= 0) {
                            panels.remove(index);
                        }
                    }
                    if (panel instanceof AdminRolesMain) {
                        ((AdminRolesMain)panel).refreshData();
                    }

                    if (!panelAdded) {
                        panels.add(cp);
                        panelAdded = true;
                    }
                    updateActions(panel);

                    // Show the panel
                    if (showThePanel) {
                        workspace.showPanel(panel);
                    }
                } else {
                    // Must re-select previous panel or tree will highlight current
                    // selection as being displayed.
                    if (currentlyDisplayedPanel != null) {
                        PanelsTree.getInstance(connection).selectNodeForPanel(currentlyDisplayedPanel);
                    }
                }

                if (panel != null) {
                    currentlyDisplayedPanel = new ConnectionAndPanel(connection, clazz, panel);
                }
            } catch (Exception theException) {
                ExceptionUtility.showMessage("Problem occurred changing to panel " + clazz, theException); //$NON-NLS-1$
                LogManager.logError(LogContexts.GENERAL, theException, "Changing tree selection to " + clazz); //$NON-NLS-1$
            }
             
        }
    }

    public void handleUpdateNotification(ConnectionInfo connection,
                                         RuntimeUpdateNotification notification) {
        int numPanels = panels.size();
        for (int i = numPanels - 1; i >= 0; i--) {
            ConnectionAndPanel cp = (ConnectionAndPanel)panels.get(i);
            ConnectionInfo thisConnection = cp.getConnection();
            if (thisConnection == connection) {
                WorkspacePanel panel = cp.getPanel();
                panel.receiveUpdateNotification(notification);
            }
        }
    }

    private int panelsIndex(ConnectionAndPanel cp) {
        int matchIndex = -1;
        int index = 0;
        int panelsSize = panels.size();
        while ((matchIndex < 0) && (index < panelsSize)) {
            ConnectionAndPanel curCP = (ConnectionAndPanel)panels.get(index);
            if (cp.getConnection().equals(curCP.getConnection())
                && cp.getPanelClass().getName().equals(curCP.getPanelClass().getName())) {
                matchIndex = index;
            } else {
                index++;
            }
        }
        return matchIndex;
    }

    public void updateActions(WorkspacePanel panel) {
        // Set the actions to the panel's current list of actions
        // This can be called by a WorkspacePanel when the wsp desires
        // the menus to change, for example:
        // Workspacecontroller.getInstance().updateActions(this);
        java.util.List /* <Action> */actions = panel.resume();
        if (actions == null) {
            actions = new ArrayList(1);
        }
        if (actions.size() > 0) {
            // determine if the array contains Actions or MenuEntrys

            Object oTemp = actions.get(0);

            if (oTemp instanceof Action) {
                // JMenuItem[] aryMenuItems =
                ConsoleMenuBar.getInstance().setActions(actions);
            } else if (oTemp instanceof MenuEntry) {
                // JMenuItem[] aryMenuItems =
                ConsoleMenuBar.getInstance().addActionsFromMenuEntryObjects(actions);
            }
        } else {
            // JMenuItem[] aryMenuItems =
            ConsoleMenuBar.getInstance().addActionsFromMenuEntryObjects(actions);
        }
        boolean refreshable = (panel instanceof Refreshable);
        ConsoleMenuBar.getInstance().setDefaultRefreshEnabled(refreshable);
    }

    public WorkspacePanel getPanelOfClass(Class cls,
                                          ConnectionInfo connection) {
        String className = cls.getName();
        WorkspacePanel panel = null;
        int i = panels.size() - 1;
        while ((panel == null) && (i >= 0)) {
            ConnectionAndPanel cp = (ConnectionAndPanel)panels.get(i);
            if (connection.equals(cp.getConnection())) {
                WorkspacePanel current = cp.getPanel();
                String currentClassName = current.getClass().getName();
                if (className.equals(currentClassName)) {
                    panel = current;
                } else {
                    i--;
                }
            } else {
                i--;
            }
        }
        return panel;
    }

    public WorkspacePanel createPanelOfClass(Class cls,
                                             ConnectionInfo connection) {
        WorkspacePanel panel = null;
        if (cls == PanelsTreeModel.class) {
            panel = new PendingPanel();
        } else if (cls == SessionPanel.class) {
            panel = createSessionsPanel(connection);
        } else if (cls == SummaryPanel.class) {
            panel = createSummaryPanel(connection);
        } else if (cls == QueryPanel.class) {
            panel = createQueriesPanel(connection);
        } else if (cls == SummaryMain.class) {
            panel = createAuthSummaryPanel(connection);
        } else if (cls == ProvidersMain.class) {
            panel = createProvidersPanel(connection);
        } else if (cls == AdminRolesMain.class) {
            panel = createAdminRolesPanel(connection);
        } else if (cls == VdbMainPanel.class) {
            panel = createVDBPanel(connection);
        } else if (cls == PropertiesMasterPanel.class) {
            panel = createSystemPropertiesPanel(connection);
        } else if (cls == ConnectorPanel.class) {
            panel = createConnectorDefsPanel(connection);
        } else if (cls == ConnectorBindingPanel.class) {
            panel = createConnectorBindingsPanel(connection);
        } else if (cls == EntitlementsPanel.class) {
            panel = createEntitlementsViewPanel(connection);
        } else if (cls == ExtensionSourcesPanel.class) {
            panel = createExtensionSourcesPanel(connection);
        } else if (cls == DeployMainPanel.class) {
            panel = createDeployMainPanel(connection);
        } else if (cls == SysLogPanel.class) {
            panel = createLogPanel(connection);
        } else if (cls == RuntimeMgmtPanel.class) {
            panel = createRuntimeMgmtPanel(connection);
        } else if (cls == SystemLogSetUpPanel.class) {
            panel = createSystemLogSetupPanel(connection);
        } else if (cls == ResourcesMainPanel.class) {
            panel = createResourcesPanel(connection);
        }
        if (panel instanceof AutoRefreshable) {
            addToAutoRefreshableXref(panel);
        }
        return panel;
    }

    public void deletePanelsForConnection(ConnectionInfo connection) {
        ConnectionInfo currentConnection = currentlyDisplayedPanel.getConnection();
        boolean replaceCurrentPanel = currentConnection.equals(connection) && panels.size() > 0;
        int listSize = panels.size();
        for (int i = listSize - 1; i >= 0; i--) {
            ConnectionAndPanel panel = (ConnectionAndPanel)panels.get(i);
            if (panel.getConnection().equals(connection)) {
                panels.remove(i);
            }
        }
        if (replaceCurrentPanel) {
            ConnectionAndPanel lastPanel = (ConnectionAndPanel)panels.get(panels.size() - 1);
            PanelsTree.getInstance(lastPanel.getConnection()).selectNodeForPanel(lastPanel);
            programmaticConnectionSelectionChange = true;
            ConsoleMainFrame.getInstance().selectConnection(connection);
            programmaticConnectionSelectionChange = false;
        }
    }

    private WorkspacePanel createSummaryPanel(ConnectionInfo connection) {
        WorkspacePanel panel = new SummaryPanel(connection);
        return panel;
    }

    private WorkspacePanel createSessionsPanel(ConnectionInfo connection) {
        SessionPanel panel = null;
        try {
            panel = new SessionPanel(connection);
            panel.createComponent();
        } catch (Exception ex) {
            ExceptionUtility.showMessage("Create Sessions Panel", ex); //$NON-NLS-1$
        }
        return panel;
    }

    private DeployMainPanel createDeployMainPanel(ConnectionInfo connection) {
        DeployMainPanel pnl = null;
        try {
            pnl = new DeployMainPanel(connection);
        } catch (Exception theException) {
            ExceptionUtility.showMessage("Create DeployMainPanel", theException); //$NON-NLS-1$
        }
        return pnl;
    }

    private RuntimeMgmtPanel createRuntimeMgmtPanel(ConnectionInfo connection) {
        RuntimeMgmtPanel pnl = null;
        try {
            pnl = new RuntimeMgmtPanel(connection);
        } catch (Exception theException) {
            ExceptionUtility.showMessage("Create RuntimeMgmtPanel", theException); //$NON-NLS-1$
        }
        return pnl;
    }

    private WorkspacePanel createQueriesPanel(ConnectionInfo connection) {
        QueryPanel panel = null;
        try {
            panel = new QueryPanel(connection);
        } catch (Exception ex) {
            ExceptionUtility.showMessage("Create Queries Panel", ex); //$NON-NLS-1$
        }
        return panel;
    }

    private WorkspacePanel createSystemPropertiesPanel(ConnectionInfo connection) {
        PropertiesMasterPanel pPanel = new PropertiesMasterPanel(connection);
        pPanel.createComponent();
        return pPanel;
    }

    private WorkspacePanel createConnectorDefsPanel(ConnectionInfo connection) {
        ConnectorPanel panel = new ConnectorPanel(connection);
        return panel;
    }

    private WorkspacePanel createConnectorBindingsPanel(ConnectionInfo connection) {
        ConnectorBindingPanel panel = new ConnectorBindingPanel(connection);
        return panel;
    }

    private WorkspacePanel createAuthSummaryPanel(ConnectionInfo connection) {
        SummaryMain panel = new SummaryMain(connection);
        return panel;
    }
    
    private WorkspacePanel createProvidersPanel(ConnectionInfo connection) {
        ProvidersMain panel = new ProvidersMain(connection);
        return panel;
    }

    private WorkspacePanel createAdminRolesPanel(ConnectionInfo connection) {
        AdminRolesMain panel = null;
        try {
            UserCapabilities cap = UserCapabilities.getInstance();
            GroupsManager userManager = ModelManager.getGroupsManager(connection);
            panel = new AdminRolesMain(userManager, true, cap.canViewPrincipalInfo(connection),
                                  false, cap.canViewRoleInfo(connection),
                                  cap.canModifyRoleInfo(connection), false);
            panel.createComponent();
        } catch (Exception e) {
            ExceptionUtility.showMessage("Create Roles panel", e); //$NON-NLS-1$
        }
        return panel;
    }

    private WorkspacePanel createEntitlementsViewPanel(ConnectionInfo connection) {
        EntitlementsPanel entPanel = null;
        try {
            // Note-- If ever pursuing on Console whether showing only MetaMatrix
            // or only Enterprise principals, then change hard-coding of booleans
            // below
            entPanel = new EntitlementsPanel(ModelManager.getEntitlementManager(connection), true, true, connection);
        } catch (Exception ex) {
            ExceptionUtility.showMessage("Create Data Roles panel", ex); //$NON-NLS-1$
        }
        return entPanel;
    }

    private WorkspacePanel createResourcesPanel(ConnectionInfo connection) {
        ResourcesMainPanel rp = null;
        try {
            UserCapabilities cap = UserCapabilities.getInstance();
            boolean canModify = cap.canModifyResources(connection);
            ResourceManager mgr = ModelManager.getResourceManager(connection);
            rp = new ResourcesMainPanel(mgr, canModify, connection);
        } catch (Exception ex) {
            ExceptionUtility.showMessage("Create Resources panel", ex); //$NON-NLS-1$
        }
        return rp;
    }

    private WorkspacePanel createExtensionSourcesPanel(ConnectionInfo connection) {
        ExtensionSourceManager manager = ModelManager.getExtensionSourceManager(connection);
        UserCapabilities cap = UserCapabilities.getInstance();
        ExtensionSourcesPanel extPanel = new ExtensionSourcesPanel(manager, cap.canModifyExtensionSources(connection), connection);
        return extPanel;
    }

    private WorkspacePanel createVDBPanel(ConnectionInfo connection) {
        VdbMainPanel panel = new VdbMainPanel(connection);
        return panel;
    }

    private WorkspacePanel createLogPanel(ConnectionInfo connection) {
        SysLogPanel panel = null;
        try {
            panel = new SysLogPanel(connection);
        } catch (Exception ex) {
            panel = null;
            String msg;
            if (ExceptionUtility.containsExceptionOfType(ex, ClassNotFoundException.class)) {
                msg = "Unable to create Log Viewer.  Please ensure that " //$NON-NLS-1$
                      + "JDBC driver is correctly installed."; //$NON-NLS-1$
            } else {
                msg = "Unable to create Log Viewer panel."; //$NON-NLS-1$
            }
            ExceptionUtility.showMessage("Error creating Log Viewer Panel", //$NON-NLS-1$
                                         msg, ex);
            LogManager.logError(LogContexts.SYSTEMLOGGING, ex, "Error creating SysLogPanel"); //$NON-NLS-1$
        }
        return panel;
    }

    private WorkspacePanel createSystemLogSetupPanel(ConnectionInfo connection) {
        ServerLogManager manager = ModelManager.getServerLogManager(connection);
        boolean canModify = false;
        try {
            canModify = UserCapabilities.getInstance().canModifyLoggingConfig(connection);
        } catch (Exception ex) {
            // Cannot occur
        }
        SystemLogSetUpPanel panel = new SystemLogSetUpPanel(manager, canModify, connection);
        return panel;
    }

    public boolean havePendingChanges(ConnectionInfo connection) {
        boolean pending = false;
        int i = panels.size() - 1;
        while ((i >= 0) && (!pending)) {
            ConnectionAndPanel cp = (ConnectionAndPanel)panels.get(i);
            if ((connection == null) || connection.equals(cp.getConnection())) {
                WorkspacePanel panel = cp.getPanel();
                if (panel instanceof NotifyOnExitConsole) {
                    pending = ((NotifyOnExitConsole)panel).havePendingChanges();
                }
            }
            i--;
        }
        return pending;
    }

    public boolean havePendingChanges() {
        return havePendingChanges(null);
    }

    public boolean finishUp(ConnectionInfo connection) {
        boolean exiting = true;
        java.util.List panelsBeforeReordering = new ArrayList(panels.size());
        Iterator it = panels.iterator();
        while (it.hasNext()) {
            panelsBeforeReordering.add(it.next());
        }
        int i = panelsBeforeReordering.size() - 1;
        while ((i >= 0) && exiting) {
            ConnectionAndPanel cp = (ConnectionAndPanel)panelsBeforeReordering.get(i);
            if ((connection == null) || connection.equals(cp.getConnection())) {
                WorkspacePanel panel = cp.getPanel();
                if (panel instanceof NotifyOnExitConsole) {
                    NotifyOnExitConsole notifyee = (NotifyOnExitConsole)panel;
                    if (notifyee.havePendingChanges()) {
                        ConnectionInfo conn = panel.getConnection();
                        cp = new ConnectionAndPanel(conn, notifyee.getClass(), panel);
                        programmaticConnectionSelectionChange = true;
                        ConsoleMainFrame.getInstance().selectConnection(conn);
                        ConsoleMainFrame.getInstance().displayTreeForConnection(conn);
                        ConsoleMainFrame.getInstance().selectPanel(cp);

                        // Event to cause showPanel() to be called will not be
                        // fired if the tree node corresponding to this panel was
                        // still the selected node in the tree which we have just
                        // displayed. So we have to explicitly call showPanel().

                        workspace.showPanel(cp.getPanel());

                        programmaticConnectionSelectionChange = false;
                        exiting = notifyee.finishUp();
                    }
                }
            }
            i--;
        }
        return exiting;
    }

    public boolean finishUp() {
        return finishUp(null);
    }

    private HashMap getAutoRefreshableXref() {
        if (hmAutoRefreshables == null) {
            hmAutoRefreshables = new HashMap();
        }
        return hmAutoRefreshables;
    }

    private void addToAutoRefreshableXref(WorkspacePanel wsp) {
        if (wsp instanceof AutoRefreshable) {
            getAutoRefreshableXref().put(((AutoRefreshable)wsp).getName(), wsp);
            applyAutoRefreshParmsToWorkspacePanel(wsp);
        }
    }

    public void applyAutoRefreshParmsToAll() {
        Collection colAutoRefreshableWorkspacePanels = getAutoRefreshableXref().values();

        Iterator itWSPs = colAutoRefreshableWorkspacePanels.iterator();

        while (itWSPs.hasNext()) {
            WorkspacePanel wsp = (WorkspacePanel)itWSPs.next();
            applyAutoRefreshParmsToWorkspacePanel(wsp);
        }

    }

    public void applyAutoRefreshParmsToWorkspacePanel(WorkspacePanel wsp) {
        if (!(wsp instanceof AutoRefreshable)) {
            return;
        }
        if (wsp instanceof SummaryPanel) {
            applyAutoRefreshParms((AutoRefreshable)wsp,
                                  StaticProperties.getSummaryRefreshEnabled(),
                                  StaticProperties.getSummaryRefreshRate());
        }

        if (wsp instanceof QueryPanel) {
            applyAutoRefreshParms((AutoRefreshable)wsp,
                                  StaticProperties.getQueryRefreshEnabled(),
                                  StaticProperties.getQueryRefreshRate());
        }

        if (wsp instanceof SessionPanel) {
            applyAutoRefreshParms((AutoRefreshable)wsp,
                                  StaticProperties.getSessionRefreshEnabled(),
                                  StaticProperties.getSessionRefreshRate());
        }

        if (wsp instanceof SysLogPanel) {
            applyAutoRefreshParms((AutoRefreshable)wsp,
                                  StaticProperties.getSysLogRefreshEnabled(),
                                  StaticProperties.getSysLogRefreshRate());
        }

    }

    public void applyAutoRefreshParms(AutoRefreshable refAutoRefreshablePanel,
                                      boolean bEnabled,
                                      int iRate) {
        refAutoRefreshablePanel.setAutoRefreshEnabled(bEnabled);
        refAutoRefreshablePanel.setRefreshRate(iRate);
    }
}// end WorkspaceController

class PendingPanel extends JPanel implements
                                 WorkspacePanel {

    public PendingPanel() {
        super();
        JLabel label = new JLabel("Hold yer dadburn horses we ain't bin writ yet."); //$NON-NLS-1$
        GridBagLayout layout = new GridBagLayout();
        setLayout(layout);
        add(label);
        layout.setConstraints(label, new GridBagConstraints(0, 0, 1, 1, 0.0, 0.0, GridBagConstraints.CENTER,
                                                            GridBagConstraints.NONE, new Insets(0, 0, 0, 0), 0, 0));
        ConsoleMenuBar.getInstance().emptyTheActionsMenu();
    }

    public java.util.List /* <Action> */resume() {
        return null;
    }

    public void postRealize() {
    }

    public String getTitle() {
        return "Coming Attraction"; //$NON-NLS-1$
    }

    public ConnectionInfo getConnection() {
        return null;
    }

    public void receiveUpdateNotification(RuntimeUpdateNotification notification) {
    }
}// end PendingPanel
