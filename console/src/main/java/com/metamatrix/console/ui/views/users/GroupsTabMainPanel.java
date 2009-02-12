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

package com.metamatrix.console.ui.views.users;

import java.awt.Graphics;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;
import java.awt.event.ActionEvent;
import java.util.ArrayList;
import java.util.List;

import javax.swing.Action;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JSplitPane;

import com.metamatrix.common.log.LogManager;
import com.metamatrix.console.connections.ConnectionInfo;
import com.metamatrix.console.models.GroupsManager;
import com.metamatrix.console.notification.RuntimeUpdateNotification;
import com.metamatrix.console.ui.layout.BasePanel;
import com.metamatrix.console.ui.layout.MenuEntry;
import com.metamatrix.console.ui.layout.WorkspacePanel;
import com.metamatrix.console.ui.util.AbstractPanelAction;
import com.metamatrix.console.ui.util.RepaintController;
import com.metamatrix.console.ui.util.property.PropertyProvider;
import com.metamatrix.console.util.ExceptionUtility;
import com.metamatrix.console.util.ExternalException;
import com.metamatrix.console.util.LogContexts;
import com.metamatrix.toolbox.ui.widget.ButtonWidget;
import com.metamatrix.toolbox.ui.widget.Splitter;

/**
 * The main panel of the Users tab.  Itself it does no updates.  Those are
 * done by its UserTabSelectionHandler object.
 *
 * This panel makes the assumption that at least one of canViewPrincipalInfo and
 * canViewRoleInfo is true.  If not, it should not be instantiated.
 */
public class GroupsTabMainPanel extends BasePanel implements RepaintController,
        WorkspacePanel {
//    public static final boolean SHOW_GROUPS_TREE_USER_NODES = true;
//    public static final boolean ALLOW_SELECTION_OF_GROUPS_TREE_USER_NODES = false;

    private GroupsManager manager;
    private RolesList rolesList;
    private ButtonWidget addButton;
    private JPanel rightPanel = new JPanel();
    private GroupTabSelectionHandler selectionHandler;
    private boolean canViewPrincipalInfo;
    private boolean canModifyPrincipalInfo;
    private boolean canViewRoleInfo;
    private boolean canModifyRoleInfo;
    private JSplitPane overallSplitPane;
    private boolean hasBeenPainted = false;
    private boolean showingRolesList = true;

    private PanelAction actionRefresh;
    
    public GroupsTabMainPanel(GroupsManager mgr, boolean seesEnterprise,
            boolean viewsPrincipals, boolean modifiesPrincipals,
            boolean viewsRoles, boolean modifiesRoles) {
        super();
        manager = mgr;
        canViewRoleInfo = viewsRoles;
        canModifyRoleInfo = modifiesRoles;
        actionRefresh = new PanelAction(this, 0);
	}

    public void createComponent() throws Exception {
        //Create the GUI subcomponents
        selectionHandler = new GroupTabSelectionHandler(manager, this,
                null, rightPanel, canViewPrincipalInfo, canModifyPrincipalInfo,
                canViewRoleInfo, canModifyRoleInfo, this);
        if (canViewRoleInfo) {
            rolesList = new RolesList(manager, selectionHandler, showingRolesList);
            selectionHandler.setRolesList(rolesList);
        }
        doTheLayout();
    }

    public void refreshData(){
        if (rolesList != null){
            rolesList.refreshData();
        }
    }

    public java.util.List /*<Action>*/ resume() {
        ArrayList actions = new ArrayList();
        actions.add(new MenuEntry(MenuEntry.VIEW_REFRESH_MENUITEM, actionRefresh));
        List otherActions = selectionHandler.getCurrentActionsAsList();
        for (int size=otherActions.size(), i=0; i<size; i++) {
            actions.add(new MenuEntry(MenuEntry.ACTION_MENUITEM,
                        (Action)otherActions.get(i)));
        }
        return actions;
    }
    
    public void receiveUpdateNotification(RuntimeUpdateNotification notification) {
    	//TODO
    }

	public boolean isShowingRolesList() {
		return showingRolesList;
	}
	
    public void setAddUser(boolean enableAddUser){
        if (addButton != null)
            addButton.setEnabled(enableAddUser);
    }

    public String getTitle() {
        //Unused-- needed by WorkspacePanel
        return "";
    }

	public ConnectionInfo getConnection() {
		return manager.getConnection();
	}
	
    public RolesList getRolesList() {
        return rolesList;
    }

    private void doTheLayout() {
        //Put all of the subcomponents in scroll panes, and the scroll panes
        //in split panes as necessary.  There will always be a vertical split
        //pane (overallSplitPane).  There are three possibilities for what is
        //displayed on the left side of the overall split pane--
        //1) Users tree, Groups tree, and Roles list.  In this case the Users
        //tree and Groups tree are contained in innerLeftSplitPane, which then
        //is contained in outerLeftSplitPane along with the Roles list.
        //2) Just Users tree and Groups tree.  They share innerSplitPane.
        //3) Just Roles list.  No left-side split pane.
        JPanel rolesPanel = new JPanel();
//        GridBagLayout ul = new GridBagLayout();

        if (canViewRoleInfo) {
            GridBagLayout rl = new GridBagLayout();
            rolesPanel.setLayout(rl);
            //IconLabel rolesLabel = new IconLabel(ConsoleCellRenderer.ROLE_ICON,
            //        "Roles");
            //rolesPanel.add(rolesLabel);
            //rl.setConstraints(rolesLabel, new GridBagConstraints(0, 0, 1, 1,
            //        0.0, 0.0, GridBagConstraints.WEST, GridBagConstraints.NONE,
            //        new Insets(0, 0, 0, 0), 0, 0));
            JScrollPane rolesScrollPane = new JScrollPane(rolesList);
            rolesPanel.add(rolesScrollPane);
            rl.setConstraints(rolesScrollPane, new GridBagConstraints(0, 0, 1, 1,
                    1.0, 1.0, GridBagConstraints.CENTER, GridBagConstraints.BOTH,
                    new Insets(0, 0, 0, 0), 0, 0));
        }

        overallSplitPane = new Splitter(JSplitPane.HORIZONTAL_SPLIT,
                true, rolesPanel, rightPanel);

        overallSplitPane.setDividerLocation(0.28);

        //Add overallSplitPane as the one and only component of a GridBagLayout.
        GridBagLayout layout = new GridBagLayout();
        setLayout(layout);
        add(overallSplitPane);
        layout.setConstraints(overallSplitPane, new GridBagConstraints(0, 0,
                1, 1, 1.0, 1.0, GridBagConstraints.CENTER,
                GridBagConstraints.BOTH, new Insets(0, 0, 0, 0), 0, 0));
    }


    public void paint(Graphics g) {
        super.paint(g);
        if (!hasBeenPainted) {
            overallSplitPane.invalidate();
            jiggleVerticalSplitter();
            overallSplitPane.setDividerLocation(0.28);
            hasBeenPainted = true;
            super.paint(g);
        }
    }


    public void repaintNeeded() {
        jiggleVerticalSplitter();
    }

    private void jiggleVerticalSplitter() {
        if (overallSplitPane != null) {
            int splitterLoc = overallSplitPane.getDividerLocation();
            int increment = -1;
            if (splitterLoc == 0) {
                increment = 1;
            }
            overallSplitPane.setDividerLocation(splitterLoc + increment);
            overallSplitPane.setDividerLocation(splitterLoc);
        }
    }

    private void refreshImpl() throws ExternalException {
    	if (showingRolesList) {
    		rolesList.refreshData();
    	} 
    }

    ///////////////////////////////////////////////////////////////////////////
    // INNER CLASSES
    ///////////////////////////////////////////////////////////////////////////

    private class PanelAction extends AbstractPanelAction {
        public static final int REFRESH = 0;
        
//        private UserTabMainPanel caller;

        public PanelAction(GroupsTabMainPanel caller, int theType) {
            super(theType);
//            this.caller = caller;
            if (theType == REFRESH) {
            	String desc = "";
            	if (caller.isShowingRolesList()) {
            		desc = "Refreshes roles data.";
            	}
                putValue(SHORT_DESCRIPTION, desc);
                putValue(MENU_ITEM_NAME, "Refresh");
                PropertyProvider pp =
                    new PropertyProvider(
                        "com/metamatrix/console/ui/data/common_ui");
                putValue(Action.SMALL_ICON, pp.getIcon("icon.refresh"));
            }
            else {
                throw new IllegalArgumentException(
                    "Invalid action type <" + theType + ">.");
            }
        }
        public void actionImpl(ActionEvent theEvent)
            throws ExternalException {

            if (type == REFRESH) {
                refreshImpl();
            }
        }
        protected void handleError(Exception theException) {
            if (type == REFRESH) {
                String emsg = "Error refreshing group data.";
                ExceptionUtility.showMessage(emsg,
                                             theException.getMessage(),
                                             theException);
                LogManager.logError(LogContexts.USERS,
                                    theException,
                                    paramString());
            }
            else {
                super.handleError(theException);
            }
        }
    }

}
