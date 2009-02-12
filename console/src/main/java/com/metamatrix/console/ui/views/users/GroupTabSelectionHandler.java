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

import java.awt.Component;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

import javax.swing.Action;
import javax.swing.JPanel;
import javax.swing.JPopupMenu;

import com.metamatrix.console.connections.ConnectionInfo;
import com.metamatrix.console.models.GroupsManager;
import com.metamatrix.console.ui.layout.ConsoleMenuBar;
import com.metamatrix.console.ui.util.RepaintController;
import com.metamatrix.console.util.ExceptionUtility;
import com.metamatrix.console.util.StaticUtilities;
import com.metamatrix.platform.security.api.MetaMatrixPrincipal;
import com.metamatrix.toolbox.ui.widget.ListWidget;

/**
 * Auxilliary class to UserTabMainPanel.  The UserTabMainPanel will instaniate
 * one UserTabSelectionHandler, which will handle:
 * <PRE>
 *  communicating with the UserManager re API calls
 *  repopulating panels as a result of user actions
 *  implementing and populating pop-up menus
 * </PRE>
 */
public class GroupTabSelectionHandler implements NotifyOnSelectionChange {
//        NotifyOnMembersOfGroupChange, NotifyOnPrincipalMembershipsChange,
//        PopupMenuFactory {

    public static final boolean SHOW_USERS_ON_GROUPS_TREE = true;
    public static final String TOP_OF_METAMATRIX_BRANCH_INDICATOR =
            "THE TOP OF METAMATRIX BRANCH";

    private GroupsManager manager;
    private RolesList rolesList;
    private JPanel rightPanel;
//    private boolean canViewPrincipalInfo;
    private boolean canEditPrincipalInfo;
//    private boolean canViewRoleInfo;
    private boolean canEditRoleInfo;
    private boolean showingGroupsTree;
    private RepaintController repaintController;
    private JPopupMenu popupMenu = new JPopupMenu();
        //Will re-use the same popup menu in whatever location
//    private Action createUserAction = null;
    private Action createGroupAction = null;
    private Action[] currentActions = null;
    private RoleDetail currentRoleDetail;
//    private SpecialChangePasswordAction actSpecialChangePassword = null;
//    private final static String SPECIAL_ADMIN_USERID  = "MetaMatrixAdmin";
    public static String SIGNED_ON_USERNAME  = "";


	public GroupTabSelectionHandler(GroupsManager mgr, GroupsTabMainPanel mainPnl, 
    		RolesList rl, JPanel rPanel, 
    		boolean viewsPrincipals, boolean modifiesPrincipals, 
    		boolean viewsRoles, boolean modifiesRoles, 
    		RepaintController rc) {
        super();
        manager = mgr;
//        mainPanel = mainPnl;
//        SIGNED_ON_USERNAME = getSignedOnUserName(manager.getConnection());
//        groupsTree = gt;
//        if (groupsTree != null) {
//            groupsTree.setPopupMenuFactory(this);
//        }
        rolesList = rl;
        rightPanel = rPanel;
        if (rightPanel == null) {
            rightPanel = new JPanel();
        }
//        canViewPrincipalInfo = viewsPrincipals;
        canEditPrincipalInfo = modifiesPrincipals;
//        canViewRoleInfo = viewsRoles;
        canEditRoleInfo = modifiesRoles;
//        canResetPassword = resetsPassword;
        repaintController = rc;
//        showingUsersTree = showUsers;
//        showingGroupsTree = showGroups;
//        showingRolesList = showRoles;
//        createUserAction = new CreateUserAction(manager, this);
//        createGroupAction = new CreateGroupAction(this, manager);
        setCommandsMenuToCreate();
        popupMenu.setName("UserTabSelectionHandler.popupMenu");
//        if (groupsTree != null) {
//            groupsTree.setName("UserTabSelectionHandler.groupsTree");
//        }
    }

    public void setRolesList(RolesList rl) {
        rolesList = rl;
    }

    public JPanel getRightPanel() {
        return rightPanel;
    }

    public void principalCreated(MetaMatrixPrincipal principal) {
//        int type = principal.getType();
//        if (type == MetaMatrixPrincipal.TYPE_GROUP) {
//            addPrincipalToGroupsTree(principal);
//        }
//        Collection /*<String>*/ groups = principal.getGroupNames();
//        Iterator it = groups.iterator();
//        while (it.hasNext()) {
//            String group = (String)it.next();
//            addPrincipalToGroup(group, new Entity(principal));
//        }
    }

    public void selectionChanged(Component component, Object selectionObject) {
        removeSelections(component);
        //Must be role
        ListWidget list = (ListWidget)selectionObject;
        int selectedIndex = list.getSelectedIndex();
        if (selectedIndex >= 0) {
            String role = list.getModel().getElementAt(selectedIndex).toString();
            displaySelectedRole(role);
        }
        setCommandsMenuToCreate();
    }

    public void setProperty(MetaMatrixPrincipal principal, String propertyName,
            String value) {
        Properties changedProperties = new Properties();
        changedProperties.setProperty(propertyName, value);
    }

    private java.util.List /*<Action>*/ formCreateActions() {
        java.util.List /*<Action>*/ act = new ArrayList();
        if (showingGroupsTree) {
            act.add(createGroupAction);
        }
		return act;
    }

    private void setCommandsMenuToCreate() {
        if (canEditPrincipalInfo) {
            java.util.List /*<Action>*/ act = formCreateActions();
            Action[] actions = new Action[act.size()];
            Iterator it = act.iterator();
            for (int i = 0; it.hasNext(); i++) {
                actions[i] = (Action)it.next();
            }
            ConsoleMenuBar.getInstance().setActions(actions);
            currentActions = actions;
        }
    }

    public ConnectionInfo getConnection() {
        return this.manager.getConnection();
    }

    public Action[] getCurrentActions() {
        if (currentActions == null) {
            return new Action[] {};
        }
        return currentActions;
    }

    public java.util.List /*<Action>*/ getCurrentActionsAsList() {
        if (currentActions == null) {
            return new ArrayList(1);
        }
        return Arrays.asList(currentActions);
    }

    /**
     * Removes selections for components OTHER THAN the given component.
     */
    private void removeSelections(Component comp) {
        if ((comp != rolesList) && (rolesList != null)) {
            rolesList.setIgnoreValueChange(true);
            rolesList.removeSelectionInterval(0, rolesList.getModel().getSize() - 1);
            rolesList.setIgnoreValueChange(false);
        }
    }

    private void displaySelectedRole(String displayName) {
        try {
            StaticUtilities.startWait(rightPanel);
            // this role is actually a displayName so we need to look it up
            Map tempRoles = manager.getRoles();
            Iterator it = tempRoles.entrySet().iterator();
            String roleName = null;
            while (it.hasNext()) {
                Map.Entry entry = (Map.Entry)it.next();
                RoleDisplay disp = (RoleDisplay)entry.getValue();
                if (disp.getDisplayName().indexOf(displayName)>=0) {
                    roleName = disp.getName();
                }
            }
            
            //De-register current role detail
            if(currentRoleDetail != null) {
                currentRoleDetail.deregister();
            }
            
            RoleDisplay rDisp = manager.getRoleDisplay(roleName);
            RoleDetail rd = new RoleDetail(rDisp, manager,
                    !canEditRoleInfo, repaintController, getConnection());
            currentRoleDetail = rd;
            setRightPanel(rd);
        } catch (Exception e) {
            ExceptionUtility.showMessage("Create Role Detail", e);
        } finally {
            StaticUtilities.endWait(rightPanel);
        }
    }

    private void setRightPanel(JPanel component) {
        rightPanel.removeAll();
        GridBagLayout layout = new GridBagLayout();
        rightPanel.setLayout(layout);
        rightPanel.add(component);
        layout.setConstraints(component, new GridBagConstraints(0, 0, 1, 1,
                1.0, 1.0, GridBagConstraints.CENTER, GridBagConstraints.BOTH,
                new Insets(0, 0, 0, 0), 0, 0));
    }

}
