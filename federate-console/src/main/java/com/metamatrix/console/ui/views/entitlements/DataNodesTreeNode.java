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

package com.metamatrix.console.ui.views.entitlements;

import java.util.Enumeration;
import java.util.Iterator;

import com.metamatrix.common.vdb.api.SystemVdbUtility;
import com.metamatrix.console.ui.tree.ChildSortingTreeNode;
import com.metamatrix.console.ui.tree.SortsChildren;
import com.metamatrix.console.util.StaticTreeUtilities;
import com.metamatrix.platform.admin.api.PermissionNode;
import com.metamatrix.platform.security.api.AuthorizationActions;
import com.metamatrix.platform.security.api.StandardAuthorizationActions;
import com.metamatrix.toolbox.ui.widget.CheckBox;

public class DataNodesTreeNode extends ChildSortingTreeNode {
    public static boolean hasDelete(AuthorizationActions actions) {
        String[] actionLabels = actions.getLabels();
        return DataNodesTreeNode.stringArrayContains(actionLabels,
        		StandardAuthorizationActions.DATA_DELETE_LABEL);
    }
    
    public static boolean hasCreate(AuthorizationActions actions) {
        String[] actionLabels = actions.getLabels();
        return DataNodesTreeNode.stringArrayContains(actionLabels,
        		StandardAuthorizationActions.DATA_CREATE_LABEL);
    }
    
    public static boolean hasRead(AuthorizationActions actions) {
        String[] actionLabels = actions.getLabels();
        return DataNodesTreeNode.stringArrayContains(actionLabels,
        		StandardAuthorizationActions.DATA_READ_LABEL);
    }
    
    public static boolean hasUpdate(AuthorizationActions actions) {
        String[] actionLabels = actions.getLabels();
        return DataNodesTreeNode.stringArrayContains(actionLabels,
        		StandardAuthorizationActions.DATA_UPDATE_LABEL);
    }
    
    public static boolean stringArrayContains(String[] array, String str) {
        int i = 0;
        boolean found = false;
        while ((!found) && (i < array.length)) {
            if (array[i].equals(str)) {
                found = true;
            } else {
                i++;
            }
        }
        return found;
    }
    
    //Is also a PermissionNode:
    private com.metamatrix.common.tree.TreeNode correspondingTreeNode;

    private boolean rootNode;
    private String nodeDisplayName;
    private String nodeFullName;
    private boolean createEnabled;
    private boolean readEnabled;
    private boolean updateEnabled;
    private boolean deleteEnabled;
    private int createState;
    private int readState;
    private int updateState;
    private int deleteState;
    private int initialCreateState;
    private int initialReadState;
    private int initialUpdateState;
    private int initialDeleteState;
    private int recalcCreateStateFlag = -1;
    private int recalcReadStateFlag = -1;
    private int recalcUpdateStateFlag = -1;
    private int recalcDeleteStateFlag = -1;
    private AuthorizationsTreeTable treeTable;
    
    public DataNodesTreeNode(String displayName, String fullName,
            com.metamatrix.common.tree.TreeNode treeNode,
            boolean root, int create, int read, int update, int delete,
            boolean userCanModifyEntitlements) {
        super(null, displayName);
        nodeDisplayName = displayName;
        nodeFullName = fullName;
        correspondingTreeNode = treeNode;
        if (correspondingTreeNode != null) {
            setEnabledFlags(userCanModifyEntitlements);
        }
        rootNode = root;
        createState = create;
        readState = read;
        updateState = update;
        deleteState = delete;
		initialCreateState = createState;
        initialReadState = readState;
        initialUpdateState = updateState;
        initialDeleteState = deleteState;
    }

    private void setEnabledFlags(boolean userCanModifyEntitlements) {
        createEnabled = false;
        readEnabled = false;
        updateEnabled = false;
        deleteEnabled = false;
        if (userCanModifyEntitlements) {
            PermissionNode node = (PermissionNode)correspondingTreeNode;
            AuthorizationActions allowedActions = node.getAllowedActions();
            String[] labels = allowedActions.getLabels();
            for (int i = 0; i < labels.length; i++) {
                if (labels[i].equals(
                		StandardAuthorizationActions.DATA_CREATE_LABEL)) {
                    createEnabled = true;
                } else if (labels[i].equals(
                		StandardAuthorizationActions.DATA_READ_LABEL)) {
                    readEnabled = true;
                } else if (labels[i].equals(
                		StandardAuthorizationActions.DATA_UPDATE_LABEL)) {
                    updateEnabled = true;
                } else if (labels[i].equals(
                		StandardAuthorizationActions.DATA_DELETE_LABEL)) {
                    deleteEnabled = true;
                }
            }
        }
    }

    public void setTreeTable(AuthorizationsTreeTable treeTbl) {
        treeTable = treeTbl;
    }

    public boolean isRootNode() {
        return rootNode;
    }

	public void setRootNode(boolean flag) {
	    rootNode = flag;
	}
	
    public boolean parentIsRoot() {
        boolean parentIsRoot = false;
        if (!isRoot()) {
            DataNodesTreeNode parent = (DataNodesTreeNode)this.getParent();
            parentIsRoot = parent.isRoot();
        }
        return parentIsRoot;
    }
    
    public com.metamatrix.common.tree.TreeNode getCorrespondingTreeNode() {
        return correspondingTreeNode;
    }

    public String getNodeDisplayName() {
        return nodeDisplayName;
    }

	public void setNodeDisplayName(String name) {
	    nodeDisplayName = name;
	    this.setUserObject(nodeDisplayName);
	}
	
    public String getNodeFullName() {
        return nodeFullName;
    }

    public int getCreateState() {
        return createState;
    }

    public void setCreateState(Integer val) {
        setCreateState(val.intValue());
    }

    public void setCreateState(int val) {
        if (createState != val) {
            Integer newState = null;
            if (treeTable != null) {
                newState = treeTable.proceedWithAuthorizationChange(this,
                        AuthorizationsModel.CREATE_COLUMN_NUM);
            }
            if (newState != null) {
                createState = val;
                //Note-- For unknown reasons which time constraints do not allow
                //investigating, having DataNodesAuthorizationControl add itself as
                //a TableModelListener to the treetable is not causing its
                //tableChanged() method to be called.  So we are here directly
                //telling the table that it has been changed.  The table will pass
                //this info on to DataNodesAuthorizationControl, which will then
                //determine whether or not the Apply and Reset buttons should be
                //enabled.  BWP 11/07/01
                if (treeTable != null) {
                    treeTable.checkBoxChanged(this, AuthorizationsModel.CREATE_COLUMN_NUM,
                            newState.intValue());
                }
            }
        }
    }

    public int getInitialCreateState() {
        return initialCreateState;
    }

    public void setInitialCreateState(int state) {
        initialCreateState = state;
    }

    public int getReadState() {
        return readState;
    }

    public void setReadState(Integer val) {
        setReadState(val.intValue());
    }

    public void setReadState(int val) {
        if (readState != val) {
            Integer newState = null;
            if (treeTable != null) {
                newState = treeTable.proceedWithAuthorizationChange(this,
                        AuthorizationsModel.READ_COLUMN_NUM);
            }
            if (newState != null) {
                readState = val;
                //Note-- For unknown reasons which time constraints do not allow
                //investigating, having DataNodesAuthorizationControl add itself as
                //a TableModelListener to the treetable is not causing its
                //tableChanged() method to be called.  So we are here directly
                //telling the table that it has been changed.  The table will pass
                //this info on to DataNodesAuthorizationControl, which will then
                //determine whether or not the Apply and Reset buttons should be
                //enabled.  BWP 11/07/01
                if (treeTable != null) {
                    treeTable.checkBoxChanged(this,
                            AuthorizationsModel.READ_COLUMN_NUM,
                            newState.intValue());
                }
            }
        }
    }

    public int getInitialReadState() {
        return initialReadState;
    }

    public void setInitialReadState(int state) {
        initialReadState = state;
    }

    public int getUpdateState() {
        return updateState;
    }

    public void setUpdateState(Integer val) {
        setUpdateState(val.intValue());
    }

    public void setUpdateState(int val) {
        if (updateState != val) {
            Integer newState = null;
            if (treeTable != null) {
                newState = treeTable.proceedWithAuthorizationChange(this,
                        AuthorizationsModel.UPDATE_COLUMN_NUM);
            }
            if (newState != null) {
                updateState = val;
                //Note-- For unknown reasons which time constraints do not allow
                //investigating, having DataNodesAuthorizationControl add itself as
                //a TableModelListener to the treetable is not causing its
                //tableChanged() method to be called.  So we are here directly
                //telling the table that it has been changed.  The table will pass
                //this info on to DataNodesAuthorizationControl, which will then
                //determine whether or not the Apply and Reset buttons should be
                //enabled.  BWP 11/07/01
                if (treeTable != null) {
                    treeTable.checkBoxChanged(this,
                            AuthorizationsModel.UPDATE_COLUMN_NUM,
                            newState.intValue());
                }
            }
        }
    }

    public int getInitialUpdateState() {
        return initialUpdateState;
    }

    public void setInitialUpdateState(int state) {
        initialUpdateState = state;
    }

    public int getDeleteState() {
        return deleteState;
    }

    public void setDeleteState(Integer val) {
        setDeleteState(val.intValue());
    }

    public void setDeleteState(int val) {
        if (deleteState != val) {
            Integer newState = null;
            if (treeTable != null) {
                newState = treeTable.proceedWithAuthorizationChange(this,
                        AuthorizationsModel.DELETE_COLUMN_NUM);
            }
            if (newState != null) {
                deleteState = val;
                //Note-- For unknown reasons which time constraints do not allow
                //investigating, having DataNodesAuthorizationControl add itself as
                //a TableModelListener to the treetable is not causing its
                //tableChanged() method to be called.  So we are here directly
                //telling the table that it has been changed.  The table will pass
                //this info on to DataNodesAuthorizationControl, which will then
                //determine whether or not the Apply and Reset buttons should be
                //enabled.  BWP 11/07/01
                if (treeTable != null) {
                    treeTable.checkBoxChanged(this,
                            AuthorizationsModel.DELETE_COLUMN_NUM,
                            newState.intValue());
                }
            }
        }
    }

    public int getInitialDeleteState() {
        return initialDeleteState;
    }

    public void setInitialDeleteState(int state) {
        initialDeleteState = state;
    }

    public boolean isCreateEnabled() {
        return createEnabled;
    }

    public boolean isReadEnabled() {
        return readEnabled;
    }

    public boolean isUpdateEnabled() {
        return updateEnabled;
    }

    public boolean isDeleteEnabled() {
        return deleteEnabled;
    }

    public boolean sortChildren() {

        int numChildren = this.getChildCount();
        boolean changed = super.sortChildren();
        if (isRootNode() && (numChildren > 1)) {
            int matchLoc = -1;
            int loc = 0;
            while ((matchLoc < 0) && (loc < numChildren)) {
                DataNodesTreeNode curChild = (DataNodesTreeNode)this.getChildAt(loc);
                if (curChild.getNodeDisplayName().equals(SystemVdbUtility.PHYSICAL_MODEL_NAME)) {
                    matchLoc = loc;
                } else {
                    loc++;
                }
            }
            if (matchLoc >= 0) {
                DataNodesTreeNode nodeToMove = (DataNodesTreeNode)this.getChildAt(
                        matchLoc);
                this.remove(matchLoc);
                this.setSortType(SortsChildren.NO_SORT);
                this.add(nodeToMove);
                //Must reset to any value != NO_SORT
                this.setSortType(SortsChildren.ALPHABETIC_SORT);
                changed = true;
            }

            matchLoc = -1;
            loc = 0;
            while ((matchLoc < 0) && (loc < numChildren)) {
                DataNodesTreeNode curChild = (DataNodesTreeNode)this.getChildAt(loc);
                if (curChild.getNodeDisplayName().equals(SystemVdbUtility.VIRTUAL_MODEL_NAME)) {
                    matchLoc = loc;
                } else {
                    loc++;
                }
            }
            if (matchLoc >= 0) {
                DataNodesTreeNode nodeToMove = (DataNodesTreeNode)this.getChildAt(
                        matchLoc);
                this.remove(matchLoc);
                this.setSortType(SortsChildren.NO_SORT);
                this.add(nodeToMove);
                //Must reset to any value != NO_SORT
                this.setSortType(SortsChildren.ALPHABETIC_SORT);
                changed = true;
            }
        }
        return changed;
    }

    public boolean hasDescendantUnauthorized(int columnID) {
        java.util.List /*<DataNodesTreeNode>*/ descendants =
                StaticTreeUtilities.descendantsOfNode(this, false);
        boolean unauthorized = false;
        Iterator it = descendants.iterator();
        while ((!unauthorized) && it.hasNext()) {
            DataNodesTreeNode node = (DataNodesTreeNode)it.next();
            PermissionNode pn = 
            		(PermissionNode)node.getCorrespondingTreeNode();
            AuthorizationActions allowedActions = pn.getAllowedActions();
            boolean authorizable = false;
            int value = -1;
            switch (columnID) {
                case AuthorizationsModel.CREATE_COLUMN_NUM:
                	authorizable = DataNodesTreeNode.hasCreate(allowedActions);
                    value = node.getCreateState();
                    break;
                case AuthorizationsModel.READ_COLUMN_NUM:
                	authorizable = DataNodesTreeNode.hasRead(allowedActions);
                    value = node.getReadState();
                    break;
                case AuthorizationsModel.UPDATE_COLUMN_NUM:
                	authorizable = DataNodesTreeNode.hasUpdate(allowedActions);
                    value = node.getUpdateState();
                    break;
                case AuthorizationsModel.DELETE_COLUMN_NUM:
                	authorizable = DataNodesTreeNode.hasDelete(allowedActions);
                    value = node.getDeleteState();
                    break;
            }
            if (value == CheckBox.DESELECTED) {
                if (authorizable) {
                    unauthorized = true;
                }
            }
        }
        return unauthorized;
    }

    public boolean hasDescendantAuthorized(int columnID) {
        java.util.List /*<DataNodesTreeNode>*/ descendants =
                StaticTreeUtilities.descendantsOfNode(this, false);
        boolean authorized = false;
        Iterator it = descendants.iterator();
        while ((!authorized) && it.hasNext()) {
            DataNodesTreeNode node = (DataNodesTreeNode)it.next();
            int value = -1;
            switch (columnID) {
                case AuthorizationsModel.CREATE_COLUMN_NUM:
                    value = node.getCreateState();
                    break;
                case AuthorizationsModel.READ_COLUMN_NUM:
                    value = node.getReadState();
                    break;
                case AuthorizationsModel.UPDATE_COLUMN_NUM:
                    value = node.getUpdateState();
                    break;
                case AuthorizationsModel.DELETE_COLUMN_NUM:
                    value = node.getDeleteState();
                    break;
            }
            if (value != CheckBox.DESELECTED) {
                authorized = true;
            }
        }
        return authorized;
    }

    public void recalculateStates() {
        for (int counter = 0; counter <= 3; counter++) {
            int col = -1;
            switch (counter) {
                case 0:
                    col = AuthorizationsModel.CREATE_COLUMN_NUM;
                    break;
                case 1:
                    col = AuthorizationsModel.READ_COLUMN_NUM;
                    break;
                case 2:
                    col = AuthorizationsModel.UPDATE_COLUMN_NUM;
                    break;
                case 3:
                    col = AuthorizationsModel.DELETE_COLUMN_NUM;
                    break;
            }
            recalculateState(col);
        }
    }

    /**
     * Recalculates the initial state of a node for a given column (c, r, u, or d).
     * Will change to SELECTED all setable descendant nodes are set for the
     * column.  
     *
     * @param   col     column indicator:  AuthorizationsModel.CREATE_COLUMN_NUM, etc.
     * @return  int     one of CheckBox.DESELECTED or CheckBox.SELECTED
     */
    public int recalculateState(int col) {
        boolean isSet = false;
        int value = -1;
        switch (col) {
            case AuthorizationsModel.CREATE_COLUMN_NUM:
                isSet = (this.recalcCreateStateFlag >= 0);
                value = this.createState;
                break;
            case AuthorizationsModel.READ_COLUMN_NUM:
                isSet = (this.recalcReadStateFlag >= 0);
                value = this.readState;
                break;
            case AuthorizationsModel.UPDATE_COLUMN_NUM:
                isSet = (this.recalcUpdateStateFlag >= 0);
                value = this.updateState;
                break;
            case AuthorizationsModel.DELETE_COLUMN_NUM:
                isSet = (this.recalcDeleteStateFlag >= 0);
                value = this.deleteState;
                break;
        }
        if (!isSet) {
            boolean changingToChecked = false;
            int numChildren = this.getChildCount();
            if (numChildren > 0) {
                Enumeration enumeration = this.children();
                boolean uncheckedFound = false;
                boolean checkedFound = false;
                while (enumeration.hasMoreElements()) {
                    DataNodesTreeNode child = (DataNodesTreeNode)enumeration.nextElement();
                    int childsState = child.recalculateState(col);
                    if (childsState == CheckBox.DESELECTED) {
                        PermissionNode pn = 
                        		(PermissionNode)child.getCorrespondingTreeNode();
                        AuthorizationActions allowedActions = 
                        		pn.getAllowedActions();
                        boolean selectable = false;
                        switch (col) {
                            case AuthorizationsModel.CREATE_COLUMN_NUM: 
                            	selectable = DataNodesTreeNode.hasCreate(
                            			allowedActions);
                            	break;
                            case AuthorizationsModel.READ_COLUMN_NUM:
                            	selectable = DataNodesTreeNode.hasRead(
                            			allowedActions);
                            	break;
                            case AuthorizationsModel.UPDATE_COLUMN_NUM:
                            	selectable = DataNodesTreeNode.hasUpdate(
                            			allowedActions);
                            	break;
                            case AuthorizationsModel.DELETE_COLUMN_NUM:
                            	selectable = DataNodesTreeNode.hasDelete(
                            			allowedActions);
                            	break;
                        }
                        if (selectable) {
                            uncheckedFound = true;
                        }
                    } else {
                        checkedFound = true;
                    }
                }
                changingToChecked = (checkedFound && (!uncheckedFound));
            }
            if (changingToChecked) {
                value = CheckBox.SELECTED;
            }
            switch (col) {
                case AuthorizationsModel.CREATE_COLUMN_NUM:
                    this.recalcCreateStateFlag = 1;
                    if (changingToChecked) {
						this.createState = CheckBox.SELECTED;
                        this.initialCreateState = CheckBox.SELECTED;
                    }
                    break;
                case AuthorizationsModel.READ_COLUMN_NUM:
                    this.recalcReadStateFlag = 1;
                    if (changingToChecked) {
                        this.readState = CheckBox.SELECTED;
                        this.initialReadState = CheckBox.SELECTED;
                    }
                    break;
                case AuthorizationsModel.UPDATE_COLUMN_NUM:
                    this.recalcUpdateStateFlag = 1;
                    if (changingToChecked) {
                        this.updateState = CheckBox.SELECTED;
                        this.initialUpdateState = CheckBox.SELECTED;
                    }
                    break;
                case AuthorizationsModel.DELETE_COLUMN_NUM:
                    this.recalcDeleteStateFlag = 1;
                    if (changingToChecked) {
                        this.deleteState = CheckBox.SELECTED;
                        this.initialDeleteState = CheckBox.SELECTED;
                    }
                    break;
            }
        }
		return value;
    }
}
