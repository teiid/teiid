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

import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.GridLayout;
import java.awt.Insets;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.ItemEvent;
import java.awt.event.ItemListener;
import java.util.Date;
import java.util.Enumeration;
import java.util.Iterator;

import javax.swing.AbstractButton;
import javax.swing.ButtonGroup;
import javax.swing.JCheckBox;
import javax.swing.JDialog;
import javax.swing.JPanel;
import javax.swing.JRadioButton;
import javax.swing.JScrollPane;

import com.metamatrix.console.ui.layout.ConsoleMainFrame;
import com.metamatrix.console.ui.tree.TreePathExpansion;
import com.metamatrix.console.util.StaticTreeUtilities;
import com.metamatrix.console.util.StaticUtilities;

import com.metamatrix.platform.admin.api.PermissionNode;
import com.metamatrix.platform.admin.api.PermissionTreeView;
import com.metamatrix.platform.security.api.AuthorizationActions;

import com.metamatrix.toolbox.ui.widget.ButtonWidget;
import com.metamatrix.toolbox.ui.widget.CheckBox;
import com.metamatrix.toolbox.ui.widget.LabelWidget;
import com.metamatrix.toolbox.ui.widget.TitledBorder;

public class DataNodeAuthorizationsControl extends JPanel
        implements AuthorizationPropagationListener,
        AuthorizationsCheckBoxListener {
    private DataNodesTreeModel treeModel;
    private AuthorizationsModel authModel;
    private AuthorizationsTreeTable treeTable;
    private ButtonsStateController buttonsStateController;
    private DataNodesTreeModelGenerator treeModelGenerator;
//    private boolean canModify;
    private boolean programmaticChange = false;
    private boolean promptUserOnPropagatedChanges = true;
    private JCheckBox systemModelsCheckBox;
    private int stateChangingTo = -1;
    private boolean rootVisible;
    private boolean useDataColNames;
    private boolean propagateChanges;
    
    public DataNodeAuthorizationsControl(ButtonsStateController bsc,
            boolean modifiable, boolean rootVisible, boolean useDataColNames,
            boolean propagateChanges) {
        super();
        this.rootVisible = rootVisible;
        this.useDataColNames = useDataColNames;
        this.propagateChanges = propagateChanges;
        buttonsStateController = bsc;
//        canModify = modifiable;
        systemModelsCheckBox = new CheckBox("Show System models");
        systemModelsCheckBox.addItemListener(new ItemListener() {
            public void itemStateChanged(ItemEvent ev) {
                showModelsCheckBoxChanged();
            }
        });
        systemModelsCheckBox.setSelected(true);
        createComponent();
        if (this.propagateChanges) {
        	treeModel.setPropagationListener(this);
        }
        //treeTable.getModel().addTableModelListener(this);
    }

	public DataNodesTreeModel getTreeModel() {
	    return treeModel;
	}
	
    public void setTreeView(PermissionTreeView tView,
            boolean repopulatingSameEntitlement,
            boolean canModifyEntitlements) {
        treeModelGenerator = new DataNodesTreeModelGenerator(tView,
        		canModifyEntitlements, rootVisible);
        TreePathExpansion[] tpe = null;
        try {
            Date startingTime = new Date();
            tpe = StaticTreeUtilities.expansionState(treeTable.getTree());
            Date endingTime = new Date();
            long startingTimeLong = startingTime.getTime();
            long endingTimeLong = endingTime.getTime();
            long timeDiffLong = endingTimeLong - startingTimeLong;
            double timeDiff = ((double)timeDiffLong) / 1000;
            if (timeDiff >= 2.0) {
//                String message = "DNAC..setTreeView() expansionState() call took "
//                        + timeDiff + " seconds";
//                LogManager.logInfo(LogContexts.ENTITLEMENTS, message);
//System.err.println(message);
            }
        } catch (Exception ex) {
//System.err.println("expansionState() threw exception of " + ex);
        }
        createComponent();
        if (tpe != null) {
            try {
                StaticTreeUtilities.restoreExpansionState(treeTable.getTree(),
                        tpe, false);
            } catch (Exception ex) {
            }
        }
    }

    private void createComponent() {
        removeAll();
        if (treeModelGenerator == null) {
            treeModel = new DataNodesTreeModel(rootVisible, -1, -1, -1, -1);
        } else {
            try {
                treeModel = treeModelGenerator.generateModel();
            } catch (Exception ex) {
				//Exception handled below.
            }
        }
        if (treeModel == null) {
            treeModel = new DataNodesTreeModel(rootVisible, -1, -1, -1, -1);
        }
		authModel = new AuthorizationsModel(treeModel);
        treeTable = new AuthorizationsTreeTable(authModel, this,
        		rootVisible);
		treeTable.setSystemModelRootNodes();
        treeModel.setTreeTableForEachNode(treeTable);
        programmaticChange = true;
        if (propagateChanges) {
        	treeModel.setStatesToChecked();
        	treeModel.setStatesToPartial();
        }
        programmaticChange = false;
        GridBagLayout layout = new GridBagLayout();
        this.setLayout(layout);
        JScrollPane treeTableJSP = new JScrollPane(treeTable);
        this.add(treeTableJSP);
        layout.setConstraints(treeTableJSP, new GridBagConstraints(0, 0, 1, 1, 1.0, 1.0,
                GridBagConstraints.CENTER, GridBagConstraints.BOTH, new Insets(0, 0, 0, 0),
                0, 0));
        this.add(systemModelsCheckBox);
        layout.setConstraints(systemModelsCheckBox, new GridBagConstraints(
                0, 1, 1, 1, 0.0, 0.0, GridBagConstraints.WEST, GridBagConstraints.NONE,
                new Insets(2, 5, 2, 5), 0, 0));

        //Not working yet, so set to invisible.  BWP 11/06/01
        systemModelsCheckBox.setVisible(false);

        if (!systemModelsCheckBox.isSelected()) {
            treeTable.removeSystemModelRootNodes();
        }
    }

    private void showModelsCheckBoxChanged() {
        if (treeTable != null) {
            boolean selected = systemModelsCheckBox.isSelected();
            if (selected) {
                treeTable.insertSystemModelRootNodes();
            } else {
                treeTable.removeSystemModelRootNodes();
            }
        }
    }

    public boolean isProgrammaticChange() {
        return programmaticChange;
    }

    public void authorizationPropagated(DataNodesTreeNode node,
            int authorizationType) {
        int row = treeTable.getRowForNode(node);
        if (row >= 0) {
            int state = -1;
            switch (authorizationType) {
                case AuthorizationsModel.CREATE_COLUMN_NUM:
                    state = node.getCreateState();
                    break;
                case AuthorizationsModel.READ_COLUMN_NUM:
                    state = node.getReadState();
                    break;
                case AuthorizationsModel.UPDATE_COLUMN_NUM:
                    state = node.getUpdateState();
                    break;
                case AuthorizationsModel.DELETE_COLUMN_NUM:
                    state = node.getDeleteState();
                    break;
            }
            programmaticChange = true;
            treeTable.setValueAt(new Integer(state), row, authorizationType);
            programmaticChange = false;
        }
    }

    public void reset() {
        java.util.List /*<DataNodesTreeNode>*/ allNodes =
                StaticTreeUtilities.descendantsOfNode(
                (DataNodesTreeNode)treeTable.getTree().getModel().getRoot(),
                rootVisible);
        Iterator it = allNodes.iterator();
        while (it.hasNext()) {
            DataNodesTreeNode node = (DataNodesTreeNode)it.next();
            int initialCreateState = node.getInitialCreateState();
            int currentCreateState = node.getCreateState();
            if (currentCreateState != initialCreateState) {
                programmaticChange = true;
                stateChangingTo = initialCreateState;
                node.setCreateState(initialCreateState);
                programmaticChange = false;
            }

            int initialReadState = node.getInitialReadState();
            int currentReadState = node.getReadState();
            if (currentReadState != initialReadState) {
                programmaticChange = true;
                stateChangingTo = initialReadState;
                node.setReadState(initialReadState);
                programmaticChange = false;
            }

            int initialUpdateState = node.getInitialUpdateState();
            int currentUpdateState = node.getUpdateState();
            if (currentUpdateState != initialUpdateState) {
                programmaticChange = true;
                stateChangingTo = initialUpdateState;
                node.setUpdateState(initialUpdateState);
                programmaticChange = false;
            }

            int initialDeleteState = node.getInitialDeleteState();
            int currentDeleteState = node.getDeleteState();
            if (currentDeleteState != initialDeleteState) {
                programmaticChange = true;
                stateChangingTo = initialDeleteState;
                node.setDeleteState(initialDeleteState);
                programmaticChange = false;
            }
        }
        if (propagateChanges) {
        	programmaticChange = true;
        	treeModel.setStatesToPartial();
        	programmaticChange = false;
        }
        DataNodesTreeNode changedNode = treeTable.anyAuthorizationsChangesMade();
        if (changedNode != null) {
            System.err.println("missed resetting node " + changedNode +
                    ", initial CRUD states: " + changedNode.getInitialCreateState() +
                    changedNode.getInitialReadState() + changedNode.getInitialUpdateState() +
                    changedNode.getInitialDeleteState() + ", current CRUD states: " +
                    changedNode.getCreateState() + changedNode.getReadState() +
                    changedNode.getUpdateState() + changedNode.getDeleteState());
        }
        treeTable.clearChangedNodesList();
    }

    /**
     * Return new state to change to (CheckBox.SELECTED or CheckBox.DESELECTED),
     * or null if not proceeding.
     */
    public Integer proceedWithAuthorizationChange(DataNodesTreeNode node,
            int columnID) {
        return promptUserOnChange(node, columnID);
    }

    public void checkBoxChanged(DataNodesTreeNode node, int column, int newState) {
        if (!programmaticChange) {
            programmaticChange = true;
            stateChangingTo = newState;
            int curState = -1;
            switch (column) {
                case AuthorizationsModel.CREATE_COLUMN_NUM:
                    curState = node.getCreateState();
                    if (curState != newState) {
                        node.setCreateState(newState);
                        curState = newState;
                    }
                    break;
                case AuthorizationsModel.READ_COLUMN_NUM:
                    curState = node.getReadState();
                    if (curState != newState) {
                        node.setReadState(newState);
                        curState = newState;
                    }
                    break;
                case AuthorizationsModel.UPDATE_COLUMN_NUM:
                    curState = node.getUpdateState();
                    if (curState != newState) {
                        node.setUpdateState(newState);
                        curState = newState;
                    }
                    break;
                case AuthorizationsModel.DELETE_COLUMN_NUM:
                    curState = node.getDeleteState();
                    if (curState != newState) {
                        node.setDeleteState(newState);
                        curState = newState;
                    }
                    break;
            }
            if (propagateChanges) {
            	this.propagateValueDownward(node, column,
                	    (curState == CheckBox.SELECTED));
            	this.handleAuthorizationChangeUpward(node, column, curState);
            }
            buttonsStateController.permissionsChanged();
            programmaticChange = false;
        }
    }

    private void handleAuthorizationChangeUpward(DataNodesTreeNode node,
            int columnID, int columnState) {
        if (columnState == CheckBox.DESELECTED) {
            handleChangeToUnauthorizedUpward(node, columnID);
        } else {
            handleChangeToAuthorizedUpward(node, columnID);
        }
    }

    private void handleChangeToAuthorizedUpward(DataNodesTreeNode node, int columnID) {
        DataNodesTreeNode curNode = node;
        boolean done = false;
        boolean setTheRestToPartial = false;
        boolean firstTime = true;
        while (!done) {
            DataNodesTreeNode ancestor;
            if (firstTime) {
                ancestor = curNode;
                firstTime = false;
            } else {
                ancestor = (DataNodesTreeNode)curNode.getParent();
            }
            if ((ancestor == null) || ancestor.isRootNode()) {
                done = true;
            } else {
                int row = treeTable.getRowForNode(ancestor);
//                int curVal = -1;
                switch (columnID) {
                    case AuthorizationsModel.CREATE_COLUMN_NUM:
//                        curVal =
                            ancestor.getCreateState();
                        break;
                    case AuthorizationsModel.READ_COLUMN_NUM:
//                        curVal =
                            ancestor.getReadState();
                        break;
                    case AuthorizationsModel.UPDATE_COLUMN_NUM:
//                        curVal =
                            ancestor.getUpdateState();
                        break;
                    case AuthorizationsModel.DELETE_COLUMN_NUM:
//                        curVal =
                            ancestor.getDeleteState();
                        break;
                }
                if (setTheRestToPartial) {
                    setToValue(columnID, row, ancestor,
                            CheckBox.PARTIALLY_SELECTED);
                    curNode = ancestor;
                } else {
                    boolean hasUnauthorizedDescendant =
                            ancestor.hasDescendantUnauthorized(columnID);
                    int state;
                    if (hasUnauthorizedDescendant) {
                        setTheRestToPartial = true;
                        state = CheckBox.PARTIALLY_SELECTED;
                    } else {
                        state = CheckBox.SELECTED;
                    }
                    setToValue(columnID, row, ancestor, state);
                }
            }
            curNode = ancestor;
        }
    }

    private void handleChangeToUnauthorizedUpward(DataNodesTreeNode node,
            int columnID) {
        DataNodesTreeNode curNode = node;
        boolean done = false;
        while (!done) {
            DataNodesTreeNode ancestor = (DataNodesTreeNode)curNode.getParent();
            if ((ancestor == null) || ancestor.isRootNode()) {
                done = true;
            } else {
                int row = treeTable.getRowForNode(ancestor);
                int curVal = -1;
                switch (columnID) {
                    case AuthorizationsModel.CREATE_COLUMN_NUM:
                        curVal = ancestor.getCreateState();
                        break;
                    case AuthorizationsModel.READ_COLUMN_NUM:
                        curVal = ancestor.getReadState();
                        break;
                    case AuthorizationsModel.UPDATE_COLUMN_NUM:
                        curVal = ancestor.getUpdateState();
                        break;
                    case AuthorizationsModel.DELETE_COLUMN_NUM:
                        curVal = ancestor.getDeleteState();
                        break;
                }
                if (curVal != CheckBox.DESELECTED) {
                    int checkBoxState;
                    if (ancestor.hasDescendantAuthorized(columnID)) {
                        checkBoxState = CheckBox.PARTIALLY_SELECTED;
                    } else {
                        checkBoxState = CheckBox.DESELECTED;
                    }
                    setToValue(columnID, row, ancestor, checkBoxState);
                }
            }
            curNode = ancestor;
        }
    }

    private void setToValue(int columnID, int row, DataNodesTreeNode node,
            int value) {
        switch (columnID) {
            case AuthorizationsModel.CREATE_COLUMN_NUM:
                node.setCreateState(value);
                treeTable.getModel().setValueAt(new Integer(value),
                        row, AuthorizationsModel.CREATE_COLUMN_NUM);
                break;
            case AuthorizationsModel.READ_COLUMN_NUM:
                node.setReadState(value);
                treeTable.getModel().setValueAt(new Integer(value),
                        row, AuthorizationsModel.READ_COLUMN_NUM);
                break;
            case AuthorizationsModel.UPDATE_COLUMN_NUM:
                node.setUpdateState(value);
                treeTable.getModel().setValueAt(new Integer(value),
                        row, AuthorizationsModel.UPDATE_COLUMN_NUM);
                break;
            case AuthorizationsModel.DELETE_COLUMN_NUM:
                node.setDeleteState(value);
                treeTable.getModel().setValueAt(new Integer(value),
                        row, AuthorizationsModel.DELETE_COLUMN_NUM);
                break;
        }
    }

    private void propagateValueDownward(DataNodesTreeNode startingNode,
            int columnID, boolean authorized) {
        java.util.List /*<DataNodesTreeNode>*/ nodes =
                StaticTreeUtilities.descendantsOfNode(startingNode, true);
        Iterator it = nodes.iterator();
        while (it.hasNext()) {
            DataNodesTreeNode node = (DataNodesTreeNode)it.next();
            PermissionNode pn = (PermissionNode)node.getCorrespondingTreeNode();
            AuthorizationActions allowedActions = pn.getAllowedActions();
            boolean setable = false;
            switch (columnID) {
                case AuthorizationsModel.CREATE_COLUMN_NUM:
                	setable = DataNodesTreeNode.hasCreate(allowedActions);
                	break;
                case AuthorizationsModel.READ_COLUMN_NUM:
                	setable = DataNodesTreeNode.hasRead(allowedActions);
                	break;
                case AuthorizationsModel.UPDATE_COLUMN_NUM:
                	setable = DataNodesTreeNode.hasUpdate(allowedActions);
                	break;
                case AuthorizationsModel.DELETE_COLUMN_NUM:
                	setable = DataNodesTreeNode.hasDelete(allowedActions);
                	break;
            }
            if (setable) {
                int row = treeTable.getRowForNode(node);
                switch (columnID) {
                    case AuthorizationsModel.CREATE_COLUMN_NUM:
                        node.setCreateState(authorized? CheckBox.SELECTED:
                                CheckBox.DESELECTED);
                        if (row >= 0) {
                            treeTable.getModel().setValueAt(
                                    new Integer(node.getCreateState()),
                                    row, AuthorizationsModel.CREATE_COLUMN_NUM);
                        }
                        break;
                    case AuthorizationsModel.READ_COLUMN_NUM:
                        node.setReadState(authorized? CheckBox.SELECTED:
                                CheckBox.DESELECTED);
                        if (row >= 0) {
                            treeTable.getModel().setValueAt(
                                    new Integer(node.getReadState()),
                                    row, AuthorizationsModel.READ_COLUMN_NUM);
                        }
                        break;
                    case AuthorizationsModel.UPDATE_COLUMN_NUM:
                        node.setUpdateState(authorized? CheckBox.SELECTED:
                                CheckBox.DESELECTED);
                        if (row >= 0) {
                            treeTable.getModel().setValueAt(
                                    new Integer(node.getUpdateState()),
                                    row, AuthorizationsModel.UPDATE_COLUMN_NUM);
                        }
                        break;
                    case AuthorizationsModel.DELETE_COLUMN_NUM:
                        node.setDeleteState(authorized? CheckBox.SELECTED:
                                CheckBox.DESELECTED);
                        if (row >= 0) {
                            treeTable.getModel().setValueAt(
                                    new Integer(node.getDeleteState()),
                                    row, AuthorizationsModel.DELETE_COLUMN_NUM);
                        }
                        break;
                }
            }
        }
    }

    /**
     * Return new state to change to (CheckBox.SELECTED or CheckBox.DESELECTED),
     * or null if not proceeding.
     */
    private Integer promptUserOnChange(DataNodesTreeNode node, int columnID) {
        Integer changeTo = null;
        String permissionName = "";
        int curVal = -1;
        switch (columnID) {
            case AuthorizationsModel.CREATE_COLUMN_NUM:
                curVal = node.getCreateState();
                permissionName = "Create";
                break;
            case AuthorizationsModel.READ_COLUMN_NUM:
                curVal = node.getReadState();
                permissionName = "Read";
                break;
            case AuthorizationsModel.UPDATE_COLUMN_NUM:
                curVal = node.getUpdateState();
                permissionName = "Update";
                break;
            case AuthorizationsModel.DELETE_COLUMN_NUM:
                curVal = node.getDeleteState();
                permissionName = "Delete";
                break;
        }
        //Set default value for response (changeTo).  This may be changed or
        //cancelled if we display a dialog.
        switch (curVal) {
            case CheckBox.PARTIALLY_SELECTED:
                break;
            case CheckBox.DESELECTED:
                changeTo = new Integer(CheckBox.SELECTED);
                break;
            case CheckBox.SELECTED:
                changeTo = new Integer(CheckBox.DESELECTED);
                break;
        }
        if (!programmaticChange) {
        	int numChildren = 0;
        	if (propagateChanges) {
        		numChildren = numChildrenPropagatableTo(node, columnID);
        	}
            if (((numChildren > 0) && promptUserOnPropagatedChanges) ||
                    (curVal == CheckBox.PARTIALLY_SELECTED)) {
                PropagateChangesDialog dialog;
                boolean toAuthorized = false;
                boolean proceeding;
                if (curVal == CheckBox.PARTIALLY_SELECTED) {
                    dialog = new PropagateChangesDialog(permissionName);
                    dialog.show();
                    proceeding = dialog.proceeding();
                } else {
                    toAuthorized = (curVal != CheckBox.SELECTED);
                    dialog = new PropagateChangesDialog(permissionName,
                            toAuthorized);
                    dialog.show();
                    promptUserOnPropagatedChanges =
                            (!dialog.discontinueDisplaying());
                    proceeding = dialog.proceeding();
                }
                if (proceeding) {
                    if (curVal != CheckBox.PARTIALLY_SELECTED) {
                        if (toAuthorized) {
                            changeTo = new Integer(CheckBox.SELECTED);
                        } else {
                            changeTo = new Integer(CheckBox.DESELECTED);
                        }
                    } else {
                        if (dialog.setToAuthorized()) {
                            changeTo = new Integer(CheckBox.SELECTED);
                        } else {
                            changeTo = new Integer(CheckBox.DESELECTED);
                        }
                    }
                } else {
                    changeTo = null;
                }
                dialog = null;
            }
        }
        if ((changeTo == null) && programmaticChange && (stateChangingTo >= 0)) {
            changeTo = new Integer(stateChangingTo);
        }
        return changeTo;
    }

    private int numChildrenPropagatableTo(DataNodesTreeNode node, int columnID) {
        int numPropagatableTo = 0;
        Enumeration enumeration = node.children();
        while (enumeration.hasMoreElements()) {
        	DataNodesTreeNode child = (DataNodesTreeNode)enumeration.nextElement();
        	PermissionNode pn = 
        			(PermissionNode)child.getCorrespondingTreeNode();
        	AuthorizationActions allowableActions = pn.getAllowedActions();
            boolean propagatable = false;
            switch (columnID) {
                case AuthorizationsModel.CREATE_COLUMN_NUM:
                	propagatable = DataNodesTreeNode.hasCreate(allowableActions);
                	break;
                case AuthorizationsModel.READ_COLUMN_NUM:
                	propagatable = DataNodesTreeNode.hasRead(allowableActions);
                	break;
              	case AuthorizationsModel.UPDATE_COLUMN_NUM:
                	propagatable = DataNodesTreeNode.hasUpdate(allowableActions);
                	break;
              	case AuthorizationsModel.DELETE_COLUMN_NUM:
                	propagatable = DataNodesTreeNode.hasDelete(allowableActions);
                	break;
            }
            if (propagatable) {
                numPropagatableTo++;
            }
        }
        return numPropagatableTo;
    }

    public boolean anyAuthorizationsChangesMade() {
        return (treeTable.anyAuthorizationsChangesMade() != null);
    }

    public java.util.List /*<DataNodePermissionsWithNodeName>*/ nodesWithAuthorizationChanges() {
        return treeTable.nodesWithAuthorizationChanges();
    }
}//end DataNodeAuthorizationsControl




class PropagateChangesDialog extends JDialog {
    private JCheckBox suppressDialogInFutureCheckBox;
    private boolean proceeding = true;
    private AbstractButton setToAuthorizedButton;
    private AbstractButton setToUnauthorizedButton;

    /**
     * Constructor for case where check box is currently either selected or
     * deselected.
     */
    public PropagateChangesDialog(String permissionName, boolean toAuthorized) {
        super(ConsoleMainFrame.getInstance(), "Change Will be Propagated");
        setModal(true);
        init(permissionName, toAuthorized);
    }

    /**
     * Constructor for case where check box is currently partially selected.
     */
    public PropagateChangesDialog(String permissionName) {
        super(ConsoleMainFrame.getInstance(), "Change Will be Propagated");
        setModal(true);
        initForPartial(permissionName);
    }

    private void init(String permissionName, boolean toAuthorized) {
        GridBagLayout layout = new GridBagLayout();
        getContentPane().setLayout(layout);
        String toWhat;
        if (toAuthorized) {
            toWhat = "unauthorized to authorized";
        } else {
            toWhat = "authorized to unauthorized";
        }
        String firstLineStr = "\"" + permissionName + "\" permission change " +
                "from " + toWhat + " will be propagated";
        LabelWidget firstLine = new LabelWidget(firstLineStr);
        LabelWidget secondLine = new LabelWidget(
                "to all descendant nodes of the selected node.");
        LabelWidget questionLine = new LabelWidget("Proceed with this change?");
        suppressDialogInFutureCheckBox = new CheckBox(
                "In the future, do not show this warning");
        JPanel buttonsPanel = createButtonsPanel();
        getContentPane().add(firstLine);
        getContentPane().add(secondLine);
        getContentPane().add(questionLine);
        getContentPane().add(buttonsPanel);
        getContentPane().add(suppressDialogInFutureCheckBox);
        layout.setConstraints(firstLine, new GridBagConstraints(0, 0, 1, 1,
                0.0, 0.0, GridBagConstraints.WEST, GridBagConstraints.NONE,
                new Insets(5, 10, 2, 10), 0, 0));
        layout.setConstraints(secondLine, new GridBagConstraints(0, 1, 1, 1,
                0.0, 0.0, GridBagConstraints.WEST, GridBagConstraints.NONE,
                new Insets(2, 10, 10, 10), 0, 0));
        layout.setConstraints(questionLine, new GridBagConstraints(0, 2, 1, 1,
                0.0, 0.0, GridBagConstraints.WEST, GridBagConstraints.NONE,
                new Insets(10, 10, 10, 10), 0, 0));
        layout.setConstraints(buttonsPanel, new GridBagConstraints(0, 3, 1, 1,
                0.0, 0.0, GridBagConstraints.CENTER, GridBagConstraints.NONE,
                new Insets(10, 10, 5, 10), 0, 0));
        layout.setConstraints(suppressDialogInFutureCheckBox,
                new GridBagConstraints(0, 4, 1, 1, 0.0, 0.0,
                GridBagConstraints.CENTER, GridBagConstraints.NONE,
                new Insets(5, 10, 5, 10), 0, 0));
        pack();
        setLocation(StaticUtilities.centerFrame(this.getSize()));
    }

    private void initForPartial(String permissionName) {
        GridBagLayout layout = new GridBagLayout();
        getContentPane().setLayout(layout);
        String firstLineStr = "\"" + permissionName + "\" permission change " +
                " will be propagated to all descendant nodes ";
        LabelWidget firstLine = new LabelWidget(firstLineStr);
        LabelWidget secondLine = new LabelWidget(
                "of the selected node.");
        LabelWidget questionLine = new LabelWidget("Proceed with this change?");
        JPanel buttonsPanel = createButtonsPanel();
        JPanel changeToWhatPanel = createChangeToPanel(permissionName);
        getContentPane().add(changeToWhatPanel);
        getContentPane().add(firstLine);
        getContentPane().add(secondLine);
        getContentPane().add(questionLine);
        getContentPane().add(buttonsPanel);
        layout.setConstraints(changeToWhatPanel, new GridBagConstraints(
                0, 0, 1, 1, 0.0, 0.0, GridBagConstraints.CENTER,
                GridBagConstraints.NONE, new Insets(5, 10, 5, 10), 0, 0));
        layout.setConstraints(firstLine, new GridBagConstraints(0, 1, 1, 1,
                0.0, 0.0, GridBagConstraints.WEST, GridBagConstraints.NONE,
                new Insets(10, 10, 2, 10), 0, 0));
        layout.setConstraints(secondLine, new GridBagConstraints(0, 2, 1, 1,
                0.0, 0.0, GridBagConstraints.WEST, GridBagConstraints.NONE,
                new Insets(2, 10, 10, 10), 0, 0));
        layout.setConstraints(questionLine, new GridBagConstraints(0, 3, 1, 1,
                0.0, 0.0, GridBagConstraints.WEST, GridBagConstraints.NONE,
                new Insets(10, 10, 10, 10), 0, 0));
        layout.setConstraints(buttonsPanel, new GridBagConstraints(0, 4, 1, 1,
                0.0, 0.0, GridBagConstraints.CENTER, GridBagConstraints.NONE,
                new Insets(10, 10, 5, 10), 0, 0));
        pack();
        setLocation(StaticUtilities.centerFrame(this.getSize()));
    }

    private JPanel createButtonsPanel() {
        ButtonWidget yesButton = new ButtonWidget("Yes");
        ButtonWidget noButton = new ButtonWidget("No");
        JPanel buttonsPanel = new JPanel(new GridLayout(1, 2, 15, 15));
        buttonsPanel.add(yesButton);
        buttonsPanel.add(noButton);
        yesButton.addActionListener(new ActionListener() {
            public void actionPerformed(ActionEvent ev) {
                yesPressed();
            }
        });
        noButton.addActionListener(new ActionListener() {
            public void actionPerformed(ActionEvent ev) {
                noPressed();
            }
        });
        return buttonsPanel;
    }

    private JPanel createChangeToPanel(String permissionName) {
        setToAuthorizedButton = new JRadioButton("Authorized (checked)");
        setToUnauthorizedButton = new JRadioButton("Unauthorized (unchecked)");
        ButtonGroup group = new ButtonGroup();
        group.add(setToAuthorizedButton);
        group.add(setToUnauthorizedButton);
        setToAuthorizedButton.setSelected(true);
        LabelWidget changeLabel = new LabelWidget("Change \"" +
                permissionName + "\" permission to:");
        GridBagLayout layout = new GridBagLayout();
        JPanel panel = new JPanel();
        panel.setLayout(layout);
        panel.add(setToAuthorizedButton);
        panel.add(setToUnauthorizedButton);
        panel.add(changeLabel);
        layout.setConstraints(changeLabel, new GridBagConstraints(0, 0, 1, 2,
                0.0, 0.0, GridBagConstraints.WEST, GridBagConstraints.NONE,
                new Insets(0, 0, 0, 10), 0, 0));
        layout.setConstraints(setToAuthorizedButton, new GridBagConstraints(
                1, 0, 1, 1, 0.0, 0.0, GridBagConstraints.WEST, GridBagConstraints.NONE,
                new Insets(0, 10, 0, 0), 0, 0));
        layout.setConstraints(setToUnauthorizedButton, new GridBagConstraints(
                1, 1, 1, 1, 0.0, 0.0, GridBagConstraints.WEST, GridBagConstraints.NONE,
                new Insets(0, 10, 0, 0), 0, 0));
        panel.setBorder(new TitledBorder(""));
        return panel;
    }

    private void yesPressed() {
        proceeding = true;
        dispose();
    }

    private void noPressed() {
        proceeding = false;
        dispose();
    }

    public boolean proceeding() {
        return proceeding;
    }

    public boolean discontinueDisplaying() {
        return ((suppressDialogInFutureCheckBox != null) &&
                suppressDialogInFutureCheckBox.isSelected());
    }

    public boolean setToAuthorized() {
        return setToAuthorizedButton.isSelected();
    }
}
