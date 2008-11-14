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

import java.awt.Color;
import java.awt.Component;
import java.awt.Graphics;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.Iterator;

import javax.swing.DefaultCellEditor;
import javax.swing.JTable;
import javax.swing.JTree;
import javax.swing.ListSelectionModel;
import javax.swing.table.TableCellRenderer;
import javax.swing.tree.DefaultTreeModel;
import javax.swing.tree.TreeCellRenderer;
import javax.swing.tree.TreePath;

import com.metamatrix.console.ui.treetable.JTreeTable;
import com.metamatrix.console.util.StaticTreeUtilities;
import com.metamatrix.core.util.ResourceNameUtil;
import com.metamatrix.platform.admin.api.PermissionNode;
import com.metamatrix.toolbox.ui.widget.CheckBox;

public class AuthorizationsTreeTable extends JTreeTable {
	public final static int TREE_COLUMN_NUM = 0;
	
    private AuthorizationsCheckBoxRenderer checkBoxRenderer;
    private java.util.List /*<DataNodesTreeNode>*/ systemModelRootNodes;
    private AuthorizationsCheckBoxListener checkBoxListener;
    private java.util.List /*<DataNodePermissionChange>*/ changedNodes = new ArrayList();
    private boolean usingRoot;
    private TreeCellRenderer treeCellRenderer;

    public AuthorizationsTreeTable(AuthorizationsModel model,
            AuthorizationsCheckBoxListener checkBoxLsnr, 
            boolean rootVisible) {
        super(model);
        this.usingRoot = rootVisible;
        checkBoxListener = checkBoxLsnr;
		treeCellRenderer = new AuthorizationsTreeCellRenderer();
        getTree().setCellRenderer(treeCellRenderer);
        getTree().setRootVisible(rootVisible);
        getTree().setShowsRootHandles(true);
        checkBoxRenderer = new AuthorizationsCheckBoxRenderer(this);
        this.setDefaultRenderer(Integer.class, checkBoxRenderer);
        this.setDefaultEditor(Integer.class, checkBoxRenderer);
        this.getTableHeader().setReorderingAllowed(false);
        this.setShowHorizontalLines(true);
        this.setShowVerticalLines(true);
        this.getSelectionModel().setSelectionMode(
        		ListSelectionModel.SINGLE_SELECTION);
        model.setTreeTable(this);
        checkBoxRenderer.setClickCountToStart(1);
    }

	public TreeCellRenderer getTreeCellRenderer() {
		return treeCellRenderer;
	}
	
    public void setSystemModelRootNodes() {
        DataNodesTreeNode root = (DataNodesTreeNode)getTree().getModel().getRoot();
        systemModelRootNodes = new ArrayList(2);
        Enumeration enumeration = root.children();
        while (enumeration.hasMoreElements()) {
            DataNodesTreeNode node = (DataNodesTreeNode)enumeration.nextElement();
            String displayName = node.getNodeDisplayName();
            if (displayName.equals(ResourceNameUtil.SYSTEMPHYSICAL_NAME) ||
                displayName.equals(ResourceNameUtil.SYSTEMADMINPHYSICAL_NAME) ||
                displayName.equals(ResourceNameUtil.SYSTEMADMIN_NAME) ||
                displayName.equals(ResourceNameUtil.SYSTEM_NAME)) {
                systemModelRootNodes.add(node);
            }
        }
    }

    public DataNodesTreeNode getNodeForRow(int row) {
        JTree tree = this.getTree();
        TreePath path = tree.getPathForRow(row);
        DataNodesTreeNode node = null;
        if (path != null) {
            node = (DataNodesTreeNode)path.getLastPathComponent();
        }
        return node;
    }

    public int getRowForNode(DataNodesTreeNode node) {
        JTree tree = this.getTree();
        int row = -1;
        int numRows = tree.getRowCount();
        int i = 0;
        while ((row < 0) && (i < numRows)) {
            TreePath path = tree.getPathForRow(i);
            if (path != null) {
                if (node == path.getLastPathComponent()) {
                    row = i;
                }
            }
            i++;
        }
        return row;
    }

    /**
     * If there were any authorizations changes made, returns first changed
     * node found.  Else null.
     */
    public DataNodesTreeNode anyAuthorizationsChangesMade() {
        java.util.List /*<DataNodesTreeNode>*/ allNodes =
                StaticTreeUtilities.descendantsOfNode(
                (DataNodesTreeNode)getTree().getModel().getRoot(), usingRoot);
        DataNodesTreeNode changedNode = null;
        boolean changeFound = false;
        Iterator it = allNodes.iterator();
        while ((!changeFound) && it.hasNext()) {
            DataNodesTreeNode node = (DataNodesTreeNode)it.next();
            int createState = node.getCreateState();
            int initialCreateState = node.getInitialCreateState();
            if (((initialCreateState == CheckBox.DESELECTED) && (createState !=
                    CheckBox.DESELECTED)) || ((initialCreateState !=
                    CheckBox.DESELECTED) && (createState == CheckBox.DESELECTED))) {
                changeFound = true;
                changedNode = node;
            } else {
                int readState = node.getReadState();
                int initialReadState = node.getInitialReadState();
                if (((initialReadState == CheckBox.DESELECTED) && (readState !=
                        CheckBox.DESELECTED)) || ((initialReadState !=
                        CheckBox.DESELECTED) && (readState == CheckBox.DESELECTED))) {
                    changeFound = true;
                    changedNode = node;
                } else {
                    int updateState = node.getUpdateState();
                    int initialUpdateState = node.getInitialUpdateState();
                    if (((initialUpdateState == CheckBox.DESELECTED) &&
                            (updateState != CheckBox.DESELECTED)) ||
                            ((initialUpdateState != CheckBox.DESELECTED) &&
                            (updateState == CheckBox.DESELECTED))) {
                        changeFound = true;
                        changedNode = node;
                    } else {
                        int deleteState = node.getDeleteState();
                        int initialDeleteState = node.getInitialDeleteState();
                        if (((initialDeleteState == CheckBox.DESELECTED) &&
                                (deleteState != CheckBox.DESELECTED)) ||
                                ((initialDeleteState != CheckBox.DESELECTED) &&
                                (deleteState == CheckBox.DESELECTED))) {
                            changeFound = true;
                            changedNode = node;
                        }
                    }
                }
            }
        }
        return changedNode;
    }

    public Integer proceedWithAuthorizationChange(DataNodesTreeNode node,
            int column) {
        return checkBoxListener.proceedWithAuthorizationChange(node, column);
    }

    public void checkBoxChanged(DataNodesTreeNode node, int column,
            int newState) {
        boolean ignore = checkBoxListener.isProgrammaticChange();
        checkBoxListener.checkBoxChanged(node, column, newState);
        if (!ignore) {
            changedNodes.add(new DataNodePermissionChange(
                    (PermissionNode)node.getCorrespondingTreeNode(), column,
                    (!(newState == CheckBox.DESELECTED))));
        }
    }

    public void clearChangedNodesList() {
        changedNodes.clear();
    }


	public java.util.List /*<DataNodePermissionsWithNodeName>*/
            nodesWithAuthorizationChanges() {
        java.util.List /*<DataNodesTreeNode>*/ allNodes =
                StaticTreeUtilities.descendantsOfNode(
                (DataNodesTreeNode)getTree().getModel().getRoot(), usingRoot);
        java.util.List /*<DataNodePermissionsWithNodeName>*/ nodesWithAuthChanges =
              new ArrayList(50);
        Iterator it = allNodes.iterator();
        while (it.hasNext()) {
            boolean addNode = false;
            DataNodesTreeNode node = (DataNodesTreeNode)it.next();
            int createState = node.getCreateState();
            int initialCreateState = node.getInitialCreateState();
            if (((initialCreateState == CheckBox.DESELECTED) && (createState !=
                    CheckBox.DESELECTED)) || ((initialCreateState !=
                    CheckBox.DESELECTED) && (createState == CheckBox.DESELECTED))) {
                addNode = true;
            } else {
                int readState = node.getReadState();
                int initialReadState = node.getInitialReadState();
                if (((initialReadState == CheckBox.DESELECTED) && (readState !=
                        CheckBox.DESELECTED)) || ((initialReadState !=
                        CheckBox.DESELECTED) && (readState == CheckBox.DESELECTED))) {
                    addNode = true;
                } else {
                    int updateState = node.getUpdateState();
                    int initialUpdateState = node.getInitialUpdateState();
                    if (((initialUpdateState == CheckBox.DESELECTED) &&
                            (updateState != CheckBox.DESELECTED)) ||
                            ((initialUpdateState != CheckBox.DESELECTED) &&
                            (updateState == CheckBox.DESELECTED))) {
                        addNode = true;
                    } else {
                        int deleteState = node.getDeleteState();
                        int initialDeleteState = node.getInitialDeleteState();
                        if (((initialDeleteState == CheckBox.DESELECTED) &&
                                (deleteState != CheckBox.DESELECTED)) ||
                                ((initialDeleteState != CheckBox.DESELECTED) &&
                                (deleteState == CheckBox.DESELECTED))) {
                            addNode = true;
                        }
                    }
                }
            }
            if (addNode) {
                DataNodePermissions oldPermissions = stateAsDataNodePermissions(
                        node.getInitialCreateState(), node.getInitialReadState(),
                        node.getInitialUpdateState(), node.getInitialDeleteState());
                DataNodePermissions newPermissions = stateAsDataNodePermissions(
                        node.getCreateState(), node.getReadState(),
                        node.getUpdateState(), node.getDeleteState());
                DataNodePermissionsWithNodeName perm =
                        new DataNodePermissionsWithNodeName(
                        (PermissionNode)node.getCorrespondingTreeNode(),
                        oldPermissions,
                        newPermissions, node.getNodeDisplayName(),
                        node.getNodeFullName());
                nodesWithAuthChanges.add(perm);
            }
        }
        return nodesWithAuthChanges;
    }

    private DataNodePermissions stateAsDataNodePermissions(int create, int read,
            int update, int delete) {
        boolean hasCreate = (create != CheckBox.DESELECTED);
        boolean hasRead = (read != CheckBox.DESELECTED);
        boolean hasUpdate = (update != CheckBox.DESELECTED);
        boolean hasDelete = (delete != CheckBox.DESELECTED);
        return new DataNodePermissions(hasCreate, hasRead, hasUpdate, hasDelete);
    }

    public void removeSystemModelRootNodes() {
        DataNodesTreeNode root = (DataNodesTreeNode)getTree().getModel().getRoot();
        Iterator it = systemModelRootNodes.iterator();
        boolean changed = false;
        while (it.hasNext()) {
            DataNodesTreeNode node = (DataNodesTreeNode)it.next();
            int index = root.getIndex(node);
            if (index != 0) {
                root.remove(index);
                changed = true;
            }
        }
        if (changed) {
            Object obj = getTree().getModel();
            if (obj instanceof DefaultTreeModel) {
                ((DefaultTreeModel)obj).reload();
            }
        }
    }

    public void insertSystemModelRootNodes() {
        DataNodesTreeNode root = (DataNodesTreeNode)getTree().getModel().getRoot();
        Iterator it = systemModelRootNodes.iterator();
        boolean changed = false;
        while (it.hasNext()) {
            DataNodesTreeNode node = (DataNodesTreeNode)it.next();
            int index = root.getIndex(node);
            if (index < 0) {
                root.add(node);
                changed = true;
            }
        }
        if (changed) {
            Object obj = getTree().getModel();
            if (obj instanceof DefaultTreeModel) {
                ((DefaultTreeModel)obj).reload();
            }
        }
    }

	public Object getValueAt(int row, int col) {
		Object obj = super.getValueAt(row, col);
		return obj;
	}
	
	public void paint(Graphics g) {
        super.paint(g);
    }
}//end AuthorizationsTreeTable




class AuthorizationsCheckBoxRenderer extends DefaultCellEditor
        implements TableCellRenderer {

	private AuthorizationsTreeTable treeTable;
    private AuthorizationsCheckBox editor;
    private AuthorizationsCheckBox renderer;
        
    public AuthorizationsCheckBoxRenderer(AuthorizationsTreeTable treeTbl) {
        super(new AuthorizationsCheckBox());
        treeTable = treeTbl;
        editor = (AuthorizationsCheckBox)getComponent();
        editor.addActionListener(new ActionListener() {
            public void actionPerformed(ActionEvent ev) {
                setModel();
            }
        });
        renderer = new AuthorizationsCheckBox();
    }

    public Component getTableCellEditorComponent(JTable theTable,
            Object theValue, boolean isSelected, int theRow, int theColumn) {
        DataNodesTreeNode node = ((AuthorizationsTreeTable)theTable).getNodeForRow(theRow);
        boolean enabling = false;
        switch (theColumn) {
            case AuthorizationsModel.CREATE_COLUMN_NUM:
                enabling = node.isCreateEnabled();
                break;
            case AuthorizationsModel.READ_COLUMN_NUM:
                enabling = node.isReadEnabled();
                break;
            case AuthorizationsModel.UPDATE_COLUMN_NUM:
                enabling = node.isUpdateEnabled();
                break;
            case AuthorizationsModel.DELETE_COLUMN_NUM:
                enabling = node.isDeleteEnabled();
                break;
        }
        editor.setEnabled(enabling);
        int state = ((Integer)treeTable.getModel().getValueAt(theRow, 
        		theColumn)).intValue();
        switch (state) {
            case CheckBox.DESELECTED:
                editor.setPartiallySelected(false);
                editor.setSelected(false);
                break;
            case CheckBox.PARTIALLY_SELECTED:
                editor.setPartiallySelected(true);
                break;
            case CheckBox.SELECTED:
                editor.setPartiallySelected(false);
                editor.setSelected(true);
                break;
        }
        editor.setCurrentRow(theRow);
        editor.setCurrentColumn(theColumn);
        editor.setUseType("editor");
        return editor;
    }

    public Component getTableCellRendererComponent(JTable theTable,
            Object theValue, boolean isSelected, boolean hasFocus,
            int theRow, int theColumn) {
        AuthorizationsTreeTable treeTable = (AuthorizationsTreeTable)theTable;
        DataNodesTreeNode node = treeTable.getNodeForRow(theRow);
        boolean enabling = false;
        switch (theColumn) {
            case AuthorizationsModel.CREATE_COLUMN_NUM:
                enabling = node.isCreateEnabled();
                break;
            case AuthorizationsModel.READ_COLUMN_NUM:
                enabling = node.isReadEnabled();
                break;
            case AuthorizationsModel.UPDATE_COLUMN_NUM:
                enabling = node.isUpdateEnabled();
                break;
            case AuthorizationsModel.DELETE_COLUMN_NUM:
                enabling = node.isDeleteEnabled();
                break;
        }
        renderer.setEnabled(enabling);
        int state = ((Integer)theValue).intValue();
        switch (state) {
            case CheckBox.DESELECTED:
                renderer.setPartiallySelected(false);
                renderer.setSelected(false);
                break;
            case CheckBox.PARTIALLY_SELECTED:
                renderer.setPartiallySelected(true);
                break;
            case CheckBox.SELECTED:
                renderer.setPartiallySelected(false);
                renderer.setSelected(true);
				break;
        }

        Color backgroundColor = (isSelected) ? theTable.getSelectionBackground()
                                             : theTable.getBackground();
//        if (treeTable.getEditingRow() == theRow &&
//            treeTable.getEditingColumn() == theColumn) {
//            backgroundColor = UIManager.getColor("Table.focusCellBackground");
//        }
        renderer.setBackground(backgroundColor);
        renderer.setCurrentRow(theRow);
        renderer.setCurrentColumn(theColumn);
        renderer.setUseType("renderer");
        return renderer;
    }

    private void setModel() {
        int row = treeTable.getSelectedRow();
        int col = treeTable.getSelectedColumn();
        int value;
        if (editor.isPartiallySelected()) {
            //If partially selected, has been changed to this from deselected.
            //We need for deselected to go to selected.
            value = CheckBox.SELECTED;
        } else if (editor.isSelected()) {
            //If selected, has been changed to this from partially selected.
            //We need for partially selected to go to deselected.
            value = CheckBox.DESELECTED;
        } else {
            //If deselected, has been changed to this from selected.
            //Deselected is what we want.
            value = CheckBox.DESELECTED;
        }
        Integer integerVal = new Integer(value);
        treeTable.getModel().setValueAt(integerVal, row, col);
        //treeTable.editingCanceled(new ChangeEvent(editor));
    }

}//end AuthorizationsCheckBoxRenderer
