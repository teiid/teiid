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

package com.metamatrix.console.ui.views.entitlements;

import javax.swing.tree.TreeNode;

import com.metamatrix.console.ui.treetable.DynamicTreeTableModel;
import com.metamatrix.console.ui.treetable.TreeTableModel;

public class AuthorizationsModel extends DynamicTreeTableModel {
    public final static int TREE_COLUMN_NUM = 0;
    public final static int CREATE_COLUMN_NUM = 1;
    public final static int READ_COLUMN_NUM = 2;
    public final static int UPDATE_COLUMN_NUM = 3;
    public final static int DELETE_COLUMN_NUM = 4;

    private static final String[] DATA_COLUMN_NAMES = {"Data Nodes", "Create",
            "Read", "Update", "Delete"};

    private static final String[] GETTER_METHOD_NAMES = {"getName",
            "getCreateState", "getReadState", "getUpdateState",
            "getDeleteState"};

    private static final String[] SETTER_METHOD_NAMES = {null,
            "setCreateState", "setReadState", "setUpdateState",
            "setDeleteState"};

    private static final Class[] CLASSES = {TreeTableModel.class,
            Integer.class, Integer.class, Integer.class, Integer.class};

    public static String columnNumToString(int columnNum) {
        return DATA_COLUMN_NAMES[columnNum];
    }

    private AuthorizationsTreeTable treeTable = null;
    
    public AuthorizationsModel(javax.swing.tree.DefaultTreeModel model) {
        super((TreeNode)model.getRoot(), 
        		DATA_COLUMN_NAMES, 
        		GETTER_METHOD_NAMES, SETTER_METHOD_NAMES, CLASSES);
   	}

    public void setTreeTable(AuthorizationsTreeTable treeTbl) {
        treeTable = treeTbl;
    }

    public boolean isCellEditable(Object nodeObj, int column) {
        boolean editable = true;
        if (column != TREE_COLUMN_NUM) {
            if (treeTable != null) {
                DataNodesTreeNode node = (DataNodesTreeNode)nodeObj;
                if (node != null) {
                   	switch (column) {
                        case CREATE_COLUMN_NUM:
                            editable = node.isCreateEnabled();
                            break;
                        case READ_COLUMN_NUM:
                            editable = node.isReadEnabled();
                            break;
                        case UPDATE_COLUMN_NUM:
                            editable = node.isUpdateEnabled();
                            break;
                        case DELETE_COLUMN_NUM:
                            editable = node.isDeleteEnabled();
                            break;
                    }
            }
            }
        }
        return editable;
    }
    
    public Object getValueAt(Object node, int col) {
		Object val = super.getValueAt(node, col);
		return val;
    }
}
