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

package com.metamatrix.console.ui.views.vdb;

import java.awt.GridLayout;

import javax.swing.JPanel;
import javax.swing.JScrollPane;

import com.metamatrix.common.log.LogManager;
import com.metamatrix.console.connections.ConnectionInfo;
import com.metamatrix.console.models.ModelManager;
import com.metamatrix.console.models.VdbManager;
import com.metamatrix.console.util.ExceptionUtility;
import com.metamatrix.console.util.LogContexts;
import com.metamatrix.metadata.runtime.api.VirtualDatabase;
import com.metamatrix.metadata.runtime.api.VirtualDatabaseID;
import com.metamatrix.platform.admin.api.PermissionDataNodeTreeView;
import com.metamatrix.toolbox.ui.widget.TreeWidget;

public class VDBMetadataTreePanel extends JPanel implements
                                                VdbDisplayer {

    private VirtualDatabase vdb = null;
    private TreeWidget tree;
    private DataNodesTreeModel treeModel;
    private VDBMetadataTreeNode root;
    private ConnectionInfo connection = null;

    public VDBMetadataTreePanel(ConnectionInfo connection) {
        this.connection = connection;

        initialize();
    }

    private VdbManager getVdbManager() {
        return ModelManager.getVdbManager(connection);
    }

    public void setVirtualDatabase(VirtualDatabase vdb) {
        boolean same;
        if (this.vdb == null) {
            same = (vdb == null);
        } else {
            same = (this.vdb == vdb);
        }
        if (!same) {
            this.vdb = vdb;
            if (vdb == null) {
                setTreeEmpty();
            } else {
                setTreeFromVDB();
            }
        }
    }

    private void initialize() {
        root = new VDBMetadataTreeNode("root", null);
        // treeModel = DataNodesTreeModel.createDefaultTreeModelFromTreeView(root);
        treeModel = new DataNodesTreeModel(root);
        tree = new TreeWidget(treeModel);
        tree.setRootVisible(false);
        tree.setShowsRootHandles(true);
        tree.setCellRenderer(DataNodesTreeModel.createCellRenderer());
        this.setLayout(new GridLayout(1, 1));
        this.add(new JScrollPane(tree));
    }

    private void setTreeEmpty() {
        root.removeAllChildren();
        treeModel.reload();
    }

    private void setTreeFromVDB() {
        PermissionDataNodeTreeView treeView = null;
        try {
            String vdbName = vdb.getName();
            VirtualDatabaseID id = (VirtualDatabaseID)vdb.getID();
            String vdbVersion = id.getVersion();
            treeView = getVdbManager().getTreeViewForVDB(vdbName, vdbVersion);
            root.removeAllChildren();

            treeModel = DataNodesTreeModel.createDefaultTreeModelFromTreeView(treeView, treeModel, root);
            treeModel.reload();

        } catch (Exception ex) {
            String msg = "Error retrieving VDB metadata tree.";
            LogManager.logError(LogContexts.VIRTUAL_DATABASE, ex, msg);
            ExceptionUtility.showMessage(msg, ex);
        }
    }
}// end VDBMetadataTreePanel

