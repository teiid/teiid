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

import java.util.*;

import javax.swing.event.TreeSelectionEvent;
import javax.swing.event.TreeSelectionListener;
import javax.swing.tree.TreePath;

import com.metamatrix.console.connections.ConnectionInfo;

public class TreeAndControllerCoordinator {
	private static Map /*<ConnectionInfo to TreeAndControllerCoordinator>*/
			controllerMap = new HashMap();
    private static WorkspaceController controller;
    
    private PanelsTree tree;
    
    private TreeAndControllerCoordinator(PanelsTree tr,
            WorkspaceController ctrlr) {
        super();
        tree = tr;
        controller = ctrlr;
        init();
    }

    public static void createInstance(PanelsTree tree,
            WorkspaceController cntrlr, ConnectionInfo connection) {
        TreeAndControllerCoordinator coordinator = 
        		new TreeAndControllerCoordinator(tree, controller);
        controllerMap.put(connection, coordinator);
        if (controller == null) {
        	controller = cntrlr;
        }
    }

    public static TreeAndControllerCoordinator getInstance(
    		ConnectionInfo connection) {
    	TreeAndControllerCoordinator coordinator = null;
    	Iterator it = controllerMap.entrySet().iterator();
    	while (it.hasNext() && (coordinator == null)) {
    		Map.Entry me = (Map.Entry)it.next();
    		ConnectionInfo conn = (ConnectionInfo)me.getKey();
    		if (conn.equals(connection)) {
    			coordinator = (TreeAndControllerCoordinator)me.getValue();
    		}
    	}
        return coordinator;
    }

    private void init() {
        tree.addTreeSelectionListener(new TreeSelectionListener() {
            public void valueChanged(TreeSelectionEvent ev) {
                treeSelectionChanged(ev);
            }
        });
    }

    private void treeSelectionChanged(TreeSelectionEvent ev) {
        if (!tree.isSelectionCancellation()) {
        	TreePath tp = ev.getNewLeadSelectionPath();
        	if (tp != null) {
        		ConnectionInfo connection = tree.getConnection();
				PanelsTreeNode node = (PanelsTreeNode)tp.getLastPathComponent();
            	Class cls = node.getPanelClass();
            	if (cls != null) {
					if (!controller.isChangingConnections()) {            	
            			controller.treeSelectionChangedToClass(cls, true, 
                				connection);
					}
            	}
            }
        }
    }
}
