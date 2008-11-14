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

/*
 * All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Sun Microsystems, Inc. ("Confidential Information").  You
 * shall not disclose such Confidential Information and shall use
 * it only in accordance with the terms of the license agreement
 * you entered into with Sun.
 */
package com.metamatrix.console.ui.treetable;

import javax.swing.JTree;
import javax.swing.SwingUtilities;
import javax.swing.event.TreeExpansionEvent;
import javax.swing.event.TreeExpansionListener;
import javax.swing.event.TreeModelEvent;
import javax.swing.event.TreeModelListener;
import javax.swing.table.AbstractTableModel;
import javax.swing.tree.TreePath;

/**
 * This is a wrapper class takes a TreeTableModel and implements 
 * the table model interface. The implementation is trivial, with 
 * all of the event dispatching support provided by the superclass: 
 * the AbstractTableModel. 
 *
 * @version 1.2 10/27/98
 *
 * @author Philip Milne
 * @author Scott Violet
 */
public class TreeTableModelAdapter extends AbstractTableModel
{
    JTree tree;
    TreeTableModel treeTableModel;

    public TreeTableModelAdapter(TreeTableModel treeTableModel, JTree tree) {
        this.tree = tree;
        this.treeTableModel = treeTableModel;

	    tree.addTreeExpansionListener(new TreeExpansionListener() {
	        // Don't use fireTableRowsInserted() here; the selection model
	        // would get updated twice.
	        public void treeExpanded(TreeExpansionEvent event) {
	            fireTableDataChanged();
	        }
            public void treeCollapsed(TreeExpansionEvent event) {
	            fireTableDataChanged();
	        }
	    });

	    // Installs a TreeModelListener that can update the table when
	    // the tree changes. We use delayedFireTableDataChanged as we can
	    // not be guaranteed the tree will have finished processing
	    // the event before us.
	    treeTableModel.addTreeModelListener(new TreeModelListener() {
	        public void treeNodesChanged(TreeModelEvent e) {
		        delayedFireTableDataChanged();
	        }

	        public void treeNodesInserted(TreeModelEvent e) {
		        delayedFireTableDataChanged();
	        }

	        public void treeNodesRemoved(TreeModelEvent e) {
		        delayedFireTableDataChanged();
	        }

	        public void treeStructureChanged(TreeModelEvent e) {
		        delayedFireTableDataChanged();
	        }
	    });
    }

    // Wrappers, implementing TableModel interface.

    public int getColumnCount() {
	    return treeTableModel.getColumnCount();
    }

    public String getColumnName(int column) {
	    return treeTableModel.getColumnName(column);
    }

    public Class getColumnClass(int column) {
	    return treeTableModel.getColumnClass(column);
    }

    public int getRowCount() {
	    return tree.getRowCount();
    }

    protected Object nodeForRow(int row) {
	    TreePath treePath = tree.getPathForRow(row);
	    return treePath.getLastPathComponent();
    }

    public Object getValueAt(int row, int column) {
	    return treeTableModel.getValueAt(nodeForRow(row), column);
    }

    public boolean isCellEditable(int row, int column) {
        return treeTableModel.isCellEditable(nodeForRow(row), column);
    }

    public void setValueAt(Object value, int row, int column) {
	    treeTableModel.setValueAt(value, nodeForRow(row), column);
    }

    /**
     * Invokes fireTableDataChanged after all the pending events have been
     * processed. SwingUtilities.invokeLater is used to handle this.
     */
    protected void delayedFireTableDataChanged() {
	    SwingUtilities.invokeLater(new Runnable() {
	        public void run() {
		        fireTableDataChanged();
	        }
	    });
    }
}

