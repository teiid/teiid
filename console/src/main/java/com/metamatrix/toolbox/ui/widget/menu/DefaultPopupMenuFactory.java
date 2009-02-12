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

//################################################################################################################################
package com.metamatrix.toolbox.ui.widget.menu;

// System imports
import java.awt.Component;
import java.awt.Toolkit;
import java.awt.datatransfer.Clipboard;
import java.awt.datatransfer.StringSelection;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;

import javax.swing.JMenuItem;
import javax.swing.JPopupMenu;
import javax.swing.tree.TreeModel;
import javax.swing.tree.TreePath;

import com.metamatrix.common.tree.TreeNode;
import com.metamatrix.common.tree.TreeNodeEditor;
import com.metamatrix.toolbox.ui.widget.ListWidget;
import com.metamatrix.toolbox.ui.widget.PopupMenuFactory;
import com.metamatrix.toolbox.ui.widget.TableWidget;
import com.metamatrix.toolbox.ui.widget.TreeWidget;
import com.metamatrix.toolbox.ui.widget.tree.DefaultTreeModel;

/**
@since 2.0
@version 2.1
@author <a href="mailto:jverhaeg@metamatrix.com">John P. A. Verhaeg</a>
*/
public class DefaultPopupMenuFactory
implements PopupMenuFactory {
    //############################################################################################################################
    //# Instance Methods                                                                                                         #
    //############################################################################################################################

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    protected JPopupMenu createListPopupMenu(final ListWidget list) {
        final JPopupMenu popupMenu = new JPopupMenu();
        final JMenuItem item = new JMenuItem("Copy");
        item.addActionListener(new ActionListener() {
            final Clipboard clipBoard = Toolkit.getDefaultToolkit().getSystemClipboard();
            public void actionPerformed(final ActionEvent event) {
                final Object val = list.getSelectedValue();
                if (val == null) {
                    return;
                }
                final StringSelection selection = new StringSelection(val.toString());
                clipBoard.setContents(selection, selection);
            }
        });
        popupMenu.add(item);
        return popupMenu;
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    protected JPopupMenu createTablePopupMenu(final TableWidget table) {
        final JPopupMenu popupMenu = new JPopupMenu();
        final JMenuItem item = new JMenuItem("Copy");
        item.addActionListener(new ActionListener() {
            final Clipboard clipBoard = Toolkit.getDefaultToolkit().getSystemClipboard();
            public void actionPerformed(final ActionEvent event) {
                if (table.getSelectedColumnCount() != 1  ||  table.getSelectedRowCount() != 1) {
                    return;
                }
                final StringSelection selection = 
                    new StringSelection(table.getValueAt(table.getSelectedRow(), table.getSelectedColumn()).toString());
                clipBoard.setContents(selection, selection);
            }
        });
        popupMenu.add(item);
        return popupMenu;
    }
    
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    protected JPopupMenu createTreePopupMenu(final TreeWidget tree) {
        final TreePath path = tree.getSelectionPath();
        if (path == null) {
            return null;
        }
        final JPopupMenu popupMenu = new JPopupMenu();
        JMenuItem item = new JMenuItem("Copy");
        item.addActionListener(new ActionListener() {
            final Clipboard clipBoard = Toolkit.getDefaultToolkit().getSystemClipboard();
            public void actionPerformed(final ActionEvent event) {
                if (tree.getSelectionCount() != 1) {
                    return;
                }
                final StringSelection selection = 
                    new StringSelection(((TreeNode)path.getLastPathComponent()).getName());
                clipBoard.setContents(selection, selection);
            }
        });
        popupMenu.add(item);
        final TreeModel model = tree.getModel();
        if (tree.isEditable()  &&  model instanceof DefaultTreeModel) {
            final TreeNodeEditor editor = ((DefaultTreeModel)model).getTreeView().getTreeNodeEditor();
            final TreeNode node = (TreeNode)path.getLastPathComponent();
            if (!editor.isReadOnly(node, editor.getNamePropertyDefinition(node))) {
                popupMenu.addSeparator();
                item = new JMenuItem("Rename");
                item.addActionListener(new ActionListener() {
                    public void actionPerformed(final ActionEvent event) {
                        tree.startEditingAtPath(path);
                    }
                });
                popupMenu.add(item);
            }
        }
        return popupMenu;
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    public JPopupMenu getPopupMenu(final Component context) {
        if (context instanceof TreeWidget) {
            return createTreePopupMenu((TreeWidget)context);
        }
        if (context instanceof TableWidget) {
            final TableWidget table = (TableWidget)context;
            if (table.getCellSelectionEnabled()) {
                return createTablePopupMenu(table);
            }
        } else if (context instanceof ListWidget) {
            return createListPopupMenu((ListWidget)context);
        }
        return null;
    }
}
