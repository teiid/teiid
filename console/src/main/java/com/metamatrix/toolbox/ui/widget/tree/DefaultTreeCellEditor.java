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

//################################################################################################################################
package com.metamatrix.toolbox.ui.widget.tree;

import java.awt.Component;
import java.awt.event.FocusAdapter;
import java.awt.event.FocusEvent;
import java.util.EventObject;

import javax.swing.DefaultCellEditor;
import javax.swing.JTree;
import javax.swing.tree.DefaultTreeCellRenderer;
import javax.swing.tree.TreeCellEditor;
import javax.swing.tree.TreeModel;
import javax.swing.tree.TreePath;

import com.metamatrix.common.object.PropertyDefinition;
import com.metamatrix.common.tree.TreeNode;
import com.metamatrix.common.tree.TreeNodeEditor;

import com.metamatrix.toolbox.ui.widget.TextFieldWidget;
import com.metamatrix.toolbox.ui.widget.TreeWidget;

/**
 * This class is intended to be used everywhere within the application that a tree needs to be displayed.
 * @since 2.0
 * @version 2.1
 * @author <a href="mailto:jverhaeg@metamatrix.com">John P. A. Verhaeg</a>
 */
public class DefaultTreeCellEditor extends javax.swing.tree.DefaultTreeCellEditor {
    //############################################################################################################################
    //# Constructors                                                                                                             #
    //############################################################################################################################

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    public DefaultTreeCellEditor(final TreeWidget tree, final DefaultTreeCellRenderer renderer) {
        super(tree, renderer);
    }
    
    //############################################################################################################################
    //# Instance Methods                                                                                                         #
    //############################################################################################################################

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    protected TreeCellEditor createTreeCellEditor() {
        final DefaultCellEditor editor = new DefaultCellEditor(new TextFieldWidget()) {
            public boolean shouldSelectCell(EventObject event) {
                final boolean retValue = super.shouldSelectCell(event);
                getComponent().requestFocus();
                return retValue;
            }
        };
        // One click to edit.
        editor.setClickCountToStart(1);
        return editor;
    }

	//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
     * @since 2.1
     */
    public Component getTreeCellEditorComponent(final JTree tree, final Object value, final boolean selected,
    											final boolean expanded, final boolean leaf, final int row) {
		renderer.getTreeCellRendererComponent(tree, value, selected, expanded, leaf, row, false);
		return super.getTreeCellEditorComponent(tree, value, selected, expanded, leaf, row);
	}
	
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    protected void prepareForEditing() {
        super.prepareForEditing();
        if (editingComponent != null) {
            editingComponent.addFocusListener(new FocusAdapter() {
                public void focusLost(final FocusEvent event) {
                    if (tree.isEditing()) {
                        stopCellEditing();
                    }
                }
            });
        }
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    public boolean isCellEditable(final EventObject event) {
        final TreeModel model = tree.getModel();
        final TreePath path = tree.getSelectionPath();
        if (!(model instanceof DefaultTreeModel)  ||  path == null) {
            return super.isCellEditable(event);
        }
        final TreeNodeEditor editor = ((DefaultTreeModel)model).getTreeView().getTreeNodeEditor();
        final TreeNode node = (TreeNode)path.getLastPathComponent();
        final PropertyDefinition nameDef = editor.getNamePropertyDefinition(node);
        if (nameDef == null  ||  !editor.isReadOnly(node, nameDef)) {
            return super.isCellEditable(event);
        }
        return false;
    }
}
