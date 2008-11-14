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

import javax.swing.tree.*;
import java.awt.*;
import javax.swing.*;

import com.metamatrix.console.ui.util.ConsoleCellRenderer;
import com.metamatrix.console.util.StaticUtilities;
import com.metamatrix.platform.admin.api.*;

class AuthorizationsTreeCellRenderer extends DefaultTreeCellRenderer {
    public final static Color XML_DOCUMENT_FOREGROUND_COLOR = 
    		StaticUtilities.averageRGBVals(new Color[] {Color.black,
    		Color.red});
    public final static Color STORED_PROCEDURE_FOREGROUND_COLOR = Color.blue;

//Constructors

    /**
     * Constructor.
     */
    public AuthorizationsTreeCellRenderer() {
        super();
    }

//Overridden methods

    public Component getTreeCellRendererComponent(
            final JTree tree,
            final Object value,
            boolean isSelected,
            final boolean isExpanded,
            final boolean isLeaf,
            final int row,
            final boolean hasFocus) {

        JLabel comp = (JLabel)super.getTreeCellRendererComponent(
                    tree, value, isSelected, isExpanded, isLeaf, row, hasFocus);
        if (value != null) {
            DataNodesTreeNode node = (DataNodesTreeNode)value;
            Icon icon = null;
            PermissionNode pNode =
            		(PermissionNode)node.getCorrespondingTreeNode();
            if (pNode instanceof PermissionDataNode) {
                PermissionDataNode pdNode = (PermissionDataNode)pNode;
            	int type = pdNode.getDataNodeType();
				if (type == PermissionDataNodeDefinition.TYPE.PROCEDURE) {
            		icon = ConsoleCellRenderer.STORED_PROCEDURE_ICON;
            	    comp.setForeground(STORED_PROCEDURE_FOREGROUND_COLOR);
            	} else if (type == PermissionDataNodeDefinition.TYPE.DOCUMENT) {
            		icon = ConsoleCellRenderer.XML_DOCUMENT_ICON;
            	    comp.setForeground(XML_DOCUMENT_FOREGROUND_COLOR);
            	} else { 
                	if (node.parentIsRoot()) {
                		boolean isPhysicalModel = pdNode.isPhysical();
                		if (isPhysicalModel) {
                    		icon = ConsoleCellRenderer.PHYSICAL_MODEL_ICON;
                		} else {
                    		icon = ConsoleCellRenderer.VIRTUAL_MODEL_ICON;
                		}
            		} else {
                		boolean isAttribute = (node.getChildCount() == 0);
                		if (isAttribute) {
                    		icon = ConsoleCellRenderer.ATTRIBUTE_ICON;
                		}
                	}
                }
            }
            comp.setIcon(icon);
        }
		return comp;
    }
}//end AuthorizationsTreeCellRenderer
