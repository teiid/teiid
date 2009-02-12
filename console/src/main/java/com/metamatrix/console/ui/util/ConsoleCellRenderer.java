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

package com.metamatrix.console.ui.util;

import java.awt.Component;

import javax.swing.DefaultListCellRenderer;
import javax.swing.Icon;
import javax.swing.ImageIcon;
import javax.swing.JList;
import javax.swing.JTree;
import javax.swing.ListCellRenderer;
import javax.swing.tree.DefaultMutableTreeNode;
import javax.swing.tree.DefaultTreeCellRenderer;
import javax.swing.tree.TreeCellRenderer;
import javax.swing.tree.TreePath;

import com.metamatrix.metadata.runtime.api.MetadataID;
import com.metamatrix.metadata.runtime.api.MetadataObject;

import com.metamatrix.toolbox.ui.widget.util.IconFactory;

/**
 * The universal List and Tree CellRenderer for the Console
 */
public class ConsoleCellRenderer implements ListCellRenderer, TreeCellRenderer {

    static{
        IconFactory.setDefaultJarPath("/com/metamatrix/console/images/"); //$NON-NLS-1$
        IconFactory.setDefaultRelativePath("../images/"); //$NON-NLS-1$
    }
    public static final ImageIcon FOLDER_ICON = IconFactory.getIconForImageFile("folder.gif"); //$NON-NLS-1$
    public static final ImageIcon FOLDER_OPEN_ICON = IconFactory.getIconForImageFile("folderOpen.gif"); //$NON-NLS-1$
    public static final ImageIcon LOCKED_MODEL_ICON = IconFactory.getIconForImageFile("locked_model.gif"); //$NON-NLS-1$
    public static final ImageIcon MODEL_ICON = IconFactory.getIconForImageFile("model.gif"); //$NON-NLS-1$
    public static final ImageIcon LOCKED_DATASOURCE_ICON = IconFactory.getIconForImageFile("locked_catalog.gif"); //$NON-NLS-1$
    public static final ImageIcon VIRTUAL_MODEL_ICON = IconFactory.getIconForImageFile("rt_virtual_model_node.gif"); //$NON-NLS-1$
    public static final ImageIcon PHYSICAL_MODEL_ICON = IconFactory.getIconForImageFile("rt_physical_model_node.gif"); //$NON-NLS-1$
    public static final ImageIcon ATTRIBUTE_ICON = IconFactory.getIconForImageFile("dt_element_node.gif"); //$NON-NLS-1$
    public static final ImageIcon STORED_PROCEDURE_ICON = IconFactory.getIconForImageFile("dt_physical_procedure_node.gif"); //$NON-NLS-1$
    public static final ImageIcon XML_DOCUMENT_ICON = IconFactory.getIconForImageFile("dt_xml_document.gif"); //$NON-NLS-1$
    public static final ImageIcon SERVICE_GROUP_ICON = IconFactory.getIconForImageFile("serviceGroup.gif"); //$NON-NLS-1$
    public static final ImageIcon QUERY_ICON = IconFactory.getIconForImageFile("query.gif"); //$NON-NLS-1$
    public static final ImageIcon EXTRACTOR_ICON = IconFactory.getIconForImageFile("extractpr.gif"); //$NON-NLS-1$
    public static final ImageIcon LOGGER_ICON = IconFactory.getIconForImageFile("logger.gif"); //$NON-NLS-1$
    public static final ImageIcon USER_GROUP_ICON = IconFactory.getIconForImageFile("groups16.gif"); //$NON-NLS-1$
    public static final ImageIcon USER_ICON = IconFactory.getIconForImageFile("users16.gif"); //$NON-NLS-1$
    public static final ImageIcon ROLE_ICON = IconFactory.getIconForImageFile("role.gif"); //$NON-NLS-1$
    public static final ImageIcon SERVICE_ICON = IconFactory.getIconForImageFile("extractor.gif"); //$NON-NLS-1$
    public static final ImageIcon VIRTUAL_MACHINE_ICON = IconFactory.getIconForImageFile("virtualMachine.gif"); //$NON-NLS-1$
    public static final ImageIcon HOST_MACHINE_ICON = IconFactory.getIconForImageFile("hostMachine.gif"); //$NON-NLS-1$
    public static final ImageIcon MACHINE_GROUP_ICON = IconFactory.getIconForImageFile("machineGroup.gif"); //$NON-NLS-1$

    private DefaultListCellRenderer listCellRenderer = null;
    private DefaultTreeCellRenderer treeCellRenderer = null;
    private boolean useFullName = false;

//    private boolean serviceNotMachineGrouping = false;

    /**
     * Default constructor
     */
    public ConsoleCellRenderer(){
        this(new DefaultListCellRenderer(), new DefaultTreeCellRenderer());
    }

    /**
     * Constructor which accepts a DefaultListCellRenderer and a
     *DefaultTreeCellRenderer to be used internally
     */
    public ConsoleCellRenderer(DefaultListCellRenderer listCellRenderer,
                               DefaultTreeCellRenderer treeCellRenderer){
        super();
        this.listCellRenderer = listCellRenderer;
        this.treeCellRenderer = treeCellRenderer;
    }

    public void setUseFullName(boolean flag) {
        useFullName = flag;
    }

    public Component getTreeCellRendererComponent( JTree tree,
                                                   Object value,
                                                   boolean selected,
                                                   boolean expanded,
                                                   boolean leaf,
                                                   int row,
                                                   boolean hasFocus ) {

        treeCellRenderer.getTreeCellRendererComponent(tree, value, selected, expanded, leaf, row, hasFocus);

        /*
        if (value instanceof GDDRepositoryTreeNode) {
            value = ((GDDRepositoryTreeNode) value).getUserObject();
        } else if (value instanceof LazyBranchNode) {
            //do nothing
        } else if (value instanceof BasicDirectoryTreeNode) {
            value = ((BasicDirectoryTreeNode) value).getDirectoryNode();
        } else if (value instanceof DefaultMutableTreeNode) {
            value = ((DefaultMutableTreeNode) value).getUserObject();
        }
        //user object may be a wrapper around the true user object
        if (value instanceof UniversalDisplayWrapper){
            value = ((UniversalDisplayWrapper)value).getObject();
        }
        */


        treeCellRenderer.setText(getValueString(value));
        TreePath path = tree.getPathForRow(row);
//        int level = -1; // set it out of bounds
        if ( path != null ) {
//            level = 
            path.getPathCount();
        }
        treeCellRenderer.setIcon(getValueIcon(value, expanded));

        return treeCellRenderer;
    }

    /**
     * Sort of a kludge to control what Icon is used for a GroupNode - a
     *GroupNode is used in both the service config domain and the service
     *deployment domain to group services and host machines, respectively.
     *But a GroupNode is indistinguishable at runtime so we need this control.
     */
    public void setServiceNotMachineGrouping(boolean serviceNotMachineGrouping){
//        this.serviceNotMachineGrouping = serviceNotMachineGrouping;
    }

    public Component getListCellRendererComponent( JList list,
                                                   Object value,
                                                   int index,
                                                   boolean selected,
                                                   boolean hasFocus ) {

        listCellRenderer.getListCellRendererComponent(list, value, index, selected, hasFocus);
        listCellRenderer.setText(getValueString(value));
        listCellRenderer.setIcon(getValueIcon(value, false));
        return listCellRenderer;
    }

    public String getValueString(Object value) {
        String result;


        if (value instanceof LazyBranchNode) {
        //    result = ((LazyBranchNode)value).getIcon();
        //    return result;
        } else if (value instanceof DefaultMutableTreeNode) {
            value = ((DefaultMutableTreeNode) value).getUserObject();
        }
        
        if ( value instanceof MetadataObject ) {
            result = getValueString(((MetadataObject) value).getID());

        } else if ( value instanceof MetadataID ) {
            if ( useFullName ) {
                result = ((MetadataID) value).getFullName();
            } else {
                result = ((MetadataID) value).getName();
            }

        //} else if ( value instanceof Version ) {
        //    Version v = (Version) value;
        //    result = new String(v.getVersionNumber() + " - ");
        //    result += v.getName();
//RMH        } else if ( value instanceof DirectoryNode ) {
//RMH            result = ((DirectoryNode)value).getName();
        } else {
            if ( value != null ) {
                result = value.toString();
            } else {
                result = "null"; //$NON-NLS-1$
            }
        }

        return result;
    }

    public Icon getValueIcon(Object value, boolean expanded) {

        Icon result = null;

        if (value == null){
            return result;
        }

        if (value instanceof DefaultMutableTreeNode) {
            value = ((DefaultMutableTreeNode) value).getUserObject();
        }
        return result;
    }

}

