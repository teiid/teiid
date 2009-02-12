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
package com.metamatrix.toolbox.ui.widget;

// System imports
import java.awt.Component;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;

import javax.swing.JTable;
import javax.swing.table.DefaultTableCellRenderer;

import com.metamatrix.common.tree.directory.DirectoryEntry;
import com.metamatrix.common.tree.directory.DirectoryEntryView;

import com.metamatrix.toolbox.ui.UIDefaults;
import com.metamatrix.toolbox.ui.widget.table.DirectoryEntryTableComparator;
import com.metamatrix.toolbox.ui.widget.table.TableComparator;

/**
This class is intended to be used everywhere within the application that a table needs to be displayed.
@since Golden Gate
@version Golden Gate
@author K. Goring
*/
public class DirectoryEntryTable extends PropertiedObjectArrayTable {
    //############################################################################################################################
    //# Instance Variables                                                                                                       #
    //############################################################################################################################

//    private int propertyCount = 0;
//    private DirectoryEntry directoryEntry;
    private DirectoryEntryView directoryEntryView;
//    private DirectoryEntryEditor directoryEntryEditor;
    private Collection showProperties = Collections.EMPTY_LIST;
//    private boolean isModelerChooserPanel = false;


    //############################################################################################################################
    //# Constructors                                                                                                             #
    //############################################################################################################################

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    /*
    @since Golden Gate
    */
    public DirectoryEntryTable(DirectoryEntryView view) {
        this.setPropertiedObjectEditor(view.getDirectoryEntryEditor());
        directoryEntryView = view;
    }

    public void setTableArray(DirectoryEntry entry) {
//        directoryEntry = entry;
        //if entry has no children, it is a file, show info about itself
        if (directoryEntryView.getChildren(entry).size() == 0 && !directoryEntryView.allowsChildren(entry)) {
            //Make it a list of one, a List is what the super method setTableArray() is looking for
            ArrayList fileEntry = new ArrayList();
            fileEntry.add(entry);
            if (showProperties != Collections.EMPTY_LIST) {
                super.setTableArray(fileEntry, showProperties);
            } else {
                super.setTableArray(fileEntry);
            }
        } else {
            if (showProperties != Collections.EMPTY_LIST) {
                super.setTableArray(directoryEntryView.getChildren(entry), showProperties);
            } else {
                super.setTableArray(directoryEntryView.getChildren(entry));
            }
        }
    }

    public void setTableArray(DirectoryEntry entry, Collection showProperties) {
        this.showProperties = showProperties;
        setTableArray(entry);
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since Golden Gate
    */
    protected TableComparator createDefaultComparator() {
        return DirectoryEntryTableComparator.getInstance();
    }


    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since Golden Gate
    */
    protected DefaultTableCellRenderer getCellRenderer() {
        return new IconTableCellRenderer(directoryEntryView);
    }
}


/**
 * IconTableCellRenderer is used by the details table to display DirectoryEntries, which appear in the first column,
 * with their appropriate icon (folder or element).
 */
class IconTableCellRenderer extends DefaultTableCellRenderer {
    private DirectoryEntryView directoryEntryView;

    public IconTableCellRenderer(DirectoryEntryView tv) {
        directoryEntryView = tv;
    }

    public void setValue(Object value) {
        if (value instanceof DirectoryEntry) {
            DirectoryEntry tn = (DirectoryEntry)value;
            if(directoryEntryView.allowsChildren(tn)) {
                setIcon(UIDefaults.getInstance().getIcon("Tree.closedIcon"));
            } else {
                setIcon(UIDefaults.getInstance().getIcon("Tree.leafIcon"));
            }
            setText(tn.getName());
        } else {
            setText(value.toString());
        }
    }


    public Component getTableCellRendererComponent(final JTable table, final Object value, final boolean isSelected,
                                                   final boolean hasFocus, final int rowIndex, final int columnIndex) {

    	PropertiedObjectArrayTable thisTable = (PropertiedObjectArrayTable) table;
        Object selectedObject = thisTable.getSelectedObject(rowIndex);
        return super.getTableCellRendererComponent(thisTable, selectedObject, isSelected, hasFocus, rowIndex, columnIndex);
    }
}

