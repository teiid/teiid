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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.List;

import javax.swing.event.ListSelectionListener;
import javax.swing.event.TableModelEvent;
import javax.swing.table.DefaultTableCellRenderer;
import javax.swing.table.TableCellRenderer;
import javax.swing.table.TableColumn;

import com.metamatrix.common.object.PropertiedObject;
import com.metamatrix.common.object.PropertiedObjectEditor;
import com.metamatrix.common.object.PropertyDefinition;

import com.metamatrix.toolbox.ui.widget.table.DefaultTableModel;
import com.metamatrix.toolbox.ui.widget.table.DirectoryEntryTableComparator;
import com.metamatrix.toolbox.ui.widget.table.EnhancedTableColumn;
import com.metamatrix.toolbox.ui.widget.table.EnhancedTableColumnModel;
import com.metamatrix.toolbox.ui.widget.table.TableComparator;

/**
This class is intended to be used everywhere within the application that a table needs to be displayed.
@since 2.0
@version 2.0
@author K. Goring
*/
public class PropertiedObjectArrayTable extends TreeNodeTableWidget {
    //############################################################################################################################
    //# Instance Variables                                                                                                       #
    //############################################################################################################################

    private static final String EMPTY_MESSAGE = "Folder empty or all files filtered out";
//    private static final String OBJECT_PROPERTY_DEF_NAME = "Object";
    private int propertyCount = 0;
    private PropertiedObjectEditor propertiedObjectEditor;
    private Collection propertiedObjects = Collections.EMPTY_LIST;
    private Object[] propertiedObjectsArray;
    private Collection propDefnsToShow = Collections.EMPTY_LIST;
    private List propertyDefinitions = Collections.EMPTY_LIST;
//    private DefaultTableCellRenderer defaultRenderer;
//    private String[] propDefNames;
    private String emptyMessage = EMPTY_MESSAGE;
	private DefaultTableCellRenderer renderer;

    //############################################################################################################################
    //# Constructors                                                                                                             #
    //############################################################################################################################

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /*
    @since 2.0
    */
    public PropertiedObjectArrayTable() {
        this(null);
    }


    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /*
    @since 2.0
    */
    public PropertiedObjectArrayTable(PropertiedObjectEditor editor) {
        propertiedObjectEditor = editor;
    }

   /**
    * addListSelectionListener accepts a Listener for events that occur to the
    * Table that are selection events.
    * @param lsl the Listener of the Table Events.
    */
    public void addListSelectionListener(ListSelectionListener lsl) {
        this.getSelectionModel().addListSelectionListener(lsl);
    }

   /**
    * sets the message that appears in the table when the selected entry is empty
    * @param msg the String to be displayed.
    */
    public void setEmptyMessage(String msg) {
        if (msg != null) {
        	this.emptyMessage = msg;
        }
    }

   /**
    * flags whether or not the table is empty
    * @param isEmpty the state of emptiness
    */
    public boolean isEmpty() {
        return (this.getRowCount() == 0);
    }


    /**
     * Initialize the PropertiedObjectArrayTable
     */
    protected void initializePropertiedObjectArrayEntryTable() {
        if (propDefnsToShow != null && propDefnsToShow.size() > 0) {
            propertyDefinitions = new ArrayList(propDefnsToShow.size());
            Iterator propIter = propDefnsToShow.iterator();
            while (propIter.hasNext()) {
                PropertyDefinition propDef = (PropertyDefinition)propIter.next();
                //weed out hidden PropertyDefinitions and PropertyDefinitions not to be shown
                if (!propDef.isHidden()) {
                    propertyDefinitions.add(propDef);
                }
            }
        } else if (propertiedObjects.size() > 0) {
            // Use property definitions from first propertied object
            propertyDefinitions =
                propertiedObjectEditor.getPropertyDefinitions((PropertiedObject)propertiedObjects.iterator().next());
            //weed out hidden PropertyDefinitions
            Iterator propIter = propertyDefinitions.iterator();
            ArrayList displayedPropertyDefinitions = new ArrayList(propertyDefinitions.size());
            while (propIter.hasNext()) {
                PropertyDefinition propDef = (PropertyDefinition)propIter.next();
                if (!propDef.isHidden()) {
                    displayedPropertyDefinitions.add(propDef);
                }
            }
            propertyDefinitions = displayedPropertyDefinitions;
        }
        propertyCount = propertyDefinitions.size();

        DefaultTableModel tableModel;
        if (propertiedObjects.size() > 0) {
        	tableModel = new DefaultTableModel(getData(), getPropertyDefNames());
        } else {
        	tableModel = new DefaultTableModel(getData(), getEmptyTableHeaderArray());
        }
        tableModel.setEditable(false);
        //populate the table
        setModel(tableModel);
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    protected TableComparator createDefaultComparator() {
        return DirectoryEntryTableComparator.getInstance();
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    protected DefaultTableCellRenderer getCellRenderer() {
        if (renderer == null) {
        	return new DefaultTableCellRenderer();
        }
        return renderer;
    }


    /**
     * Creates the data object used to populate the DirectoryEntry details table.
     */
    protected Object[][] getData() {
        //get the list of children for the selected DirectoryEntry and make it an array
        Object[] showingObjects = propertiedObjects.toArray();

        //construct the Object[][] to return sized by the number of directory entries and the directoryEntryView's
        //property count
        Object[][] data = new Object[showingObjects.length][propertyCount];
        if (showingObjects.length == 0 ) {
            Object[][] noData = new Object[0][1];
            return noData;
        }
        for(int i = 0; i< showingObjects.length; i++) {
            data[i] = getRowData((PropertiedObject)showingObjects[i]);
        }
        return data;
    }

    /**
     * Creates the data array for each row of the details table from the directoryEntryView's properties.  The first
     * element of the data array is the DirectoryElement itself.
     */
    protected Object[] getRowData(PropertiedObject node) {
    	boolean isDirectoryEntryTable = false;
    	//check if this is a DirectoryEntryTable.  In a DirectoryEntryTable, the first column displayed needs to be the actual object
    	//so file/foloder tests can be run against them for sorting purposes.
		if (this.getClass() == DirectoryEntryTable.class) {
			isDirectoryEntryTable = true;
		}	
        Object[] rowData = new Object[propertyCount];
        Iterator iter = propertyDefinitions.iterator();
        for (int i=0; i<propertyCount; i++) {
            if (i == 0 && isDirectoryEntryTable) {
            	//if a DirectoryEntryTable, the first column displayed is also the DirectoryEntry itself.
                rowData[i] = node;
                iter.next();
            } else {
                //all other elements of the array are the individual directoryEntryView properties
                rowData[i] = propertiedObjectEditor.getValue(node, (PropertyDefinition)iter.next());
            }
        }
        return rowData;
    }

    public Object getSelectedObject() {
        Object selectedObject = null;
        if (this.isShowing()) {
            int selectedRow = this.getSelectedRow();
            int modelRow = this.convertRowIndexToModel(selectedRow);

            selectedObject = propertiedObjectsArray[modelRow];
        }
        return selectedObject;
    }
    
    public Object getSelectedObject(int row) {
        Object selectedObject = null;
        if (this.isShowing()) {
            int selectedRow = row;
            int modelRow = this.convertRowIndexToModel(selectedRow);

            selectedObject = propertiedObjectsArray[modelRow];
        }
        return selectedObject;
    }


    /**
     *
     */
    public Collection getSelectedObjects() {
        int[] selectedRows = this.getSelectedRows();
        int[] selectedModelRows = new int[selectedRows.length];
        for (int row = 0; row < selectedRows.length; row++) {
            selectedModelRows[row] = convertRowIndexToModel(selectedRows[row]);
        }
        //for now, grab the first column, will have to add locating logic
        Collection selectedObjects = new ArrayList();
        for (int i=0; i<selectedRows.length; i++) {
            selectedObjects.add(propertiedObjectsArray[selectedModelRows[i]]);
        }
        return selectedObjects;
    }


    public void refresh() {
        ArrayList cellRenderers = new ArrayList(getColumnCount());
        ArrayList headerRenderers = new ArrayList(getColumnCount());
        int[] widths = new int[getColumnCount()];
        int[] maxs = new int[getColumnCount()];
        int[] mins = new int[getColumnCount()];
        int[] prefs = new int[getColumnCount()];
        int i = 0;
        Enumeration enumeration = getColumnModel().getColumns();
        while ( enumeration.hasMoreElements() ) {
            EnhancedTableColumn column = (EnhancedTableColumn) enumeration.nextElement();
            cellRenderers.add(column.getCellRenderer());
            headerRenderers.add(column.getHeaderRenderer());
            maxs[i] = column.getMaxWidth();
            mins[i] = column.getMinWidth();
            prefs[i] = column.getPreferredWidth();
            widths[i++] = column.getWidth();
        }

        DefaultTableModel tableModel = new DefaultTableModel(getData(), getPropertyDefNames());
        tableModel.setEditable(false);
        //populate the table
        setModel(tableModel);

        i = 0;
        enumeration = getColumnModel().getColumns();
        while ( enumeration.hasMoreElements() ) {
            EnhancedTableColumn column = (EnhancedTableColumn) enumeration.nextElement();
            column.setCellRenderer((TableCellRenderer) cellRenderers.get(i));
            column.setHeaderRenderer((TableCellRenderer) headerRenderers.get(i));
            column.setMaxWidth(maxs[i]);
            column.setMinWidth(mins[i]);
            column.setPreferredWidth(prefs[i]);
            column.setWidth(widths[i++]);
        }
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    Get the DirectoryEntryView's property's definition names to be displayed by the details table
    */
    protected String[] getPropertyDefNames() {
        String[] propDefNames;
        propDefNames = new String[propertyCount];
        Iterator iter = propertyDefinitions.iterator();
        for(int i=0; i<propertyCount; i++) {
            propDefNames[i] = iter.next().toString();
        }
        return propDefNames;
    }
    
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    Get the DirectoryEntryView's property's definition names to be displayed by the details table
    */
    protected String[] getEmptyTableHeaderArray() {
	    String[] emptyTableHeaderArray = new String[]{emptyMessage};
	    return emptyTableHeaderArray;
    }

    /**
     *
     * 
     */
    public void setPropertiedObjectEditor(PropertiedObjectEditor editor) {
        propertiedObjectEditor = editor;
    }

    /**
     *
     */
    public void setTableArray(Collection propertiedObjects) {
        setTableArray(propertiedObjects, null, propertiedObjectEditor);
    }
    /**
     *
     */

    public void setTableArray(Collection propertiedObjects, Collection propDefnsToShow) {
        setTableArray(propertiedObjects, propDefnsToShow, propertiedObjectEditor);
    }
    /**
     *
     */

    public void setTableArray(Collection propertiedObjects, PropertiedObjectEditor editor) {
        setTableArray(propertiedObjects, null, editor);
    }
    /**
     *
     */

    public void setTableArray(Collection propertiedObjects, Collection propDefnsToShow, PropertiedObjectEditor editor) {
        this.propertiedObjects = propertiedObjects;
        this.propertiedObjectsArray = propertiedObjects.toArray();
        this.propDefnsToShow = propDefnsToShow;
        this.propertiedObjectEditor = editor;
        initializePropertiedObjectArrayEntryTable();
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    Overridden to hide the column containing the PropertiedObjects and to set the renderer on the Name column (if it exists) to
    one that displays appropriate icons.
    @since 2.0
    */
    public void tableChanged(final TableModelEvent event) {
        super.tableChanged(event);
        final EnhancedTableColumnModel colModel = getEnhancedColumnModel();
        int count = colModel.getColumnCount();
        if (count == 0  ||  event.getType() != TableModelEvent.UPDATE
        				||  event.getColumn() >= 0
        				||  event.getFirstRow() >= 0) {
            return;
        }
        // Hide column containing PropertiedObject
        TableColumn col = colModel.getColumn(0);
        // Set renderer on Name column, if exists, to one that displays appropriate icons
        for (int ndx = 0;  ndx < count;  ++ndx) {
            col = colModel.getColumn(ndx);
            if (col.getIdentifier().equals("Name")) {
                col.setCellRenderer(getCellRenderer());
                break;
            }
        }
    }
}

