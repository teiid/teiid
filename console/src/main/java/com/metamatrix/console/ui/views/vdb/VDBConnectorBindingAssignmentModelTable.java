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

import java.awt.Color;
import java.awt.Component;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.util.Iterator;
import java.util.Map;
import java.util.Vector;

import javax.swing.BorderFactory;
import javax.swing.DefaultCellEditor;
import javax.swing.JButton;
import javax.swing.JCheckBox;
import javax.swing.JLabel;
import javax.swing.JScrollPane;
import javax.swing.JTable;
import javax.swing.border.Border;
import javax.swing.table.TableCellRenderer;

import com.metamatrix.console.ConsolePlugin;
import com.metamatrix.toolbox.ui.widget.ButtonWidget;
import com.metamatrix.toolbox.ui.widget.LabelWidget;
import com.metamatrix.toolbox.ui.widget.TableWidget;
import com.metamatrix.toolbox.ui.widget.table.DefaultTableModel;


/** 
 * @since 4.2
 */
public class VDBConnectorBindingAssignmentModelTable extends TableWidget {
    public final static int NUM_COLUMNS = 2;
    public final static String MODEL_NAME = ConsolePlugin.Util.getString(
            "VDBConnectorBindingAssignmentModelTable.ModelName"); //$NON-NLS-1$
    public final static int MODEL_NAME_COL_NUM = 0;
    public final static String CONNECTOR_BINDING = ConsolePlugin.Util.getString(
            "VDBConnectorBindingAssignmentModelTable.ConnectorBinding"); //$NON-NLS-1$
    public final static int CONNECTOR_BINDING_COL_NUM = 1;
    
    public final static int ROW_HEIGHT;

    public final static String SOURCE = ConsolePlugin.Util.getString(
            "VDBConnectorBindingAssignmentModelTable.sourceSingular"); //$NON-NLS-1$
    public final static String SOURCES = ConsolePlugin.Util.getString(
            "VDBConnectorBindingAssignmentModelTable.sourcePlural"); //$NON-NLS-1$
    public final static String EDIT = ConsolePlugin.Util.getString(
            "General.Edit..."); //$NON-NLS-1$                                                                   
        
    static {
        ROW_HEIGHT = (new ButtonWidget("Testing")).getPreferredSize().height;
    }
    
    public static String getBindingsText(VDBConnectorBindingNames bindingInfo) {
        String labelText = "";
        int numSources = bindingInfo.getBindings().length;
        if (bindingInfo.isMultiSource()) {
            String src;
            if (numSources == 1) {
                src = SOURCE;
            } else {
                src = SOURCES;
            }
            labelText = EDIT + ' ' + '(' + (new Integer(numSources)).toString() + ' ' +
                    src + ')';
        } else {
            if (numSources == 1) {
                labelText = bindingInfo.getBindings()[0].getBindingName();
            }
        }
        return labelText;
    }
    
    private ModelTableModel model = null;
    private JButton editorComponent;
    private BindingsColEditor editor = null;
    private MultiSourceModelBindingEditRequestHandler editRequestHandler;
    
    private JScrollPane containerScrollPane;
    
    public VDBConnectorBindingAssignmentModelTable(
            MultiSourceModelBindingEditRequestHandler handler) {
        super();
        this.editRequestHandler = handler;
        Vector colNames = new Vector();
        colNames.add(MODEL_NAME);
        colNames.add(CONNECTOR_BINDING);
        model = new ModelTableModel(colNames);
        setModel(model);
        setDefaultRenderer(VDBConnectorBindingNames.class, new BindingsColRenderer());
        editorComponent = new ButtonWidget();
        editorComponent.addActionListener(new ActionListener() {
            public void actionPerformed(ActionEvent ev) {
                editPressed(ev);
            }
        });
        editor = new BindingsColEditor(editorComponent);
        setDefaultEditor(VDBConnectorBindingNames.class, editor);
        setRowHeight(ROW_HEIGHT);
    }
    
    public void setContainerScrollPane(JScrollPane scrollPane) {
        containerScrollPane = scrollPane;
    }
    
    public Class getColumnClass(int colIndex) {
        Class cls = Object.class;
        if (colIndex == MODEL_NAME_COL_NUM) {
            cls = String.class;
        } else if (colIndex == CONNECTOR_BINDING_COL_NUM) {
            cls = VDBConnectorBindingNames.class;
        }
        return cls;
    }
    
    public void populate(VDBConnectorBindingNames[] rowData) {
        model.populate(rowData);
        sizeColumnsToFitViewport(containerScrollPane);
    }
    
    public void updateMultiSource(
            Map /*<String model name to Boolean multi-source selected>*/ multiSourceInfo) {
        VDBConnectorBindingNames[] rows = model.getRowData();
        Iterator it = multiSourceInfo.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry me = (Map.Entry)it.next();
            String modelName = (String)me.getKey();
            boolean multiSourceSelected = ((Boolean)me.getValue()).booleanValue();
            int rowIndex = model.indexForModel(modelName);
            if (rowIndex >= 0) {
                VDBConnectorBindingNames row = rows[rowIndex];
                if (multiSourceSelected) {
                    if (!row.isMultiSource()) {
                        row = row.copy();
                        row.setMultiSource(true);
                        reviseRow(row, rowIndex);
                    }
                } else {
                    if (row.isMultiSource()) {
                        row = row.copy();
                        row.setMultiSource(false);
                        ConnectorBindingNameAndUUID[] bindings = row.getBindings();
                        if (bindings.length > 1) {
                            /*If changing from multi-source to not multi-source, with more 
                              than one source already assigned, which can be done by backing 
                              up in the wizard, then we will discard all but the first source 
                              specified.*/
                            ConnectorBindingNameAndUUID[] newBindings = 
                                    new ConnectorBindingNameAndUUID[1];
                            newBindings[0] = bindings[0];
                            row.setBindings(newBindings);
                        }
                        reviseRow(row, rowIndex);
                    }
                }
            }
        }
    }
    
    public void reviseRow(VDBConnectorBindingNames rowData, int rowIndex) {
        model.reviseRow(rowData, rowIndex);
    }
    
    public void reviseRow(VDBConnectorBindingNames rowData) {
        model.reviseRow(rowData);
    }
    
    public boolean isCellEditable(int row, int col) {
        int modelRow = this.convertRowIndexToModel(row);
        return model.isCellEditable(modelRow, col);
    }
    
    private void editPressed(ActionEvent ev) {
        VDBConnectorBindingNames bindingsInfo = 
                (VDBConnectorBindingNames)editor.getCellEditorValue();
        editRequestHandler.editRequested(bindingsInfo);
    }
    
    public boolean isRowMultiSource(int rowIndex) {
        return model.isRowMultiSource(rowIndex);
    }
    
    public boolean isAssigned(int rowIndex) {
        return model.isAssigned(rowIndex);
    }
    public VDBConnectorBindingNames getObjectForRow(int rowIndex) {
        return model.getObjectForRow(rowIndex);
    }
}//end VDBConnectorBindingAssignmentModelTable




class ModelTableModel extends DefaultTableModel {
    private VDBConnectorBindingNames[] rowData = new VDBConnectorBindingNames[0];
    
    public ModelTableModel(Vector colNames) {
        super(colNames);
    }
    
    public void populate(VDBConnectorBindingNames[] rows) {
        depopulate();
        this.rowData = rows;
        for (int i = 0; i < rowData.length; i++) {
            Object[] rowArray = 
                    new Object[VDBConnectorBindingAssignmentModelTable.NUM_COLUMNS];
            rowArray[VDBConnectorBindingAssignmentModelTable.MODEL_NAME_COL_NUM] =
                    rowData[i].getModelName();
            rowArray[VDBConnectorBindingAssignmentModelTable.CONNECTOR_BINDING_COL_NUM] =
                    rowData[i];
            this.addRow(rowArray);
        }
    }
    
    private void depopulate() {
        int numRows = this.getRowCount();
        for (int i = numRows - 1; i >= 0; i--) {
            this.removeRow(i);
        }
        rowData = new VDBConnectorBindingNames[0];
    }
    
    public void reviseRow(VDBConnectorBindingNames row, int rowIndex) {
        if (rowIndex >= 0) {
            this.setValueAt(row, rowIndex, 
                    VDBConnectorBindingAssignmentModelTable.CONNECTOR_BINDING_COL_NUM);
            rowData[rowIndex] = row;
        }
    }
    
    public void reviseRow(VDBConnectorBindingNames row) {
        int rowIndex = indexForModel(row.getModelName());
        reviseRow(row, rowIndex);
    }
    
    public int indexForModel(String modelName) {
        int index = -1;
        int i = 0;
        while ((index < 0) && (i < rowData.length)) {
            if (modelName.equals(rowData[i].getModelName())) {
                index = i;
            } else {
                i++;
            }
        }
        return index;
    }
    
    public boolean isCellEditable(int row, int col) {
        boolean editable = false;
        if ((row >= 0) && (row < this.getRowCount())) {
            if (col == VDBConnectorBindingAssignmentModelTable.CONNECTOR_BINDING_COL_NUM) {
                if (rowData[row].isMultiSource()) {
                    editable = true;
                }
            }
        }
        return editable;
    }
    
    public boolean isRowMultiSource(int row) {
        boolean multiSource = false;
        if ((row >= 0) && (row < this.getRowCount())) {
            multiSource = rowData[row].isMultiSource();
        }
        return multiSource;
    }
    
    public boolean isAssigned(int row) {
        boolean assigned = false;
        if ((row >= 0) && (row < this.getRowCount())) {
            assigned = (rowData[row].getBindings().length > 0);
        }
        return assigned;
    }
    
    public VDBConnectorBindingNames getObjectForRow(int row) {
        VDBConnectorBindingNames result = null;
        if ((row >= 0) && (row < this.getRowCount())) {
            result = rowData[row];
        }
        return result;
    }
    
    public VDBConnectorBindingNames[] getRowData() {
        return rowData;
    }
}//end ModelTableModel




class BindingsColRenderer implements TableCellRenderer {
    private final static Color UNSELECTED_BUTTON_BACKGROUND = 
            (new ButtonWidget()).getBackground();
    
    private JLabel label;
    private JButton button;
    
    public BindingsColRenderer() {
        super();
        label = new LabelWidget();
        label.setOpaque(true);
        button = new ButtonWidget();
    }
    
    public Component getTableCellRendererComponent(JTable table, Object bindingsInfo,
            boolean isSelected, boolean hasFocus, int row, int col) {
        VDBConnectorBindingNames info = (VDBConnectorBindingNames)bindingsInfo;
        String labelText = VDBConnectorBindingAssignmentModelTable.getBindingsText(info);
        boolean editable = info.isMultiSource();
        Component component;
        if (editable) {
            Border border;
            Color background;
            if (isSelected) {
                border = BorderFactory.createMatteBorder(2, 5, 2, 5,
                        table.getSelectionBackground());
                background = table.getSelectionBackground();
            } else {
                border = BorderFactory.createMatteBorder(2, 5, 2, 5,
                        table.getBackground());
                background = UNSELECTED_BUTTON_BACKGROUND;
            }
            button.setBorder(border);
            button.setBackground(background);
            button.setText(labelText);
            component = button;
        } else {
            Color background;
            if (isSelected) {
                background = table.getSelectionBackground();
            } else {
                background = table.getBackground();
            }
            label.setBackground(background);
            label.setText(labelText);
            component = label;
        }
        return component;
    }
}//end BindingsColRenderer




class BindingsColEditor extends DefaultCellEditor {
    private VDBConnectorBindingNames bindingsInfo = null;
    
    public BindingsColEditor(JButton b) {
        super(new JCheckBox());  //unused required argument
        editorComponent = b;
        setClickCountToStart(1);
        b.addActionListener(new ActionListener() {
            public void actionPerformed(ActionEvent ev) {
                fireEditingStopped();
            }
        });
    }
    
    protected void fireEditingStopped() {
        super.fireEditingStopped();
    }
    
    public Component getTableCellEditorComponent(JTable table, Object value,
            boolean isSelected, int row, int col) {
        bindingsInfo = (VDBConnectorBindingNames)value;
        String labelText = VDBConnectorBindingAssignmentModelTable.getBindingsText(
                bindingsInfo);
        ((JButton)editorComponent).setText(labelText);
        return editorComponent;
    }
    
    public Object getCellEditorValue() {
        return bindingsInfo;
    }
}//end BindingsColEditor        
