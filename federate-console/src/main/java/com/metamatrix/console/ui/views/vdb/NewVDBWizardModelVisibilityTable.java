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

package com.metamatrix.console.ui.views.vdb;

import java.util.Vector;

import com.metamatrix.toolbox.ui.widget.TableWidget;
import com.metamatrix.toolbox.ui.widget.table.DefaultTableModel;

/**
 * NOTE-- despite the name of NewVDBWizardModelVisibilityTable, this table has been
 * expanded to include a column on Multiple Source, a boolean indicator as to whether or
 * not the model is a multiple-source model.  BWP 01/17/05
 */
public class NewVDBWizardModelVisibilityTable extends TableWidget {
    public final static int NUM_COLUMNS = 4;
    public final static int NAME_COLUMN_NUM = 0;
    public final static String NAME = "Name"; //$NON-NLS-1$
    public final static int TYPE_COLUMN_NUM = 1;
    public final static int VISIBILITY_COLUMN_NUM = 2;
    public final static int MULTIPLE_SOURCE_COLUMN_NUM = 3;
    
    ModelVisibilityInfo[] info;
    VisibilityTableModel model;

    public NewVDBWizardModelVisibilityTable(ModelVisibilityInfo[] inf) {
        super();
        info = inf;
        if (info == null) {
            info = new ModelVisibilityInfo[] {};
        }
        init();
    }

    public NewVDBWizardModelVisibilityTable() {
        this(null);
    }

    private void init() {
        model = new VisibilityTableModel(info);
        this.setModel(model);
        this.getColumnModel().setColumnMargin(8);
    }

    public void populateTable(ModelVisibilityInfo[] inf) {
        info = inf;
        model.populateModel(info);
    }

    public boolean isCellEditable(int row, int column) {
        boolean editable = false;
        if (column == VISIBILITY_COLUMN_NUM) {
            editable = true;
        } else if (column == MULTIPLE_SOURCE_COLUMN_NUM) {
            editable = (info[row].isMultipleSourceEligible() &&
                    info[row].isMultipleSourceFlagEditable());
        }
        return editable;
    }

    public ModelVisibilityInfo[] getUpdatedVisibilityInfo() {
        ModelVisibilityInfo[] updatedInfo = new ModelVisibilityInfo[info.length];
        for (int i = 0; i < updatedInfo.length; i++) {
            updatedInfo[i] = new ModelVisibilityInfo(info[i].getModelName(),
                    info[i].getModelType(),
                    model.publicCheckedForRow(i), info[i].isMultipleSourceEligible(),
                    info[i].isMultipleSourceFlagEditable(), 
                    model.multipleSourceCheckedForRow(i));
        }
        return updatedInfo;
    }

    public boolean publicCheckedForRow(int rowNum) {
        return model.publicCheckedForRow(rowNum);
    }
    
    public boolean anyPublic() {
        return model.anyPublic();
    }
    
    public boolean isMultiSourceEligible(int rowNum) {
        return info[rowNum].isMultipleSourceEligible();
    }
}//end NewVDBWizardModelVisibilityTable




class VisibilityTableModel extends DefaultTableModel {

    //NOTE-- must be in same order as constants above;
    private final static String[] COLUMN_HEADERS = {NewVDBWizardModelVisibilityTable.NAME,
            "Type", "Visible", "Multiple Source"};
    private final static Vector COLUMN_HEADERS_VECTOR;

    static {
        COLUMN_HEADERS_VECTOR = new Vector(COLUMN_HEADERS.length);
        for (int i = 0; i < COLUMN_HEADERS.length; i++) {
            COLUMN_HEADERS_VECTOR.add(COLUMN_HEADERS[i]);
        }
    }

    public VisibilityTableModel(ModelVisibilityInfo[] data) {
        super(COLUMN_HEADERS_VECTOR);
        populateModel(data);
    }

    public void populateModel(ModelVisibilityInfo[] data) {
        int numRows = this.getRowCount();
        for (int i = numRows - 1; i >= 0; i--) {
            this.removeRow(i);
        }
        for (int i = 0; i < data.length; i++) {
            Object[] rowData = new Object[NewVDBWizardModelVisibilityTable.NUM_COLUMNS];
            rowData[NewVDBWizardModelVisibilityTable.NAME_COLUMN_NUM] = data[i].getModelName();
            rowData[NewVDBWizardModelVisibilityTable.TYPE_COLUMN_NUM] = data[i].getModelType();
            rowData[NewVDBWizardModelVisibilityTable.VISIBILITY_COLUMN_NUM] = new Boolean(data[i].isVisible());
            rowData[NewVDBWizardModelVisibilityTable.MULTIPLE_SOURCE_COLUMN_NUM] =
                new Boolean(data[i].isMultipleSourcesSelected());
            this.addRow(rowData);
        }
    }

    public boolean publicCheckedForRow(int rowNum) {
        Boolean b = (Boolean)this.getValueAt(rowNum, 
                NewVDBWizardModelVisibilityTable.VISIBILITY_COLUMN_NUM);
        return b.booleanValue();
    }

    public boolean anyPublic() {
        boolean publicFound = false;
        int i = 0;
        int numRows = this.getRowCount();
        while ((!publicFound) && (i < numRows)) {
            publicFound = publicCheckedForRow(i);
            if (!publicFound) {
                i++;
            }
        }
        return publicFound;
    }

    public boolean multipleSourceCheckedForRow(int rowNum) {
        Boolean b = (Boolean)this.getValueAt(rowNum, 
                NewVDBWizardModelVisibilityTable.MULTIPLE_SOURCE_COLUMN_NUM);
        return b.booleanValue();
    }
    
    public Class getColumnClass(int index) {
        Class cls;
        switch (index) {
            case NewVDBWizardModelVisibilityTable.VISIBILITY_COLUMN_NUM:
            case NewVDBWizardModelVisibilityTable.MULTIPLE_SOURCE_COLUMN_NUM:
                cls = Boolean.class;
                break;
            default:
                cls = String.class;
                break;
        }
        return cls;
    }
}//end VisibilityTableModel
