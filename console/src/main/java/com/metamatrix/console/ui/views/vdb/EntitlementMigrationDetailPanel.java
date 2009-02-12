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

import java.awt.FlowLayout;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.util.Arrays;
import java.util.Vector;

import javax.swing.AbstractButton;
import javax.swing.JButton;
import javax.swing.JPanel;
import javax.swing.JScrollPane;

import com.metamatrix.console.ui.views.entitlements.DataNodePermissions;
import com.metamatrix.toolbox.ui.widget.TableWidget;
import com.metamatrix.toolbox.ui.widget.table.DefaultTableModel;

public class EntitlementMigrationDetailPanel extends JPanel {
    public final static int NODE_COLUMN_NUM = 0;
    public final static int RESULT_COLUMN_NUM = 1;
    public final static int CREATE_COLUMN_NUM = 2;
    public final static int READ_COLUMN_NUM = 3;
    public final static int UPDATE_COLUMN_NUM = 4;
    public final static int DELETE_COLUMN_NUM = 5;

    private EntitlementMigrationDetailInfo[] details;
    private JButton saveButton = new JButton("Save to File"); //$NON-NLS-1$
    private JButton cancelButton;
    private TableWidget table;

    public EntitlementMigrationDetailPanel(EntitlementMigrationDetailInfo[] det,
            boolean showCancelButton) {
        super();
        details = det;
        init(showCancelButton);
    }

    private void init(boolean showCancelButton) {
        //NOTE-- column headers must be in order of NODE_COLUMN_NUM, etc., above
        Vector /*<String>*/ columns = new Vector(Arrays.asList(new String[]
                {"Node", "Result", "Create", "Read", "Update", "Delete"})); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$ //$NON-NLS-5$ //$NON-NLS-6$
        DetailTableModel tableModel = new DetailTableModel(columns);
        table = new TableWidget(tableModel);
        GridBagLayout layout = new GridBagLayout();
        setLayout(layout);
        JScrollPane tableSP = new JScrollPane(table);
        add(tableSP);
        layout.setConstraints(tableSP, new GridBagConstraints(0, 0, 1, 1, 1.0, 1.0,
                GridBagConstraints.CENTER, GridBagConstraints.BOTH,
                new Insets(0, 0, 0, 0), 0, 0));
        saveButton.addActionListener(new ActionListener() {
            public void actionPerformed(ActionEvent ev) {
                savePressed();
            }
        });
        layout.setConstraints(saveButton, new GridBagConstraints(0, 1, 1, 1,
                0.0, 0.0, GridBagConstraints.CENTER, GridBagConstraints.NONE,
                new Insets(5, 5, 5, 10), 0, 0));
        if (showCancelButton) {
            JPanel buttonsPanel = new JPanel();
            add(buttonsPanel);
            layout.setConstraints(buttonsPanel, new GridBagConstraints(0, 1, 1, 1,
                    0.0, 0.0, GridBagConstraints.CENTER, GridBagConstraints.NONE,
                    new Insets(0, 0, 0, 0), 0, 0));
            buttonsPanel.setLayout(new FlowLayout());
            buttonsPanel.add(saveButton);
            cancelButton = new JButton("Cancel"); //$NON-NLS-1$
            buttonsPanel.add(cancelButton);
        } else {
            add(saveButton);
            layout.setConstraints(saveButton, new GridBagConstraints(0, 1, 1, 1,
                    0.0, 0.0, GridBagConstraints.CENTER, GridBagConstraints.NONE,
                    new Insets(5, 5, 5, 5), 0, 0));
        }
        insertData();
    }

    public AbstractButton getCancelButton() {
        return cancelButton;
    }
    
    private void insertData() {
        for (int i = 0; i < details.length; i++) {
            Vector rowData = new Vector();
            //NOTE-- Adds must be in order by column num as defined above
            rowData.add(details[i].getNodeName());
            switch (details[i].getMigrationResult()) {
                case EntitlementMigrationDetailInfo.MATCHED:
                    rowData.add("Matched"); //$NON-NLS-1$
                    break;
                case EntitlementMigrationDetailInfo.DROPPED:
                    rowData.add("Dropped"); //$NON-NLS-1$
                    break;
                case EntitlementMigrationDetailInfo.NEW:
                    rowData.add("New"); //$NON-NLS-1$
                    break;
                default:
                    throw new RuntimeException("Illegal value of " + //$NON-NLS-1$
                            details[i].getMigrationResult() + " for migration result in EntitlementMigrationDetailPanel"); //$NON-NLS-1$
            }
            boolean create = details[i].getPermissions().hasCreate();
            rowData.add(new Boolean(create));
            boolean read = details[i].getPermissions().hasRead();
            rowData.add(new Boolean(read));
            boolean update = details[i].getPermissions().hasUpdate();
            rowData.add(new Boolean(update));
            boolean delete = details[i].getPermissions().hasDelete();
            rowData.add(new Boolean(delete));
            ((DefaultTableModel)table.getModel()).addRow(rowData);
        }
    }

    private void savePressed() {
        //TODO
    }

    public static EntitlementMigrationDetailInfo[] getMigrationData() {
        EntitlementMigrationDetailInfo[] info = new EntitlementMigrationDetailInfo[10];
        info[0] = new EntitlementMigrationDetailInfo(
                "TableA", EntitlementMigrationDetailInfo.MATCHED, //$NON-NLS-1$
                new DataNodePermissions(false, true, false, false));
        info[1] = new EntitlementMigrationDetailInfo(
                "TableA.column1", EntitlementMigrationDetailInfo.DROPPED, //$NON-NLS-1$
                new DataNodePermissions(false, false, false, false));
        info[2] = new EntitlementMigrationDetailInfo(
                "TableA.column2", EntitlementMigrationDetailInfo.DROPPED, //$NON-NLS-1$
                new DataNodePermissions(false, false, false, false));
        info[3] = new EntitlementMigrationDetailInfo(
                "TableA.column3", EntitlementMigrationDetailInfo.MATCHED, //$NON-NLS-1$
                new DataNodePermissions(true, true, true, true));
        info[4] = new EntitlementMigrationDetailInfo(
                "TableA.column4", EntitlementMigrationDetailInfo.MATCHED, //$NON-NLS-1$
                new DataNodePermissions(false, true, false, false));
        info[5] = new EntitlementMigrationDetailInfo(
                "TableA.column5", EntitlementMigrationDetailInfo.MATCHED, //$NON-NLS-1$
                new DataNodePermissions(false, true, false, false));
        info[6] = new EntitlementMigrationDetailInfo(
                "TableA.column6", EntitlementMigrationDetailInfo.MATCHED, //$NON-NLS-1$
                new DataNodePermissions(false, true, false, false));
        info[7] = new EntitlementMigrationDetailInfo(
                "TableA.column7", EntitlementMigrationDetailInfo.MATCHED, //$NON-NLS-1$
                new DataNodePermissions(false, true, false, false));
        info[8] = new EntitlementMigrationDetailInfo(
                "TableB.column1", EntitlementMigrationDetailInfo.NEW, //$NON-NLS-1$
                new DataNodePermissions(true, true, true, true));
        info[9] = new EntitlementMigrationDetailInfo(
                "TableB.column2", EntitlementMigrationDetailInfo.NEW, //$NON-NLS-1$
                new DataNodePermissions(true, true, true, true));
        return info;
    }
}//end EntitlementMigrationDetailPanel

class DetailTableModel extends DefaultTableModel {
    public DetailTableModel(Vector columns) {
        super(columns, 0);
    }

    public Class getColumnClass(int index) {
        if ((index == EntitlementMigrationDetailPanel.NODE_COLUMN_NUM) ||
                (index == EntitlementMigrationDetailPanel.RESULT_COLUMN_NUM)) {
            return String.class;
        }
        return Boolean.class;
    }
}//end DetailTableModel

