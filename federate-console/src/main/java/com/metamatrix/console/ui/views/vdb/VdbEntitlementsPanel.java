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

import java.awt.BorderLayout;
import java.awt.Dimension;
import java.awt.FlowLayout;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.Vector;

import javax.swing.BorderFactory;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JTextField;
import javax.swing.border.Border;

import com.metamatrix.console.connections.ConnectionInfo;
import com.metamatrix.console.models.ModelManager;
import com.metamatrix.console.models.VdbManager;
import com.metamatrix.console.ui.ViewManager;
import com.metamatrix.console.ui.views.DefaultConsoleTableComparator;
import com.metamatrix.console.util.ExceptionUtility;
import com.metamatrix.metadata.runtime.api.VirtualDatabase;
import com.metamatrix.metadata.runtime.api.VirtualDatabaseID;
import com.metamatrix.platform.security.api.AuthorizationPolicyID;
import com.metamatrix.toolbox.ui.widget.ButtonWidget;
import com.metamatrix.toolbox.ui.widget.TableWidget;
import com.metamatrix.toolbox.ui.widget.TitledBorder;
import com.metamatrix.toolbox.ui.widget.table.DefaultTableModel;
import com.metamatrix.toolbox.ui.widget.table.EnhancedTableColumn;

public class VdbEntitlementsPanel extends JPanel implements
                                                VdbDisplayer {

    public final static String ENTITLEMENT = "Role Name";

    BorderLayout borderLayout1 = new BorderLayout();
    JPanel pnlOuter = new JPanel();
    BorderLayout borderLayout2 = new BorderLayout();
    Border border1;
    JPanel pnlVdbName = new JPanel();
    FlowLayout flowLayout1 = new FlowLayout();
    JPanel pnlEntitlementsTable = new JPanel();
    JTextField txfVdbName = new JTextField();
    JLabel lblVdbName = new JLabel();
    JScrollPane jScrollPane1 = new JScrollPane();
    TableWidget tblVdbEntitlements = new TableWidget();
    Border border2;
    TitledBorder titledBorder1;
    Border border3;
    JLabel lblVersion = new JLabel("Version:");
    JTextField txfVersion = new JTextField();
    JFrame frParent;
    ButtonWidget btnExportRoles = new ButtonWidget(" Export Roles... ");
    ButtonWidget btnImportRoles = new ButtonWidget(" Import Roles... ");
    VirtualDatabase vdbCurrent = null;
    private ConnectionInfo connection = null;

    String[] aryColNames = {
        ENTITLEMENT, "Description"
    };

    public VdbEntitlementsPanel(ConnectionInfo connection) {
        super();
        this.connection = connection;
        try {
            this.frParent = ViewManager.getMainFrame();
            jbInit();
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    private VdbManager getVdbManager() {
        return ModelManager.getVdbManager(connection);
    }

    public void setVirtualDatabase(VirtualDatabase vdb) {
        if (vdbCurrent != vdb) {
            vdbCurrent = vdb;
            refresh();
        }
    }

    public VirtualDatabase getVirtualDatabase() {
        return vdbCurrent;
    }

    public ButtonWidget getExportRolesButton() {
        return btnExportRoles;
    }
    
    public ButtonWidget getImportRolesButton() {
        return btnImportRoles;
    }

    public void runImportRolesDialog() {
        String vdbName = "Import Roles for Virtual Database " + getVirtualDatabase().getName();
        VdbEditConnBindDlg editDialog = new VdbEditConnBindDlg(
        /* TODO: getThisParent(), */frParent, vdbName, (VirtualDatabaseID)getVirtualDatabase().getID(), connection);
        editDialog.pack();
        VdbMainPanel.setLocationOn(editDialog);
        editDialog.setModal(true);
        editDialog.show();
        refresh();
    }

    public void refresh() {
        if (getVirtualDatabase() == null) {
            clear();
            return;
        }
        Collection colEntitlements = null;
        try {
            VirtualDatabaseID id = (VirtualDatabaseID)getVirtualDatabase().getID();
            colEntitlements = getVdbManager().getVdbEntitlements(id);
        } catch (Exception e) {
            ExceptionUtility.showMessage("Failed retrieving Roles for a vdb", e);
        }
        depopulateTableModel();
        repopulateTableModel(colEntitlements);
        txfVdbName.setText(getVirtualDatabase().getName());
        VirtualDatabaseID vdbid = (VirtualDatabaseID)getVirtualDatabase().getID();
        txfVersion.setText(vdbid.getVersion());
    }

    private void depopulateTableModel() {
        DefaultTableModel tmdl = (DefaultTableModel)tblVdbEntitlements.getModel();
        int numRows = tmdl.getRowCount();
        for (int i = numRows - 1; i >= 0; i--) {
            tmdl.removeRow(i);
        }
    }

    private void repopulateTableModel(Collection colEntitlements) {
        DefaultTableModel tmdl = (DefaultTableModel)tblVdbEntitlements.getModel();
        AuthorizationPolicyID entEntitlementIDTemp;
        Iterator itEntIDs = colEntitlements.iterator();
        while (itEntIDs.hasNext()) {
            Vector vEntRow = new Vector();
            entEntitlementIDTemp = (AuthorizationPolicyID)itEntIDs.next();
            vEntRow.add(entEntitlementIDTemp.getDisplayName());
            vEntRow.add(entEntitlementIDTemp.getDescription());
            tmdl.addRow(vEntRow.toArray());
        }
        tblVdbEntitlements.sort();
    }
    
    public boolean hasRoles() {
    	return (tblVdbEntitlements.getRowCount()>0);
    }

    public void clear() {
        txfVersion.setText("");
        txfVdbName.setText("");
        depopulateTableModel();
    }

    private void jbInit() throws Exception {
        border1 = BorderFactory.createEmptyBorder(5, 5, 5, 5);
        titledBorder1 = new TitledBorder("Models");
        border3 = BorderFactory.createCompoundBorder(new TitledBorder(""), BorderFactory.createEmptyBorder(10, 5, 5, 5));
        this.setLayout(borderLayout1);
        pnlOuter.setLayout(new GridBagLayout());
        pnlOuter.setBorder(border1);
        pnlVdbName.setLayout(flowLayout1);
        txfVdbName.setPreferredSize(new Dimension(263, 21));
        txfVdbName.setMinimumSize(txfVdbName.getPreferredSize());
        txfVdbName.setEditable(false);
        txfVdbName.setText("Sales VDB");
        lblVdbName.setText("VDB Name:");
        pnlEntitlementsTable.setBorder(border3);
        pnlEntitlementsTable.setLayout(new java.awt.GridBagLayout());
        txfVersion.setPreferredSize(new Dimension(50, 21));
        txfVersion.setMinimumSize(txfVersion.getPreferredSize());
        txfVersion.setEditable(false);
        txfVersion.setText("17");
        pnlOuter.add(lblVdbName, new GridBagConstraints(0, 0, 1, 1, 0.0, 0.0, GridBagConstraints.EAST, GridBagConstraints.NONE,
                                                        new Insets(5, 5, 5, 5), 0, 0));

        pnlOuter.add(txfVdbName, new GridBagConstraints(1, 0, 1, 1, 0.0, 0.0, GridBagConstraints.WEST, GridBagConstraints.NONE,
                                                        new Insets(5, 5, 5, 5), 0, 0));

        pnlOuter.add(lblVersion, new GridBagConstraints(2, 0, 1, 1, 0.0, 0.0, GridBagConstraints.EAST, GridBagConstraints.NONE,
                                                        new Insets(5, 5, 5, 5), 0, 0));

        pnlOuter.add(txfVersion, new GridBagConstraints(3, 0, 1, 1, 0.0, 0.0, GridBagConstraints.WEST, GridBagConstraints.NONE,
                                                        new Insets(5, 5, 5, 5), 0, 0));

        JPanel buttonsPanel = new JPanel();
        buttonsPanel.add(btnImportRoles);
        buttonsPanel.add(btnExportRoles);
        
        pnlOuter.add(buttonsPanel, new GridBagConstraints(4, 0, 1, 1, 0.0, 0.0, GridBagConstraints.EAST, GridBagConstraints.NONE,
                new Insets(5, 5, 5, 5), 0, 0));

        DefaultTableModel tmdl = new DefaultTableModel(new Vector(Arrays.asList(aryColNames)));
        tblVdbEntitlements.setModel(tmdl);
        tblVdbEntitlements.setPreferredScrollableViewportSize(new Dimension(tblVdbEntitlements.getPreferredSize().width,
                                                                            2 * tblVdbEntitlements.getRowHeight()));
        tblVdbEntitlements.setEditable(false);
        tblVdbEntitlements.setSortable(true);
        tblVdbEntitlements.setComparator(DefaultConsoleTableComparator.getInstance());
        tblVdbEntitlements.sizeColumnsToFitData(100);
        tblVdbEntitlements.setCellSelectionEnabled(false);
        EnhancedTableColumn col = (EnhancedTableColumn)tblVdbEntitlements.getColumn(ENTITLEMENT);
        tblVdbEntitlements.setColumnSortedAscending(col, false);

        this.add(pnlOuter, BorderLayout.CENTER);

        GridBagConstraints gridBagConstraints29 = new GridBagConstraints();
        gridBagConstraints29.gridx = 0;
        gridBagConstraints29.gridy = 0;
        gridBagConstraints29.insets = new java.awt.Insets(3, 3, 3, 3);
        gridBagConstraints29.anchor = java.awt.GridBagConstraints.WEST;

        GridBagConstraints gridBagConstraints2a = new GridBagConstraints();
        gridBagConstraints2a.gridx = 0;
        gridBagConstraints2a.gridy = 1;
        gridBagConstraints2a.fill = java.awt.GridBagConstraints.BOTH;
        gridBagConstraints2a.insets = new java.awt.Insets(3, 3, 3, 3);
        gridBagConstraints2a.anchor = java.awt.GridBagConstraints.WEST;
        gridBagConstraints2a.weightx = 1.0;
        gridBagConstraints2a.weighty = 1.0;
        gridBagConstraints2a.gridwidth = 5;

        pnlOuter.add(pnlEntitlementsTable, gridBagConstraints2a);

        jScrollPane1.setViewportView(tblVdbEntitlements);

        GridBagConstraints gridBagConstraints2 = new GridBagConstraints();
        gridBagConstraints2.gridx = 0;
        gridBagConstraints2.gridy = 1;
        gridBagConstraints2.fill = java.awt.GridBagConstraints.BOTH;
        gridBagConstraints2.insets = new java.awt.Insets(3, 3, 3, 3);
        gridBagConstraints2.anchor = java.awt.GridBagConstraints.WEST;
        gridBagConstraints2.weightx = 1.0;
        gridBagConstraints2.weighty = 1.0;
        pnlEntitlementsTable.add(jScrollPane1, gridBagConstraints2);
        refresh();
    }
}
