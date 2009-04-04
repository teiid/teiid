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

import java.awt.BorderLayout;
import java.awt.Dimension;
import java.awt.FlowLayout;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Vector;

import javax.swing.BorderFactory;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JTextField;
import javax.swing.border.Border;

import com.metamatrix.common.log.LogManager;
import com.metamatrix.console.connections.ConnectionInfo;
import com.metamatrix.console.models.ConnectorManager;
import com.metamatrix.console.models.ModelManager;
import com.metamatrix.console.models.VdbManager;
import com.metamatrix.console.ui.ViewManager;
import com.metamatrix.console.ui.views.DefaultConsoleTableComparator;
import com.metamatrix.console.ui.views.deploy.event.ConfigurationChangeEvent;
import com.metamatrix.console.ui.views.deploy.event.ConfigurationChangeListener;
import com.metamatrix.console.util.ExceptionUtility;
import com.metamatrix.console.util.LogContexts;
import com.metamatrix.core.util.StringUtil;
import com.metamatrix.metadata.runtime.api.Model;
import com.metamatrix.metadata.runtime.api.VirtualDatabase;
import com.metamatrix.metadata.runtime.api.VirtualDatabaseID;
import com.metamatrix.toolbox.ui.widget.ButtonWidget;
import com.metamatrix.toolbox.ui.widget.TableWidget;
import com.metamatrix.toolbox.ui.widget.TitledBorder;
import com.metamatrix.toolbox.ui.widget.table.DefaultTableModel;
import com.metamatrix.toolbox.ui.widget.table.EnhancedTableColumn;

public class VdbConnBindPanel extends JPanel implements
                                            VdbDisplayer,
                                            ConfigurationChangeListener {

    public final static String MODEL_NAME = "Model Name";
    public final static String MODEL_TYPE = "Type";
    public final static String CONNECTOR_BINDING = "Connector Binding";

    BorderLayout borderLayout1 = new BorderLayout();
    JPanel pnlOuter = new JPanel();
    BorderLayout borderLayout2 = new BorderLayout();
    Border border1;
    JPanel pnlVdbName = new JPanel();
    FlowLayout flowLayout1 = new FlowLayout();
    JPanel pnlModelTable = new JPanel();
    JTextField txfVdbName = new JTextField();
    JLabel lblVdbName = new JLabel();
    JScrollPane jScrollPane1 = new JScrollPane();
    TableWidget tblConnBndsModels = new TableWidget();
    JFrame frParent;
    Border border2;
    TitledBorder titledBorder1;
    Border border3;
    ButtonWidget btnEdit = new ButtonWidget(" Edit ");
    JLabel lblVersion = new JLabel("Version:");
    JTextField txfVersion = new JTextField();
    VirtualDatabase vdbCurrent = null;
    private ConnectionInfo connection = null;

    private String[] aryColNames = {
        MODEL_NAME, MODEL_TYPE,
        // private String[] aryColNames = {MODEL_NAME, MODEL_VERSION,
        CONNECTOR_BINDING
    };
    private HashMap hmConnBindUUIDToName = null;
    private Collection colModels = null;

    public VdbConnBindPanel(ConnectionInfo connection) {
        super();
        this.connection = connection;
        try {
            this.frParent = ViewManager.getMainFrame();
            jbInit();
        } catch (Exception ex) {
        	LogManager.logError(LogContexts.INITIALIZATION, ex, ex.getMessage());
        }
    }

    private VdbManager getVdbManager() {
        return ModelManager.getVdbManager(connection);
    }

    private ConnectorManager getConnectorManager() {
        return ModelManager.getConnectorManager(connection);
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

    public ButtonWidget getEditButton() {
        return btnEdit;
    }

    public void refresh() {
        if (getVirtualDatabase() == null) {
            clear();
            return;
        }
        depopulateTableModel();
        try {
            colModels = new ArrayList();
            VirtualDatabase vdb = getVirtualDatabase();
            VirtualDatabaseID vID = (VirtualDatabaseID)vdb.getID();
            Collection mdls = getVdbManager().getVdbModels(vID);
            // filter the models that dont require a binding
            Iterator it = mdls.iterator();
            while (it.hasNext()) {
                Model mdlTemp = (Model)it.next();
                // add only models that require a binding
                if (mdlTemp.requireConnectorBinding()) {
                    colModels.add(mdlTemp);

                }
            }

        } catch (Exception e) {
            ExceptionUtility.showMessage("Failed retrieving models for a vdb", e);
        }
        repopulateTableModel(colModels);

        txfVdbName.setText(getVirtualDatabase().getName());
        VirtualDatabaseID vdbid = (VirtualDatabaseID)getVirtualDatabase().getID();

        txfVersion.setText(vdbid.getVersion());
    }

    public void clear() {
        txfVdbName.setText("");
        txfVersion.setText("");
        depopulateTableModel();
    }

    private void repopulateTableModel(Collection colModels) {
        DefaultTableModel tmdl = (DefaultTableModel)tblConnBndsModels.getModel();
        int iCount = 0;
        Model vdbmodelTemp;
        try {
            hmConnBindUUIDToName = getConnectorManager().getUUIDConnectorBindingsMap(true);
        } catch (Exception e) {
            ExceptionUtility.showMessage("Failed to retrieve the ConnBind UUID Map", e);
        }
        String sRealConnBindName = "";
        Iterator itModels = colModels.iterator();
        Map /* <UUID to name> */uuidMap = getConnBindUUIDNameXref();

        while (itModels.hasNext()) {
            iCount++;
            vdbmodelTemp = (Model)itModels.next();
            Collection /* <String> */bindingNames = vdbmodelTemp.getConnectorBindingNames();
            Iterator it = bindingNames.iterator();
            while (it.hasNext()) {
                Vector vModelRow = new Vector();
                vModelRow.add(vdbmodelTemp.getName());
                vModelRow.add(vdbmodelTemp.getModelTypeName());
                String bindingName = (String)it.next();
                sRealConnBindName = (String)uuidMap.get(bindingName);
                if (sRealConnBindName == null) {
                    sRealConnBindName = StringUtil.Constants.EMPTY_STRING;
                }
                vModelRow.add(sRealConnBindName);
                tmdl.addRow(vModelRow.toArray());
            }
        }
        tblConnBndsModels.sort();
    }

    private void depopulateTableModel() {
        DefaultTableModel tmdl = (DefaultTableModel)tblConnBndsModels.getModel();
        int numRows = tmdl.getRowCount();
        for (int i = numRows - 1; i >= 0; i--) {
            tmdl.removeRow(i);
        }
    }

    private HashMap getConnBindUUIDNameXref() {
        try {
            hmConnBindUUIDToName = getConnectorManager().getUUIDConnectorBindingsMap(true);
        } catch (Exception e) {
            ExceptionUtility.showMessage("Failed to retrieve the ConnBind UUID Map", e);
        }
        return hmConnBindUUIDToName;
    }

    public void runEditConnBindDialog() {
        String vdbName = "Edit Bindings for Virtual Database " + getVirtualDatabase().getName();
        VdbEditConnBindDlg editDialog = new VdbEditConnBindDlg(
        /* TODO: getThisParent(), */frParent, vdbName, (VirtualDatabaseID)getVirtualDatabase().getID(), connection);
        editDialog.pack();
        VdbMainPanel.setLocationOn(editDialog);
        editDialog.setModal(true);
        editDialog.show();
        refresh();
    }

    private void jbInit() throws Exception {
        border1 = BorderFactory.createEmptyBorder(5, 5, 5, 5);
        titledBorder1 = new TitledBorder("Models");
        border3 = BorderFactory.createCompoundBorder(new TitledBorder(""), BorderFactory.createEmptyBorder(10, 5, 5, 5));
        this.setLayout(borderLayout1);
        pnlOuter.setLayout(new GridBagLayout());
        pnlOuter.setBorder(border1);
        pnlVdbName.setLayout(flowLayout1);
        txfVdbName.setPreferredSize(new Dimension(163, 21));
        txfVdbName.setEditable(false);
        txfVdbName.setMinimumSize(txfVdbName.getPreferredSize());
        txfVdbName.setText("");
        lblVdbName.setText("VDB Name:");
        txfVersion.setPreferredSize(new Dimension(50, 21));
        txfVersion.setEditable(false);
        txfVersion.setMinimumSize(txfVersion.getPreferredSize());
        txfVersion.setText("17");

        pnlOuter.add(lblVdbName, new GridBagConstraints(0, 0, 1, 1, 0.0, 0.0, GridBagConstraints.EAST, GridBagConstraints.NONE,
                                                        new Insets(5, 5, 5, 5), 0, 0));

        pnlOuter.add(txfVdbName, new GridBagConstraints(1, 0, 1, 1, 0.0, 0.0, GridBagConstraints.WEST, GridBagConstraints.NONE,
                                                        new Insets(5, 5, 5, 5), 0, 0));

        pnlOuter.add(lblVersion, new GridBagConstraints(2, 0, 1, 1, 0.0, 0.0, GridBagConstraints.EAST, GridBagConstraints.NONE,
                                                        new Insets(5, 5, 5, 5), 0, 0));

        pnlOuter.add(txfVersion, new GridBagConstraints(3, 0, 1, 1, 0.0, 0.0, GridBagConstraints.WEST, GridBagConstraints.NONE,
                                                        new Insets(5, 5, 5, 5), 0, 0));

        pnlOuter.add(btnEdit, new GridBagConstraints(4, 0, 1, 1, 0.0, 0.0, GridBagConstraints.EAST, GridBagConstraints.NONE,
                                                     new Insets(5, 5, 5, 5), 0, 0));

        pnlModelTable.setLayout(new java.awt.GridLayout(1, 1));

        pnlModelTable.setBorder(border3);
        this.add(pnlOuter, BorderLayout.CENTER);
        GridBagConstraints gridBagConstraints2a = new GridBagConstraints();
        gridBagConstraints2a.gridx = 0;
        gridBagConstraints2a.gridy = 1;
        gridBagConstraints2a.fill = java.awt.GridBagConstraints.BOTH;
        gridBagConstraints2a.insets = new java.awt.Insets(3, 3, 3, 3);
        gridBagConstraints2a.anchor = java.awt.GridBagConstraints.WEST;
        gridBagConstraints2a.weightx = 1.0;
        gridBagConstraints2a.weighty = 1.0;
        gridBagConstraints2a.gridwidth = 5;
        pnlOuter.add(pnlModelTable, gridBagConstraints2a);

        jScrollPane1.setViewportView(tblConnBndsModels);
        pnlModelTable.add(jScrollPane1);

        DefaultTableModel tmdl = new DefaultTableModel(new Vector(Arrays.asList(aryColNames)));
        tblConnBndsModels.setModel(tmdl);
        tblConnBndsModels.setPreferredScrollableViewportSize(new Dimension(tblConnBndsModels.getPreferredSize().width,
                                                                           4 * tblConnBndsModels.getRowHeight()));
        tblConnBndsModels.setSortable(true);
        tblConnBndsModels.setComparator(DefaultConsoleTableComparator.getInstance());
        tblConnBndsModels.setEditable(false);
        tblConnBndsModels.sizeColumnsToFitData(100);
        tblConnBndsModels.setCellSelectionEnabled(false);
        EnhancedTableColumn col = (EnhancedTableColumn)tblConnBndsModels.getColumn(MODEL_NAME);
        tblConnBndsModels.setColumnSortedAscending(col, false);
        col = (EnhancedTableColumn)tblConnBndsModels.getColumn(CONNECTOR_BINDING);
        tblConnBndsModels.setColumnSortedAscending(col, true);
        refresh();
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.metamatrix.console.ui.views.deploy.event.ConfigurationChangeListener#configurationChanged(com.metamatrix.console.ui.views.deploy.event.ConfigurationChangeEvent)
     */
    public void configurationChanged(ConfigurationChangeEvent theEvent) {
        if (theEvent.getType() == ConfigurationChangeEvent.REFRESH_END) {
            this.refresh();
        }

    }

}
