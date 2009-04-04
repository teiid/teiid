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
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.Vector;

import javax.swing.BorderFactory;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JTextField;
import javax.swing.border.Border;

import com.metamatrix.common.log.LogManager;
import com.metamatrix.console.connections.ConnectionInfo;
import com.metamatrix.console.models.ModelManager;
import com.metamatrix.console.models.VdbManager;
import com.metamatrix.console.ui.views.DefaultConsoleTableComparator;
import com.metamatrix.console.util.ExceptionUtility;
import com.metamatrix.console.util.LogContexts;
import com.metamatrix.metadata.runtime.api.Model;
import com.metamatrix.metadata.runtime.api.VirtualDatabase;
import com.metamatrix.metadata.runtime.api.VirtualDatabaseID;
import com.metamatrix.toolbox.ui.widget.TableWidget;
import com.metamatrix.toolbox.ui.widget.table.EnhancedTableColumn;

public class VdbModelsPanel extends JPanel implements
                                          VdbDisplayer {

    BorderLayout borderLayout1 = new BorderLayout();
    JPanel pnlOuter = new JPanel();
    BorderLayout borderLayout2 = new BorderLayout();
    Border border1;
    JPanel pnlVdbName = new JPanel();
    FlowLayout flowLayout1 = new FlowLayout();
    JPanel pnlModelTable = new JPanel();
    JTextField txfVdbName = new JTextField();
    JLabel lblVdbName = new JLabel();
    JScrollPane jScrollPane1;
    VDBModelsPanelTableModel tableModel;
    TableWidget tblVdbModels;
    Border border2;
    com.metamatrix.toolbox.ui.widget.TitledBorder titledBorder1;
    Border border3;
    // JLabel lblVersion = new JLabel("Version:");
    JTextField txfVersion = new JTextField();
    VirtualDatabase vdbCurrent = null;
    ConnectionInfo connection = null;

    public VdbModelsPanel(ConnectionInfo connection) {
        super();
        this.connection = connection;
        try {
            init();
        } catch (Exception ex) {
        	LogManager.logError(LogContexts.INITIALIZATION, ex, ex.getMessage());
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

    public void refresh() {
        if (getVirtualDatabase() == null) {
            clear();
        } else {
            Collection colModels = null;
            // Map visMap = null;
            VirtualDatabaseID vdbID = (VirtualDatabaseID)getVirtualDatabase().getID();
            boolean continuing = true;
            try {
                colModels = getVdbManager().getVdbModels(vdbID);
                // visMap = runtimeMetadataAPI.getModelVisibilityLevels(vdbID);
            } catch (Exception e) {
                ExceptionUtility.showMessage("Failed retrieving models for a VDB", e);
                continuing = false;
            }
            if (continuing) {
                tableModel.populate(colModels);
                // tableModel.populate(colModels, visMap);
                tblVdbModels.sort();
                txfVdbName.setText(getVirtualDatabase().getName());
                // txfVersion.setText(vdbID.getVersion());
            }
        }
    }

    public void clear() {
        txfVdbName.setText("");
        txfVersion.setText("");
        tableModel.populate(null);
    }

    private void init() throws Exception {
        tableModel = new VDBModelsPanelTableModel();
        tblVdbModels = new TableWidget(tableModel);
        EnhancedTableColumn col = (EnhancedTableColumn)tblVdbModels.getColumn(VDBModelsPanelTableModel.MODEL_NAME);
        tblVdbModels.setColumnSortedAscending(col, false);
        // col = (EnhancedTableColumn)tblVdbModels.getColumn(
        // VDBModelsPanelTableModel.MODEL_VERSION);
        tblVdbModels.setColumnSortedAscending(col, true);
        tblVdbModels.setPreferredScrollableViewportSize(new Dimension(tblVdbModels.getPreferredSize().width,
                                                                      4 * tblVdbModels.getRowHeight()));
        tblVdbModels.setEditable(false);
        tblVdbModels.setSortable(true);
        tblVdbModels.sizeColumnsToFitData(100);
        tblVdbModels.setCellSelectionEnabled(false);
        tblVdbModels.setComparator(DefaultConsoleTableComparator.getInstance());

        border1 = BorderFactory.createEmptyBorder(5, 5, 5, 5);
        titledBorder1 = new com.metamatrix.toolbox.ui.widget.TitledBorder("");
        border3 = BorderFactory.createCompoundBorder(titledBorder1, BorderFactory.createEmptyBorder(10, 5, 5, 5));
        this.setLayout(borderLayout1);
        pnlOuter.setLayout(new GridBagLayout());
        pnlOuter.setBorder(border1);
        pnlVdbName.setLayout(flowLayout1);
        txfVdbName.setPreferredSize(new Dimension(263, 21));
        txfVdbName.setMinimumSize(txfVdbName.getPreferredSize());

        txfVdbName.setEditable(false);
        txfVdbName.setText("Sales VDB");
        lblVdbName.setText("VDB Name:");

        pnlModelTable.setLayout(new GridBagLayout());

        pnlModelTable.setBorder(border3);

        txfVersion.setPreferredSize(new Dimension(50, 21));
        txfVersion.setMinimumSize(txfVersion.getPreferredSize());
        txfVersion.setEditable(false);

        txfVersion.setText("17");

        pnlOuter.add(lblVdbName, new GridBagConstraints(0, 0, 1, 1, 0.0, 0.0, GridBagConstraints.EAST, GridBagConstraints.NONE,
                                                        new Insets(5, 5, 5, 5), 0, 0));

        pnlOuter.add(txfVdbName, new GridBagConstraints(1, 0, 1, 1, 0.0, 0.0, GridBagConstraints.WEST, GridBagConstraints.NONE,
                                                        new Insets(5, 5, 5, 5), 1, 0));

        // pnlOuter.add(lblVersion, new GridBagConstraints(2, 0, 1, 1, 0.0, 0.0,
        // GridBagConstraints.EAST, GridBagConstraints.NONE,
        // new Insets(5, 5, 5, 5), 0, 0));

        pnlOuter.add(txfVersion, new GridBagConstraints(3, 0, 1, 1, 0.0, 0.0, GridBagConstraints.WEST, GridBagConstraints.NONE,
                                                        new Insets(5, 5, 5, 5), 1, 0));

        this.add(pnlOuter, BorderLayout.CENTER);

        GridBagConstraints gridBagConstraints2a = new GridBagConstraints();

        gridBagConstraints2a.gridx = 0;
        gridBagConstraints2a.gridy = 1;
        gridBagConstraints2a.fill = GridBagConstraints.BOTH;
        gridBagConstraints2a.insets = new Insets(3, 3, 3, 3);
        gridBagConstraints2a.anchor = GridBagConstraints.WEST;
        gridBagConstraints2a.weightx = 1.0;
        gridBagConstraints2a.weighty = 1.0;
        gridBagConstraints2a.gridwidth = 4;
        pnlOuter.add(pnlModelTable, gridBagConstraints2a);

        jScrollPane1 = new JScrollPane(tblVdbModels);

        pnlModelTable.add(jScrollPane1, null);

        jScrollPane1.setViewportView(tblVdbModels);

        GridBagConstraints gridBagConstraints2 = new GridBagConstraints();
        gridBagConstraints2.gridx = 0;
        gridBagConstraints2.gridy = 1;
        gridBagConstraints2.fill = GridBagConstraints.BOTH;
        gridBagConstraints2.insets = new Insets(3, 3, 3, 3);
        gridBagConstraints2.anchor = GridBagConstraints.WEST;
        gridBagConstraints2.weightx = 1.0;
        gridBagConstraints2.weighty = 1.0;
        pnlModelTable.add(jScrollPane1, gridBagConstraints2);

        refresh();
    }

}// end VdbModelsPanel

class VDBModelsPanelTableModel extends com.metamatrix.toolbox.ui.widget.table.DefaultTableModel {

    public final static String MODEL_NAME = "Model Name";
    public final static String TYPE = "Type";
    public final static String PUBLIC = "Visible";
    public final static String MULTIPLE_SOURCE = "Multiple Source";

    // public final static String MODEL_VERSION = "Version";
    private final static int NUM_COLUMNS = 5;
    private final static int MODEL_NUM_COL_NUM = 0;
    // private final static int VERSION_COL_NUM = 1;
    // private final static int ID_COL_NUM = 1;
    private final static int TYPE_COL_NUM = 1;
    private final static int PUBLIC_COL_NUM = 2;
    private final static int MULTIPLE_SOURCE_COL_NUM = 3;

    // Must be in same order as constants above:
    // private final static String[] COLUMN_NAMES = {MODEL_NAME, MODEL_VERSION, "ID",
    private final static String[] COLUMN_NAMES = {
        MODEL_NAME, TYPE, PUBLIC, MULTIPLE_SOURCE
    };

    public VDBModelsPanelTableModel() {
        super(new Vector(Arrays.asList(COLUMN_NAMES)));
    }

    public void populate(Collection /* <Model> */models) {

        // Map /*<ModelID to Short (visibility level indicator)>*/ visMap) {
        depopulate();
        // if ((models != null) && (visMap != null)) {

        if (models != null) {

            Iterator it = models.iterator();
            while (it.hasNext()) {
                Model model = (Model)it.next();
                Object[] values = new Object[NUM_COLUMNS];
                values[MODEL_NUM_COL_NUM] = model.getName();
                // ModelID modelID = (ModelID)model.getID();
                // Integer versionInt = null;
                // try {
                // versionInt = new Integer(modelID.getVersion());
                // } catch (Exception ex) {
                // //Ignore
                // }
                // values[VERSION_COL_NUM] = versionInt;
                // values[ID_COL_NUM] = modelID.toString();
                String modelType = model.getModelTypeName();
                // String modelType;
                // if (model.containsPhysicalGroup()) {
                // modelType = "PhysicalModel";
                // } else {
                // modelType = "VirtualModel";
                // }
                values[TYPE_COL_NUM] = modelType;
                // Short visLevel = (Short)visMap.get(modelID);
                // short visValue = visLevel.shortValue();
                // boolean isPublic = (visValue ==
                // MetadataConstants.VISIBILITY_TYPES.PUBLIC_VISIBILITY);

                boolean isPublic = model.isVisible();
                values[PUBLIC_COL_NUM] = new Boolean(isPublic);
                boolean isMultipleSource = model.isMultiSourceBindingEnabled();
                values[MULTIPLE_SOURCE_COL_NUM] = new Boolean(isMultipleSource);
                this.addRow(values);
            }
        }
    }

    private void depopulate() {
        int numRows = this.getRowCount();
        for (int i = numRows - 1; i >= 0; i--) {
            this.removeRow(i);
        }
    }

    public Class getColumnClass(int colIndex) {
        Class cls;
        switch (colIndex) {
            case PUBLIC_COL_NUM:
            case MULTIPLE_SOURCE_COL_NUM:
                cls = Boolean.class;
                break;
            // case VERSION_COL_NUM:
            // cls = Integer.class;
            // break;
            default:
                cls = String.class;
                break;
        }
        return cls;
    }
}// end VDBModelsPanelTableModel
