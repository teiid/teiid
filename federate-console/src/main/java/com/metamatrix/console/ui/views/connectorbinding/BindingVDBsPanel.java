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

package com.metamatrix.console.ui.views.connectorbinding;

import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.Iterator;
import java.util.Vector;

import javax.swing.JScrollPane;

import com.metamatrix.common.config.api.ServiceComponentDefn;
import com.metamatrix.common.log.LogManager;
import com.metamatrix.console.connections.ConnectionInfo;
import com.metamatrix.console.models.ModelManager;
import com.metamatrix.console.models.VdbManager;
import com.metamatrix.console.ui.layout.BasePanel;
import com.metamatrix.console.util.ExceptionUtility;
import com.metamatrix.console.util.LogContexts;
import com.metamatrix.metadata.runtime.api.Model;
import com.metamatrix.metadata.runtime.api.VirtualDatabase;
import com.metamatrix.metadata.runtime.api.VirtualDatabaseID;
import com.metamatrix.toolbox.ui.widget.TableWidget;

public class BindingVDBsPanel extends BasePanel {

    public final static int VDB_COL_NUM = 0;
    public final static int VDB_VERSION_COL_NUM = 1;
    public final static int MODEL_COL_NUM = 2;
    public final static int MODEL_VERSION_COL_NUM = 3;
    public final static int BOUND_COL_NUM = 4;
    public final static int BOUND_BY_COL_NUM = 5;

    private TableWidget table;
    private BindingVDBsTableModel tableModel;
    private ServiceComponentDefn connectorBindingDefn;
    private ConnectionInfo connection;

    public BindingVDBsPanel(ConnectionInfo connection) {
        super();
        this.connection = connection;
        init();
    }

    private VdbManager getVdbManager() {
        return ModelManager.getVdbManager(connection);
    }

    private void init() {
        // NOTE-- columns must be in same order as indices above
        // "Bound" and "Bound by" info apparently unavailable. BWP 08/27/01
        // tableModel = new BindingVDBsTableModel(new Vector(Arrays.asList(
        // new String[] {"VDB", "VDB Version", "Model", "Model Version",
        // "Bound", "Bound by"})));
        tableModel = new BindingVDBsTableModel(new Vector(Arrays.asList(

        new String[] {
            "VDB", "VDB Version", "Model"}))); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$

        // new String[] {"VDB", "VDB Version", "Model", "Model Version"}))); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        // //$NON-NLS-4$
        table = new TableWidget(tableModel, true);

        table.setEditable(false);
        table.setSortable(true);

        JScrollPane tableSP = new JScrollPane(table);
        add(tableSP);
        GridBagLayout layout = new GridBagLayout();
        setLayout(layout);
        layout.setConstraints(tableSP, new GridBagConstraints(0, 0, 1, 1, 1.0, 1.0, GridBagConstraints.CENTER,
                                                              GridBagConstraints.BOTH, new Insets(5, 5, 5, 5), 0, 0));
    }

    public void populateTable() {

        boolean continuing = true;
        if (connectorBindingDefn == null) {
            tableModel.setNumRows(0);
        } else {
            BindingVDBInfo[] info = null;
            try {
                // info = getConnectorManager().getBindingVDBInfo(connectorBindingDefn);

                info = getBindingInfo(connectorBindingDefn);
            } catch (Exception ex) {
                ExceptionUtility.showMessage("Retrieve VDB info for Connector Binding", ex); //$NON-NLS-1$
                LogManager.logError(LogContexts.CONNECTOR_BINDINGS, ex, "Error retrieving VDB info for connector binding."); //$NON-NLS-1$
                continuing = false;
            }
            if (continuing) {
                tableModel.setNumRows(0);

                for (int i = 0; i < info.length; i++) {
                    Vector vec = new Vector(Arrays.asList(new Object[] {
                        null, null, null, null, null
                    }));
                    // null, null, null, null}));

                    vec.setElementAt(info[i].getVDB(), VDB_COL_NUM);
                    vec.setElementAt(new Integer(info[i].getVDBVersion()), VDB_VERSION_COL_NUM);
                    vec.setElementAt(info[i].getModel(), MODEL_COL_NUM);
                    // Integer modelVersion = info[i].getModelVersion();
                    // vec.setElementAt(modelVersion, MODEL_VERSION_COL_NUM);
                    // "Bound" and "Bound by" info apparently unavailable. BWP 08/27/01
                    // vec.setElementAt(info[i].getBound(), BOUND_COL_NUM);
                    // vec.setElementAt(info[i].getBoundBy(), BOUND_BY_COL_NUM);
                    tableModel.addRow(vec);
                }
            }
        }
    }

    private BindingVDBInfo[] getBindingInfo(ServiceComponentDefn scdBinding) {
        Collection colVdbs = null;
        int iCount = 0;

        try {

            colVdbs = getVdbManager().getVDBsForConnectorBinding(scdBinding.getRoutingUUID());
        } catch (Exception e) {
            ExceptionUtility.showMessage("Failed loading VBDs panel", e); //$NON-NLS-1$
        }

        Vector vModels = new Vector();
        Iterator itVdbs = colVdbs.iterator();

        while (itVdbs.hasNext()) {
            VirtualDatabase vdbTemp = (VirtualDatabase)itVdbs.next();

            Collection colModels = getModelsForVdb(vdbTemp);
            Iterator itModels = colModels.iterator();
            while (itModels.hasNext()) {
                iCount++;
                Model vdbmodelTemp = (Model)itModels.next();

                String sModelCBName = ""; //$NON-NLS-1$
                Collection names = vdbmodelTemp.getConnectorBindingNames();
                if ((names != null) && (names.size() > 0)) {

                    for (Iterator ix = names.iterator(); ix.hasNext();) {
                        sModelCBName = (String)ix.next();
                        if (sModelCBName != null) {
                            String ruuid = scdBinding.getRoutingUUID();
                            if (sModelCBName.equals(ruuid)) {

                                BindingVDBInfo bindinfo = new BindingVDBInfo(vdbTemp.getName(),
                                                                             Integer.parseInt(getVdbVersion(vdbTemp)),
                                                                             vdbmodelTemp.getName(), new Integer(0), new Date(),
                                                                             "" //$NON-NLS-1$
                                );

                                vModels.add(bindinfo);
                                break;
                            } // uuids match
                        } // modelcbname not null
                    } // iterate names
                } // has names
            } // end of WHILE models
        } // end of WHILE VDBs
        // Object[] toArray(Object[] a)
        BindingVDBInfo[] aryBindingInfo = new BindingVDBInfo[vModels.size()];

        vModels.toArray(aryBindingInfo);

        return aryBindingInfo;
    }

    private Collection getModelsForVdb(VirtualDatabase vdb) {
        Collection colModels = null;
        try {
            colModels = getVdbManager().getVdbModels((VirtualDatabaseID)vdb.getID());
        } catch (Exception e) {
            ExceptionUtility.showMessage("Failed retrieving models for a vdb", //$NON-NLS-1$
                                         e);
        }
        return colModels;
    }

    private String getVdbVersion(VirtualDatabase vdb) {
        VirtualDatabaseID vdbid = (VirtualDatabaseID)vdb.getID();
        return vdbid.getVersion();
    }

    // private String getModelVersion(Model model) {
    // ModelID mdlId = (ModelID)model.getID();
    // return mdlId.getVersion();
    // }

    public void setConnectorBinding(ServiceComponentDefn connectorBindingDefn) {
        this.connectorBindingDefn = connectorBindingDefn;
        populateTable();
    }

    public ServiceComponentDefn getConnectorBinding() {
        return connectorBindingDefn;
    }
}// end BindingVDBsPanel

class BindingVDBsTableModel extends com.metamatrix.toolbox.ui.widget.table.DefaultTableModel {

    public BindingVDBsTableModel(Vector cols) {
        super(cols, 0);
    }

    public Class getColumnClass(int index) {
        Class cls = null;
        switch (index) {
            case BindingVDBsPanel.VDB_COL_NUM:
                cls = super.getColumnClass(index);
                break;
            case BindingVDBsPanel.VDB_VERSION_COL_NUM:
                cls = Integer.class;
                break;
            case BindingVDBsPanel.MODEL_COL_NUM:
                cls = super.getColumnClass(index);
                break;
            // case BindingVDBsPanel.MODEL_VERSION_COL_NUM:
            // cls = Integer.class;
            // break;
            case BindingVDBsPanel.BOUND_COL_NUM:
                cls = Date.class;
                break;
            case BindingVDBsPanel.BOUND_BY_COL_NUM:
                cls = super.getColumnClass(index);
                break;
        }
        return cls;
    }
}// end BindingVDBsTableModel
