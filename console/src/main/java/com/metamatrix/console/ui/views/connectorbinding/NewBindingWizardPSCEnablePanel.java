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
import java.awt.GridLayout;
import java.awt.Insets;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Vector;

import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.event.ListSelectionEvent;
import javax.swing.event.ListSelectionListener;
import javax.swing.event.TableModelEvent;
import javax.swing.event.TableModelListener;
import javax.swing.table.TableCellEditor;
import javax.swing.table.TableColumn;

import com.metamatrix.common.config.api.Configuration;
import com.metamatrix.common.config.api.ProductServiceConfig;
import com.metamatrix.common.log.LogManager;
import com.metamatrix.console.connections.ConnectionInfo;
import com.metamatrix.console.models.ConnectorManager;
import com.metamatrix.console.models.ModelManager;
import com.metamatrix.console.ui.util.BasicWizardSubpanelContainer;
import com.metamatrix.console.ui.util.WizardInterface;
import com.metamatrix.console.ui.views.deploy.util.DeployPkgUtils;
import com.metamatrix.console.ui.views.deploy.util.DeployTableSorter;
import com.metamatrix.console.util.ExceptionUtility;
import com.metamatrix.console.util.LogContexts;
import com.metamatrix.toolbox.ui.widget.CheckBox;
import com.metamatrix.toolbox.ui.widget.LabelWidget;
import com.metamatrix.toolbox.ui.widget.TableWidget;
import com.metamatrix.toolbox.ui.widget.TextFieldWidget;
import com.metamatrix.toolbox.ui.widget.table.DefaultTableModel;
import com.metamatrix.toolbox.ui.widget.table.EnhancedTableColumn;
import com.metamatrix.toolbox.ui.widget.table.EnhancedTableColumnModel;

// ===

public class NewBindingWizardPSCEnablePanel extends BasicWizardSubpanelContainer implements
                                                                                ActionListener,
                                                                                ListSelectionListener,
                                                                                TableModelListener {

    // /////////////////////////////////////////////////////////////////////////
    // CONSTANTS
    // /////////////////////////////////////////////////////////////////////////

    private static/* final */String[] SERVICE_HDRS;
    private static final int PSC_COL = 0;
    private static final int CONFIGURATION_COL = 1;
    private static final int ENABLED_COL = 2;

    private static final String NEXT_STARTUP_CONFIG = "Next Startup"; //$NON-NLS-1$

    public static/* final */SimpleDateFormat DATE_FORMATTER;

    // /////////////////////////////////////////////////////////////////////////
    // INITIALIZER
    // /////////////////////////////////////////////////////////////////////////

    static {
        SERVICE_HDRS = new String[3];
        SERVICE_HDRS[PSC_COL] = "PSC"; //$NON-NLS-1$
        SERVICE_HDRS[CONFIGURATION_COL] = "Configuration"; //$NON-NLS-1$
        SERVICE_HDRS[ENABLED_COL] = "Enabled"; //$NON-NLS-1$

        String pattern = DeployPkgUtils.getString("pfp.datepattern", true); //$NON-NLS-1$
        if (pattern == null) {
            pattern = "MMM dd, yyyy hh:mm:ss"; //$NON-NLS-1$
        }
        DATE_FORMATTER = new SimpleDateFormat(pattern);
    }

    // /////////////////////////////////////////////////////////////////////////
    // CONTROLS
    // /////////////////////////////////////////////////////////////////////////

    private TableWidget tblPSCs;

    // /////////////////////////////////////////////////////////////////////////
    // FIELDS
    // /////////////////////////////////////////////////////////////////////////

    private DefaultTableModel tmdlPSCs;

    // Next Startup PSC Map: Map PSC to enabled state
    private HashMap mapPSCtoEnabledForNextStartup = new HashMap();

    // PSC Name to Next Startup PSC Xref
    private HashMap mapNextStartupPSCXref = new HashMap();

    private int numRowsDifferentNextStartup = 0;
    private CheckBox chk; // the table cell editor component for the enabled col

    // private NewBindingWizardController controller = null;

    private LabelWidget lblConnectorName = new LabelWidget("Connector Type:"); //$NON-NLS-1$
    private TextFieldWidget txfConnectorName = new TextFieldWidget();
    private TextFieldWidget txfBindingName = new TextFieldWidget();
    private LabelWidget lblBindingName = new LabelWidget("Binding Name:"); //$NON-NLS-1$
    private JPanel pnlOuter = new JPanel();

    private JPanel pnlTable = new JPanel();

    private ConnectionInfo connection;

    public NewBindingWizardPSCEnablePanel(WizardInterface wizardInterface,
                                          ConnectionInfo connection) {
        super(wizardInterface);
        this.connection = connection;
        init();
    }

    private ConnectorManager getConnectorManager() {
        return ModelManager.getConnectorManager(connection);
    }

    public void setNewConnectorBindingInfo(String bindingName,
                                           String connectorTypeName) {
        txfBindingName.setText(bindingName);
        txfConnectorName.setText(connectorTypeName);
    }

    private void init() {
        createTablePanel();
        txfBindingName.setEditable(false);
        txfConnectorName.setEditable(false);
        Insets insDefault = new Insets(3, 3, 3, 3);

        pnlOuter.setLayout(new GridBagLayout());
        pnlOuter.add(lblBindingName, new GridBagConstraints(0, 0, 1, 1, 0.0, 0.0, GridBagConstraints.EAST,
                                                            GridBagConstraints.NONE, insDefault, 0, 0));
        pnlOuter.add(txfBindingName, new GridBagConstraints(1, 0, 1, 1, 1.0, 0.0, GridBagConstraints.WEST,
                                                            GridBagConstraints.HORIZONTAL, insDefault, 0, 0));

        pnlOuter.add(lblConnectorName, new GridBagConstraints(0, 1, 1, 1, 0.0, 0.0, GridBagConstraints.EAST,
                                                              GridBagConstraints.NONE, insDefault, 0, 0));
        pnlOuter.add(txfConnectorName, new GridBagConstraints(1, 1, 1, 1, 1.0, 0.0, GridBagConstraints.WEST,
                                                              GridBagConstraints.HORIZONTAL, insDefault, 0, 0));

        pnlOuter.add(pnlTable, new GridBagConstraints(0, 2, GridBagConstraints.REMAINDER, GridBagConstraints.REMAINDER, 1.0, 1.0,
                                                      GridBagConstraints.CENTER, GridBagConstraints.BOTH,
                                                      new Insets(12, 3, 3, 3), 0, 0));

        setMainContent(pnlOuter);
        setStepText(3, "Set Enabled State of New Binding in PSCs, and Create the Binding."); //$NON-NLS-1$
        populateTable();
    }

    private JPanel createTablePanel() {
        pnlTable = new JPanel();
        pnlTable.setLayout(new GridLayout(1, 1));
        tblPSCs = new TableWidget();

        tmdlPSCs = DeployPkgUtils.setup(tblPSCs, SERVICE_HDRS, 10, new int[] {
            ENABLED_COL
        });
        tblPSCs.getSelectionModel().addListSelectionListener(this);
        tblPSCs.setComparator(new DeployTableSorter());

        doTableSetup();

        tmdlPSCs.addTableModelListener(this);

        JScrollPane spnServices = new JScrollPane(tblPSCs);
        pnlTable.add(spnServices);

        return pnlTable;
    }

    private void populateTable() {
        loadPSCsForNextStartup();

        createPSCXrefs();

        tmdlPSCs.setNumRows(0);
        try {

            // Process Next Startup set
            if (mapPSCtoEnabledForNextStartup != null) {
                Iterator itPsc = mapPSCtoEnabledForNextStartup.keySet().iterator();

                // drive the process by walking the NextStartup hashmap
                while (itPsc.hasNext()) {
                    ProductServiceConfig psc = (ProductServiceConfig)itPsc.next();

                    Vector row = new Vector(SERVICE_HDRS.length);
                    row.setSize(SERVICE_HDRS.length);

                    row.setElementAt(psc, PSC_COL);

                    row.setElementAt(NEXT_STARTUP_CONFIG, CONFIGURATION_COL);

                    Boolean enabledForNextStartup = (Boolean)mapPSCtoEnabledForNextStartup.get(psc);

                    row.setElementAt(enabledForNextStartup, ENABLED_COL);

                    tmdlPSCs.addRow(row);
                }
            }
        } catch (Exception theException) {
            ExceptionUtility.showMessage("  ", //$NON-NLS-1$
                                         // getString("msg.configmgrproblem",
                                         // new Object[] {getClass(), "setDomainObject"}),
                                         theException);
            LogManager.logError(LogContexts.PSCDEPLOY, theException, getClass() + ":setDomainObject"); //$NON-NLS-1$
        }
    }

    private void doTableSetup() {
        // customize the table
        tblPSCs.sizeColumnsToFitData();

        // fix column for Next Startup
        EnhancedTableColumnModel etcmNextStartup = tblPSCs.getEnhancedColumnModel();
        TableColumn clmConnBindColumnNextStartup = etcmNextStartup.getColumn(ENABLED_COL);
        tblPSCs.sizeColumnToFitData((EnhancedTableColumn)clmConnBindColumnNextStartup);
        sortPSCTable();
    }

    private void sortPSCTable() {
        EnhancedTableColumnModel etcmPSCs = tblPSCs.getEnhancedColumnModel();
        TableColumn clmPSCColumn = etcmPSCs.getColumn(PSC_COL);
        etcmPSCs.setColumnSortedAscending((EnhancedTableColumn)clmPSCColumn, false);
    }

    private void loadPSCsForNextStartup() {
        mapPSCtoEnabledForNextStartup.clear();
        try {
            Configuration config = getNextStartupConfig();
            Collection colPsc = getConnectorManager().getAllConnectorsPSCsByConfig(config);

            if (colPsc != null) {
                Iterator itPsc = colPsc.iterator();
                while (itPsc.hasNext()) {
                    ProductServiceConfig psc = (ProductServiceConfig)itPsc.next();

                    Boolean enabled = null;
                    // if (ConnectorManager.isStandardConnectorPSCName(
                    // psc.getName())) {
                    // enabled = Boolean.TRUE;
                    // } else
                    if (psc.getServiceComponentDefnIDs().size() > 0) {
                        enabled = Boolean.TRUE;
                    } else {
                        enabled = Boolean.FALSE;
                    }
                    mapPSCtoEnabledForNextStartup.put(psc, enabled);
                }
            }
        } catch (Exception theException) {
            ExceptionUtility.showMessage("  ", //$NON-NLS-1$
                                         // getString("msg.configmgrproblem",
                                         // new Object[] {getClass(), "setDomainObject"}),
                                         theException);
            LogManager.logError(LogContexts.PSCDEPLOY, theException, getClass() + ":setDomainObject"); //$NON-NLS-1$
        }
    }

    private void createPSCXrefs() {
        // Next Startup
        Iterator itPscs = mapPSCtoEnabledForNextStartup.keySet().iterator();
        while (itPscs.hasNext()) {
            ProductServiceConfig psc = (ProductServiceConfig)itPscs.next();
            mapNextStartupPSCXref.put(psc.getName(), psc);
        }
    }

    public void actionPerformed(ActionEvent theEvent) {
    }

    public void tableChanged(TableModelEvent theEvent) {
        int iSelectedRow = theEvent.getFirstRow();
        String sConfigOfSelectedRow = getConfigTypeForRow(iSelectedRow);

        if (sConfigOfSelectedRow.equals(NEXT_STARTUP_CONFIG)) {
            int row = theEvent.getFirstRow();
            ProductServiceConfig psc = (ProductServiceConfig)tmdlPSCs.getValueAt(row, PSC_COL);

            // get the proper PSC for NextStartup
            ProductServiceConfig pscNS = (ProductServiceConfig)mapNextStartupPSCXref.get(psc.getName());

            Object saveEnabled = mapPSCtoEnabledForNextStartup.get(pscNS);

            if (!saveEnabled.equals(tmdlPSCs.getValueAt(row, ENABLED_COL))) {
                numRowsDifferentNextStartup++;
            } else {
                if (numRowsDifferentNextStartup > 0) {
                    numRowsDifferentNextStartup--;
                }
            }
            checkResetState();
        }
    }

    private String getConfigTypeForRow(int iRow) {
        String sConfigOfSelectedRow = (String)tmdlPSCs.getValueAt(iRow, CONFIGURATION_COL);
        return sConfigOfSelectedRow;
    }

    public void valueChanged(ListSelectionEvent theEvent) {
        // done one time to setup the checkbox action listener
        int row = tblPSCs.getSelectedRow();
        if (row != -1) {
            TableCellEditor editor = tblPSCs.getCellEditor(row, ENABLED_COL);
            chk = (CheckBox)editor.getTableCellEditorComponent(tblPSCs,
                                                               tblPSCs.getValueAt(row, ENABLED_COL),
                                                               true,
                                                               row,
                                                               ENABLED_COL);
            chk.addActionListener(this);
            tblPSCs.getSelectionModel().removeListSelectionListener(this);
        }
    }

    private void checkResetState() {
    }

    private Configuration getNextStartupConfig() {
        Configuration cfg = null;
        try {
            cfg = getConnectorManager().getNextStartupConfig();
        } catch (Exception e) {
            ExceptionUtility.showMessage("Failed retrieving the Next Startup Config", e); //$NON-NLS-1$
        }
        return cfg;
    }

    public ProductServiceConfig[] getEnabledConfigs() {
        int numRows = tblPSCs.getRowCount();
        java.util.List pscsList = new ArrayList(numRows);
        for (int i = 0; i < numRows; i++) {
            Boolean enabledBool = (Boolean)tblPSCs.getModel().getValueAt(i, ENABLED_COL);
            if (enabledBool.booleanValue()) {
                ProductServiceConfig psc = (ProductServiceConfig)tblPSCs.getModel().getValueAt(i, PSC_COL);
                pscsList.add(psc);
            }
        }
        ProductServiceConfig[] pscs = new ProductServiceConfig[pscsList.size()];
        Iterator it = pscsList.iterator();
        for (int i = 0; it.hasNext(); i++) {
            pscs[i] = (ProductServiceConfig)it.next();
        }
        return pscs;
    }
}
