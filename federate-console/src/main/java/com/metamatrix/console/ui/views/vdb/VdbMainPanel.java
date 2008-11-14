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

import java.awt.Component;
import java.awt.Dimension;
import java.awt.FlowLayout;
import java.awt.Font;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.GridLayout;
import java.awt.Insets;
import java.awt.Point;
import java.awt.Toolkit;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Vector;

import javax.swing.AbstractButton;
import javax.swing.Action;
import javax.swing.ButtonGroup;
import javax.swing.JButton;
import javax.swing.JDialog;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JPopupMenu;
import javax.swing.JScrollPane;
import javax.swing.JSplitPane;
import javax.swing.JTable;
import javax.swing.ListSelectionModel;
import javax.swing.event.ChangeEvent;
import javax.swing.event.ChangeListener;
import javax.swing.event.ListSelectionEvent;
import javax.swing.event.ListSelectionListener;

import com.metamatrix.common.util.PropertiesUtils;
import com.metamatrix.console.connections.ConnectionInfo;
import com.metamatrix.console.models.ModelManager;
import com.metamatrix.console.models.VdbManager;
import com.metamatrix.console.notification.DataEntitlementChangeNotification;
import com.metamatrix.console.notification.RuntimeUpdateNotification;
import com.metamatrix.console.security.UserCapabilities;
import com.metamatrix.console.ui.ViewManager;
import com.metamatrix.console.ui.layout.BasePanel;
import com.metamatrix.console.ui.layout.ConsoleMainFrame;
import com.metamatrix.console.ui.layout.MenuEntry;
import com.metamatrix.console.ui.layout.WorkspaceController;
import com.metamatrix.console.ui.layout.WorkspacePanel;
import com.metamatrix.console.ui.util.AbstractPanelAction;
import com.metamatrix.console.ui.views.DefaultConsoleTableComparator;
import com.metamatrix.console.util.DialogUtility;
import com.metamatrix.console.util.ExceptionUtility;
import com.metamatrix.console.util.Refreshable;
import com.metamatrix.console.util.StaticUtilities;
import com.metamatrix.core.vdb.VDBStatus;
import com.metamatrix.metadata.runtime.api.VirtualDatabase;
import com.metamatrix.metadata.runtime.api.VirtualDatabaseID;
import com.metamatrix.toolbox.ui.widget.ButtonWidget;
import com.metamatrix.toolbox.ui.widget.Splitter;
import com.metamatrix.toolbox.ui.widget.TableWidget;
import com.metamatrix.toolbox.ui.widget.TitledBorder;
import com.metamatrix.toolbox.ui.widget.menu.DefaultPopupMenuFactory;
import com.metamatrix.toolbox.ui.widget.table.EnhancedTableColumn;

public class VdbMainPanel extends BasePanel implements
                                           ChangeListener,
                                           WorkspacePanel,
                                           Refreshable {

    private boolean bIsStale = false;

    private static final String VDB_MANAGEMENT_TITLE = "Virtual Databases"; //$NON-NLS-1$

    //private static final String REDEPLOY_VDB_TITLE = "Redeploy"; //$NON-NLS-1$
    private static final String IMPORT_VDB_TITLE = "Import VDB..."; //$NON-NLS-1$
    private static final String EXPORT_VDB_TITLE = "Export VDB..."; //$NON-NLS-1$
    private static final String EDIT_CONN_BIND_TITLE = "Edit Bindings..."; //$NON-NLS-1$
    private static final String CHANGE_VDB_DEFAULT_STATUS_TITLE = "Change VDB Default Status..."; //$NON-NLS-1$
    private static final String CHANGE_VDB_STATUS_TITLE = "Change VDB Status..."; //$NON-NLS-1$
    private static final String CHANGE_VDB_STATUS_ROOT = "Make VDB "; //$NON-NLS-1$
    private static final String DELETE_VDB_TITLE = "Delete VDB"; //$NON-NLS-1$
    private static final String EXPORT_ROLES_TITLE = "Export Roles..."; //$NON-NLS-1$
    private static final String IMPORT_ROLES_TITLE = "Import Roles..."; //$NON-NLS-1$

    private static final String ACTIVE = "Active..."; //$NON-NLS-1$
    private static final String ACTIVE_DEFAULT = "Active (Default)..."; //$NON-NLS-1$
    private static final String INACTIVE = "Inactive..."; //$NON-NLS-1$

    private static final String DETAILS_TAB_TITLE = "Details"; //$NON-NLS-1$
    private static final String MODELS_TAB_TITLE = "Models Summary"; //$NON-NLS-1$
    private static final String TREE_TAB_TITLE = "Metadata Tree"; //$NON-NLS-1$
    private static final String CONNBIND_TAB_TITLE = "Connector Bindings"; //$NON-NLS-1$
    private static final String ENTITLEMENTS_TAB_TITLE = "Roles"; //$NON-NLS-1$
    private static final String MATERIALIZATION_TAB_TITLE = "Materialization"; //$NON-NLS-1$
    
    private static final String ACTIVE_DEFAULT_DISPLAY_LABEL = "Active (Default)"; //$NON-NLS-1$

    private ConnectionInfo connection;

    private PanelAction actionRedeployVDB;
    private PanelAction actionImportVDB;
    private PanelAction actionExportVDB;
    private PanelAction actionEditConnBind;
    private PanelAction actionChangeStatus;
    private PanelAction actionChangeDefaultStatus;
    private PanelAction actionDeleteVdb;
    private PanelAction actionExportRoles;
    private PanelAction actionImportRoles;
    
    private ArrayList arylActions = new ArrayList();
    private ArrayList arylPopupActions = new ArrayList();

    JFrame frParentFrame;
    private Collection colVdbs = null;
    private Collection colDisplayedVdbs = null;
    private VirtualDatabase vdbSelected = null;

    public static final int NUMERIC_FIELD = 10111;
    public static final int ALPHA_FIELD = 11;

    private javax.swing.JSplitPane splitMain;
    private javax.swing.JPanel pnlTop;
    private javax.swing.JPanel pnlFilter;
    private javax.swing.JLabel lblName;
    private javax.swing.JTextField txfName;
    private javax.swing.JPanel pnlStatus;
    // private javax.swing.JCheckBox chkAll;
    // private javax.swing.JSeparator sepStatus;
    private javax.swing.JCheckBox chkActive;
    private javax.swing.JCheckBox chkActiveDefault;
    private javax.swing.JCheckBox chkInactive;
    private javax.swing.JCheckBox chkIncomplete;
    private javax.swing.JCheckBox chkDeleted;
    private javax.swing.JPanel pnlVersion;
    private javax.swing.JRadioButton rdbAllVersions;
    private javax.swing.JRadioButton rdbLatestVersion;
    private ButtonGroup bgrpVersionGroup;
    private javax.swing.JPanel pnlFilterOps;
    private javax.swing.JPanel pnlFilterOpsSizer;
    private ButtonWidget btnApply;
    private ButtonWidget btnReset;
    private ButtonWidget btnHideFilter;
    private ButtonWidget btnShowFilter;
    private javax.swing.JScrollPane spnVdb;
    private TableWidget tblVdb;
    private com.metamatrix.toolbox.ui.widget.table.DefaultTableModel tableModel;

    private VdbDetailPanel pnlDetails;

    private VdbConnBindPanel pnlBindings;
    private VdbEntitlementsPanel pnlEntitlements;
    private javax.swing.JTabbedPane tpnDetails;

    private ButtonWidget btnRedeployVDB;
    private ButtonWidget btnImportVDB;
    private ButtonWidget btnExportVDB;
    // private ButtonWidget btnChangeStatus;
    private javax.swing.JPanel pnlNoFilter;
    private GridBagConstraints gbcFilterPanel;

    private boolean bInactiveBaseState = true;
    private boolean bIncompleteBaseState = true;
    private boolean bActiveBaseState = true;
    private boolean bActiveDefaultBaseState = true;
    private boolean bDeletedBaseState = true;

    private boolean bAllVersionsBaseState = true;
    private boolean bLatestVersionsBaseState = false;

    private String sNameFilterBaseState = "*"; //$NON-NLS-1$

    private ListSelectionModel rowSM = null;
    private UserCapabilities ucapsCapabilities = null;
    private boolean bCanModify = false;
    private boolean programmaticChange = false;

    /** Creates new form FilterPanel */
    public VdbMainPanel(ConnectionInfo conn) {
        super();
        this.connection = conn;
        initComponents();
        establishActionArray();

        // nothing selected, so:
        updateDetailForTableDeselection();
    }

    private VdbManager getVdbManager() {
        return ModelManager.getVdbManager(connection);
    }

    /**
     * This method is called from within the constructor to initialize the form. WARNING: Do NOT modify this code. The content of
     * this method is always regenerated by the FormEditor.
     */
    private void initComponents() {
        establishUserCapabilities();
        createActions();

        setLayout(new java.awt.GridLayout(2, 1, 0, 0));

        pnlTop = new javax.swing.JPanel();
        spnVdb = new javax.swing.JScrollPane();
        tableModel = new VDBTableModel();
        tblVdb = new TableWidget(tableModel);
        tblVdb.setAutoResizeMode(JTable.AUTO_RESIZE_OFF);
        tblVdb.setColumnSelectionAllowed(false);
        tblVdb.setEditable(false);
        tblVdb.setSortable(true);
        tblVdb.sizeColumnsToFitData(100);
        tblVdb.setPopupMenuFactory(new TableMenuFactory(tblVdb, this));
        EnhancedTableColumn nameColumn = (EnhancedTableColumn)tblVdb.getColumn(VDBTableModel.VDB_NAME_HDR);
        tblVdb.setColumnSortedAscending(nameColumn, false);
        EnhancedTableColumn versionColumn = (EnhancedTableColumn)tblVdb.getColumn(VDBTableModel.VDB_VERSION_HDR);
        tblVdb.setColumnSortedAscending(versionColumn, true);
        tblVdb.setComparator(DefaultConsoleTableComparator.getInstance());
        setTableListening();
        javax.swing.JPanel pnlBottom = new javax.swing.JPanel();
        tpnDetails = new javax.swing.JTabbedPane();
        pnlDetails = new VdbDetailPanel(connection, bCanModify);
        javax.swing.JPanel pnlModels = new VdbModelsPanel(connection);
        JPanel pnlTree = new VDBMetadataTreePanel(connection);
        pnlBindings = new VdbConnBindPanel(connection);
        pnlEntitlements = new VdbEntitlementsPanel(connection);
        javax.swing.JPanel pnlMaterialization = new MaterializationPanel(connection);
        javax.swing.JPanel pnlOps = new javax.swing.JPanel();
        btnRedeployVDB = new ButtonWidget();
        setup(MenuEntry.ACTION_MENUITEM, btnRedeployVDB, actionRedeployVDB);
        actionRedeployVDB.setEnabled(false);
        btnImportVDB = new ButtonWidget();
        setup(MenuEntry.ACTION_MENUITEM, btnImportVDB, actionImportVDB);
        actionImportVDB.setEnabled(bCanModify);
        btnExportVDB = new ButtonWidget();
        setup(MenuEntry.ACTION_MENUITEM, btnExportVDB, actionExportVDB);
        actionExportVDB.setEnabled(false);
        // btnChangeStatus = new ButtonWidget();
        // no setup, button not used
        setLayout(new java.awt.GridBagLayout());
        java.awt.GridBagConstraints gridBagConstraints1;

        pnlTop.setLayout(new java.awt.GridBagLayout());
        pnlTop.setBorder(new javax.swing.border.EmptyBorder(new java.awt.Insets(3, 3, 3, 3)));
        java.awt.GridBagConstraints gridBagConstraints2;

        gbcFilterPanel = new java.awt.GridBagConstraints();
        gbcFilterPanel.insets = new java.awt.Insets(3, 3, 3, 3);
        gbcFilterPanel.anchor = java.awt.GridBagConstraints.EAST;

        pnlTop.add(getNoFilterPanel(), gbcFilterPanel);
        pnlTop.setMinimumSize(new Dimension(0, 0));

        pnlBottom.setMinimumSize(new Dimension(0, 0));
        pnlBottom.setBorder(new javax.swing.border.EmptyBorder(new java.awt.Insets(3, 3, 3, 3)));

        spnVdb.setViewportView(tblVdb);

        gridBagConstraints2 = new java.awt.GridBagConstraints();
        gridBagConstraints2.gridx = 0;
        gridBagConstraints2.gridy = 1;
        gridBagConstraints2.fill = java.awt.GridBagConstraints.BOTH;
        gridBagConstraints2.insets = new java.awt.Insets(3, 3, 3, 3);
        gridBagConstraints2.anchor = java.awt.GridBagConstraints.WEST;
        gridBagConstraints2.weightx = 1.0;
        gridBagConstraints2.weighty = 1.0;
        pnlTop.add(spnVdb, gridBagConstraints2);

        pnlBottom.setLayout(new java.awt.GridLayout(1, 1));
        tpnDetails.addTab(DETAILS_TAB_TITLE, pnlDetails);
        tpnDetails.addTab(MODELS_TAB_TITLE, pnlModels);
        tpnDetails.addTab(TREE_TAB_TITLE, pnlTree);
        tpnDetails.addTab(CONNBIND_TAB_TITLE, pnlBindings);
        tpnDetails.addTab(ENTITLEMENTS_TAB_TITLE, pnlEntitlements);
        tpnDetails.addTab(MATERIALIZATION_TAB_TITLE, pnlMaterialization);
        tpnDetails.setSelectedIndex(0);
        pnlBottom.add(tpnDetails);
        splitMain = new Splitter(JSplitPane.VERTICAL_SPLIT, true, pnlTop, pnlBottom);
        splitMain.setOneTouchExpandable(true);

        gridBagConstraints1 = new java.awt.GridBagConstraints();
        gridBagConstraints1.fill = java.awt.GridBagConstraints.BOTH;
        gridBagConstraints1.insets = new java.awt.Insets(3, 3, 3, 3);
        gridBagConstraints1.weightx = 1.0;
        gridBagConstraints1.weighty = 1.0;
        add(splitMain, gridBagConstraints1);

        javax.swing.JPanel mbrpanel = new javax.swing.JPanel();
        TitledBorder tBorder = new TitledBorder("MetaBase Repository"); //$NON-NLS-1$
        tBorder.setTitleJustification(TitledBorder.LEFT);
        tBorder.setTitleFont(tBorder.getTitleFont().deriveFont(Font.BOLD));
        mbrpanel.setBorder(tBorder);

        mbrpanel.setLayout(new FlowLayout());
        mbrpanel.add(btnRedeployVDB);

        javax.swing.JPanel filepanel = new javax.swing.JPanel();

        tBorder = new TitledBorder("File"); //$NON-NLS-1$
        tBorder.setTitleJustification(TitledBorder.LEFT);
        tBorder.setTitleFont(tBorder.getTitleFont().deriveFont(Font.BOLD));
        filepanel.setBorder(tBorder);
        filepanel.setLayout(new FlowLayout());

        filepanel.add(btnImportVDB);
        filepanel.add(btnExportVDB);

        pnlOps.setLayout(new java.awt.GridLayout(1, 2, 3, 0));

        pnlOps.add(mbrpanel);
        pnlOps.add(filepanel);

        gridBagConstraints1 = new java.awt.GridBagConstraints();
        gridBagConstraints1.gridx = 0;
        gridBagConstraints1.gridy = 1;
        gridBagConstraints1.insets = new java.awt.Insets(3, 3, 3, 3);
        add(pnlOps, gridBagConstraints1);

        tpnDetails.addChangeListener(this);

        populateTable(false, null);
    }

    private void establishUserCapabilities() {
        // ucapsCapabilities
        try {
            ucapsCapabilities = UserCapabilities.getInstance();
            bCanModify = ucapsCapabilities.canModifyVDBs(connection);
        } catch (Exception ex) {
            // Cannot occur
        }
    }

    public void createActions() {
        actionImportVDB = new PanelAction(PanelAction.IMPORT_VDB);
        actionImportVDB.putValue(Action.NAME, IMPORT_VDB_TITLE);

        actionExportVDB = new PanelAction(PanelAction.EXPORT_VDB);
        actionExportVDB.putValue(Action.NAME, EXPORT_VDB_TITLE);

        actionEditConnBind = new PanelAction(PanelAction.EDIT_CONN_BIND);
        actionEditConnBind.putValue(Action.NAME, EDIT_CONN_BIND_TITLE);

        actionChangeStatus = new PanelAction(PanelAction.CHANGE_STATUS);
        actionChangeStatus.putValue(Action.NAME, CHANGE_VDB_STATUS_TITLE);

        actionChangeDefaultStatus = new PanelAction(PanelAction.CHANGE_DEFAULT_STATUS);
        actionChangeDefaultStatus.putValue(Action.NAME, CHANGE_VDB_DEFAULT_STATUS_TITLE);

        actionDeleteVdb = new PanelAction(PanelAction.DELETE_VDB);
        actionDeleteVdb.putValue(Action.NAME, DELETE_VDB_TITLE);
        
        actionExportRoles = new PanelAction(PanelAction.EXPORT_ROLES);
        actionExportRoles.putValue(Action.NAME, EXPORT_ROLES_TITLE);
        
        actionImportRoles = new PanelAction(PanelAction.IMPORT_ROLES);
        actionImportRoles.putValue(Action.NAME, IMPORT_ROLES_TITLE);
    }

    public void establishActionArray() {
        setup(MenuEntry.ACTION_MENUITEM, pnlBindings.getEditButton(), actionEditConnBind);
        actionEditConnBind.setEnabled(false);

        setup(MenuEntry.ACTION_MENUITEM, pnlEntitlements.getImportRolesButton(), actionImportRoles);
        actionImportRoles.setEnabled(false);
        
        setup(MenuEntry.ACTION_MENUITEM, pnlEntitlements.getExportRolesButton(), actionExportRoles);
        actionExportRoles.setEnabled(false);
        
        addSeparatorToActionList();
        addActionToList(MenuEntry.ACTION_MENUITEM, actionChangeStatus);
        addActionToList(MenuEntry.ACTION_MENUITEM, actionChangeDefaultStatus);
        addActionToList(MenuEntry.ACTION_MENUITEM, actionDeleteVdb);

        // load popup action array
        arylPopupActions.add(actionRedeployVDB);
        arylPopupActions.add(actionImportVDB);
        arylPopupActions.add(actionExportVDB);
        arylPopupActions.add(actionEditConnBind);
        arylPopupActions.add(actionImportRoles);
        arylPopupActions.add(actionExportRoles);
        arylPopupActions.add(actionChangeStatus);
        arylPopupActions.add(actionChangeDefaultStatus);
        arylPopupActions.add(actionDeleteVdb);
    }

    private void hideFilter() {
        gbcFilterPanel = new java.awt.GridBagConstraints();
        gbcFilterPanel.insets = new java.awt.Insets(3, 3, 3, 3);
        gbcFilterPanel.anchor = java.awt.GridBagConstraints.EAST;

        // 1. remove pnlFilter
        pnlTop.remove(getFilterPanel());

        // 2. add pnlNoFilter
        pnlTop.add(getNoFilterPanel(), gbcFilterPanel);
        forceRepaint();

        // Adjust the divider location back
        int iFilterHeight = getFilterPanel().getHeight();
        if (iFilterHeight == 0) {
            iFilterHeight = 126;
        }
        int iNoFilterHeight = getNoFilterPanel().getHeight();
        int iCurrentLocation = splitMain.getDividerLocation();

        int iOrigLocation = iCurrentLocation - (iFilterHeight - iNoFilterHeight);
        splitMain.setDividerLocation(iOrigLocation);
    }

    private void showFilter() {
        gbcFilterPanel = new java.awt.GridBagConstraints();
        gbcFilterPanel.insets = new java.awt.Insets(3, 3, 3, 3);

        // 1. remove pnlNoFilter
        pnlTop.remove(getNoFilterPanel());

        // 2. add pnlFilter
        pnlTop.add(getFilterPanel(), gbcFilterPanel);

        forceRepaint();

        // Adjust the divider location
        int iFilterHeight = getFilterPanel().getHeight();
        if (iFilterHeight == 0) {
            iFilterHeight = 126;
        }

        int iNoFilterHeight = getNoFilterPanel().getHeight();
        int iOrigLocation = splitMain.getDividerLocation();
        splitMain.setDividerLocation(iOrigLocation + (iFilterHeight - iNoFilterHeight));
    }

    private void forceRepaint() {
        // splitMain
        int iWiggleIncrement = 10;
        int iOrigLocation = splitMain.getDividerLocation();
        splitMain.setDividerLocation(iOrigLocation + iWiggleIncrement);
        splitMain.setDividerLocation(iOrigLocation);
        ConsoleMainFrame.getInstance().repaintNeeded();
    }

    private void editConnectorBindings() {
        pnlBindings.runEditConnBindDialog();

        // will it work if we run it from here?
        editConnectorBindingsFinishProcess();
    }

    public void editConnectorBindingsFinishProcess() {
        setIsStale(true);
        refresh();
    }

    
    private void exportRoles() {
        VirtualDatabase vdbToExport = vdbSelected;
        VDBRolesExporter exporter = new VDBRolesExporter(vdbToExport, connection);
        exporter.go();
    }

    private void importRoles() {
    	VirtualDatabase vdbForRolesImport = vdbSelected;
        boolean continuing = true;
        ImportVdbRolesWizardDlg importRolesWizardDlg = null;
        try {

        	importRolesWizardDlg = new ImportVdbRolesWizardDlg(getThisParent(), vdbForRolesImport, connection);
        } catch (Exception ex) {
            continuing = false;
        }
        if (continuing) {
        	importRolesWizardDlg.pack();
            setLocationOn(importRolesWizardDlg);
            importRolesWizardDlg.show();
            if (importRolesWizardDlg.finishClicked()) {
                setSelectedVdb(importRolesWizardDlg.getVdb());
                setIsStale(true);
                refresh();
            }
        }

    }  
    
    public void importVDB() {

        boolean continuing = true;
        // ImportVdbWizardDlg nvwWizardDlg = null;
        ImportVdbVersionWizardDlg nvwWizardDlg = null;
        try {

            nvwWizardDlg = new ImportVdbVersionWizardDlg(getThisParent(), connection);
        } catch (Exception ex) {
            continuing = false;
        }
        if (continuing) {
            nvwWizardDlg.pack();
            setLocationOn(nvwWizardDlg);
            nvwWizardDlg.show();
            if (nvwWizardDlg.finishClicked()) {
                setSelectedVdb(nvwWizardDlg.getNewVdb());
                setIsStale(true);
                refresh();
            }
        }

    }

    public void exportVDB() {
        VirtualDatabase vdbToExport = vdbSelected;
        VDBExporter exporter = new VDBExporter(vdbToExport, connection);
        // boolean exported =
        exporter.go();
    }

    private void runDeleteVdb() {
        boolean bConfirmed = false;
        String vdbName = getSelectedVdb().getName();
        VirtualDatabaseID vdbID = (VirtualDatabaseID)getSelectedVdb().getID();
        int vdbVersion = (new Integer(vdbID.getVersion())).intValue();
        java.util.List /* <String> */entitlementsForVDB = null;
        try {
            entitlementsForVDB = ModelManager.getEntitlementManager(connection).getEntitlementsForVDB(vdbName, vdbVersion);
        } catch (Exception ex) {
            // Oh well, we tried
            entitlementsForVDB = new ArrayList(0);
        }
        if (entitlementsForVDB.size() > 0) {
            DeleteConfirmationDialogForVDBWithEntitlements dlg = new DeleteConfirmationDialogForVDBWithEntitlements(vdbName,
                                                                                                                    vdbVersion,
                                                                                                                    entitlementsForVDB);
            dlg.show();
            bConfirmed = dlg.confirmed();
        } else {
            bConfirmed = DialogUtility.yesNoDialog(null, "Delete VirtualDatabase " + vdbName + ", Version " + //$NON-NLS-1$ //$NON-NLS-2$
                                                         vdbVersion + "?", DialogUtility.CONFIRM_DELETE_HDR); //$NON-NLS-1$
        }
        if (bConfirmed) {
            deleteVdb();
            setIsStale(true);
            refresh();
            WorkspaceController.getInstance().handleUpdateNotification(getVdbManager().getConnection(),new DataEntitlementChangeNotification());
        }
    }

    private void deleteVdb() {
        try {
            VirtualDatabaseID vdbID = (VirtualDatabaseID)getSelectedVdb().getID();
            getVdbManager().markVDBForDelete(vdbID);
        } catch (Exception e) {
            ExceptionUtility.showMessage("Failed to mark this VDB for delete ", e); //$NON-NLS-1$
        }
    }

    private JPanel getNoFilterPanel() {
        if (pnlNoFilter == null) {
            pnlNoFilter = new JPanel(new FlowLayout(FlowLayout.RIGHT, 10, 10));
            btnShowFilter = new ButtonWidget("Show Filter"); //$NON-NLS-1$
            btnShowFilter.addActionListener(new ActionListener() {

                public void actionPerformed(ActionEvent event) {
                    showFilter();
                }
            });

            pnlNoFilter.add(btnShowFilter);

            // Adjust the width to be the same as the 'filter' panel:
            Dimension dim = new Dimension(getFilterPanel().getPreferredSize().width, pnlNoFilter.getPreferredSize().height);

            pnlNoFilter.setPreferredSize(dim);
        }
        return pnlNoFilter;
    }

    private JPanel getFilterPanel() {
        if (pnlFilter == null) {
            pnlFilter = new javax.swing.JPanel();

            lblName = new javax.swing.JLabel();
            txfName = new javax.swing.JTextField();
            pnlStatus = new javax.swing.JPanel();
            // chkAll = new javax.swing.JCheckBox();
            // sepStatus = new javax.swing.JSeparator();
            chkActive = new javax.swing.JCheckBox();
            chkActiveDefault = new javax.swing.JCheckBox();
            chkInactive = new javax.swing.JCheckBox();
            chkIncomplete = new javax.swing.JCheckBox();
            chkDeleted = new javax.swing.JCheckBox();
            pnlVersion = new javax.swing.JPanel();
            rdbAllVersions = new javax.swing.JRadioButton();
            rdbLatestVersion = new javax.swing.JRadioButton();

            bgrpVersionGroup = new ButtonGroup();
            rdbAllVersions.setSelected(true);
            bgrpVersionGroup.add(rdbAllVersions);
            bgrpVersionGroup.add(rdbLatestVersion);

            // add listeners for filter components
            txfName.addActionListener(new ActionListener() {

                public void actionPerformed(ActionEvent event) {
                    enableApplyResetButtons(true);
                }
            });
            chkActive.addActionListener(new ActionListener() {

                public void actionPerformed(ActionEvent event) {
                    enableApplyResetButtons(true);
                }
            });
            chkActiveDefault.addActionListener(new ActionListener() {

                public void actionPerformed(ActionEvent event) {
                    enableApplyResetButtons(true);
                }
            });
            chkInactive.addActionListener(new ActionListener() {

                public void actionPerformed(ActionEvent event) {
                    enableApplyResetButtons(true);
                }
            });
            chkIncomplete.addActionListener(new ActionListener() {

                public void actionPerformed(ActionEvent event) {
                    enableApplyResetButtons(true);
                }
            });
            chkDeleted.addActionListener(new ActionListener() {

                public void actionPerformed(ActionEvent event) {
                    enableApplyResetButtons(true);
                }
            });
            rdbAllVersions.addActionListener(new ActionListener() {

                public void actionPerformed(ActionEvent event) {
                    enableApplyResetButtons(true);
                }
            });
            rdbLatestVersion.addActionListener(new ActionListener() {

                public void actionPerformed(ActionEvent event) {
                    enableApplyResetButtons(true);
                }
            });

            pnlFilterOps = new javax.swing.JPanel();
            pnlFilterOpsSizer = new javax.swing.JPanel();
            btnApply = new ButtonWidget(); // javax.swing.JButton();
            btnReset = new ButtonWidget();
            btnHideFilter = new ButtonWidget();
            btnHideFilter.addActionListener(new ActionListener() {

                public void actionPerformed(ActionEvent event) {
                    hideFilter();
                }
            });

            // start Filter panel construction:
            pnlFilter.setLayout(new java.awt.GridBagLayout());
            java.awt.GridBagConstraints gridBagConstraints3;
            pnlFilter.setBorder(new TitledBorder("Filter")); //$NON-NLS-1$

            lblName.setText("Name: "); //$NON-NLS-1$
            gridBagConstraints3 = new java.awt.GridBagConstraints();
            gridBagConstraints3.insets = new java.awt.Insets(3, 3, 3, 3);
            gridBagConstraints3.anchor = java.awt.GridBagConstraints.WEST;
            pnlFilter.add(lblName, gridBagConstraints3);

            txfName.setColumns(25);
            gridBagConstraints3 = new java.awt.GridBagConstraints();
            gridBagConstraints3.gridx = 1;
            gridBagConstraints3.gridy = 0;
            gridBagConstraints3.gridwidth = 2;
            gridBagConstraints3.fill = java.awt.GridBagConstraints.HORIZONTAL;
            gridBagConstraints3.insets = new java.awt.Insets(3, 3, 3, 3);
            gridBagConstraints3.anchor = java.awt.GridBagConstraints.WEST;
            pnlFilter.add(txfName, gridBagConstraints3);

            pnlStatus.setBorder(new TitledBorder("Statuses")); //$NON-NLS-1$

            chkActive.setText("Active"); //$NON-NLS-1$
            pnlStatus.add(chkActive);

            chkActiveDefault.setText("Active-Default"); //$NON-NLS-1$
            pnlStatus.add(chkActiveDefault);

            chkInactive.setText("Inactive"); //$NON-NLS-1$
            pnlStatus.add(chkInactive);

            chkIncomplete.setText("Incomplete"); //$NON-NLS-1$
            pnlStatus.add(chkIncomplete);

            chkDeleted.setText("Deleted"); //$NON-NLS-1$
            pnlStatus.add(chkDeleted);

            gridBagConstraints3 = new java.awt.GridBagConstraints();
            gridBagConstraints3.gridx = 0;
            gridBagConstraints3.gridy = 1;
            gridBagConstraints3.gridwidth = 3;
            gridBagConstraints3.insets = new java.awt.Insets(3, 3, 3, 3);
            gridBagConstraints3.anchor = java.awt.GridBagConstraints.WEST;
            pnlFilter.add(pnlStatus, gridBagConstraints3);

            pnlVersion.setBorder(new TitledBorder("Versions")); //$NON-NLS-1$

            rdbAllVersions.setText("All"); //$NON-NLS-1$
            pnlVersion.add(rdbAllVersions);

            rdbLatestVersion.setText("Latest Only"); //$NON-NLS-1$
            pnlVersion.add(rdbLatestVersion);

            gridBagConstraints3 = new java.awt.GridBagConstraints();
            gridBagConstraints3.gridx = 3;
            gridBagConstraints3.gridy = 1;
            gridBagConstraints3.gridwidth = 2;
            gridBagConstraints3.insets = new java.awt.Insets(3, 3, 3, 3);
            gridBagConstraints3.anchor = java.awt.GridBagConstraints.WEST;
            pnlFilter.add(pnlVersion, gridBagConstraints3);

            pnlFilterOpsSizer.setLayout(new java.awt.GridLayout(2, 1, 0, 5));

            btnApply.setText("Apply"); //$NON-NLS-1$
            pnlFilterOpsSizer.add(btnApply);

            btnReset.setText("Reset"); //$NON-NLS-1$
            pnlFilterOpsSizer.add(btnReset);
            pnlFilterOps.add(pnlFilterOpsSizer);
            btnApply.addActionListener(new ActionListener() {

                public void actionPerformed(ActionEvent event) {
                    applyFilter();
                }
            });
            btnReset.addActionListener(new ActionListener() {

                public void actionPerformed(ActionEvent event) {
                    resetFilterState();
                }
            });
            gridBagConstraints3 = new java.awt.GridBagConstraints();
            gridBagConstraints3.gridx = 5;
            gridBagConstraints3.gridy = 1;
            gridBagConstraints3.insets = new java.awt.Insets(3, 3, 3, 3);
            pnlFilter.add(pnlFilterOps, gridBagConstraints3);
            btnHideFilter.setText("Hide Filter"); //$NON-NLS-1$
            gridBagConstraints3 = new java.awt.GridBagConstraints();
            gridBagConstraints3.gridx = 2;
            gridBagConstraints3.gridy = 0;
            gridBagConstraints3.gridwidth = 0;
            gridBagConstraints3.insets = new java.awt.Insets(3, 3, 3, 3);
            gridBagConstraints3.anchor = java.awt.GridBagConstraints.EAST;
            pnlFilter.add(btnHideFilter, gridBagConstraints3);
            initFilterState();
        }
        return pnlFilter;
    }

    private void applyFilter() {
        populateTable(true, getSelectedVdb());
        setBaseFilterFromCurrentFilterState();
    }

    private void enableApplyResetButtons(boolean bNewState) {
        if (bCanModify) {
            btnApply.setEnabled(bNewState);
            btnReset.setEnabled(bNewState);
        } else {
            btnApply.setEnabled(false);
            btnReset.setEnabled(false);
        }
    }

    private void initFilterState() {
        bIncompleteBaseState = true;
        bInactiveBaseState = true;
        bActiveBaseState = true;
        bActiveDefaultBaseState = true;
        bDeletedBaseState = true;

        bAllVersionsBaseState = true;
        bLatestVersionsBaseState = false;

        sNameFilterBaseState = "*"; //$NON-NLS-1$

        resetFilterState();
    }

    private void resetFilterState() {
        chkIncomplete.setSelected(bIncompleteBaseState);
        chkInactive.setSelected(bInactiveBaseState);
        chkActive.setSelected(bActiveBaseState);
        chkActiveDefault.setSelected(bActiveDefaultBaseState);
        chkDeleted.setSelected(bDeletedBaseState);

        rdbAllVersions.setSelected(bAllVersionsBaseState);
        rdbLatestVersion.setSelected(bLatestVersionsBaseState);

        txfName.setText(sNameFilterBaseState);
    }

    private void setBaseFilterFromCurrentFilterState() {
        bIncompleteBaseState = chkIncomplete.isSelected();
        bInactiveBaseState = chkInactive.isSelected();
        bActiveBaseState = chkActive.isSelected();
        bActiveDefaultBaseState = chkActiveDefault.isSelected();
        bDeletedBaseState = chkDeleted.isSelected();

        bAllVersionsBaseState = rdbAllVersions.isSelected();
        bLatestVersionsBaseState = rdbLatestVersion.isSelected();

        sNameFilterBaseState = txfName.getText();
    }

    private Collection applyFilter(Collection colVdbs) {
        VirtualDatabase vdbTemp = null;
        Collection colResultVdbs = new Vector();

        Iterator it = colVdbs.iterator();

        while (it.hasNext()) {
            vdbTemp = (VirtualDatabase)it.next();
            if (passesExcludeFilters(vdbTemp)) {
                colResultVdbs.add(vdbTemp);
            }
        }

        // Apply the version filter in a separate pass:
        if (!rdbAllVersions.isSelected()) {
            colResultVdbs = applyVersionFilter(colResultVdbs);
        }
        return colResultVdbs;
    }

    private boolean passesExcludeFilters(VirtualDatabase vdb) {
        boolean bResult = true;

        // test status
        if (vdb.getStatus() == VDBStatus.ACTIVE && !chkActive.isSelected()) {
            bResult = false;
        } else if (vdb.getStatus() == VDBStatus.ACTIVE_DEFAULT && !chkActiveDefault.isSelected()) {
            bResult = false;
        } else if (vdb.getStatus() == VDBStatus.INACTIVE && !chkInactive.isSelected()) {
            bResult = false;
        } else if (vdb.getStatus() == VDBStatus.INCOMPLETE && !chkIncomplete.isSelected()) {
            bResult = false;
        } else if (vdb.getStatus() == VDBStatus.DELETED && !chkDeleted.isSelected()) {
            bResult = false;
        }

        // test name string
        if (bResult) {
            // If we passed the previous test, we can try this one;
            // (if we failed the previous test there is no need to
            // try this one)
            bResult = PropertiesUtils.filterTest(txfName.getText(), vdb.getName());
        }
        // done:
        return bResult;
    }

    private Collection applyVersionFilter(Collection colVdbs) {
        HashMap hmapHighVersionVdbs = new HashMap();
        VirtualDatabase vdbCandidateVdb = null;
        VirtualDatabase vdbTemp = null;
        // Collection colResultVdbs = new Vector();
        String sCandidateVdbVersion = ""; //$NON-NLS-1$
        String sTempVdbVersion = ""; //$NON-NLS-1$

        int iCandidateVdbVersion = 0;
        int iTempVdbVersion = 0;

        Iterator it = colVdbs.iterator();

        while (it.hasNext()) {
            vdbCandidateVdb = (VirtualDatabase)it.next();

            // Strategy:
            // See if this vdb is already in the hashmap;
            // if it is, see if this vdb has a higher version than
            // the one in the map;
            // if it does, remove the old one and add the new one.
            //
            // When the process is complete, we'll have a Collection
            // containing only the highest numbered VDB version for
            // each VDB in the input collection.
            // hmapHighVersionVdbs
            // HashMap definition:
            // key: vdb name; value: vdb

            // 1. See if the map contains this vdb:
            vdbTemp = (VirtualDatabase)hmapHighVersionVdbs.get(vdbCandidateVdb.getName());

            if (vdbTemp == null) {
                // if no other version of this vdb exists, add it:
                hmapHighVersionVdbs.put(vdbCandidateVdb.getName(), vdbCandidateVdb);
            } else {
                sCandidateVdbVersion = getVdbVersion(vdbCandidateVdb);
                iCandidateVdbVersion = Integer.parseInt(sCandidateVdbVersion);

                sTempVdbVersion = getVdbVersion(vdbTemp);
                iTempVdbVersion = Integer.parseInt(sTempVdbVersion);

                if (iCandidateVdbVersion > iTempVdbVersion) {
                    // if no other version of this vdb exists, add it:
                    // remove the older version
                    hmapHighVersionVdbs.remove(vdbTemp.getName());

                    // add this later version
                    // if no other version of this vdb exists, add it:

                    hmapHighVersionVdbs.put(vdbCandidateVdb.getName(), vdbCandidateVdb);
                }
            }
        }
        return hmapHighVersionVdbs.values();
    }

    private String getVdbVersion(VirtualDatabase vdb) {
        VirtualDatabaseID vdbid = (VirtualDatabaseID)vdb.getID();
        return vdbid.getVersion();
    }

    private void runChangeStatusDialog(boolean changeDefaultStatus) {
        VdbSetStatusDlg vssdlg = new VdbSetStatusDlg(getSelectedVdb(), connection, changeDefaultStatus);
        vssdlg.pack();
        setLocationOn(vssdlg);
        vssdlg.setModal(true);
        vssdlg.show();
        setIsStale(true);
        refresh();
    }

    public static void setLocationOn(Component comp) {
        Point p = StaticUtilities.centerFrame(comp.getSize());
        comp.setLocation(p.x, p.y);
    }

    public void setParent(JFrame frame) {
        this.frParentFrame = frame;
    }

    public JFrame getThisParent() {
        if (frParentFrame == null) {
            frParentFrame = ViewManager.getMainFrame();
        }
        return frParentFrame;
    }

    private void emptyTheTable() {
        tableModel.setRowCount(0);
    }

    private void populateTable(boolean bApplyFilter,
                               VirtualDatabase vdbToSelect) {
        emptyTheTable();
        // SimpleDateFormat formatter =
        StaticUtilities.getDefaultDateFormat();
        Collection colVdbs = null;
        try {
            colVdbs = getVdbs();
        } catch (Exception e) {
            ExceptionUtility.showMessage("Failed while retrieving VDBs ", e); //$NON-NLS-1$
        }
        if (colVdbs != null) {
            if (bApplyFilter) {
                colDisplayedVdbs = applyFilter(colVdbs);
            } else {
                // no filtering:
                colDisplayedVdbs = colVdbs;
            }

            VirtualDatabase vdbTemp;
            short siStatus = 0;
            Iterator it = colDisplayedVdbs.iterator();
            int iCount = 0;
            while (it.hasNext()) {
                iCount++;
                Vector vVdbRow = new Vector();
                vdbTemp = (VirtualDatabase)it.next();
                vVdbRow.add(vdbTemp.getName());

                VirtualDatabaseID vdbid = (VirtualDatabaseID)vdbTemp.getID();
                vVdbRow.add(new Integer(vdbid.getVersion()));
                siStatus = vdbTemp.getStatus();
                vVdbRow.add(getVdbManager().getVdbStatusAsString(siStatus));
                Date versionDate = vdbTemp.getVersionDate();
                vVdbRow.add(versionDate);
                vVdbRow.add(vdbTemp.getVersionBy());
                tableModel.addRow(vVdbRow);
            }
        }
        programmaticChange = true;
        tblVdb.sort();
        programmaticChange = false;
        setSelectedVdb(vdbToSelect);
        reselectCurrentVdbInTable();
    }

    public void setIsStale(boolean bIsStale) {
        this.bIsStale = bIsStale;
    }

    public boolean isStale() {
        return bIsStale;
    }

    private java.util.List getVdbs() throws Exception {
        if (colVdbs == null) {
            colVdbs = getVdbManager().getVDBs();
        }

        Vector vVdbs = new Vector(colVdbs);
        return vVdbs;
    }

    private java.util.List getDisplayedVdbs() {
        Vector vVdbs = new Vector(colDisplayedVdbs);
        return vVdbs;
    }

    private void setTableListening() {
        tblVdb.setSelectionMode(ListSelectionModel.SINGLE_SELECTION);
        rowSM = tblVdb.getSelectionModel();
        rowSM.addListSelectionListener(new ListSelectionListener() {

            public void valueChanged(ListSelectionEvent e) {
                // Ignore extra messages.
                if ((!e.getValueIsAdjusting()) && (!programmaticChange)) {
                    int iTrueModelRow = 0;
                    int iSelectionIndex = 0;
                    ListSelectionModel lsm = (ListSelectionModel)e.getSource();
                    if (lsm.isSelectionEmpty()) {
                        updateDetailForTableDeselection();
                    } else {
                        iSelectionIndex = lsm.getMinSelectionIndex();
                        if (iSelectionIndex >= 0) {
                            iTrueModelRow = tblVdb.convertRowIndexToModel(iSelectionIndex);
                            updateDetailForTableSelection(iTrueModelRow);
                        } else {
                            updateDetailForTableDeselection();
                        }
                    }
                }
            }
        });
    }

    private void updateDetailForTableSelection(int iRow) {
        VirtualDatabase vdbSelected = null;
        try {
            vdbSelected = (VirtualDatabase)getDisplayedVdbs().get(iRow);
        } catch (Exception e) {
            e.printStackTrace();
        }
        setSelectedVdb(vdbSelected);
        alignVisibleDetailPanelWithTableChoice();
        // Can export if user has System.READ role
        actionExportVDB.setEnabled(true);
        if (bCanModify) {
            setEnabledForChangeStatusAction();
            setEnabledForChangeDefaultStatusAction();
            setEnabledForDeleteAction();
            actionExportVDB.setEnabled(true);
        } else {
            actionRedeployVDB.setEnabled(false);
            actionDeleteVdb.setEnabled(false);
            actionChangeStatus.setEnabled(false);
            actionChangeDefaultStatus.setEnabled(false);
        }
        forceRepaint();
    }

    private void setEnabledForChangeStatusAction() {
        // we only allow the Change Status feature when the current status
        // is Active or Inactive:
        VirtualDatabase vdb = getSelectedVdb();

        if (vdb != null) {
            short siCurrStatus = vdb.getStatus();

            if (siCurrStatus == VDBStatus.ACTIVE || siCurrStatus == VDBStatus.ACTIVE_DEFAULT ||
                	siCurrStatus == VDBStatus.INACTIVE ) {
                actionChangeStatus.setEnabled(true);
                actionChangeStatus.putValue(Action.NAME, getChangeStatusLabel(siCurrStatus));
            } else {
                actionChangeStatus.putValue(Action.NAME, CHANGE_VDB_STATUS_TITLE);
                actionChangeStatus.setEnabled(false);
            }
        } else {
            actionChangeStatus.setEnabled(false);
        }
    }

    private String getChangeStatusLabel(short siCurrStatus) {
        String sChangeStatusBase = CHANGE_VDB_STATUS_ROOT;
        if (siCurrStatus == VDBStatus.ACTIVE || siCurrStatus == VDBStatus.ACTIVE_DEFAULT) {
            sChangeStatusBase += INACTIVE;
        } else if (siCurrStatus == VDBStatus.INACTIVE) {
            sChangeStatusBase += ACTIVE;
        }
        return sChangeStatusBase;
    }

    private void setEnabledForChangeDefaultStatusAction() {
        // we only allow the Change Default feature when the current status
        // is Active or Inactive:
        VirtualDatabase vdb = getSelectedVdb();

        int nVersions = this.getNumberOfVersionsForVdb(vdb);
        boolean hasActiveDefault = this.hasActiveDefault(vdb);
        
        if (vdb != null) {
            short siCurrStatus = vdb.getStatus();
            
            // If only one version, disable the action
            if(nVersions<=1) {
            	actionChangeDefaultStatus.setEnabled(false);
            	actionChangeDefaultStatus.putValue(Action.NAME, CHANGE_VDB_DEFAULT_STATUS_TITLE);
            // Has multiple versions
            } else {
            	// Does not already have active default
            	if(!hasActiveDefault) {
            		actionChangeDefaultStatus.putValue(Action.NAME, getChangeDefaultStatusLabel(siCurrStatus));
            		actionChangeDefaultStatus.setEnabled(true);
                // Already has active default
            	} else {
            		actionChangeDefaultStatus.putValue(Action.NAME, getChangeDefaultStatusLabel(siCurrStatus));
                    // If selected vdb is ACTIVE-DEFAULT, enable action to allow demotion
            		if(siCurrStatus==VDBStatus.ACTIVE_DEFAULT) {
            			actionChangeDefaultStatus.setEnabled(true);
            		} else {
            			actionChangeDefaultStatus.setEnabled(false);
            		}
            	}
            }
        } else {
        	actionChangeDefaultStatus.setEnabled(false);
        }
    }

    private String getChangeDefaultStatusLabel(short siCurrStatus) {
        String sChangeStatusBase = CHANGE_VDB_STATUS_ROOT;
        if (siCurrStatus == VDBStatus.ACTIVE || siCurrStatus == VDBStatus.INACTIVE) {
            sChangeStatusBase += ACTIVE_DEFAULT;
        } else if (siCurrStatus == VDBStatus.ACTIVE_DEFAULT) {
            sChangeStatusBase += ACTIVE;
        }
        return sChangeStatusBase;
    }

    private void setEnabledForDeleteAction() {
        // we only allow the Change Status feature when the current status
        // is Active or Inactive:
        VirtualDatabase vdb = getSelectedVdb();

        if (vdb != null) {
            short siCurrStatus = vdb.getStatus();

            if (siCurrStatus == VDBStatus.INCOMPLETE || siCurrStatus == VDBStatus.INACTIVE) {
                actionDeleteVdb.setEnabled(true);
            } else {
                actionDeleteVdb.setEnabled(false);
            }
        } else {
            actionDeleteVdb.setEnabled(false);
        }
    }

    private VirtualDatabase getSelectedVdb() {
        return vdbSelected;
    }

    private void setSelectedVdb(VirtualDatabase vdb) {
        vdbSelected = vdb;
    }

    private void updateDetailForTableDeselection() {
        actionRedeployVDB.setEnabled(false);
        actionExportVDB.setEnabled(false);
        actionChangeStatus.setEnabled(false);
        actionChangeDefaultStatus.setEnabled(false);
        actionDeleteVdb.setEnabled(false);
        actionEditConnBind.setEnabled(false);
        actionExportRoles.setEnabled(false);
        actionImportRoles.setEnabled(false);
        setSelectedVdb(null);
        alignVisibleDetailPanelWithTableChoice();
        forceRepaint();
    }

    public void receiveUpdateNotification(RuntimeUpdateNotification notification) {
        // TODO
    }

    public java.util.List /* <Action> */resume() {
        return arylActions;
    }

    public ConnectionInfo getConnection() {
        return connection;
    }

    public void postRealize() {
        splitMain.setDividerLocation(0.50);
        // int iLoc =
        splitMain.getDividerLocation();
        if (tblVdb.getRowCount() > 0) {
            tblVdb.setRowSelectionInterval(0, 0);
        }
    }

    public java.util.List /* <Action> */getCurrentActions() {
        return arylActions;
    }

    public void addActionToList(String sID,
                                Action act) {
        arylActions.add(new MenuEntry(sID, act));
    }

    private void addSeparatorToActionList() {
        arylActions.add(MenuEntry.getSeparator());
    }

    public void refresh() {
        VirtualDatabase vdbCurrent = getSelectedVdb();
        // pull the data from the server again
        this.colVdbs = null;
        populateTable(false, vdbCurrent);
        colDisplayedVdbs = colVdbs;

        forceRepaint();
    }

    private void reselectCurrentVdbInTable() {
        VirtualDatabase vdbCurrent = getSelectedVdb();
        if (vdbCurrent != null) {
            int iRow = getModelRowForVdb(vdbCurrent);
            if (iRow >= 0) {
                int viewRow = tblVdb.convertRowIndexToView(iRow);
                tblVdb.getSelectionModel().setSelectionInterval(viewRow, viewRow);
                tblVdb.scrollRectToVisible(tblVdb.getCellRect(viewRow, 0, true));
            } else {
                setSelectedVdb(null);
                tblVdb.clearSelection();
                updateDetailForTableDeselection();
            }
        } else {
            tblVdb.clearSelection();
            updateDetailForTableDeselection();
        }
    }

    private int getModelRowForVdb(VirtualDatabase vdb) {
        int matchRow = -1;
        int numRows = tblVdb.getRowCount();
        int row = 0;
        String sSelectedVdbName = vdb.getName();
        String sSelectedVdbVersion = getVdbVersion(vdb);

        while ((row < numRows) && (matchRow < 0)) {
            String sVdbName = tblVdb.getModel().getValueAt(row, VDBTableModel.VDB_NAME_COL_NUM).toString();
            String sVdbVersion = tblVdb.getModel().getValueAt(row, VDBTableModel.VDB_VERSION_COL_NUM).toString();
            if (sSelectedVdbName.equals(sVdbName) && sSelectedVdbVersion.equals(sVdbVersion)) {
                matchRow = row;
            } else {
                row++;
            }
        }
        return matchRow;
    }

    private int getNumberOfVersionsForVdb(VirtualDatabase vdb) {
        int nVersions = 0;
        int numRows = tblVdb.getRowCount();
        int row = 0;
        String sSelectedVdbName = vdb.getName();

        while (row < numRows) {
            String sVdbName = tblVdb.getModel().getValueAt(row, VDBTableModel.VDB_NAME_COL_NUM).toString();
            if (sSelectedVdbName.equals(sVdbName)) {
                nVersions++;
            }
            row++;
        }
        return nVersions;
    }
    
    private boolean hasActiveDefault(VirtualDatabase vdb) {
        boolean hasActiveDefault = false;
        int numRows = tblVdb.getRowCount();
        int row = 0;
        String sSelectedVdbName = vdb.getName();
        
        while (row < numRows) {
            String sVdbName = tblVdb.getModel().getValueAt(row, VDBTableModel.VDB_NAME_COL_NUM).toString();
            if (sSelectedVdbName.equals(sVdbName)) {
            	String sVdbStatus = tblVdb.getModel().getValueAt(row, VDBTableModel.VDB_STATUS_COL_NUM).toString();
                if(ACTIVE_DEFAULT_DISPLAY_LABEL.equals(sVdbStatus)) {
                	hasActiveDefault = true;
                	break;
                }
            }
            row++;
        }
        return hasActiveDefault;
    }

    public boolean havePendingChanges() {
        boolean haveChanges = false;
        return haveChanges;
    }

    public boolean finishUp() {
        return true;
    }

    public String getTitle() {
        return VDB_MANAGEMENT_TITLE;
    }

    public void setMainButtons() {
        // String sTitle =
        tpnDetails.getTitleAt(tpnDetails.getSelectedIndex());
    }

    // called when the selection in the tabbed pane changes
    public void stateChanged(ChangeEvent ce) {
        setMainButtons();
        alignVisibleDetailPanelWithTableChoice();
    }

    private void alignVisibleDetailPanelWithTableChoice() {
        try {
            StaticUtilities.startWait(ViewManager.getMainFrame());
            VdbDisplayer vdspDisplayPanel = getCurrentDetailPanel();
            vdspDisplayPanel.setVirtualDatabase(getSelectedVdb());
        } catch (Exception e) {
            ExceptionUtility.showMessage("Failed while tabbing in MainPanel", e); //$NON-NLS-1$
        } finally {
            StaticUtilities.endWait(ViewManager.getMainFrame());
        }

        String sTitle = tpnDetails.getTitleAt(tpnDetails.getSelectedIndex());

        // Determine Enabled State of Edit Connector Bindings Button and
        // Import - Export Roles Buttons
        boolean editBindingsEnabled = false;
        boolean importRolesEnabled = false;
        boolean exportRolesEnabled = false;
        if (bCanModify) {
        	// Connector Bindings Tab Displayed
            if (sTitle.equals(CONNBIND_TAB_TITLE)) {
                VirtualDatabase vdb = getSelectedVdb();
                if (vdb != null) {
                    short status = vdb.getStatus();
                    if ((status == VDBStatus.INACTIVE) || (status == VDBStatus.INCOMPLETE)) {
                    	editBindingsEnabled = true;
                    }
                }
            }
        	// Roles Tab Displayed
            if (sTitle.equals(ENTITLEMENTS_TAB_TITLE) && getSelectedVdb() != null) {
            	// Import is enabled if any vdb is selected
                importRolesEnabled = true;
                // Export is enabled if vdb is selected and current selection has at least one role
                if(pnlEntitlements.hasRoles()) {
                	exportRolesEnabled = true;
                }
            }
        }
        
        // Set Action States
        actionEditConnBind.setEnabled(editBindingsEnabled);
        actionImportRoles.setEnabled(importRolesEnabled);
        actionExportRoles.setEnabled(exportRolesEnabled);
        
        forceRepaint();
    }

    public VdbDisplayer getCurrentDetailPanel() {
        VdbDisplayer vdbDisplayer = (VdbDisplayer)tpnDetails.getSelectedComponent();
        // int selectedIndex = tpnDetails.getSelectedIndex();
        // String sTitle = tpnDetails.getTitleAt(selectedIndex);
        return vdbDisplayer;
    }

    // setup associates an action with a button
    private void setup(String sID,
                       AbstractButton theButton,
                       AbstractPanelAction theAction) {
        theAction.addComponent(theButton);
        addActionToList(sID, theAction);
    }

    public java.util.List getTableActions() {
        return arylPopupActions;
    }

    // ===
    private class PanelAction extends AbstractPanelAction {

        public static final int EDIT_CONN_BIND = 2;
        public static final int CHANGE_STATUS = 3;
        public static final int DELETE_VDB = 4;
        public static final int IMPORT_VDB = 5;
        public static final int EXPORT_VDB = 6;
        public static final int EXPORT_ROLES = 8;
        public static final int IMPORT_ROLES = 9;
        public static final int CHANGE_DEFAULT_STATUS = 10;

        public PanelAction(int theType) {
            super(theType);
            // String iconId = null;
        }

        public void actionImpl(ActionEvent theEvent) {
        	if (type == IMPORT_VDB) {
                importVDB();
            } else if (type == EXPORT_VDB) {
                exportVDB();
            } else if (type == EDIT_CONN_BIND) {
                editConnectorBindings();
            } else if (type == EXPORT_ROLES) {
                exportRoles();
            } else if (type == IMPORT_ROLES) {
            	importRoles();
            } else if (type == CHANGE_STATUS) {
                runChangeStatusDialog(false);
            } else if (type == CHANGE_DEFAULT_STATUS) {
                runChangeStatusDialog(true);
            } else if (type == DELETE_VDB) {
                runDeleteVdb();
            }
        }
    }

    private static class TableMenuFactory extends DefaultPopupMenuFactory {

        JPopupMenu pop = new JPopupMenu();
        VdbMainPanel pnl;

        public TableMenuFactory(TableWidget theTable,
                                VdbMainPanel thePanel) {
            pnl = thePanel;
        }

        protected JPopupMenu createTreePopupMenu(final TableWidget table) {
            return pop;
        }

        public JPopupMenu getPopupMenu(final Component context) {
            if (context instanceof TableWidget) {
                pop.removeAll();
                java.util.List actions = pnl.getTableActions();
                if (actions != null) {
                    for (int size = actions.size(), i = 0; i < size; pop.add((Action)actions.get(i++))) {

                    }
                    return pop;
                }
            }
            return null;
        }
    }
}// end VdbMainPanel

class VDBTableModel extends com.metamatrix.toolbox.ui.widget.table.DefaultTableModel {

    public final static String VDB_NAME_HDR = "VDB Name"; //$NON-NLS-1$
    public final static String VDB_VERSION_HDR = "Version"; //$NON-NLS-1$
    public final static String VDB_STATUS_HDR = "Status"; //$NON-NLS-1$
    public final static String VDB_VERSIONED_HDR = "Versioned"; //$NON-NLS-1$
    public final static String VDB_VERSIONED_BY_HDR = "Versioned By"; //$NON-NLS-1$

    public static final int VDB_NAME_COL_NUM = 0;
    public static final int VDB_VERSION_COL_NUM = 1;
    public static final int VDB_STATUS_COL_NUM = 2;
    public static final int VDB_VERSIONED_COL_NUM = 3;
    public static final int VDB_VERSIONED_BY_COL_NUM = 4;
    public static final int NUM_COLUMNS = 5;

    private static Vector COLUMN_NAMES_VEC;

    static {
        // Code to populate Vector with order independence of column positioning
        String[] colNames = new String[NUM_COLUMNS];
        colNames[VDB_NAME_COL_NUM] = VDB_NAME_HDR;
        colNames[VDB_VERSION_COL_NUM] = VDB_VERSION_HDR;
        colNames[VDB_STATUS_COL_NUM] = VDB_STATUS_HDR;
        colNames[VDB_VERSIONED_COL_NUM] = VDB_VERSIONED_HDR;
        colNames[VDB_VERSIONED_BY_COL_NUM] = VDB_VERSIONED_BY_HDR;
        COLUMN_NAMES_VEC = new Vector(NUM_COLUMNS);
        for (int i = 0; i < colNames.length; i++) {
            COLUMN_NAMES_VEC.add(colNames[i]);
        }
    }

    public VDBTableModel() {
        super(COLUMN_NAMES_VEC);
    }

    public Class getColumnClass(int colNum) {
        Class cls = null;
        switch (colNum) {
            case VDB_NAME_COL_NUM:
                cls = String.class;
                break;
            case VDB_VERSION_COL_NUM:
                cls = Integer.class;
                break;
            case VDB_STATUS_COL_NUM:
                cls = String.class;
                break;
            case VDB_VERSIONED_COL_NUM:
                cls = Date.class;
                break;
            case VDB_VERSIONED_BY_COL_NUM:
                cls = String.class;
                break;
        }
        return cls;
    }
}// end VDBTableModel

class DeleteConfirmationDialogForVDBWithEntitlements extends JDialog {

    private boolean yesPressed = false;
    private JScrollPane entScrollPane;
    private JPanel entPanel;

    public DeleteConfirmationDialogForVDBWithEntitlements(String vdbName,
                                                          int vdbVersion,
                                                          java.util.List /* <String> */entitlements) {
        super(ConsoleMainFrame.getInstance(), DialogUtility.CONFIRM_DELETE_HDR, true);
        doLayoutStuff(vdbName, vdbVersion, entitlements);
        this.pack();
        Dimension curSize = this.getSize();
        Dimension newSize = new Dimension(curSize.width, Math.min(curSize.height, (int)(Toolkit.getDefaultToolkit()
                                                                                               .getScreenSize().height * 0.4)));
        this.setSize(newSize);
        entScrollPane.getVerticalScrollBar().setUnitIncrement((entPanel.getHeight() / entitlements.size()));
        this.setLocation(StaticUtilities.centerFrame(this.getSize()));
    }

    public boolean confirmed() {
        return yesPressed;
    }

    private void doLayoutStuff(String vdbName,
                               int vdbVersion,
                               java.util.List /* <String> */entitlements) {
        GridBagLayout layout = new GridBagLayout();
        this.getContentPane().setLayout(layout);
        JLabel topLabel = new JLabel("Delete VDB " + vdbName + ", Version " + //$NON-NLS-1$ //$NON-NLS-2$
                                     vdbVersion + "?"); //$NON-NLS-1$
        JLabel entLabel = new JLabel("Deleting will cause deletion of " + //$NON-NLS-1$
                                     "the following roles for VDB " + vdbName + ", Version " //$NON-NLS-1$ //$NON-NLS-2$
                                     + vdbVersion + ":"); //$NON-NLS-1$
        int numEntitlements = entitlements.size();
        entPanel = new JPanel(new GridLayout(numEntitlements, 1));
        Iterator it = entitlements.iterator();
        while (it.hasNext()) {
            String entName = (String)it.next();
            entPanel.add(new JLabel(entName));
        }
        entScrollPane = new JScrollPane(entPanel);
        JLabel bottomLabel = new JLabel("Proceed with deletion?"); //$NON-NLS-1$
        JButton yesButton = new JButton("  Yes  "); //$NON-NLS-1$
        yesButton.addActionListener(new ActionListener() {

            public void actionPerformed(ActionEvent ev) {
                yesButtonPressed();
            }
        });
        JButton noButton = new JButton("No"); //$NON-NLS-1$
        noButton.addActionListener(new ActionListener() {

            public void actionPerformed(ActionEvent ev) {
                noButtonPressed();
            }
        });
        JPanel buttonsPanel = new JPanel(new GridLayout(1, 2, 10, 0));
        buttonsPanel.add(yesButton);
        buttonsPanel.add(noButton);
        this.getContentPane().add(topLabel);
        this.getContentPane().add(entLabel);
        this.getContentPane().add(entScrollPane);
        this.getContentPane().add(bottomLabel);
        this.getContentPane().add(buttonsPanel);
        layout.setConstraints(topLabel, new GridBagConstraints(0, 0, 1, 1, 0.0, 0.0, GridBagConstraints.WEST,
                                                               GridBagConstraints.NONE, new Insets(10, 20, 5, 20), 0, 0));
        layout.setConstraints(entLabel, new GridBagConstraints(0, 1, 1, 1, 0.0, 0.0, GridBagConstraints.WEST,
                                                               GridBagConstraints.NONE, new Insets(10, 20, 5, 20), 0, 0));
        layout.setConstraints(entScrollPane, new GridBagConstraints(0, 2, 1, 1, 1.0, 1.0, GridBagConstraints.CENTER,
                                                                    GridBagConstraints.BOTH, new Insets(5, 30, 10, 30), 0, 0));
        layout.setConstraints(bottomLabel, new GridBagConstraints(0, 3, 1, 1, 0.0, 0.0, GridBagConstraints.CENTER,
                                                                  GridBagConstraints.NONE, new Insets(10, 20, 5, 20), 0, 0));
        layout.setConstraints(buttonsPanel, new GridBagConstraints(0, 4, 1, 1, 0.0, 0.0, GridBagConstraints.CENTER,
                                                                   GridBagConstraints.NONE, new Insets(10, 20, 10, 20), 0, 0));
    }

    private void yesButtonPressed() {
        yesPressed = true;
        this.dispose();
    }

    private void noButtonPressed() {
        this.dispose();
    }
}// end DeleteConfirmationDialogForVDBWithEntitlements
