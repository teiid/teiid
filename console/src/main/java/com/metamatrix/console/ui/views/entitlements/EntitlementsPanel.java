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

package com.metamatrix.console.ui.views.entitlements;

import java.awt.Cursor;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.GridLayout;
import java.awt.Insets;
import java.awt.Rectangle;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.util.ArrayList;

import javax.swing.AbstractAction;
import javax.swing.Action;
import javax.swing.JOptionPane;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JSplitPane;
import javax.swing.ListSelectionModel;
import javax.swing.event.ListSelectionEvent;
import javax.swing.event.ListSelectionListener;

import com.metamatrix.api.exception.ComponentNotFoundException;
import com.metamatrix.api.exception.security.AuthorizationException;
import com.metamatrix.common.log.LogManager;
import com.metamatrix.console.connections.ConnectionInfo;
import com.metamatrix.console.models.EntitlementManager;
import com.metamatrix.console.models.ModelManager;
import com.metamatrix.console.models.GroupsManager;
import com.metamatrix.console.notification.DataEntitlementChangeNotification;
import com.metamatrix.console.notification.RuntimeUpdateNotification;
import com.metamatrix.console.security.UserCapabilities;
import com.metamatrix.console.ui.NotifyOnExitConsole;
import com.metamatrix.console.ui.layout.BasePanel;
import com.metamatrix.console.ui.layout.ConsoleMainFrame;
import com.metamatrix.console.ui.layout.MenuEntry;
import com.metamatrix.console.ui.layout.PanelsTree;
import com.metamatrix.console.ui.layout.WorkspacePanel;
import com.metamatrix.console.ui.util.RepaintController;
import com.metamatrix.console.util.DialogUtility;
import com.metamatrix.console.util.ExceptionUtility;
import com.metamatrix.console.util.ExternalException;
import com.metamatrix.console.util.LogContexts;
import com.metamatrix.console.util.Refreshable;
import com.metamatrix.console.util.StaticUtilities;
import com.metamatrix.toolbox.ui.widget.ButtonWidget;
import com.metamatrix.toolbox.ui.widget.Splitter;


/**
 *
 */
public class EntitlementsPanel extends BasePanel implements WorkspacePanel,
        NotifyOnExitConsole, RepaintController, Refreshable {
    private final double DEFAULT_SPLITTER_PROPORTION = 0.25;

    private EntitlementsDataInterface dataSource;
    private ConnectionInfo connection;
    private EntitlementManager entitlementManager;
    private GroupsManager userManager;
    private ButtonWidget newButton;
    private ButtonWidget deleteButton;
    private Action deleteAction;
    private EntitlementsTable table;
    private EntitlementsTableModel tableModel;
    private EntitlementDetailPanel detailPanel;
    private boolean created = false;
    private JSplitPane splitPane = null;
    private java.util.List /*<MenuEntry>*/ currentActions = new ArrayList(10);
    private boolean programmaticTableSelection = false;
    private boolean showingEditable;
    private boolean showingReadOnly;
    private boolean canModify;
    private int iSplitterChange           = 5;
    private int currentSelectedModelIndex = 0, oldSelectedModelIndex;
    private String message = "Cannot create a role because no VDBs exist."; //$NON-NLS-1$
    private boolean showingNewlyCreatedEntitlement = false;

    public EntitlementsPanel(EntitlementsDataInterface ds, boolean showEditable,
            boolean showReadOnly, ConnectionInfo conn) throws
            AuthorizationException, ExternalException {
        super();
        dataSource = ds;
        showingEditable = showEditable;
        showingReadOnly = showReadOnly;
        this.connection = conn;
        entitlementManager = ModelManager.getEntitlementManager(connection);
        userManager = ModelManager.getGroupsManager(connection);
        //TODO-- set canModify from param in constructor
        try {
            canModify = UserCapabilities.getInstance().canModifyEntitlements(
            		connection);
        } catch (Exception ex) {
            throw new ExternalException(ex);
        }
        init();
    }

    public void postRealize() {
        splitPane.setDividerLocation((int)(getSize().height *
                DEFAULT_SPLITTER_PROPORTION));
    }

    private void init() throws AuthorizationException, ExternalException {
        int splitterLoc = -1;
        if (created) {
            splitterLoc = splitPane.getDividerLocation();
            removeAll();
        }
        GridBagLayout l = new GridBagLayout();
        setLayout(l);
        EntitlementsTableRowData[] rows = dataSource.getEntitlements();
        tableModel = new EntitlementsTableModel(rows);
        table = new EntitlementsTable(tableModel);
		table.getSelectionModel().setSelectionMode(ListSelectionModel.SINGLE_SELECTION);
        table.getSelectionModel().addListSelectionListener(
                new ListSelectionListener() {
            public void valueChanged(ListSelectionEvent ev) {
                oldSelectedModelIndex = currentSelectedModelIndex;
                if ((!programmaticTableSelection) && (!ev.getValueIsAdjusting())) {
                    int index = table.getSelectionModel().getLeadSelectionIndex();
                    if ((index >= 0) && (index < tableModel.getRowCount())) {
                        int modelIndex = table.convertRowIndexToModel( index );
                        currentSelectedModelIndex = modelIndex;
                        if (modelIndex >= 0) {
                            String entName = (String)table.getModel().getValueAt(
                                    modelIndex,
                                    EntitlementsTableModel.ENTITLEMENT_COL_NUM);
                            String vdbName = (String)table.getModel().getValueAt(
                                    modelIndex,
                                    EntitlementsTableModel.VDB_COL_NUM);
                            Integer vdbVersionInt =
                                    (Integer)table.getModel().getValueAt(modelIndex,
                                    EntitlementsTableModel.VDB_VERS_COL_NUM);
                            int vdbVersion = vdbVersionInt.intValue();
                            StaticUtilities.startWait();
                            try {
                                showDetailForEntitlement(entName, vdbName, vdbVersion);
                                StaticUtilities.endWait();
                            } catch (RuntimeException ex) {
                                StaticUtilities.endWait();
                                throw ex;
                            }
                        }
                    }
                }
                int index = table.getSelectionModel().getLeadSelectionIndex();
                if ((index >= 0) && (index < tableModel.getRowCount())) {
                    Rectangle rect = table.getCellRect(index, 0, true);
                    table.scrollRectToVisible(rect);
                } else {
                    clearDetail();
                }
            }
        });
        JScrollPane tablePane = new JScrollPane(table);
        newButton = new ButtonWidget("New..."); //$NON-NLS-1$

        newButton.addActionListener(new ActionListener() {
            public void actionPerformed(ActionEvent ev) {
                if (!anyVDBs()){
                    JOptionPane.showMessageDialog(EntitlementsPanel.this, message);
                }else{
                    boolean proceeding = true;
                    if (havePendingChanges()) {
                        proceeding = finishUp();
                    }
                    if (proceeding) {
                        createNewEntitlement();
                    }
                }
            }
        });
        deleteButton = new ButtonWidget("Delete"); //$NON-NLS-1$
        deleteButton.addActionListener(new ActionListener() {
            public void actionPerformed(ActionEvent ev) {
                deleteRequested();
            }
        });
        deleteButton.setEnabled(false);
        JPanel topPanel = new JPanel();
        GridBagLayout tl = new GridBagLayout();
        topPanel.setLayout(tl);
        topPanel.add(tablePane);
        tl.setConstraints(tablePane, new GridBagConstraints(0, 0, 1, 1,
                1.0, 1.0, GridBagConstraints.CENTER, GridBagConstraints.BOTH,
                new Insets(10, 10, 10, 10), 0, 0));
        if (canModify) {
            EntitlementsCreateAction createAction = new EntitlementsCreateAction(this);
            currentActions.add(new MenuEntry(MenuEntry.ACTION_MENUITEM, createAction));

            deleteAction = new EntitlementsDeleteAction(this);
            currentActions.add(new MenuEntry(MenuEntry.ACTION_MENUITEM, deleteAction));
            deleteAction.setEnabled(false);

            JPanel buttonsPanel = new JPanel();
            topPanel.add(buttonsPanel);
            buttonsPanel.setLayout(new GridLayout(2, 1, 5, 5));
            buttonsPanel.add(newButton);
            buttonsPanel.add(deleteButton);
            tl.setConstraints(buttonsPanel, new GridBagConstraints(1, 0, 1, 1,
                    0.0, 0.0, GridBagConstraints.CENTER, GridBagConstraints.NONE,
                    new Insets(5, 5, 5, 5), 0, 0));
        }
        detailPanel = new EntitlementDetailPanel(dataSource, this, showingEditable,
                showingReadOnly, canModify, entitlementManager, userManager);

        splitPane = new Splitter(JSplitPane.VERTICAL_SPLIT,
                true, topPanel, detailPanel);
        splitPane.setOneTouchExpandable(true);
        add(splitPane);
        l.setConstraints(splitPane, new GridBagConstraints(0, 0, 1, 1,
                1.0, 1.0, GridBagConstraints.CENTER, GridBagConstraints.BOTH,
                new Insets(0, 0, 0, 0), 0, 0));
        created = true;
        if (splitterLoc >= 0) {
            splitPane.setDividerLocation(splitterLoc);
        }
    }

    public EntitlementsTable getTable() {
        return table;
    }

    private boolean anyVDBs() {
        boolean any = false;
        try {
            any = (dataSource.getAllVDBs().size() > 0);
        } catch (Exception ex) {
            LogManager.logError(LogContexts.ENTITLEMENTS, ex,
                    "Error retrieving list of VDBs"); //$NON-NLS-1$
            ExceptionUtility.showMessage("Retrieve list of VDBs", ex); //$NON-NLS-1$
        }
        return any;
    }

    public java.util.List /*<MenuEntry>*/ resume() {
    	// if no more rows, clear detail panel
    	if(table.getRowCount()<=0) {
    		this.clearDetail();
    	}
        refreshImpl(false);
        return currentActions;
    }
    
    public void receiveUpdateNotification(RuntimeUpdateNotification notification) {
    	if (notification instanceof DataEntitlementChangeNotification) {
    		refresh();
    	}
    }

	public void refresh() {
		refreshImpl(true);
	}
	
    private void refreshImpl(boolean repopulateSubpanels) {
        String selectedEntitlementName = null;
        String selectedVDBName = null;
        int selectedVDBVersionNum = -1;
        int curSelectedRow = table.getSelectedRow();
        if (curSelectedRow >= 0) {
            curSelectedRow = table.convertRowIndexToModel(curSelectedRow);
            selectedEntitlementName = (String)table.getModel().getValueAt(
                    curSelectedRow, EntitlementsTableModel.ENTITLEMENT_COL_NUM);
            selectedVDBName = (String)table.getModel().getValueAt(
                    curSelectedRow, EntitlementsTableModel.VDB_COL_NUM);
            selectedVDBVersionNum = ((Integer)table.getModel().getValueAt(
                    curSelectedRow,
                    EntitlementsTableModel.VDB_VERS_COL_NUM)).intValue();
        }
        try {
            StaticUtilities.startWait();
            PanelsTree tree = PanelsTree.getInstance(getConnection());
            tree.setCursor(Cursor.getPredefinedCursor(Cursor.WAIT_CURSOR));
            repopulateTable(selectedEntitlementName, selectedVDBName,
                    selectedVDBVersionNum, repopulateSubpanels);
            tree.setCursor(Cursor.getPredefinedCursor(Cursor.DEFAULT_CURSOR));
            StaticUtilities.endWait();
        } catch (RuntimeException ex) {
            StaticUtilities.endWait();
            throw ex;
        }
    }

    public String getTitle() {
        return " Data Roles"; //$NON-NLS-1$
    }

	public ConnectionInfo getConnection() {
		return connection;
	}
	
    public java.util.List /*<MenuEntry>*/ getCurrentActions() {
        return currentActions;
    }

    public boolean doesEntitlementExist(String entName, String vdbName,
            int vdbVersion) throws AuthorizationException, ExternalException,
            ComponentNotFoundException {
        return dataSource.doesEntitlementExist(entName, vdbName, vdbVersion);
    }

    public void createNewEntitlement() {
        NewEntitlementController controller = null;
        try {
            controller = new NewEntitlementController(this, dataSource);
        } catch (Exception ex) {
            LogManager.logError(LogContexts.ENTITLEMENTS, ex,
                    "Error creating NewEntitlementController"); //$NON-NLS-1$
            ExceptionUtility.showMessage("Create New Role", ex); //$NON-NLS-1$
        }
        if (controller != null) {
            controller.go();
            if (controller.isCreated()) {
                EntitlementsTableRowData dataNodesEntitlement =
                        controller.getDataNodesEntitlement();
                String dataNodesEntName =
                        dataNodesEntitlement.getEntitlementName();
                if ((dataNodesEntName == null) ||
                        dataNodesEntName.equals(NewEntitlementConfirmationPanel.NONE)
                        || dataNodesEntName.equals("")) { //$NON-NLS-1$
                    dataNodesEntitlement = null;
                }
                EntitlementsTableRowData principalsEntitlement =
                        controller.getPrincipalsEntitlement();
                String principalsEntName =
                        principalsEntitlement.getEntitlementName();
                if ((principalsEntName == null) ||
                        principalsEntName.equals(NewEntitlementConfirmationPanel.NONE)
                        || principalsEntName.equals("")) { //$NON-NLS-1$
                    principalsEntitlement = null;
                }
                boolean created = false;
                StaticUtilities.startWait();
                try {
                    entitlementManager.createNewEntitlement(
                    		controller.getEntitlementName(),
                            controller.getEntitlementDescription(),
                            controller.getVDBName(), controller.getVDBVersion(),
                            dataNodesEntitlement, principalsEntitlement);
                    created = true;
                } catch (Exception ex) {
                    StaticUtilities.endWait();
                    LogManager.logError(LogContexts.ENTITLEMENTS, ex,
                            "Error creating new role"); //$NON-NLS-1$
                    ExceptionUtility.showMessage("Create New Role", ex); //$NON-NLS-1$
                }
                if (created) {
                    try {
                        //Before repopulating table to include new entitlement,
                        //set flag to prevent prompting for saving changes to
                        //entitlement already being displayed.  If there were
                        //any changes, prompt has already been made and response
                        //was "No".
                        showingNewlyCreatedEntitlement = true;
                        repopulateTable( controller.getEntitlementName(),
                                controller.getVDBName(),
                                controller.getVDBVersion(), true );
                        showingNewlyCreatedEntitlement = false;
//                        EntitlementsTableRowData data =
                                new EntitlementsTableRowData(
                                controller.getEntitlementName(),
                                controller.getVDBName(),
                                controller.getVDBVersion());
                        StaticUtilities.endWait();
                    } catch (RuntimeException ex) {
                        StaticUtilities.endWait();
                        throw ex;
                    }
                }
            }
        }
    }

    private void repopulateTable( String selectionName, String vdbName,
            int vdbVersion, boolean repopulateSubpanels ) {
        if (canModify) {
            deleteButton.setEnabled(false);
            deleteAction.setEnabled(false);
        }
        try {
            EntitlementsTableRowData[] rows = dataSource.getEntitlements();
            tableModel.init(rows);
            programmaticTableSelection = true;
            table.sort();
            programmaticTableSelection = false;
            int modelSelectionRow = -1;
            if ((selectionName != null) && (vdbName != null) && (vdbVersion >= 0)) {
                modelSelectionRow = modelRowForEntitlement(selectionName, vdbName,
                        vdbVersion);
            }
            if (modelSelectionRow >= 0) {
                programmaticTableSelection = true;
                int viewSelectionRow = modelRowToViewRow(modelSelectionRow);
                table.getSelectionModel().setSelectionInterval(viewSelectionRow,
                        viewSelectionRow);
                programmaticTableSelection = false;
                if (repopulateSubpanels) {
                    showDetailForEntitlement(selectionName, vdbName,
                            vdbVersion);
                }
                forceRepaint();
            } else {
            	programmaticTableSelection = true;
                table.clearSelection();
                programmaticTableSelection = false;
                forceRepaint();
            }
        } catch (Exception ex) {
            LogManager.logError(LogContexts.ENTITLEMENTS, ex,
                    "Error populating roles table"); //$NON-NLS-1$
            ExceptionUtility.showMessage("Re-populate Roles Table", ex); //$NON-NLS-1$
        }
    }

    public void repaintNeeded() {
//        StaticUtilities.jiggleSplitter(splitPane);
        final int splitterLoc = splitPane.getDividerLocation();
        iSplitterChange = iSplitterChange * -1;


        splitPane.setDividerLocation(splitterLoc + iSplitterChange);
        detailPanel.repaint();
        //splitPane.setDividerLocation(splitterLoc );

    }

    private int modelRowForEntitlement(String name, String vdbName, int vdbVersion) {
        int matchRow = -1;
        if (name != null) {
            int numRows = table.getRowCount();
            int i = 0;
            while ((i < numRows) && (matchRow < 0)) {
                String entName = (String)table.getModel().getValueAt(i,
                        EntitlementsTableModel.ENTITLEMENT_COL_NUM);
                if (entName.equals(name)) {
                    String vdb = (String)table.getModel().getValueAt(i,
                            EntitlementsTableModel.VDB_COL_NUM);
                    if (vdb.equals(vdbName)) {
                        int vdbVers = ((Integer)table.getModel().getValueAt(i,
                                EntitlementsTableModel.VDB_VERS_COL_NUM)).intValue();
                        if (vdbVers == vdbVersion) {
                            matchRow = i;
                        }
                    }
                }
                if (matchRow < 0) {
                    i++;
                }
            }
        }
        return matchRow;
    }

    private int modelRowToViewRow(int modelRow) {
        int viewRow = table.convertRowIndexToView(modelRow);
        return viewRow;
    }

    private void showDetailForEntitlement(String entName,
            String vdbName, int vdbVersion) {
        try {
            boolean proceeding = true;
            if (!showingNewlyCreatedEntitlement) {
                if (havePendingChanges()) {
                    proceeding = finishUp();
                }
            }
            if (proceeding) {
                EntitlementInfo info = null;
                try {
                     info = dataSource.getEntitlementInfo(entName, vdbName,
                            vdbVersion);
                } catch (Exception ex) {
                    LogManager.logError(LogContexts.ENTITLEMENTS, ex,
                            "Error retrieving role details"); //$NON-NLS-1$
                    ExceptionUtility.showMessage("Retrieve Role Details", //$NON-NLS-1$
                            ex);
                    //In this case, we will reselect the old entitlement, because
                    //the displayed detail will otherwise be in an unpredictable
                    //state.
                	programmaticTableSelection = true;
                	table.getSelectionModel().setSelectionInterval(
                			oldSelectedModelIndex, oldSelectedModelIndex);
                	programmaticTableSelection = false;
                }
                if (info != null) {
                    detailPanel.populate(info, false, canModify);
                }
            } else {
                programmaticTableSelection = true;
                table.getSelectionModel().setSelectionInterval(oldSelectedModelIndex,
                        oldSelectedModelIndex);
                programmaticTableSelection = false;
            }
            if (canModify) {
                deleteButton.setEnabled(true);
                deleteAction.setEnabled(true);
            }
            forceRepaint();
        } catch( Exception e ) {
            ExceptionUtility.showMessage( "Failed populating the detail panel ", //$NON-NLS-1$
                    e );
        } finally {
            forceRepaint();
        }
    }

    private void deleteRequested() {
        boolean deleting = DialogUtility.yesNoDialog(
                ConsoleMainFrame.getInstance(),
                "Delete role \"" + detailPanel.getEntitlementName() + //$NON-NLS-1$
                "\" for VDB \"" + detailPanel.getVDBName() + //$NON-NLS-1$
                "\", version " + detailPanel.getVDBVersion() + "?", //$NON-NLS-1$ //$NON-NLS-2$
                DialogUtility.CONFIRM_DELETE_HDR);
        if (deleting) {
            boolean continuing = true;
            try {
                dataSource.deleteEntitlement(detailPanel.getPolicyID());
            } catch (Exception ex) {
                LogManager.logError(LogContexts.ENTITLEMENTS, ex,
                        "Error deleting role"); //$NON-NLS-1$
                ExceptionUtility.showMessage("Delete Role", ex); //$NON-NLS-1$
                continuing = false;
            }
            if (continuing) {
                clearDetail();
                programmaticTableSelection = true;
                repopulateTable(null, null, 0, true);
                programmaticTableSelection = false;
            }
        }
    }

    private void clearDetail() {
        detailPanel.clear();
        forceRepaint();
    }

    private void forceRepaint() {
        StaticUtilities.jiggleSplitter(splitPane);
        detailPanel.forceRepaint();
    }

    public boolean havePendingChanges() {
        boolean haveChanges = false;
        if (detailPanel != null) {
            haveChanges = detailPanel.havePendingChanges();
        }
        return haveChanges;
    }

    public boolean finishUp() {
        return detailPanel.finishUp();
    }

    //=========================================================
    // Action classes
    //=========================================================

    /**
     * Create new entitlement Action
     */
    class EntitlementsCreateAction extends AbstractAction {
        private EntitlementsPanel caller;

        public EntitlementsCreateAction(EntitlementsPanel cllr) {
            super("Create Role..."); //$NON-NLS-1$
            caller = cllr;
        }

        public void actionPerformed(ActionEvent ev) {
            if (! caller.anyVDBs()){
                JOptionPane.showMessageDialog(EntitlementsPanel.this, message);
            }else{
                boolean proceeding = true;
                if (caller.havePendingChanges()) {
                    proceeding = caller.finishUp();
                }
                if (proceeding) {
                    caller.createNewEntitlement();
                }
            }
        }
    }//end EntitlementsCreateAction

    /**
     * Delete entitlement Action
     */
    class EntitlementsDeleteAction extends AbstractAction {
        private EntitlementsPanel caller;

        public EntitlementsDeleteAction(EntitlementsPanel cllr) {
            super("Delete Role"); //$NON-NLS-1$
            caller = cllr;
        }

        public void actionPerformed(ActionEvent ev) {
            caller.deleteRequested();
        }
    }//end EntitlementsDeleteAction
}//end EntitlementsPanel
