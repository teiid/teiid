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

package com.metamatrix.console.ui.views.extensionsource;

import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.util.ArrayList;

import javax.swing.AbstractAction;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JSplitPane;
import javax.swing.ListSelectionModel;
import javax.swing.event.ListSelectionEvent;
import javax.swing.event.ListSelectionListener;

import com.metamatrix.common.log.LogManager;
import com.metamatrix.console.connections.ConnectionInfo;
import com.metamatrix.console.models.ExtensionSourceManager;
import com.metamatrix.console.notification.RuntimeUpdateNotification;
import com.metamatrix.console.ui.NotifyOnExitConsole;
import com.metamatrix.console.ui.layout.BasePanel;
import com.metamatrix.console.ui.layout.MenuEntry;
import com.metamatrix.console.ui.layout.WorkspacePanel;
import com.metamatrix.console.ui.util.AbstractPanelAction;
import com.metamatrix.console.util.DialogUtility;
import com.metamatrix.console.util.ExceptionUtility;
import com.metamatrix.console.util.ExternalException;
import com.metamatrix.console.util.LogContexts;
import com.metamatrix.console.util.Refreshable;
import com.metamatrix.platform.admin.api.ExtensionSourceAdminAPI;
import com.metamatrix.toolbox.ui.widget.ButtonWidget;
import com.metamatrix.toolbox.ui.widget.Splitter;
import com.metamatrix.toolbox.ui.widget.TableWidget;

public class ExtensionSourcesPanel extends BasePanel implements WorkspacePanel,
        NotifyOnExitConsole, ExtensionSourceDetailListener, Refreshable {
    public final static String EXTENSION_MODULES_INITIAL_FOLDER_KEY =
            "metamatrix.console.lastextensionmodulesdirectory";
    private final static int MAX_DESCRIPTION_LENGTH =
            ExtensionSourceAdminAPI.SOURCE_DESCRIPTION_LENGTH_LIMIT;
    private ExtensionSourceManager manager;
    private boolean canModify;
    private ConnectionInfo connection;
    private TableWidget table;
    private ExtensionSourcesTableModel tableModel;
    private ExtensionSourceDetailPanel detail;
    private ButtonWidget addButton;
    private String detailedModule;
    private JSplitPane splitPane;
    private boolean programmaticSelectionChange = false;
    private boolean repopulatingTableAfterAdd = false;
    private java.util.List actions = new ArrayList();
    private AbstractAction addAction;
    
    private boolean clearingSelection = false;
    private boolean doingRefresh = false;
    
    private boolean populatingTable;
    

    public ExtensionSourcesPanel(ExtensionSourceManager mgr, 
    		boolean modifiable, ConnectionInfo conn) {
        super();
        manager = mgr;
        canModify = modifiable;
        this.connection = conn;
        init();
    }

    private void init() {
        tableModel = new ExtensionSourcesTableModel();
        table = new ExtensionModulesTable(tableModel);
        table.getSelectionModel().setSelectionMode(
                ListSelectionModel.SINGLE_SELECTION);
        table.getSelectionModel().addListSelectionListener(
                new ListSelectionListener() {
            public void valueChanged(ListSelectionEvent ev) {
                if (!ev.getValueIsAdjusting()) {
                    tableSelectionChanged();
                }
            }
        });

        if (canModify) {
            addAction = new PanelAction(PanelAction.ADD);
            addAction.setEnabled(true);
            actions.add(new MenuEntry(MenuEntry.ACTION_MENUITEM,
                                      addAction));
            
            
            addButton = new ButtonWidget("   Add...   ");
            addButton.addActionListener(new ActionListener() {
                public void actionPerformed(ActionEvent ev) {
                    addPressed();
                }
            });
            
//            refreshAction = new PanelAction(PanelAction.REFRESH);
//            refreshAction.setEnabled(true);
//            refreshAction.putValue(Action.SMALL_ICON, 
//                    DeployPkgUtils.getIcon("icon.refresh")); //$NON-NLS-1$
//
//            actions.add(new MenuEntry(MenuEntry.VIEW_REFRESH_MENUITEM,
//                                      refreshAction));
            
            
      	}

        detail = new ExtensionSourceDetailPanel(this, canModify,
                MAX_DESCRIPTION_LENGTH);
        layoutStuff();

        //put in "if" block to avoid populating when running from main()
        if (manager != null) {
            populateTable(null, null);
        }
	}

    private void layoutStuff() {
        JPanel buttonsPanel = null;
        if (canModify) {
            buttonsPanel = new JPanel();
            GridBagLayout bl = new GridBagLayout();
            buttonsPanel.setLayout(bl);
            buttonsPanel.add(addButton);
            bl.setConstraints(addButton, new GridBagConstraints(0, 0, 1, 1,
                    0.0, 1.0, GridBagConstraints.CENTER, GridBagConstraints.NONE,
                    new Insets(8, 0, 8, 0), 0, 0));
        }

        JPanel upperPanel = new JPanel();
        GridBagLayout ul = new GridBagLayout();
        upperPanel.setLayout(ul);
        JScrollPane tableSP = new JScrollPane(table);
        upperPanel.add(tableSP);
        ul.setConstraints(tableSP, new GridBagConstraints(0, 0, 1, 1, 1.0, 1.0,
                GridBagConstraints.CENTER, GridBagConstraints.BOTH,
                new Insets(2, 2, 2, 4), 0, 0));
        if (canModify) {
            upperPanel.add(buttonsPanel);
            ul.setConstraints(buttonsPanel, new GridBagConstraints(1, 0, 1, 1,
                    0.0, 0.0, GridBagConstraints.CENTER,
                    GridBagConstraints.VERTICAL, new Insets(30, 8, 0, 8), 0, 0));
        }

        splitPane = new Splitter(JSplitPane.VERTICAL_SPLIT, true,
                upperPanel, detail);
        splitPane.setOneTouchExpandable(true);
        GridBagLayout layout = new GridBagLayout();
        this.setLayout(layout);
        this.add(splitPane);
        layout.setConstraints(splitPane, new GridBagConstraints(0, 0, 1, 1,
                1.0, 1.0, GridBagConstraints.CENTER, GridBagConstraints.BOTH,
                new Insets(0, 0, 0, 0), 0, 0));
    }

    public void postRealize() {
        splitPane.setDividerLocation(0.5);
    }

    public void populateTable(String moduleToSelect,
            ExtensionSourceDetailInfo[] modules) {
        
        try {
            populatingTable = true;
            ExtensionSourceDetailInfo[] orderedModules;
            if (modules != null) {
                orderedModules = modules;
            } else {
                orderedModules = manager.getModules();
            }
            //Remove all existing rows
            int numRows = tableModel.getRowCount();
            clearingSelection = true;
            table.getSelectionModel().clearSelection();
            clearingSelection = false;
            for (int i = numRows - 1; i >= 0; i--) {
                tableModel.removeRow(i);
                
            }
            //Insert new rows
            for (int i = 0; i < orderedModules.length; i++) {
                Object[] rowValues = new Object[ExtensionSourcesTableModel.NUM_COLUMNS];
                rowValues[ExtensionSourcesTableModel.SOURCE_NAME_COLUMN] =
                        orderedModules[i].getModuleName();
                rowValues[ExtensionSourcesTableModel.SOURCE_TYPE_COLUMN] =
                        orderedModules[i].getModuleType();
//                rowValues[ExtensionSourcesTableModel.ENABLED_COLUMN] =
//                        new Boolean(orderedModules[i].isEnabled());
                tableModel.insertRow(i, rowValues);
            }
            populatingTable = false;
            if (moduleToSelect == null) {
                detail.setInfo(null);
			} else {
                int selectionRow = rowForModule(moduleToSelect);
                if (selectionRow >= 0) {
                    table.getSelectionModel().setSelectionInterval(selectionRow,
                            selectionRow);
                }
            }
        } catch (Exception ex) {
            LogManager.logError(LogContexts.EXTENSION_SOURCES, ex,
                    "Error populating extension modules table.");
            ExceptionUtility.showMessage("Error populating extension modules table",
                    ex);
        }
        populatingTable = false;
    }

    public String getTitle() {
        return "Extension Modules";
    }

	public ConnectionInfo getConnection() {
		return connection;
	}
    
    public void receiveUpdateNotification(RuntimeUpdateNotification notification) {
    	//TODO
    }
	
    public java.util.List /*<Action>*/ resume() {
        return actions;
    }

    /**
     * Prompt and if requested do any pending changes on the detail panel.  Return
     * true if proceeding with new business (e.g. putting up add-wizard),
     * false if cancelling.
     */
    private boolean doDetailPendingChanges() {
        boolean continuing = true;
        String msg = "Save changes to extension module \"" + detailedModule + "\"?";
        int response = DialogUtility.showPendingChangesDialog(msg,
        		manager.getConnection().getURL(),
        		manager.getConnection().getUser());
        switch (response) {
            case DialogUtility.YES:
                detail.saveChanges();
                continuing = true;
                break;
            case DialogUtility.NO:
                continuing = true;
                break;
            case DialogUtility.CANCEL:
                continuing = false;
                break;
        }
        return continuing;
    }

    public boolean havePendingChanges() {
        boolean havePending;
        if (doingRefresh) {
            havePending = false;
        } else {
            havePending = detail.havePendingChanges();
        }
        return havePending;
    }

    public boolean finishUp() {
        boolean stayingHere = false;
        if (!doingRefresh) {
            if (detail.havePendingChanges()) {
                stayingHere = (!doDetailPendingChanges());
            }
        }
        return (!stayingHere);
    }

    private void tableSelectionChanged() {
        if (!programmaticSelectionChange && !populatingTable) {
            boolean proceeding = true;
            if ((!repopulatingTableAfterAdd) && detail.havePendingChanges()) {
                proceeding = doDetailPendingChanges();
            }
            if (proceeding) {
            	int selectedRow;
            	if (clearingSelection) {
            		selectedRow = -1;
            	} else {
            		selectedRow = 
            				table.getSelectionModel().getLeadSelectionIndex();
            	}
                displayDetailForRow(selectedRow);
            } else {
                int prevSelectedRow = rowForModule(detailedModule);
                programmaticSelectionChange = true;
                table.getSelectionModel().setSelectionInterval(prevSelectedRow,
                        prevSelectedRow);
                programmaticSelectionChange = false;
            }
        }
    }

    private void displayDetailForRow(int row) {
        String name = getNameForRow(row);
        ExtensionSourceDetailInfo detailInfo = null;
        if (row >= 0) {
            try {
                detailInfo = manager.getDetailForModule(name);
            } catch (Exception ex) {
                LogManager.logError(LogContexts.EXTENSION_SOURCES, ex,
                        "Error retrieving details for extension module.");
                ExceptionUtility.showMessage("Retrieve details for extension module",
                        ex);
            }
            if (detailInfo != null) {
                detailedModule = detailInfo.getModuleName();
                detail.setInfo(detailInfo);
            }
        } else {
            detail.setInfo(detailInfo);
        }
    }

    /**
     * Method added to be called by main().
     */
    public void displayDetail(ExtensionSourceDetailInfo detailInfo) {
        detail.setInfo(detailInfo);
    }

    private String getNameForRow(int row) {
        String name = null;
        if (row >= 0) {
            int modelRow = table.convertRowIndexToModel(row);
            name = (String)table.getModel().getValueAt(modelRow,
                    ExtensionSourcesTableModel.SOURCE_NAME_COLUMN);
        }
        return name;
    }

    private int rowForModule(String moduleName) {
        int matchRow = -1;
        int i = 0;
        int numRows = table.getRowCount();
        while ((i < numRows) && (matchRow < 0)) {
            String moduleNameForRow = getNameForRow(i);
            if (moduleNameForRow.equals(moduleName)) {
                matchRow = i;
            } else {
                i++;
            }
        }
        return matchRow;
    }

    public void deleteRequested() {
        try {
            manager.deleteModule(detailedModule);
            detailedModule = null;
            populateTable(null, null);
        } catch (Exception ex) {
            LogManager.logError(LogContexts.EXTENSION_SOURCES, ex,
                    "Error attempting to delete an extension module.");
            ExceptionUtility.showMessage("Error deleting extension module", ex);
        }
    }

    public void exportRequested() {
        doExportModuleWizard();
    }

    public void replaceRequested() {
        doReplaceModuleWizard();
    }

    public boolean modifyRequested(String newModuleName, String newDescription,
            Boolean enabled) {
        boolean modified = false;
        try {
            manager.modifyModule(detailedModule, newModuleName, newDescription,
                    enabled, null);
            modified = true;
            if (newModuleName != null) {
                int selectedRow = table.getSelectedRow();
                table.getModel().setValueAt(newModuleName, selectedRow,
                        ExtensionSourcesTableModel.SOURCE_NAME_COLUMN);
                detailedModule = newModuleName;
            }
//            if (enabled != null) {
//                int rowForChange = this.rowForModule(detailedModule);
//                if (rowForChange >= 0) {
//                    table.getModel().setValueAt(enabled, rowForChange,
//                            ExtensionSourcesTableModel.ENABLED_COLUMN);
//                }
//            }
        } catch (Exception ex) {
            LogManager.logError(LogContexts.EXTENSION_SOURCES, ex,
                    "Error attempting to modify an extension module.");
            ExceptionUtility.showMessage("Error modifying extension module", ex);
        }
        if (modified) {
            try {
                ExtensionSourceDetailInfo detailInfo =
                        manager.getDetailForModule(detailedModule);
                detail.changeLastUpdatedInfo(detailInfo.getLastUpdated(),
                        detailInfo.getLastUpdatedBy());
            } catch (Exception ex) {
            }
        }
        return modified;
    }

    private void addPressed() {
        boolean proceeding = true;
        if (detail.havePendingChanges()) {
            proceeding = doDetailPendingChanges();
        }
        if (proceeding) {
            doAddNewModuleWizard();
        }
    }

    private void doAddNewModuleWizard() {
        String[] moduleTypes = null;
        try {
            //TODO-- retrieve module types first time only, then cache
            moduleTypes = manager.getModuleTypes();
        } catch (Exception ex) {
            LogManager.logError(LogContexts.EXTENSION_SOURCES, ex,
                    "Error retrieving extension module types");
            ExceptionUtility.showMessage("Error retrieving extension module types",
                    ex);
        }
        if (moduleTypes != null) {
            ExtensionSourceAdder adder = new ExtensionSourceAdder(manager,
                    moduleTypes);
            String moduleName = adder.go();
            if (moduleName != null) {
                repopulatingTableAfterAdd = true;
                populateTable(moduleName, null);
                repopulatingTableAfterAdd = false;
            }
        }
    }

    private void doExportModuleWizard() {
        ExtensionSourceExporter exporter = new ExtensionSourceExporter(
                detailedModule, manager);
//        boolean exported =
            exporter.go();
    }

    private void doReplaceModuleWizard() {
        ExtensionSourceReplacer replacer = new ExtensionSourceReplacer(
                detailedModule, manager);
        boolean replaced = replacer.go();
        if (replaced) {
            try {
                ExtensionSourceDetailInfo detailInfo =
                        manager.getDetailForModule(detailedModule);
                detail.changeLastUpdatedInfo(detailInfo.getLastUpdated(),
                        detailInfo.getLastUpdatedBy());
            } catch (Exception ex) {
            }
        }
    }

    public void refresh() {
        doingRefresh = true;
        this.populateTable(detailedModule, null);
        doingRefresh = false;
    }
    
    
    ///////////////////////////////////////////////////////////////////////////
    // INNER CLASSES
    ///////////////////////////////////////////////////////////////////////////

    private class PanelAction extends AbstractPanelAction {
        public static final int ADD = 0;
        public static final int REFRESH = 1;

        public PanelAction(int theType) {
            super(theType);
            if (theType == ADD) {
               putValue(NAME, "Add...");
               putValue(SHORT_DESCRIPTION, "Add Extension Modules");  //$NON-NLS-1$
            }
            else if (theType == REFRESH) {
                putValue(NAME, "Refresh"); //$NON-NLS-1$
                putValue(SHORT_DESCRIPTION, "Query for current list of Extension Modules"); //$NON-NLS-1$
            }
        }
        protected void actionImpl(ActionEvent theEvent)
            throws ExternalException {

            if (type == ADD) {
                doAddNewModuleWizard();
            }
            else if (type == REFRESH) {
                refresh();
            }
        }
    }
    
}//end ExtensionSourcesPanel




class ExtensionModulesTable extends TableWidget {
    public ExtensionModulesTable(ExtensionSourcesTableModel model) {
        super(model, false);
    }

    public boolean isCellEditable(int row, int column) {
        return false;
    }
}//end ExtensionModulesTable
