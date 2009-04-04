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

package com.metamatrix.console.ui.views.connectorbinding;

import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.GridLayout;
import java.awt.Insets;
import java.awt.event.ActionEvent;
import java.io.File;
import java.io.FileOutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.Vector;

import javax.swing.AbstractButton;
import javax.swing.Action;
import javax.swing.JOptionPane;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JSplitPane;
import javax.swing.JTabbedPane;
import javax.swing.ListSelectionModel;
import javax.swing.border.Border;
import javax.swing.event.ChangeEvent;
import javax.swing.event.ChangeListener;
import javax.swing.event.ListSelectionEvent;
import javax.swing.event.ListSelectionListener;
import javax.swing.table.TableColumn;

import com.metamatrix.common.config.api.ComponentType;
import com.metamatrix.common.config.api.ConnectorBinding;
import com.metamatrix.common.config.util.ConfigurationPropertyNames;
import com.metamatrix.common.config.xml.XMLConfigurationImportExportUtility;
import com.metamatrix.common.log.LogManager;
import com.metamatrix.common.tree.directory.DirectoryEntry;
import com.metamatrix.common.tree.directory.FileSystemFilter;
import com.metamatrix.common.tree.directory.FileSystemView;
import com.metamatrix.console.connections.ConnectionInfo;
import com.metamatrix.console.models.ConfigurationManager;
import com.metamatrix.console.models.ConnectorManager;
import com.metamatrix.console.models.ModelManager;
import com.metamatrix.console.models.RuntimeMgmtManager;
import com.metamatrix.console.models.VdbManager;
import com.metamatrix.console.notification.AddedConnectorBindingNotification;
import com.metamatrix.console.notification.RuntimeUpdateNotification;
import com.metamatrix.console.security.UserCapabilities;
import com.metamatrix.console.ui.ViewManager;
import com.metamatrix.console.ui.layout.BasePanel;
import com.metamatrix.console.ui.layout.ConsoleMainFrame;
import com.metamatrix.console.ui.layout.MenuEntry;
import com.metamatrix.console.ui.layout.WorkspacePanel;
import com.metamatrix.console.ui.util.AbstractPanelAction;
import com.metamatrix.console.ui.util.CenteredOptionPane;
import com.metamatrix.console.ui.util.ConsoleConstants;
import com.metamatrix.console.ui.views.deploy.event.ConfigurationChangeEvent;
import com.metamatrix.console.ui.views.deploy.event.ConfigurationChangeListener;
import com.metamatrix.console.ui.views.deploy.util.DeployPkgUtils;
import com.metamatrix.console.util.DialogUtility;
import com.metamatrix.console.util.ExceptionUtility;
import com.metamatrix.console.util.LogContexts;
import com.metamatrix.console.util.Refreshable;
import com.metamatrix.console.util.StaticProperties;
import com.metamatrix.console.util.StaticUtilities;
import com.metamatrix.core.vdb.VDBStatus;
import com.metamatrix.metadata.runtime.model.BasicVirtualDatabase;
import com.metamatrix.toolbox.preference.UserPreferences;
import com.metamatrix.toolbox.ui.widget.ButtonWidget;
import com.metamatrix.toolbox.ui.widget.DialogWindow;
import com.metamatrix.toolbox.ui.widget.DirectoryChooserPanel;
import com.metamatrix.toolbox.ui.widget.Splitter;
import com.metamatrix.toolbox.ui.widget.TableWidget;
import com.metamatrix.toolbox.ui.widget.TitledBorder;
import com.metamatrix.toolbox.ui.widget.table.DefaultTableComparator;
import com.metamatrix.toolbox.ui.widget.table.EnhancedTableColumn;
import com.metamatrix.toolbox.ui.widget.table.EnhancedTableColumnModel;

public class ConnectorBindingPanel extends BasePanel implements
                                                    ConfigurationChangeListener,
                                                    WorkspacePanel,
                                                    Refreshable {

    public final static int CONNECTOR_BINDING_COL_NUM = 0;
    public final static int CONNECTOR_COL_NUM = 1;
    public final static int DESCRIPTION_COL_NUM = 2;

    public final static int DETAILS_TAB_NUM = 0;
    public final static int PROPERTIES_TAB_NUM = 1;
    public final static int VDBS_TAB_NUM = 2;

    private ConnectorBindingPanelAction actionNewConnectorBinding;
    private ConnectorBindingPanelAction actionImportConnectorBinding;
    private ConnectorBindingPanelAction actionExportConnectorBinding;
    private ConnectorBindingPanelAction actionDeleteConnectorBinding;
    private ConnectorBindingPanelAction actionDupConnectorBinding;

    private static final String NEW_CONNECTOR_TITLE = "New..."; //$NON-NLS-1$
    private static final String IMPORT_CONNECTOR_TITLE = "Import..."; //$NON-NLS-1$
    private static final String EXPORT_CONNECTOR_TITLE = "Export..."; //$NON-NLS-1$
    private static final String DELETE_CONNECTOR_TITLE = "Delete"; //$NON-NLS-1$
    private static final String DUP_CONNECTOR_TITLE = "Clone"; //$NON-NLS-1$

    private static final String EXPORT_EXTENSION = DeployPkgUtils.getString("ecbp.export.extension"); //$NON-NLS-1$

    private ConnectionInfo connection;

    private TableWidget table;
    private com.metamatrix.toolbox.ui.widget.table.DefaultTableModel tableModel;
    private ButtonWidget newButton;
    private ButtonWidget importButton;
    private ButtonWidget exportButton;
    private ButtonWidget deleteButton;
    private ButtonWidget dupButton;
    private BindingDetailsPanel detailsPanel;
    private BindingPropertiesPanel propertiesPanel;
    private BindingVDBsPanel vdbsPanel;

    /** List <ConnectorBinding> */
    private List selectedBindings = new ArrayList();

    private ListSelectionListener listSelectionListener;
    
    private JTabbedPane tabbedPane;
    private JSplitPane splitPane;
    private java.util.List /* <Action> */currentActions = new ArrayList();
    private HashMap nameConnectorBindingMap = null;
    private boolean canModify;

    public ConnectorBindingPanel(ConnectionInfo conn) {
        super();
        this.connection = conn;
        init();
    }

    private ConnectorManager getConnectorManager() {
        return ModelManager.getConnectorManager(connection);
    }

    private ConfigurationManager getConfigurationManager() {
        return ModelManager.getConfigurationManager(connection);
    }

    private VdbManager getVdbManager() {
        return ModelManager.getVdbManager(connection);
    }

    private void init() {

        UserCapabilities cap = null;
        try {
            cap = UserCapabilities.getInstance();
            canModify = cap.canModifyConnectorBindings(connection);

            if (canModify) {
                getConfigurationManager().addConfigurationChangeListener(this);
            }
        } catch (Exception ex) {
            // Cannot occur
        }

        createActions();

        newButton = new ButtonWidget(NEW_CONNECTOR_TITLE);
        importButton = new ButtonWidget(IMPORT_CONNECTOR_TITLE);
        exportButton = new ButtonWidget(EXPORT_CONNECTOR_TITLE);
        deleteButton = new ButtonWidget(DELETE_CONNECTOR_TITLE);
        dupButton = new ButtonWidget(DUP_CONNECTOR_TITLE);

        // Can export if user has System.READ role
        actionExportConnectorBinding.setEnabled(true);
        if (canModify) {
            setup(newButton, actionNewConnectorBinding);
            actionNewConnectorBinding.setEnabled(true);

            setup(importButton, actionImportConnectorBinding);
            actionImportConnectorBinding.setEnabled(true);

            setup(exportButton, actionExportConnectorBinding);

            setup(deleteButton, actionDeleteConnectorBinding);
            actionDeleteConnectorBinding.setEnabled(false);

            setup(dupButton, actionDupConnectorBinding);
            actionDupConnectorBinding.setEnabled(false);

        } else {
            actionNewConnectorBinding.setEnabled(false);
            actionImportConnectorBinding.setEnabled(false);
            actionDeleteConnectorBinding.setEnabled(false);
            actionDupConnectorBinding.setEnabled(false);

        }

        // NOTE-- columns must be in order by indices above
        tableModel = new com.metamatrix.toolbox.ui.widget.table.DefaultTableModel(new Vector(Arrays.asList(new String[] {
            "Connector Binding", "Connector Type"})), 0); //$NON-NLS-1$ //$NON-NLS-2$
        table = new TableWidget(tableModel, true);
        table.setEditable(false);
        table.sizeColumnsToFitContainer(CONNECTOR_BINDING_COL_NUM);
        table.sizeColumnsToFitContainer(CONNECTOR_COL_NUM);
        table.setSelectionMode(ListSelectionModel.MULTIPLE_INTERVAL_SELECTION);
        DefaultTableComparator dtcComparator = (DefaultTableComparator)table.getComparator();
        dtcComparator.setIgnoresCase(true);

        // set the binding name column to be sorted by default
        EnhancedTableColumnModel columnModel = (EnhancedTableColumnModel)table.getColumnModel();
        TableColumn firstColumn = columnModel.getColumn(CONNECTOR_BINDING_COL_NUM);
        columnModel.setColumnSortedAscending((EnhancedTableColumn)firstColumn, true);

        JScrollPane tableSP = new JScrollPane(table);

        GridBagLayout layout = new GridBagLayout();
        setLayout(layout);

        JPanel buttonsPanel = new JPanel();
        buttonsPanel.setLayout(new GridLayout(1, 4, 5, 5));
        buttonsPanel.add(newButton);
        buttonsPanel.add(dupButton);

        buttonsPanel.add(importButton);
        buttonsPanel.add(exportButton);
        buttonsPanel.add(deleteButton);

        tabbedPane = new JTabbedPane();
        detailsPanel = new BindingDetailsPanel(connection);
        propertiesPanel = new BindingPropertiesPanel(canModify, connection);
        vdbsPanel = new BindingVDBsPanel(connection);

        // NOTE-- tabs MUST be inserted in order by indices above
        tabbedPane.addTab("Details", detailsPanel); //$NON-NLS-1$
        tabbedPane.addTab("Properties", propertiesPanel); //$NON-NLS-1$
        tabbedPane.addTab("VDBs", vdbsPanel); //$NON-NLS-1$
        tabbedPane.addChangeListener(new ChangeListener() {

            public void stateChanged(ChangeEvent ev) {
                selectedTabChanged();
            }
        });
        splitPane = new Splitter(JSplitPane.VERTICAL_SPLIT, true, tableSP, tabbedPane);

        add(splitPane);
        layout.setConstraints(splitPane, new GridBagConstraints(0, 0, 1, 1, 1.0, 1.0, GridBagConstraints.CENTER,
                                                                GridBagConstraints.BOTH, new Insets(5, 5, 5, 5), 0, 0));
        if (canModify) {
            add(buttonsPanel);
            layout.setConstraints(buttonsPanel, new GridBagConstraints(0, 1, 1, 1, 0.0, 0.0, GridBagConstraints.CENTER,
                                                                       GridBagConstraints.NONE, new Insets(5, 5, 5, 5), 0, 0));
        }

        listSelectionListener = new ListSelectionListener() {
            public void valueChanged(ListSelectionEvent ev) {
                tableSelectionChanged();
            }
        };
        table.getSelectionModel().addListSelectionListener(listSelectionListener);
        refresh();
    }

    public void createActions() {
        actionNewConnectorBinding = new ConnectorBindingPanelAction(ConnectorBindingPanelAction.NEW_CONNECTOR_BINDING, this);
        actionNewConnectorBinding.putValue(Action.NAME, NEW_CONNECTOR_TITLE);
        addActionToList(MenuEntry.ACTION_MENUITEM, actionNewConnectorBinding);

        actionDupConnectorBinding = new ConnectorBindingPanelAction(ConnectorBindingPanelAction.DUP_CONNECTOR_BINDING, this);
        actionDupConnectorBinding.putValue(Action.NAME, DUP_CONNECTOR_TITLE);
        addActionToList(MenuEntry.ACTION_MENUITEM, actionDupConnectorBinding);

        actionImportConnectorBinding = new ConnectorBindingPanelAction(ConnectorBindingPanelAction.IMPORT_CONNECTOR_BINDING, this);
        actionImportConnectorBinding.putValue(Action.NAME, IMPORT_CONNECTOR_TITLE);
        addActionToList(MenuEntry.ACTION_MENUITEM, actionImportConnectorBinding);

        actionExportConnectorBinding = new ConnectorBindingPanelAction(ConnectorBindingPanelAction.EXPORT_CONNECTOR_BINDING, this);
        actionExportConnectorBinding.putValue(Action.NAME, EXPORT_CONNECTOR_TITLE);
        addActionToList(MenuEntry.ACTION_MENUITEM, actionExportConnectorBinding);

        actionDeleteConnectorBinding = new ConnectorBindingPanelAction(ConnectorBindingPanelAction.DELETE_CONNECTOR_BINDING, this);
        actionDeleteConnectorBinding.putValue(Action.NAME, DELETE_CONNECTOR_TITLE);
        addActionToList(MenuEntry.ACTION_MENUITEM, actionDeleteConnectorBinding);
    }

    public void addActionToList(String sID,
                                Action act) {
        currentActions.add(new MenuEntry(sID, act));
    }

    private void setup(AbstractButton theButton,
                       AbstractPanelAction theAction) {
        theAction.addComponent(theButton);
    }

    private void populateTable() {
        tableModel.setNumRows(0);
        try {
            Collection bindings = getConnectorManager().getAllConnectorBindings();
            for (Iterator it = bindings.iterator(); it.hasNext();) {
                ConnectorBinding cb = (ConnectorBinding)it.next();
                Vector vec = new Vector(Arrays.asList(new Object[] {
                    null, null, null
                }));
                vec.setElementAt(cb.getName(), CONNECTOR_BINDING_COL_NUM);
                vec.setElementAt(cb.getComponentTypeID().getName(), CONNECTOR_COL_NUM);
                tableModel.addRow(vec);
            }

        } catch (Exception ex) {
            ExceptionUtility.showMessage("Retrieve basic connector binding info", ex); //$NON-NLS-1$
            LogManager.logError(LogContexts.CONNECTOR_BINDINGS, ex, "Error retrieving basic connector binding info."); //$NON-NLS-1$
        }

        forceTableSelection(getSelectedConnectorBindings());
    }

    /**
     * Set the widget to display as selected the specified list of bindings.
     * 
     * @param binding
     *            List<ConnectorBinding>
     * @since 4.2
     */
    private void forceTableSelection(List bindings) {
        table.getSelectionModel().clearSelection();

        Iterator iter = bindings.iterator();
        while (iter.hasNext()) {
            ConnectorBinding binding = (ConnectorBinding)iter.next();
            int rowForSelected = rowForBinding(binding.toString());
            if (rowForSelected >= 0) {
                table.getSelectionModel().addSelectionInterval(rowForSelected, rowForSelected);
                table.scrollRectToVisible(table.getCellRect(rowForSelected, 0, true));
            }
        }
    }

    public void postRealize() {
        splitPane.setDividerLocation(0.4);
        if (table.getRowCount() > 0) {
            table.setRowSelectionInterval(0, 0);
        }
    }

    public void newPressed() {
        newBindingWanted();
    }

    private void newBindingWanted() {
        ConnectorBinding newConnectorBindingDefn = doNewBindingWizard();
        if (newConnectorBindingDefn != null) {
            setSelectedConnectorBinding(newConnectorBindingDefn);
            refresh();
        }
    }

    private ConnectorBinding doNewBindingWizard() {
        NewBindingWizardController controller = new NewBindingWizardController(connection);
        ConnectorBinding binding = (ConnectorBinding)controller.runWizard();
        return binding;
    }

    public void deletePressed() {
        checkToDelete(selectedBindings);
//        if () {
//            showDeleteConfirmDialog();
//        } else {
//            showDeleteRefuseDialog();
//        }

    }

    /**
     * Check whether it's OK to delete all of the specified bindings.
     * 
     * @param selectedBindings
     *            List <ConnectorBinding>.
     * @return
     * @since 4.2
     */
    private void checkToDelete(List selectedBindings) {
        int totalVDBs = 0;
        Iterator iter = selectedBindings.iterator();
        RuntimeMgmtManager rmm = null;
        boolean connectedToDeletedVDB = false;
        while (iter.hasNext()) {
            ConnectorBinding definition = (ConnectorBinding)iter.next();

            Collection vdbCollection = null;
            try {
                vdbCollection = getVdbManager().getVDBsForConnectorBinding(definition.getRoutingUUID());

                if (vdbCollection == null || vdbCollection.isEmpty())  {
                    if (rmm == null) {
                        rmm = ModelManager.getRuntimeMgmtManager(this.connection);
                    }
                    if (rmm.isServiceInSystemState(definition.getName())) {
                        showDeleteRefuseServiceIsDeployedDialog();
                        return;
                    }

                }
                // See if the connector is deployed to a vdb marked for deletion
                Iterator vdbIter = vdbCollection.iterator();
                while(vdbIter.hasNext()) {
                	Object obj = vdbIter.next();
                	if(obj instanceof BasicVirtualDatabase) {
                		BasicVirtualDatabase vdb = (BasicVirtualDatabase)obj;
                		if(vdb.getStatus()==VDBStatus.DELETED) {
                			connectedToDeletedVDB = true;
                			break;
                		}
                	}
                }
                
            } catch (Exception e) {
                ExceptionUtility.showMessage("Failed testing conn bind where-used", //$NON-NLS-1$
                                             e);
            }
            
            totalVDBs += vdbCollection.size();
        }

        if (totalVDBs > 0) {
            showDeleteRefuseDialog(connectedToDeletedVDB);
        } else {
            showDeleteConfirmDialog();
        }
//        return (totalVDBs == 0);
    }

    public void dupPressed() {

        String newName = null;

        ConnectorBindingRenameDialog rename = null;
        ConnectorBinding binding = null;
        try {
            // should only be one in the list, and can only process the first
            List bindings = getSelectedConnectorBindings();
            if (bindings == null || bindings.isEmpty()) {
                StaticUtilities.displayModalDialogWithOK("Unable to Clone a Connector Binding", //$NON-NLS-1$
                                                         "Please select a connector binding before trying to clone"); //$NON-NLS-1$
                return;

            }
            Iterator iter = getSelectedConnectorBindings().iterator();
            binding = (ConnectorBinding)iter.next();

            rename = new ConnectorBindingRenameDialog(connection, binding.getFullName(), "Clone Connector Binding");//$NON-NLS-1$
            rename.pack();
            rename.show();

        } catch (Exception ex) {
            ExceptionUtility.showMessage("Error trying to rename Duplicate Connector Binding", //$NON-NLS-1$
                                         ex);
            return;
        }

        newName = rename.getNewName();
        if (newName == null) {
            return;
        }

        try {
            ConnectorBinding cb = getConnectorManager().createConnectorBinding(binding,
                                                                               newName,
                                                                               getConfigurationManager().getEditor());
            refresh();
            List l = new ArrayList(1);
            l.add(cb);
            forceTableSelection(l);
        } catch (Exception e) {
            ExceptionUtility.showMessage("Unable to Duplicate Connector Binding", //$NON-NLS-1$
                                         e);

        }

    }

    public void importPressed() {
        Collection bindings = doImportBindingWizard();
        if (bindings != null && bindings.size() > 0) {
            setSelectedConnectorBinding((ConnectorBinding)bindings.iterator().next());
            refresh();
        }
    }

    private Collection doImportBindingWizard() {
        ImportBindingWizardController controller = new ImportBindingWizardController(connection);
        Collection bindings = null;
        boolean bBindingWasCreated = controller.runWizard();
        if (bBindingWasCreated) {
            // newDefn = (ConnectorBinding)
            bindings = controller.getConnectorBindings();
            populateTable();
        }
        return bindings;
    }

    public void exportPressed() {
        doExportBinding();
    }

    private void doExportBinding() {
        if (selectedBindings.size() == 0) {
            StaticUtilities.displayModalDialogWithOK("Unable to Export Connector Binding", //$NON-NLS-1$
                                                     "Please select a connector binding before trying to export"); //$NON-NLS-1$
            return;
        }

        String[] extensions = (String[])DeployPkgUtils.getObject("ecbp.importexport.extensions"); //$NON-NLS-1$
        FileSystemView view = new FileSystemView();
        String dirTxt = (String)UserPreferences.getInstance().getValue(ConsoleConstants.CONSOLE_DIRECTORY_LOCATION_KEY);
        if (dirTxt != null) {
            try {
                view.setHome(view.lookup(dirTxt));
            } catch (Exception ex) {
                // Any exception that may occur on setting the initial view is
                // inconsequential. This is merely a convenience to the user.
            }
        }
        FileSystemFilter[] filters = null;
        if (extensions != null) {
            FileSystemFilter filter = new FileSystemFilter(view, extensions,
                                                           DeployPkgUtils.getString("ecbp.importexport.description")); //$NON-NLS-1$
            filters = new FileSystemFilter[] {
                filter
            };
        }

        DirectoryChooserPanel pnlChooser = (filters == null) ? new DirectoryChooserPanel(view, DirectoryChooserPanel.TYPE_SAVE)
                        : new DirectoryChooserPanel(view, DirectoryChooserPanel.TYPE_SAVE, filters);
        pnlChooser.setAcceptButtonLabel(DeployPkgUtils.getString("ecbp.export.open")); //$NON-NLS-1$
        pnlChooser.setShowPassThruFilter(false);

        String bindingName = null;
        String exportDialogTitle = DeployPkgUtils.getString("ecbp.export.multiple.title"); //$NON-NLS-1$
        if (selectedBindings.size() == 1) {
            ConnectorBinding connectorBindingDefn = (ConnectorBinding)selectedBindings.get(0);
            bindingName = connectorBindingDefn.getName();
            exportDialogTitle = DeployPkgUtils.getString("ecbp.export.title", //$NON-NLS-1$
                                                         new Object[] {
                                                             bindingName
                                                         });
            pnlChooser.setInitialFilename(bindingName + EXPORT_EXTENSION);

        } else {
            exportDialogTitle = DeployPkgUtils.getString("ecbp.export.title", //$NON-NLS-1$
                                                         new Object[] {
                                                             "Selected Multiple Bindings"}); //$NON-NLS-1$
            pnlChooser.setInitialFilename("exportedbindings" + EXPORT_EXTENSION);//$NON-NLS-1$

        }

        DialogWindow.show(this, exportDialogTitle, pnlChooser);
        if (pnlChooser.getSelectedButton() == pnlChooser.getAcceptButton()) {
            DirectoryEntry result = (DirectoryEntry)pnlChooser.getSelectedTreeNode();
            String filename = result.getNamespace();
            //
            // first save directory to preferences file
            //
            int index = filename.lastIndexOf(File.separatorChar);
            String path = filename.substring(0, index);
            UserPreferences.getInstance().setValue(ConsoleConstants.CONSOLE_DIRECTORY_LOCATION_KEY, path);
            UserPreferences.getInstance().saveChanges();
            //
            // now do the export
            //
            try {
                FileOutputStream fileOutput = new FileOutputStream(filename);
                XMLConfigurationImportExportUtility xmlUtil = new XMLConfigurationImportExportUtility();
                String userName = UserCapabilities.getLoggedInUser(connection).getName();
                String version = StaticProperties.getVersion();
                Properties props = new Properties();
                props.put(ConfigurationPropertyNames.APPLICATION_CREATED_BY, DeployPkgUtils.getString("dmp.console.name")); //$NON-NLS-1$
                props.put(ConfigurationPropertyNames.APPLICATION_VERSION_CREATED_BY, version);
                props.put(ConfigurationPropertyNames.USER_CREATED_BY, userName);

                // get the selected bindings
                int numSelected = selectedBindings.size();
                ConnectorBinding[] bindingArray = new ConnectorBinding[numSelected];
                bindingArray = (ConnectorBinding[])selectedBindings.toArray(bindingArray);

                ComponentType[] typeArray = new ComponentType[numSelected];
                for (int i = 0; i < numSelected; i++) {
                    typeArray[i] = getConnectorManager().getConnectorForConnectorBinding(bindingArray[i]);
                }

                xmlUtil.exportConnectorBindings(fileOutput, bindingArray, typeArray, props);
            } catch (Exception theException) {
                ExceptionUtility.showMessage("Unable to Export Connector Binding", //$NON-NLS-1$
                                             theException);
                LogManager.logError(LogContexts.CONNECTOR_BINDINGS, theException, "Error exporting connector bindings"); //$NON-NLS-1$
            }
        }
    }

    public void showDeleteConfirmDialog() {
        String msg = "Delete selected connector bindings?"; //$NON-NLS-1$

        int response = CenteredOptionPane.showConfirmDialog(ConsoleMainFrame.getInstance(),
                                                            msg,
                                                            DialogUtility.CONFIRM_DELETE_HDR,
                                                            JOptionPane.YES_NO_OPTION);
        if (response == JOptionPane.YES_OPTION) {
            try {
                Iterator iter = selectedBindings.iterator();
                while (iter.hasNext()) {
                    ConnectorBinding binding = (ConnectorBinding)iter.next();
                    getConnectorManager().deleteBinding(binding);
                }

            } catch (Exception ex) {
                ExceptionUtility.showMessage("Delete a Connector Binding", ex); //$NON-NLS-1$
                LogManager.logError(LogContexts.CONNECTOR_BINDINGS, ex, "Error in deleting connector binding."); //$NON-NLS-1$
            }
            deleteButton.setEnabled(false);
            exportButton.setEnabled(false);
            refresh();
        }
    }

    public void showDeleteRefuseDialog(boolean connectedToDeletedVDB) {
    	StringBuffer sb = new StringBuffer();
    	sb.append("Delete not allowed, Connector Binding is currently assigned to 1 or more VDBs.");  //$NON-NLS-1$
    	if(connectedToDeletedVDB) {
    		sb.append("\n\n The binding may be assigned to a VDB which has been marked for deletion.\n"); //$NON-NLS-1$
    		sb.append("All connections to this VDB must be closed before the binding can be deleted."); //$NON-NLS-1$
    	}
        String sTitle = "Delete Not Allowed"; //$NON-NLS-1$

        CenteredOptionPane.showConfirmDialog(ConsoleMainFrame.getInstance(), sb.toString(), sTitle, JOptionPane.DEFAULT_OPTION);
    }
    
    public void showDeleteRefuseServiceIsDeployedDialog() {
        String msg = "Delete not allowed, Connector Binding(s) is running."; //$NON-NLS-1$
        String sTitle = DialogUtility.CONFIRM_DELETE_HDR;

        CenteredOptionPane.showConfirmDialog(ConsoleMainFrame.getInstance(), msg, sTitle, JOptionPane.DEFAULT_OPTION);
    }    

    private int rowForBinding(String binding) {
        int matchRow = -1;
        int numRows = table.getRowCount();
        int row = 0;
        while ((row < numRows) && (matchRow < 0)) {
            String curRowBinding = table.getValueAt(row, CONNECTOR_BINDING_COL_NUM).toString();
            if (curRowBinding.equals(binding)) {
                matchRow = row;
            } else {
                row++;
            }
        }
        return matchRow;
    }

    /**
     * Set the list of selected connector bindings to contain just this specified binding.
     * 
     * @param binding
     * @since 4.2
     */
    private void setSelectedConnectorBinding(ConnectorBinding binding) {
        selectedBindings.clear();
        selectedBindings.add(binding);
    }

    /**
     * Set the list of selected connector bindings to contain the specified bindings.
     * 
     * @param bindings
     *            List<ConnectorBinding>
     * @since 4.2
     */
    private void setSelectedConnectorBindings(List bindings) {
        selectedBindings.clear();
        selectedBindings.addAll(bindings);
    }

    /**
     * Clear the list of selected connector bindings.
     * 
     * @param bindings
     *            List<ConnectorBinding>
     * @since 4.2
     */
    private void clearSelectedConnectorBindings() {
        selectedBindings.clear();
    }

    /**
     * Get list of selected bindings.
     * 
     * @return List<ConnectorBinding>
     * @since 4.2
     */
    private List getSelectedConnectorBindings() {
        return selectedBindings;
    }

    public void receiveUpdateNotification(RuntimeUpdateNotification notification) {
        if (notification instanceof AddedConnectorBindingNotification) {
            refresh();
        }
    }

    public void refresh() {
        // save the currently selected bindings
        List oldSelectedConnectedBindings = new ArrayList(getSelectedConnectorBindings());

        try {
            // refresh the connector bindings in the manager
            getConnectorManager().getConnectorBindings(true);
            constructConnectorBindingXref();
            populateTable();
        } catch (Exception ex) {
            ExceptionUtility.showMessage("Failed while refreshing the Connector Binding list", ex); //$NON-NLS-1$
            LogManager.logError(LogContexts.CONNECTOR_BINDINGS, ex, "Failed while refreshing the Connector Binding list"); //$NON-NLS-1$
        }

        // restore the binding and select it in the table:
        setSelectedConnectorBindings(oldSelectedConnectedBindings);
        forceTableSelection(oldSelectedConnectedBindings);
    }

    private void tableSelectionChanged() {
        boolean cancelled = false;        

        List bindings = getSelectedConnectorBindings();
        if (bindings.size() == 1 && anyValueChanged()) {
            ConnectorBinding binding = (ConnectorBinding) bindings.get(0);
            
            String message = "Save changes to connector binding " + binding + " ?"; //$NON-NLS-1$//$NON-NLS-2$
            int response = DialogUtility.showPendingChangesDialog(message, getConnection().getURL(), getConnection().getUser());


            switch (response) {
                case DialogUtility.YES:
                    applyProperties();
                    break;
                    
                case DialogUtility.NO:
                    resetProperties();
                    break;
                    
                case DialogUtility.CANCEL:
                    cancelled = true;
                    
                    //remove listener, to avoid an endless loop
                    table.getSelectionModel().removeListSelectionListener(listSelectionListener);

                    //revert to original selection
                    ArrayList newList = new ArrayList();
                    newList.add(binding);
                    forceTableSelection(newList);
                    
                    //restore the listener
                    table.getSelectionModel().addListSelectionListener(listSelectionListener);

                    break;
            }
        }        
        
        
        if (! cancelled) {            
            StaticUtilities.startWait(ViewManager.getMainFrame());
    
            int[] selectedRows = table.getSelectedRows();
            int numSelected = selectedRows.length;
    
            if (numSelected == 0) {
                tableSelectionChangedNoneSelected();
            } else if (numSelected == 1) {
                tableSelectionChangedOneSelected(selectedRows[0]);
            } else {
                tableSelectionChangedMultipleSelected(selectedRows);
            }
    
            forceRepaint();
            StaticUtilities.endWait(ViewManager.getMainFrame());
        }        
    }

    private void tableSelectionChangedNoneSelected() {
        // clear list of selected bindings
        clearSelectedConnectorBindings();

        // disable buttons
        actionDeleteConnectorBinding.setEnabled(false);
        actionExportConnectorBinding.setEnabled(false);
        actionDupConnectorBinding.setEnabled(false);

        // clear out bottom panel
        Border border = createTabBorder(null);
        int curTab = tabbedPane.getSelectedIndex();
        switch (curTab) {
            case DETAILS_TAB_NUM:
                if (detailsPanel.getConnectorBinding() != null) {
                    detailsPanel.setConnectorBinding(null);
                    detailsPanel.setBorder(border);
                }
                break;
            case PROPERTIES_TAB_NUM:
                if (propertiesPanel.getConnectorBinding() != null) {
                    propertiesPanel.setConnectorBinding(null);
                    propertiesPanel.setBorder(border);
                }
                break;
            case VDBS_TAB_NUM:
                if (vdbsPanel.getConnectorBinding() != null) {
                    vdbsPanel.setConnectorBinding(null);
                    vdbsPanel.setBorder(border);
                }
                break;
        }
    }

    private void tableSelectionChangedOneSelected(int selectedRow) {
        // set list of selected bindings
        int convertedRow = table.convertRowIndexToModel(selectedRow);
        String selectedName = (String)table.getModel().getValueAt(convertedRow, CONNECTOR_BINDING_COL_NUM);
        ConnectorBinding binding = (ConnectorBinding)getConnectorBindingXref().get(selectedName);
        setSelectedConnectorBinding(binding);

        // enable buttons
        actionDeleteConnectorBinding.setEnabled(canModify);
        actionExportConnectorBinding.setEnabled(true);
        actionImportConnectorBinding.setEnabled(canModify);

        actionDupConnectorBinding.setEnabled(canModify);

        // populate bottom panel
        Border border = createTabBorder(binding);
        int curTab = tabbedPane.getSelectedIndex();
        switch (curTab) {
            case DETAILS_TAB_NUM:
                detailsPanel.setConnectorBinding(binding);
                detailsPanel.setBorder(border);
                break;
            case PROPERTIES_TAB_NUM:
                propertiesPanel.setConnectorBinding(binding);
                propertiesPanel.setBorder(border);
                break;
            case VDBS_TAB_NUM:
                vdbsPanel.setConnectorBinding(binding);
                vdbsPanel.setBorder(border);
                break;
        }

    }

    private void tableSelectionChangedMultipleSelected(int[] selectedRows) {
        // set list of selected bindings
        List bindings = new ArrayList();
        int numSelected = selectedRows.length;
        for (int i = 0; i < numSelected; i++) {
            int selectedRow = selectedRows[i];
            int convertedRow = table.convertRowIndexToModel(selectedRow);
            String selectedName = (String)table.getModel().getValueAt(convertedRow, CONNECTOR_BINDING_COL_NUM);

            ConnectorBinding binding = (ConnectorBinding)getConnectorBindingXref().get(selectedName);
            bindings.add(binding);
        }
        setSelectedConnectorBindings(bindings);

        // enable buttons
        actionDeleteConnectorBinding.setEnabled(canModify);
        actionExportConnectorBinding.setEnabled(true);
        actionImportConnectorBinding.setEnabled(false);

        actionDupConnectorBinding.setEnabled(false);

        // populate bottom panel
        Border border = createTabBorder(null);
        int curTab = tabbedPane.getSelectedIndex();
        switch (curTab) {
            case DETAILS_TAB_NUM:
                if (detailsPanel.getConnectorBinding() != null) {
                    detailsPanel.setConnectorBinding(null);
                    detailsPanel.setBorder(border);
                }
                break;
            case PROPERTIES_TAB_NUM:
                if (propertiesPanel.getConnectorBinding() != null) {
                    propertiesPanel.setConnectorBinding(null);
                    propertiesPanel.setBorder(border);
                }
                break;
            case VDBS_TAB_NUM:
                if (vdbsPanel.getConnectorBinding() != null) {
                    vdbsPanel.setConnectorBinding(null);
                    vdbsPanel.setBorder(border);
                }
                break;
        }
    }

    private void selectedTabChanged() {
        int curTab = tabbedPane.getSelectedIndex();
        int numSelected = selectedBindings.size();
        ConnectorBinding selectedBinding = null;
        if (numSelected == 1) {
            selectedBinding = (ConnectorBinding)selectedBindings.get(0);
        }

        switch (curTab) {
            case DETAILS_TAB_NUM:
                boolean needsRefresh;
                if (selectedBinding == null) {
                    needsRefresh = (detailsPanel.getConnectorBinding() != null);
                } else {
                    needsRefresh = (!selectedBinding.equals(detailsPanel.getConnectorBinding()));
                }
                if (needsRefresh) {
                    detailsPanel.setConnectorBinding(selectedBinding);
                    detailsPanel.setBorder(createTabBorder(selectedBinding));
                }
                break;
            case PROPERTIES_TAB_NUM:
                if (selectedBinding == null) {
                    needsRefresh = (propertiesPanel.getConnectorBinding() != null);
                } else {
                    needsRefresh = (!selectedBinding.equals(propertiesPanel.getConnectorBinding()));
                }
                if (needsRefresh) {
                    propertiesPanel.setConnectorBinding(selectedBinding);
                    propertiesPanel.setBorder(createTabBorder(selectedBinding));
                }
                break;
            case VDBS_TAB_NUM:
                if (selectedBinding == null) {
                    needsRefresh = (vdbsPanel.getConnectorBinding() != null);
                } else {
                    needsRefresh = (!selectedBinding.equals(vdbsPanel.getConnectorBinding()));
                }
                if (needsRefresh) {
                    vdbsPanel.setConnectorBinding(selectedBinding);
                    vdbsPanel.setBorder(createTabBorder(selectedBinding));
                }
                break;
        }
        forceRepaint();
    }

    private Border createTabBorder(ConnectorBinding defn) {
        TitledBorder border;
        String title;

        if (defn == null) {
            title = ""; //$NON-NLS-1$
        } else {
            title = "Connector Binding:  " + defn.toString(); //$NON-NLS-1$
        }
        border = new TitledBorder(title);
        return border;
    }

    public java.util.List /* <Action> */resume() {
        return currentActions;
    }

    public String getTitle() {
        return "Connector Bindings"; //$NON-NLS-1$
    }

    public ConnectionInfo getConnection() {
        return connection;
    }

    private HashMap getConnectorBindingXref() {
        if (nameConnectorBindingMap == null) {
            constructConnectorBindingXref();
        }
        return nameConnectorBindingMap;
    }

    private void constructConnectorBindingXref() {
        nameConnectorBindingMap = new HashMap();
        ArrayList connectorsList = null;
        try {
            connectorsList = getConnectorManager().getConnectorBindings(false);
        } catch (Exception e) {
            ExceptionUtility.showMessage("Failed while loading data into Connector Binding Panel", e); //$NON-NLS-1$
        }

        Iterator it = connectorsList.iterator();
        ConnectorBinding tempDefn = null;

        while (it.hasNext()) {
            ConnectorAndBinding cab = (ConnectorAndBinding)it.next();
            tempDefn = (ConnectorBinding)cab.getBinding();
            nameConnectorBindingMap.put(tempDefn.toString(), tempDefn);
        }

    }

    private void forceRepaint() {
        StaticUtilities.jiggleSplitter(splitPane);
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

    
    
    private void applyProperties() {
        if (propertiesPanel != null) {
            propertiesPanel.applyProperties();
        }        
    }
    private void resetProperties() {
        if (propertiesPanel != null) {
            propertiesPanel.resetProperties();
        }        
    }
    
    private boolean anyValueChanged() {
        if (propertiesPanel == null) {
            return false;
        }
        return propertiesPanel.anyValueChanged();
    }    
    
    
}// end ConnectorBindingPanel

class ConnectorBindingPanelAction extends AbstractPanelAction {

    public static final int NEW_CONNECTOR_BINDING = 0;
    public static final int IMPORT_CONNECTOR_BINDING = 1;
    public static final int EXPORT_CONNECTOR_BINDING = 2;
    public static final int DELETE_CONNECTOR_BINDING = 3;
    public static final int DUP_CONNECTOR_BINDING = 4;

    private ConnectorBindingPanel panel;

    public ConnectorBindingPanelAction(int theType,
                                       ConnectorBindingPanel panel) {
        super(theType);
        this.panel = panel;
    }

    public void actionImpl(ActionEvent theEvent) {
        if (type == NEW_CONNECTOR_BINDING) {
            panel.newPressed();
        } else if (type == IMPORT_CONNECTOR_BINDING) {
            panel.importPressed();
        } else if (type == EXPORT_CONNECTOR_BINDING) {
            panel.exportPressed();
        } else if (type == DELETE_CONNECTOR_BINDING) {
            panel.deletePressed();
        } else if (type == DUP_CONNECTOR_BINDING) {
            panel.dupPressed();
        }
    }
}// end ConnectorBindingPanelAction
