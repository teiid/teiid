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

package com.metamatrix.console.ui.views.connector;

import java.awt.Component;
import java.awt.Dimension;
import java.awt.Font;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.GridLayout;
import java.awt.Insets;
import java.awt.Point;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.io.File;
import java.io.FileOutputStream;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Properties;
import java.util.Vector;

import javax.swing.AbstractButton;
import javax.swing.Action;
import javax.swing.JLabel;
import javax.swing.JOptionPane;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.ListSelectionModel;
import javax.swing.event.ListSelectionEvent;
import javax.swing.event.ListSelectionListener;

import com.metamatrix.admin.api.server.ServerAdmin;
import com.metamatrix.common.config.api.ComponentType;
import com.metamatrix.common.config.util.ConfigurationPropertyNames;
import com.metamatrix.common.config.xml.XMLConfigurationImportExportUtility;
import com.metamatrix.common.log.LogManager;
import com.metamatrix.common.tree.directory.DirectoryEntry;
import com.metamatrix.common.tree.directory.DirectoryEntryFilter;
import com.metamatrix.common.tree.directory.DirectoryEntryView;
import com.metamatrix.common.tree.directory.FileSystemFilter;
import com.metamatrix.common.tree.directory.FileSystemView;
import com.metamatrix.console.connections.ConnectionInfo;
import com.metamatrix.console.models.ConnectorManager;
import com.metamatrix.console.models.ModelManager;
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
import com.metamatrix.console.ui.views.deploy.util.DeployPkgUtils;
import com.metamatrix.console.util.DialogUtility;
import com.metamatrix.console.util.ExceptionUtility;
import com.metamatrix.console.util.LogContexts;
import com.metamatrix.console.util.Refreshable;
import com.metamatrix.console.util.StaticProperties;
import com.metamatrix.console.util.StaticUtilities;
import com.metamatrix.core.util.FileUtils;
import com.metamatrix.toolbox.preference.UserPreferences;
import com.metamatrix.toolbox.ui.widget.ButtonWidget;
import com.metamatrix.toolbox.ui.widget.DialogWindow;
import com.metamatrix.toolbox.ui.widget.DirectoryChooserPanel;
import com.metamatrix.toolbox.ui.widget.TableWidget;
import com.metamatrix.toolbox.ui.widget.TextFieldWidget;
import com.metamatrix.toolbox.ui.widget.TitledBorder;
import com.metamatrix.toolbox.ui.widget.table.DefaultTableComparator;

public class ConnectorPanel extends BasePanel implements WorkspacePanel,
		Refreshable {
    public final static int CONNECTOR_COL_NUM = 0;
    public final static int NUM_BINDINGS_COL_NUM = 1;

    private static final String PANEL_NAME = "Connector Types"; //$NON-NLS-1$

    private PanelAction actionImportConnector;
    private PanelAction actionExportConnector;
    private PanelAction actionDeleteConnector;
        
    private static final String IMPORT_CONNECTOR_TITLE  = "Import..."; //$NON-NLS-1$
    private static final String EXPORT_CONNECTOR_TITLE  = "Export...";     //$NON-NLS-1$
    private static final String DELETE_CONNECTOR_TITLE  = "Delete"; //$NON-NLS-1$
    
    private static final String EXPORT_CAF_EXTENSION = DeployPkgUtils.getString("ecp.export.caf.extension"); //$NON-NLS-1$
    
	private ConnectionInfo connection;
	private ConnectorManager manager;
	private TableWidget table;
    private JPanel detailsPanel;
    private ConnectorTableModel tableModel;
    private TextFieldWidget createdField;
    private TextFieldWidget createdByField;
    private TextFieldWidget registeredField;
    private TextFieldWidget registeredByField;
    private ButtonWidget importButton;
    private ButtonWidget exportButton;    
    
    private ButtonWidget deleteButton;
    private ConnectorBasicInfo[] infoInTable = null;
    private String selectedConnectorName = null;
    private int selectedTableRow = -1;
    private java.util.List /*<Action>*/ currentActions = new ArrayList(5);

    private boolean canModify;

    public ConnectorPanel(ConnectionInfo conn) {
        super();
        this.connection = conn;
        manager = ModelManager.getConnectorManager(connection);
        try {
            canModify = UserCapabilities.getInstance().canModifyConnectors(
            		connection);
        } catch (Exception ignore) {
        }
        init();
    }

    private void init() {
        createActions();
        GridBagLayout layout = new GridBagLayout();
        this.setLayout(layout);
        //NOTE-- column headers must be in the same order of the columns as
        //defined above
        tableModel = new ConnectorTableModel(new Vector(Arrays.asList(
        		new String[] {"Name", "# Bindings"}))); //$NON-NLS-1$ //$NON-NLS-2$
        table = new TableWidget(tableModel, true);
        table.setEditable(false);
        table.sizeColumnsToFitData(100);
        DefaultTableComparator dtcComparator
                = (DefaultTableComparator) table.getComparator();
        dtcComparator.setIgnoresCase(true);

        table.getSelectionModel().addListSelectionListener(
        		new ListSelectionListener() {
            public void valueChanged(ListSelectionEvent ev) {
                tableSelectionChanged();
            }
        });
        table.getSelectionModel().setSelectionMode(
        		ListSelectionModel.SINGLE_SELECTION);
        table.getColumnModel().setColumnMargin(8);

        JScrollPane tableSP = new JScrollPane(table);
        tableSP.setBorder(new TitledBorder(""/*"Connectors"*/)); //$NON-NLS-1$
        
        
        
        detailsPanel = new JPanel();
        detailsPanel.setPreferredSize(new Dimension(500,200));
        TitledBorder tBorder = new TitledBorder("Details"); //$NON-NLS-1$
        detailsPanel.setBorder(tBorder);
        tBorder.setTitleFont(tBorder.getTitleFont().deriveFont(Font.BOLD));
        
        GridBagLayout dl = new GridBagLayout();
        detailsPanel.setLayout(dl);

        
        JLabel createdLabel = new JLabel("Created:"); //$NON-NLS-1$
        detailsPanel.add(createdLabel);
        dl.setConstraints(createdLabel, new GridBagConstraints(0, 0, 1, 1,
                0.0, 0.0, GridBagConstraints.EAST, GridBagConstraints.NONE,
                new Insets(5, 5, 5, 5), 0, 0));
        createdField = new TextFieldWidget();
        createdField.setEditable(false);
        detailsPanel.add(createdField);
        dl.setConstraints(createdField, new GridBagConstraints(1, 0, 1, 1,
                1.0, 0.0, GridBagConstraints.WEST, GridBagConstraints.HORIZONTAL,
                new Insets(5, 5, 5, 5), 0, 0));
        JLabel createdByLabel = new JLabel("By:"); //$NON-NLS-1$
        detailsPanel.add(createdByLabel);
        dl.setConstraints(createdByLabel, new GridBagConstraints(2, 0, 1, 1,
                0.0, 0.0, GridBagConstraints.EAST, GridBagConstraints.NONE,
                new Insets(5, 5, 5, 5), 0, 0));
        createdByField = new TextFieldWidget();
        createdByField.setEditable(false);
        detailsPanel.add(createdByField);
        dl.setConstraints(createdByField, new GridBagConstraints(3, 0, 1, 1,
                1.0, 0.0, GridBagConstraints.WEST, GridBagConstraints.HORIZONTAL,
                new Insets(5, 5, 5, 5), 0, 0));

        JLabel registeredLabel = new JLabel("Registered:"); //$NON-NLS-1$
        detailsPanel.add(registeredLabel);
        dl.setConstraints(registeredLabel, new GridBagConstraints(0, 1, 1, 1,
                0.0, 0.0, GridBagConstraints.EAST, GridBagConstraints.NONE,
                new Insets(5, 5, 5, 5), 0, 0));
        registeredField = new TextFieldWidget();
        registeredField.setEditable(false);
        detailsPanel.add(registeredField);
        dl.setConstraints(registeredField, new GridBagConstraints(1, 1, 1, 1,
                1.0, 0.0, GridBagConstraints.WEST, GridBagConstraints.HORIZONTAL,
                new Insets(5, 5, 5, 5), 0, 0));
        JLabel registeredByLabel = new JLabel("By:"); //$NON-NLS-1$
        detailsPanel.add(registeredByLabel);
        dl.setConstraints(registeredByLabel, new GridBagConstraints(2, 1, 1, 1,
                0.0, 0.0, GridBagConstraints.EAST, GridBagConstraints.NONE,
                new Insets(5, 5, 5, 5), 0, 0));
        registeredByField = new TextFieldWidget();
        registeredByField.setEditable(false);
        detailsPanel.add(registeredByField);
        dl.setConstraints(registeredByField, new GridBagConstraints(3, 1, 1, 1,
                1.0, 0.0, GridBagConstraints.WEST, GridBagConstraints.HORIZONTAL,
                new Insets(5, 5, 5, 5), 0, 0));

        importButton = new ButtonWidget("Import..."); //$NON-NLS-1$

        setup(MenuEntry.ACTION_MENUITEM, importButton, actionImportConnector);

        //only enable if we can modify these
        actionImportConnector.setEnabled(canModify);
        
        
        exportButton = new ButtonWidget("Export..."); //$NON-NLS-1$
        setup(MenuEntry.ACTION_MENUITEM, exportButton, actionExportConnector);

        // Can export if user has System.READ role
        actionExportConnector.setEnabled(true);
        

        deleteButton = new ButtonWidget("Delete"); //$NON-NLS-1$
        deleteButton.setEnabled(false);

        setup(MenuEntry.ACTION_MENUITEM, deleteButton, actionDeleteConnector);
        actionDeleteConnector.setEnabled(false);

        JPanel buttonsPanel = new JPanel();
        buttonsPanel.setLayout(new GridLayout(1, 2, 5, 5));
        buttonsPanel.add(importButton);
        buttonsPanel.add(exportButton);                
        buttonsPanel.add(deleteButton);

        add(tableSP);
        layout.setConstraints(tableSP, new GridBagConstraints(0, 0, 1, 1,
                1.0, 1.0, GridBagConstraints.CENTER, GridBagConstraints.BOTH,
                new Insets(5, 5, 5, 5), 0, 0));
        add(detailsPanel);
        layout.setConstraints(detailsPanel, new GridBagConstraints(0, 1, 1, 1,
                0.5, 0.5, GridBagConstraints.CENTER, GridBagConstraints.BOTH,
                new Insets(5, 5, 5, 5), 0, 0));
        add(buttonsPanel);
        layout.setConstraints(buttonsPanel, new GridBagConstraints(0, 2, 1, 1,
                0.0, 0.0, GridBagConstraints.CENTER, GridBagConstraints.NONE,
                new Insets(5, 5, 5, 5), 0, 0));

        populateTable();
        selectFirstConnector();
    }

	public void createActions() {
        actionImportConnector = new PanelAction(PanelAction.IMPORT_CONNECTOR);
        actionImportConnector.putValue(Action.NAME, IMPORT_CONNECTOR_TITLE);
        // Added by setup: addActionToList(MenuEntry.ACTION_MENUITEM, actionImportConnector);
        
        actionExportConnector = new PanelAction(PanelAction.EXPORT_CONNECTOR);
        actionExportConnector.putValue(Action.NAME, EXPORT_CONNECTOR_TITLE);
        

        actionDeleteConnector = new PanelAction(PanelAction.DELETE_CONNECTOR);
        actionDeleteConnector.putValue(Action.NAME, DELETE_CONNECTOR_TITLE);
        // Added by setup: addActionToList(MenuEntry.ACTION_MENUITEM, actionDeleteConnector);

	}

    public void addActionToList(String sID, Action act) {
        currentActions.add(new MenuEntry(sID, act));
    }

    /**
     * setup associates an action with a button
     * @param sID
     * @param theButton
     * @param theAction
     */
    private void setup(String sID, AbstractButton theButton,
                       AbstractPanelAction theAction) {
        theAction.addComponent(theButton);
        addActionToList(sID, theAction);
    }

    /**
     *
     */
    public void importPressed() {
        boolean bConnectorCreated = doImportWizard();
        if (bConnectorCreated) {
            // Great!
        }
    }
    
    public void exportPressed() {
        exportConnectorType();
    }  
    
    private void exportConnectorType() {
		if (selectedConnectorName == null) {
            String msg = "Must select a connector type before trying to export"; //$NON-NLS-1$
            String sTitle = "Unable to Export Connector Type"; //$NON-NLS-1$
    
            CenteredOptionPane.showConfirmDialog(ConsoleMainFrame.getInstance(),
                    msg, sTitle, JOptionPane.DEFAULT_OPTION);

            return;                    
        }

        FileSystemView view = new FileSystemView();
        String dirTxt = (String)UserPreferences.getInstance()
        		.getValue(ConsoleConstants.CONSOLE_DIRECTORY_LOCATION_KEY);
        if (dirTxt != null) {
            try {
                view.setHome(view.lookup(dirTxt));
            } catch (Exception ex) {
                //Any exception that may occur on setting the initial view is
                //inconsequential.  This is merely a convenience to the user.
            }
        }

        String[] extensionsCAF = (String[])DeployPkgUtils.getObject("ecp.export.caf.extensions"); //$NON-NLS-1$
        FileSystemFilter filterCAF = 
            new FileSystemFilter(view, extensionsCAF, DeployPkgUtils.getString("ecp.export.caf.description")); //$NON-NLS-1$

        String[] extensionsCDK = (String[])DeployPkgUtils.getObject("ecp.export.cdk.extensions"); //$NON-NLS-1$
        FileSystemFilter filterCDK = 
            new FileSystemFilter(view, extensionsCDK, DeployPkgUtils.getString("ecp.export.cdk.description")); //$NON-NLS-1$

        FileSystemFilter[] filters = new FileSystemFilter[] {filterCAF, filterCDK};
                

        ConnectorExportChooserPanel pnlChooser = new ConnectorExportChooserPanel(view, DirectoryChooserPanel.TYPE_SAVE, filters);
        pnlChooser.setAcceptButtonLabel(
            	DeployPkgUtils.getString("ecp.export.open")); //$NON-NLS-1$
        pnlChooser.setShowPassThruFilter(false);
        
       
        pnlChooser.setInitialFilename(selectedConnectorName + EXPORT_CAF_EXTENSION);//$NON-NLS-1$

        DialogWindow.show(this,
            	DeployPkgUtils.getString("ecp.export.title", //$NON-NLS-1$
				new Object[] {selectedConnectorName}),
            	pnlChooser);
        if (pnlChooser.getSelectedButton() == pnlChooser.getAcceptButton()) {
            DirectoryEntry result =
                	(DirectoryEntry)pnlChooser.getSelectedTreeNode();
            String filename = result.getNamespace();
            //
            // first save directory to preferences file
            //
            int index = filename.lastIndexOf(File.separatorChar);
            String path = filename.substring(0, index);
            UserPreferences.getInstance().setValue(ConsoleConstants.CONSOLE_DIRECTORY_LOCATION_KEY, 
            		path);
            UserPreferences.getInstance().saveChanges();
            //
            // now do the export
            //
            try {
                FileSystemFilter selectedFilter = (FileSystemFilter) pnlChooser.getSelectedFilter();
                if (selectedFilter == filterCAF) {
                    exportCAFFile(filename);
                    
                } else if (selectedFilter == filterCDK) {
                    exportCDKFile(filename);
                    
                }
			} catch (Exception theException) {
                theException.printStackTrace();
              	DeployPkgUtils.getString("ecp.msg.exporterrordetail", //$NON-NLS-1$
              			new Object[] {selectedConnectorName, filename});
             	ExceptionUtility.showMessage("Unable to Export Connector Type",  //$NON-NLS-1$
             			theException);
                LogManager.logError(LogContexts.CONNECTORS, theException,
                        "Error exporting connector type " +  //$NON-NLS-1$
                        selectedConnectorName);
			}
		}
    }     
    
    
    private void exportCAFFile(String filename) throws Exception {        
        ServerAdmin admin = getConnection().getServerAdmin();
        byte[] bytes = admin.exportConnectorArchive(selectedConnectorName);
        FileUtils.write(bytes, filename);
    }
    
    private void exportCDKFile(String filename) throws Exception {
        FileOutputStream fileOutput = new FileOutputStream(filename);
        XMLConfigurationImportExportUtility xmlUtil =
                new XMLConfigurationImportExportUtility();
        String userName = UserCapabilities.getLoggedInUser(connection).getName();
        String version = StaticProperties.getVersions() + ":" + StaticProperties.getBuild(); //$NON-NLS-1$
        Properties props = new Properties();
        props.put(ConfigurationPropertyNames.APPLICATION_CREATED_BY,DeployPkgUtils.getString("dmp.console.name")); //$NON-NLS-1$
        props.put(ConfigurationPropertyNames.APPLICATION_VERSION_CREATED_BY,version);
        props.put(ConfigurationPropertyNames.USER_CREATED_BY, userName);
        ComponentType ct = getSelectedConnector(selectedConnectorName);
        xmlUtil.exportConnector(fileOutput, ct, props);
    }
    
    
    
	/**
     * Import a new connector type
     * @return <tag>true</tag> if import was successfull.
     */
    private boolean doImportWizard() {
		ImportWizardController controller = new ImportWizardController(
				connection);

        boolean bConnectorWasCreated = controller.runWizard();
        if (bConnectorWasCreated) {
			String newConnectorName = (String)controller.getLoadedTypes().firstKey();
            populateTable();
            forceTableSelection(newConnectorName);
        }
		return bConnectorWasCreated;
    }
    
    public void receiveUpdateNotification(RuntimeUpdateNotification notification) {
    	//TODO
    }

    /**
     * Force a refresh of this panel.
     */
    public void refresh() {
		// Will refresh the connectors in the manager
		String saveSelectedConnectorName = selectedConnectorName;
        populateTable();
        selectedConnectorName = saveSelectedConnectorName;
        int modelRowForSelectedConnector = modelRowForConnector(
        		selectedConnectorName);
        if (modelRowForSelectedConnector < 0) {
        	selectedConnectorName = null;
        }
		forceTableSelection(selectedConnectorName);
        table.sizeColumnsToFitData(100);
    }

    /**
     * Return the model index for the given connector name or
     * <code>-1</code> if no name in the table matches.
     * @param name The connector name for which to find the table index.
     * @return The table index of the connector matching the given name
     * or <code>-1</code> if no name matches.
     */
    private int modelRowForConnector(String name) {
        int matchRow = -1;
        int row = 0;
        int numRows = tableModel.getRowCount();
        while ((row < numRows) && (matchRow < 0)) {
            String curName = tableModel.getValueAt(row, CONNECTOR_COL_NUM)
            		.toString();
            if (curName.equals(name)) {
                matchRow = row;
                break;
            }
            row++;
        }
		return matchRow;
    }

    /**
     *
     */
    public void deletePressed() {
		String msg = "Delete connector " + selectedConnectorName + "?"; //$NON-NLS-1$ //$NON-NLS-2$
        int response = CenteredOptionPane.showConfirmDialog(
                ConsoleMainFrame.getInstance(),
                msg, DialogUtility.CONFIRM_DELETE_HDR, 
                JOptionPane.YES_NO_OPTION);
        if (response == JOptionPane.YES_OPTION) {
            int currentModelIndex = modelRowForConnector(selectedConnectorName);
            int currentViewIndex = table.convertRowIndexToView(
            		currentModelIndex);
            boolean continuing = true;
            try {
                manager.deleteConnector(selectedConnectorName);
                tableModel.removeRow(currentModelIndex);
            } catch (Exception ex) {
                ExceptionUtility.showMessage("Delete a Connector", ex); //$NON-NLS-1$
                LogManager.logError(LogContexts.CONNECTORS, ex,
                        "Error in deleting connector."); //$NON-NLS-1$
                continuing = false;
            }
            if (continuing) {
            	int numRows = tableModel.getRowCount();
            	if (numRows > 0) {
            		//If we deleted the last item, must now point to the new
            		//last item
            		if (currentViewIndex > numRows - 1) {
            			currentViewIndex = numRows - 1;
            		}
            		int newModelIndex = table.convertRowIndexToModel(
            				currentViewIndex);
            		selectedConnectorName = tableModel.getValueAt(newModelIndex, 
            				CONNECTOR_COL_NUM).toString();
            	} else {
            		selectedConnectorName = null;
            	}
				forceTableSelection(selectedConnectorName);
           	}
        }
    }

    /**
     *
     */
    private void tableSelectionChanged() {
		StaticUtilities.startWait(ViewManager.getMainFrame());

        int viewRow = table.getSelectedRow();
        int selectedRow = -1;

        if (viewRow >= 0) {
            selectedRow = table.convertRowIndexToModel(viewRow);
        }
		if (selectedRow >= 0) {
            String oldSelectedConnectorName = selectedConnectorName;
            if (selectedRow != selectedTableRow) {
                try {
                    selectedConnectorName = tableModel.getValueAt(selectedRow, 
                    		CONNECTOR_COL_NUM).toString();
				} catch (Exception ex) {
                    ExceptionUtility.showMessage("Retrieve Details for Connector",  //$NON-NLS-1$
                    		ex);
                    LogManager.logError(LogContexts.CONNECTORS, ex,
                            "Exception in retrieving details for connector."); //$NON-NLS-1$
                    selectedRow = -1;
                }
                selectedTableRow = selectedRow;
                if (! selectedConnectorName.equals(oldSelectedConnectorName) ) {
                    populateDetails(selectedConnectorName);
                }
                int numBindings = ((Integer) tableModel.getValueAt(selectedRow,
                        NUM_BINDINGS_COL_NUM)).intValue();
                actionDeleteConnector.setEnabled(canModify && (numBindings == 0));
            }
        } else {
            selectedTableRow = -1;
            selectedConnectorName = null;
            populateDetails(null);
            actionDeleteConnector.setEnabled(false);
        }

        StaticUtilities.endWait(ViewManager.getMainFrame());

    }

    /**
     * (jh) populates the detail panel at the bottom with the details of the selected connector.
     * @param selectedConnectorName
     */
    private void populateDetails(String selConnectorName) {
		String borderString = ""; //$NON-NLS-1$
        SimpleDateFormat formatter = StaticUtilities.getDefaultDateFormat();
        if (selConnectorName == null) {
//            translatorField.setText(""); //$NON-NLS-1$
//            connectionField.setText(""); //$NON-NLS-1$
            createdField.setText(""); //$NON-NLS-1$
            createdByField.setText(""); //$NON-NLS-1$
            registeredField.setText(""); //$NON-NLS-1$
            registeredByField.setText(""); //$NON-NLS-1$
        } else {
            ComponentType ct = null;
//            ConnectorDetailInfo detailInfo = null;
            try {
                ct = getSelectedConnector(selConnectorName);
//                detailInfo = manager.getDetailForConnector(getSelectedConnector(
//                		selConnectorName));
            } catch (Exception e) {
                LogManager.logError(LogContexts.CONNECTORS, e,
                        "Error populating connector details: " +  //$NON-NLS-1$
                        selConnectorName);
            }
//            translatorField.setText("");//$NON-NLS-1$
                                    //detailInfo.getTranslator());
            
//            connectionField.setText("");   //$NON-NLS-1$                                
//                                    detailInfo.getConnection());
            createdField.setText(formatter.format(ct.getCreatedDate()));
            createdByField.setText(ct.getCreatedBy());
            registeredField.setText(formatter.format(ct.getLastChangedDate()));
            registeredByField.setText(ct.getLastChangedBy());
            borderString = "Details for " + ct.getName(); //$NON-NLS-1$
        }
        TitledBorder tBorder = new TitledBorder(borderString);
        detailsPanel.setBorder(tBorder);
        tBorder.setTitleFont(tBorder.getTitleFont().deriveFont(Font.BOLD));
    }

    /**
     *  (jh) populates the table that shows name, count of bindings
     * Refreshes connector information from server.
     */
    private void populateTable() {
		try {
            infoInTable = manager.getConnectorBasics(true);
            if (infoInTable == null) {
                return;
            }
        } catch (Exception ex) {
            ExceptionUtility.showMessage("Retrieve list of existing connectors",  //$NON-NLS-1$
            		ex);
            LogManager.logError(LogContexts.CONNECTORS, ex,
                    "Exception in retrieving list of existing connectors."); //$NON-NLS-1$
            return;
        }

        tableModel.setNumRows(0);
        for (int i = 0; i < infoInTable.length; i++) {
            if (infoInTable[i] != null) {
                Vector vec = new Vector(Arrays.asList(new Object[]
                {null, null, null}));
                vec.setElementAt(infoInTable[i].getName(), CONNECTOR_COL_NUM);
                vec.setElementAt(new Integer(infoInTable[i].getNumBindings()),
                        NUM_BINDINGS_COL_NUM);
                tableModel.addRow(vec);
            }
        }
    }

    public static void setLocationOn(Component comp) {
        Point p = StaticUtilities.centerFrame(comp.getSize());
        comp.setLocation(p.x, p.y);
    }

    public java.util.List /*<Action>*/ resume() {
    	refresh();
        return currentActions;
    }

	private void selectFirstConnector() {
		if (table.getRowCount() > 0) {
			int modelRow = table.convertRowIndexToModel(0);
			if (modelRow >= 0) {
				String connectorName = (String)tableModel.getValueAt(modelRow,
						CONNECTOR_COL_NUM);
				if (connectorName != null) {
					selectedConnectorName = connectorName;
					forceTableSelection(selectedConnectorName);
				}
			}
		}
	}
	
    private void forceTableSelection(String selConnectorName) {
		int modelRowForSelected = modelRowForConnector(selConnectorName);
		if (modelRowForSelected >= 0) {
			int viewRowForSelected = table.convertRowIndexToView(
					modelRowForSelected);
            table.getSelectionModel().setSelectionInterval(viewRowForSelected,
            		viewRowForSelected);
            table.scrollRectToVisible(table.getCellRect(viewRowForSelected, 0, 
            		true));
        } else {
            tableSelectionChanged();
        }
		populateDetails(selConnectorName);
    }

    /**
     * Get the name of this panel.
     * @return The panel name.
     */
    public String getTitle() {
        return PANEL_NAME;
    }

	public ConnectionInfo getConnection() {
		return connection;
	}
	
    /**
     * Lookup the connector associated with this name in the Model.
     * @param connectorName
     * @return The connector.
     */
    private ComponentType getSelectedConnector(String connectorName) throws Exception {
        return manager.lookupConnector(connectorName);
    }




    class PanelAction extends AbstractPanelAction {
        public static final int IMPORT_CONNECTOR = 0;
        public static final int DELETE_CONNECTOR = 1;
        public static final int EXPORT_CONNECTOR = 2;        
        
        public PanelAction(int theType) {
            super(theType);
        }

        public void actionImpl(ActionEvent theEvent) {
            if (type == IMPORT_CONNECTOR) {
                importPressed();
            } else if (type == EXPORT_CONNECTOR) {
                exportPressed();                
            } else if (type == DELETE_CONNECTOR) {
                deletePressed();
            }
        }
    }
    
    class ConnectorExportChooserPanel extends DirectoryChooserPanel {
        public ConnectorExportChooserPanel(DirectoryEntryView directoryEntryView, int type, DirectoryEntryFilter[] filters) {
            super(directoryEntryView, type, filters);
        }
        
        /**
         * Overridden to update the filename when the filter is changed. 
         * @see com.metamatrix.toolbox.ui.widget.DirectoryChooserPanel#createComponent()
         * @since 4.3
         */
        protected void createComponent() {
            super.createComponent();
            
            filterComboBox.addActionListener(new ActionListener() {
                public void actionPerformed(ActionEvent e){
                    DirectoryEntryFilter selectedFilter = (DirectoryEntryFilter)filterComboBox.getSelectedItem();
                    String extension = selectedFilter.getExtension(0);
                    
                    setInitialFilename(selectedConnectorName + "." + extension); //$NON-NLS-1$
                }
            });
        }

        
        
    }
    
    
}//end ConnectorPanel




class ConnectorTableModel 
		extends com.metamatrix.toolbox.ui.widget.table.DefaultTableModel {
    public ConnectorTableModel(Vector columns) {
        super(columns, 0);
    }
}//end ConnectorTableModel



