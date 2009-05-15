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

package com.metamatrix.console.ui.views.authorization;

import java.awt.Color;
import java.awt.Component;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.GridLayout;
import java.awt.Insets;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.Vector;

import javax.swing.AbstractButton;
import javax.swing.Action;
import javax.swing.JComponent;
import javax.swing.JOptionPane;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JSplitPane;
import javax.swing.JTabbedPane;
import javax.swing.JTable;
import javax.swing.ListSelectionModel;
import javax.swing.border.Border;
import javax.swing.event.ChangeEvent;
import javax.swing.event.ChangeListener;
import javax.swing.event.ListSelectionEvent;
import javax.swing.event.ListSelectionListener;
import javax.swing.table.TableModel;

import com.metamatrix.common.config.ResourceNames;
import com.metamatrix.common.config.api.AuthenticationProvider;
import com.metamatrix.common.config.api.Configuration;
import com.metamatrix.common.config.api.ServiceComponentDefn;
import com.metamatrix.common.config.util.ConfigurationPropertyNames;
import com.metamatrix.common.log.LogManager;
import com.metamatrix.console.connections.ConnectionInfo;
import com.metamatrix.console.models.AuthenticationProviderManager;
import com.metamatrix.console.models.ConfigurationManager;
import com.metamatrix.console.models.GroupsManager;
import com.metamatrix.console.models.ModelManager;
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
import com.metamatrix.console.ui.views.deploy.event.ConfigurationChangeEvent;
import com.metamatrix.console.ui.views.deploy.event.ConfigurationChangeListener;
import com.metamatrix.console.util.DialogUtility;
import com.metamatrix.console.util.ExceptionUtility;
import com.metamatrix.console.util.ExternalException;
import com.metamatrix.console.util.LogContexts;
import com.metamatrix.console.util.Refreshable;
import com.metamatrix.console.util.StaticUtilities;
import com.metamatrix.core.MetaMatrixRuntimeException;
import com.metamatrix.toolbox.ui.UIDefaults;
import com.metamatrix.toolbox.ui.widget.ButtonWidget;
import com.metamatrix.toolbox.ui.widget.Splitter;
import com.metamatrix.toolbox.ui.widget.TableWidget;
import com.metamatrix.toolbox.ui.widget.TitledBorder;
import com.metamatrix.toolbox.ui.widget.table.DefaultTableCellRenderer;

public class ProvidersMain extends BasePanel implements
                                                    ConfigurationChangeListener,
                                                    WorkspacePanel,
                                                    Refreshable {

    public final static int DOMAIN_NAME_COL_NUM = 0;
    public final static int PROVIDER_TYPE_COL_NUM = 1;

    public final static int DETAILS_TAB_NUM = 0;
    public final static int PROPERTIES_TAB_NUM = 1;

    private static final String NEW_CONNECTOR_TITLE = "New..."; //$NON-NLS-1$
    private static final String DELETE_CONNECTOR_TITLE = "Delete"; //$NON-NLS-1$

    private ProviderPanelAction actionNewProvider;
    private ProviderPanelAction actionDeleteProvider;

    private ConnectionInfo connection;

    private TableWidget table;
    private com.metamatrix.toolbox.ui.widget.table.DefaultTableModel tableModel;
    private ButtonWidget newButton;
    private ButtonWidget deleteButton;
    private ButtonWidget moveRowUpButton;
    private ButtonWidget moveRowDownButton;
    private AuthenticationProviderDetailsPanel detailsPanel;
    private AuthenticationProviderPropertiesPanel propertiesPanel;

    private java.util.List /* <AuthenticationProvider> */ selectedProviders = new ArrayList();

    private ListSelectionListener listSelectionListener;
    
    private JTabbedPane tabbedPane;
    private JSplitPane splitPane;
    private java.util.List /* <Action> */currentActions = new ArrayList();
    private HashMap nameProviderMap = null;
    private boolean canModify;

    public ProvidersMain(ConnectionInfo conn) {
        super();
        this.connection = conn;
        init();
    }

    private AuthenticationProviderManager getAuthenticationProviderManager() {
        return ModelManager.getAuthenticationProviderManager(connection);
    }

    private ConfigurationManager getConfigurationManager() {
        return ModelManager.getConfigurationManager(connection);
    }
    
    private GroupsManager getGroupsManager() {
        return ModelManager.getGroupsManager(connection);
    }

    private void init() {
        canModify = false;
        try {
            final UserCapabilities cap = UserCapabilities.getInstance();
            canModify = cap.canModifySecurity(connection);
            if (canModify) {
                getConfigurationManager().addConfigurationChangeListener(this);
            }
        } catch (Exception ex) {
            // Cannot occur
        }

        createActions();

        // ------------------------------------------------------------
        // Create the bottom tabbedPane
        // ------------------------------------------------------------
        JPanel tablePanel = createTablePanel();

        // ------------------------------------------------------------
        // Create the bottom tabbedPane
        // ------------------------------------------------------------
        this.tabbedPane = createTabbedPane();

        // ------------------------------------------------------------
        // Create the Splitter - top table Panel and bottom tabbedPane
        // ------------------------------------------------------------
        splitPane = new Splitter(JSplitPane.VERTICAL_SPLIT, true, tablePanel, this.tabbedPane);

        // Add the splitter to this panel
        GridBagLayout layout = new GridBagLayout();
        setLayout(layout);

        add(splitPane);
        
        layout.setConstraints(splitPane, new GridBagConstraints(0, 0, 1, 1, 1.0, 1.0, GridBagConstraints.CENTER,
                                                                GridBagConstraints.BOTH, new Insets(5, 5, 5, 5), 0, 0));
        // ------------------------------------------------------------
        // Add the New / Delete Buttons below the splitter
        // ------------------------------------------------------------
        JPanel buttonsPanel = this.createProviderButtonsPanel();
        
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

    private JPanel createTablePanel () {
    	JPanel tablePanel = new JPanel(new GridBagLayout());
    	
    	// ------------------------------------------------
    	// Create the Providers Table
    	// ------------------------------------------------
    	
        // NOTE-- columns must be in order by indices above
        tableModel = new com.metamatrix.toolbox.ui.widget.table.DefaultTableModel(new Vector(Arrays.asList(new String[] {
            "Domain Name", "Provider Type"})), 0); //$NON-NLS-1$ //$NON-NLS-2$
        table = new TableWidget(tableModel, true);
        table.setEditable(false);
        table.sizeColumnsToFitContainer(DOMAIN_NAME_COL_NUM);
        table.sizeColumnsToFitContainer(PROVIDER_TYPE_COL_NUM);
        table.setSelectionMode(ListSelectionModel.MULTIPLE_INTERVAL_SELECTION);
        table.setSortable(false);
        
//        ProvidersCellRenderer renderer = new ProvidersCellRenderer();
//        TableColumnModel columnModel = table.getColumnModel();
//        columnModel.getColumn(PROVIDER_COL_NUM).setCellRenderer(renderer);
//        columnModel.getColumn(PROVIDER_TYPE_COL_NUM).setCellRenderer(renderer);

        JScrollPane tableSP = new JScrollPane(table);
        
    	// ------------------------------------------------
    	// Add Table ScrollPane to panel
    	// ------------------------------------------------
        GridBagConstraints gbcTable = new GridBagConstraints();
        gbcTable.gridx = 0;
        gbcTable.gridy = 0;
        gbcTable.insets = new Insets(3, 3, 0, 0);
        gbcTable.fill = GridBagConstraints.BOTH;       
        gbcTable.weightx = 1.0;
        gbcTable.weighty = 1.0;

        tablePanel.add(tableSP,gbcTable);
        
        
    	// ------------------------------------------------
    	// Add Button controls to panel
    	// ------------------------------------------------

        gbcTable.gridx = 1;
        gbcTable.fill = GridBagConstraints.NONE;
        gbcTable.weightx = 0.0;
        gbcTable.weighty = 0.0;

        tablePanel.add(createTableButtonsPanel(),gbcTable);
        
    	return tablePanel;
    }
    
    private JPanel createTableButtonsPanel () {
        JPanel buttonsPanel = new JPanel(new GridLayout(2, 1, 0, 6));

        moveRowUpButton = new ButtonWidget("Up"); //$NON-NLS-1$
        moveRowUpButton.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent ev) {
				moveRowUpPressed();
			}
		});
    	
        moveRowDownButton = new ButtonWidget("Down"); //$NON-NLS-1$
        moveRowDownButton.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent ev) {
				moveRowDownPressed();
			}
		});
        
        buttonsPanel.add(moveRowUpButton);
        buttonsPanel.add(moveRowDownButton);
        
        return buttonsPanel;
    }
    
    private JPanel createProviderButtonsPanel () {
        JPanel buttonsPanel = new JPanel();
        buttonsPanel.setLayout(new GridLayout(1, 4, 5, 5));

        newButton = new ButtonWidget(NEW_CONNECTOR_TITLE);        
        deleteButton = new ButtonWidget(DELETE_CONNECTOR_TITLE);       
        

        // Can export if user has System.READ role
        if (canModify) {
            setup(newButton, actionNewProvider);
            actionNewProvider.setEnabled(true);

            setup(deleteButton, actionDeleteProvider);
            actionDeleteProvider.setEnabled(false);
        } else {
            actionNewProvider.setEnabled(false);
            actionDeleteProvider.setEnabled(false);
        }
        
        buttonsPanel.add(newButton);
        buttonsPanel.add(deleteButton);
        
        return buttonsPanel;
    }
    
    private JTabbedPane createTabbedPane () {
        tabbedPane = new JTabbedPane();
        detailsPanel = new AuthenticationProviderDetailsPanel( );
        propertiesPanel = new AuthenticationProviderPropertiesPanel(canModify, connection);

        // NOTE-- tabs MUST be inserted in order by indices above
        tabbedPane.addTab("Details", detailsPanel); //$NON-NLS-1$
        tabbedPane.addTab("Properties", propertiesPanel); //$NON-NLS-1$
        tabbedPane.addChangeListener(new ChangeListener() {

            public void stateChanged(ChangeEvent ev) {
                selectedTabChanged();
            }
        });
        return tabbedPane;
    }
    
    
    public void createActions() {
        actionNewProvider = new ProviderPanelAction(ProviderPanelAction.NEW_PROVIDER, this);
        actionNewProvider.putValue(Action.NAME, NEW_CONNECTOR_TITLE);
        addActionToList(MenuEntry.ACTION_MENUITEM, actionNewProvider);

        actionDeleteProvider = new ProviderPanelAction(ProviderPanelAction.DELETE_PROVIDER, this);
        actionDeleteProvider.putValue(Action.NAME, DELETE_CONNECTOR_TITLE);
        addActionToList(MenuEntry.ACTION_MENUITEM, actionDeleteProvider);
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
        // Order to put into the table
        List providerOrder = getAuthenticationOrder();
        try {
            List providers = new ArrayList(getAuthenticationProviderManager().getAllProviders());
            // Load the providers in order
            for (Iterator orderIter = providerOrder.iterator(); orderIter.hasNext(); ) {
            	String providerName = (String)orderIter.next();
            	// Find this provider from list of providers
                for (Iterator it = providers.iterator(); it.hasNext();) {
                    AuthenticationProvider provider = (AuthenticationProvider)it.next();
                    if(provider.getName().equalsIgnoreCase(providerName)) {
                    	addProviderToTableModel(provider);
                    	it.remove();
                    	break;
                    }
                }
            }
            // Add any extra providers at the bottom of the table
            for(Iterator iter = providers.iterator(); iter.hasNext();) {
            	addProviderToTableModel((AuthenticationProvider)iter.next());
            }
            // Update provider ordering
            updateProviderOrder();
        } catch (Exception ex) {
            ExceptionUtility.showMessage("Retrieve basic provider info", ex); //$NON-NLS-1$
            LogManager.logError(LogContexts.CONNECTOR_BINDINGS, ex, "Error retrieving basic provider info."); //$NON-NLS-1$
        }

        forceTableSelection(getSelectedProviders());
    }
    
    private void addProviderToTableModel(AuthenticationProvider provider) {
        Vector vec = new Vector( Arrays.asList(new Object[] {null, null}) );
        vec.setElementAt(provider.getName(), DOMAIN_NAME_COL_NUM);
        vec.setElementAt(provider.getComponentTypeID().getName(), PROVIDER_TYPE_COL_NUM);
        tableModel.addRow(vec);
    }
    
    private List getAuthenticationOrder() {
    	try {
			return getGroupsManager().getDomainNames();
		} catch (Exception e) {
			throw new MetaMatrixRuntimeException(e);
		}
    }
    
    private void setAuthenticationOrder(List authOrder) {
		try {
			// Updates the next startup configuration
			updateConfiguration(authOrder);
		} catch (Exception e) {
			throw new MetaMatrixRuntimeException(e);
		}
    }

    private void updateConfiguration(List authOrder) throws ExternalException {
    	StringBuffer sb = new StringBuffer();
    	Iterator iter = authOrder.iterator();
    	while(iter.hasNext()) {
    		String providerStr = (String)iter.next();
    		sb.append(providerStr);
    		if(iter.hasNext()) {
    			sb.append(","); //$NON-NLS-1$
    		}
    	}
        Configuration config = getConfigurationManager().getConfig(Configuration.NEXT_STARTUP_ID);
        ServiceComponentDefn serviceDefn = config.getServiceComponentDefn(ResourceNames.MEMBERSHIP_SERVICE);
        Properties currentProps = serviceDefn.getProperties();
        
        currentProps.put(ConfigurationPropertyNames.MEMBERSHIP_DOMAIN_ORDER,sb.toString()); 
    	
		getConfigurationManager().modifyService(serviceDefn,currentProps);
    }

    /**
     * Move the selected Provider up in the list
     */
    public void moveRowDownPressed( ) {
    	//------------------------------------------------
    	// Get the selected Row, move it down in the list
    	//------------------------------------------------
    	int selectedRow = table.getSelectedRow();
    	int nRows = table.getRowCount();
    	if(selectedRow>-1 && selectedRow<(nRows-1)) {
    		tableModel.moveRow(selectedRow,selectedRow,selectedRow+1);
    	}
    	//------------------------------------------------
    	// Move selection with the row
    	//------------------------------------------------
    	table.setRowSelectionInterval(selectedRow+1,selectedRow+1);
    	
        // Update provider ordering
        updateProviderOrder();
    }
    
    /**
     * Move the selected Provider up in the list
     */
    public void moveRowUpPressed( ) {
    	//------------------------------------------------
    	// Get the selected Row, move it up in the list
    	//------------------------------------------------
    	int selectedRow = table.getSelectedRow();
    	int nRows = table.getRowCount();
    	if(selectedRow>0 && selectedRow<(nRows)) {
    		tableModel.moveRow(selectedRow,selectedRow,selectedRow-1);
    	}
    	//------------------------------------------------
    	// Move selection with the row
    	//------------------------------------------------
    	table.setRowSelectionInterval(selectedRow-1,selectedRow-1);
    	
        // Update provider ordering
        updateProviderOrder();
    }
    
    private AuthenticationProvider getProviderForRow(int rowIndex) {
    	Vector rowVec = (Vector)tableModel.getDataVector().get(rowIndex);
        String name = (String)rowVec.elementAt(DOMAIN_NAME_COL_NUM);
        return (AuthenticationProvider)this.nameProviderMap.get(name);
    }
    
    private void updateProviderOrder() {
    	List providerOrderedList = new ArrayList();
    	
    	int nRows = table.getRowCount();
    	for(int i=0; i<nRows; i++) {
    		Vector rowVec = (Vector)tableModel.getDataVector().get(i);
            String name = (String)rowVec.elementAt(DOMAIN_NAME_COL_NUM);
            providerOrderedList.add(name);
    	}
    	
    	setAuthenticationOrder(providerOrderedList);
    }
    
    private void setUpDownButtonEnabledStates() {
    	boolean enableUpButton = false;
    	boolean enableDownButton = false;
    	
        if(canModify) {
        	int selectedRow = table.getSelectedRow();
        	int nRows = table.getRowCount();
        	if(selectedRow>-1 && selectedRow<(nRows-1)) {
        		enableDownButton = true;
        	}
        	if(selectedRow>0 && selectedRow<(nRows)) {
        		enableUpButton = true;
        	}
        }
    	    	
    	this.moveRowDownButton.setEnabled(enableDownButton);
    	this.moveRowUpButton.setEnabled(enableUpButton);
    }
    
    /**
     * Set the widget to display as selected the specified list of bindings.
     * 
     * @param binding
     *            List<AuthenticationProvider>
     * @since 4.2
     */
    private void forceTableSelection(List providers) {
        table.getSelectionModel().clearSelection();

        Iterator iter = providers.iterator();
        while (iter.hasNext()) {
            AuthenticationProvider provider = (AuthenticationProvider)iter.next();
            int rowForSelected = rowForProvider(provider.toString());
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
        AuthenticationProvider newProviderDefn = doNewProviderWizard();
        if (newProviderDefn != null) {
            setSelectedProvider(newProviderDefn);
            refresh();
        }
    }

    private AuthenticationProvider doNewProviderWizard() {
        NewAuthenticationProviderWizardController controller = new NewAuthenticationProviderWizardController(connection);
        AuthenticationProvider provider = controller.runWizard();
        return provider;
    }

    public void deletePressed() {
        showDeleteConfirmDialog();
    }

    public void showDeleteConfirmDialog() {
        String msg = "Delete selected Membership Domain providers?"; //$NON-NLS-1$

        int response = CenteredOptionPane.showConfirmDialog(ConsoleMainFrame.getInstance(),
                                                            msg,
                                                            DialogUtility.CONFIRM_DELETE_HDR,
                                                            JOptionPane.YES_NO_OPTION);
        if (response == JOptionPane.YES_OPTION) {
            try {
            	getAuthenticationProviderManager().deleteProviders(selectedProviders);
            } catch (Exception ex) {
                ExceptionUtility.showMessage("Delete Membership Domain providers", ex); //$NON-NLS-1$
                LogManager.logError(LogContexts.CONNECTOR_BINDINGS, ex, "Error in deleting Membership Domain providers."); //$NON-NLS-1$
            }
            deleteButton.setEnabled(false);
            refresh();
        }
    }

    public void showDeleteRefuseServiceIsDeployedDialog() {
        String msg = "Delete not allowed, Connector Binding(s) is running."; //$NON-NLS-1$
        String sTitle = DialogUtility.CONFIRM_DELETE_HDR;

        CenteredOptionPane.showConfirmDialog(ConsoleMainFrame.getInstance(), msg, sTitle, JOptionPane.DEFAULT_OPTION);
    }    

    private int rowForProvider(String provider) {
        int matchRow = -1;
        int numRows = table.getRowCount();
        int row = 0;
        while ((row < numRows) && (matchRow < 0)) {
            String curRowProvider = table.getValueAt(row, DOMAIN_NAME_COL_NUM).toString();
            if (curRowProvider.equals(provider)) {
                matchRow = row;
            } else {
                row++;
            }
        }
        return matchRow;
    }

    /**
     * Get list of selected AuthenticationProvider.
     * 
     * @return List<AuthenticationProvider>
     * @since 4.2
     */
    private List getSelectedProviders() {
        return selectedProviders;
    }
    
    /**
     * Set the list of selected connector bindings to contain just this specified binding.
     * 
     * @param binding
     * @since 4.2
     */
    private void setSelectedProvider(AuthenticationProvider provider) {
        selectedProviders.clear();
        selectedProviders.add(provider);
    }

    /**
     * Set the list of selected connector bindings to contain the specified bindings.
     * 
     * @param bindings
     *            List<ConnectorBinding>
     * @since 4.2
     */
    private void setSelectedProviders(List providers) {
        selectedProviders.clear();
        selectedProviders.addAll(providers);
    }

    /**
     * Clear the list of selected connector bindings.
     * 
     * @param bindings
     *            List<ConnectorBinding>
     * @since 4.2
     */
    private void clearSelectedProviders() {
    	selectedProviders.clear();
    }


    public void receiveUpdateNotification(RuntimeUpdateNotification notification) {
        if (notification instanceof AddedConnectorBindingNotification) {
            refresh();
        }
    }

    public void refresh() {
        // save the currently selected providers
        List oldSelectedProviders = new ArrayList(getSelectedProviders());

        try {
            // refresh the authentication providers in the manager
        	//getAuthenticationProviderManager().getAllProviders(true);
            constructProviderXref();
            populateTable();
        } catch (Exception ex) {
            ExceptionUtility.showMessage("Failed while refreshing the Connector Binding list", ex); //$NON-NLS-1$
            LogManager.logError(LogContexts.CONNECTOR_BINDINGS, ex, "Failed while refreshing the Connector Binding list"); //$NON-NLS-1$
        }

        // restore the provider and select it in the table:
        setSelectedProviders(oldSelectedProviders);
        forceTableSelection(oldSelectedProviders);
        
        setUpDownButtonEnabledStates();
    }

    private void tableSelectionChanged() {
        boolean cancelled = false;        

        List bindings = getSelectedProviders();
        if (bindings.size() == 1 && anyValueChanged()) {
        	AuthenticationProvider provider = (AuthenticationProvider) bindings.get(0);
            
            String message = "Save changes to Membership Domain provider " + provider + " ?"; //$NON-NLS-1$//$NON-NLS-2$
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
                    newList.add(provider);
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
        clearSelectedProviders();

        // disable buttons
        actionDeleteProvider.setEnabled(false);
        moveRowUpButton.setEnabled(false);
        moveRowDownButton.setEnabled(false);

        // clear out bottom panel
        Border border = createTabBorder(null);
        int curTab = tabbedPane.getSelectedIndex();
        switch (curTab) {
            case DETAILS_TAB_NUM:
                if (detailsPanel.getProvider() != null) {
                    detailsPanel.setProvider(null);
                    detailsPanel.setBorder(border);
                }
                break;
            case PROPERTIES_TAB_NUM:
                if (propertiesPanel.getProvider() != null) {
                    propertiesPanel.setProvider(null);
                    propertiesPanel.setBorder(border);
                }
                break;
        }
    }

    private void tableSelectionChangedOneSelected(int selectedRow) {
        // set list of selected bindings
        int convertedRow = table.convertRowIndexToModel(selectedRow);
        String selectedName = (String)table.getModel().getValueAt(convertedRow, DOMAIN_NAME_COL_NUM);
        AuthenticationProvider provider = (AuthenticationProvider)getProviderXref().get(selectedName);
        setSelectedProvider(provider);

        // enable buttons
        actionDeleteProvider.setEnabled(canModify);
        setUpDownButtonEnabledStates();

        // populate bottom panel
        Border border = createTabBorder(provider);
        int curTab = tabbedPane.getSelectedIndex();
        switch (curTab) {
            case DETAILS_TAB_NUM:
                detailsPanel.setProvider(provider);
                detailsPanel.setBorder(border);
                break;
            case PROPERTIES_TAB_NUM:
                propertiesPanel.setProvider(provider);
                propertiesPanel.setBorder(border);
                break;
        }

    }

    private void tableSelectionChangedMultipleSelected(int[] selectedRows) {
        // set list of selected bindings
    	AuthenticationProvider provider = null;
        int numSelected = selectedRows.length;
        for (int i = 0; i < numSelected; i++) {
            int selectedRow = selectedRows[i];
            int convertedRow = table.convertRowIndexToModel(selectedRow);
            String selectedName = (String)table.getModel().getValueAt(convertedRow, DOMAIN_NAME_COL_NUM);

            provider = (AuthenticationProvider)getProviderXref().get(selectedName);
        }
        setSelectedProvider(provider);

        // enable buttons
        actionDeleteProvider.setEnabled(canModify);
        moveRowUpButton.setEnabled(false);
        moveRowDownButton.setEnabled(false);

        // populate bottom panel
        Border border = createTabBorder(null);
        int curTab = tabbedPane.getSelectedIndex();
        switch (curTab) {
            case DETAILS_TAB_NUM:
                if (detailsPanel.getProvider() != null) {
                    detailsPanel.setProvider(null);
                    detailsPanel.setBorder(border);
                }
                break;
            case PROPERTIES_TAB_NUM:
                if (propertiesPanel.getProvider() != null) {
                    propertiesPanel.setProvider(null);
                    propertiesPanel.setBorder(border);
                }
                break;
        }
    }

    private void selectedTabChanged() {
        int curTab = tabbedPane.getSelectedIndex();
        int numSelected = selectedProviders.size();
        AuthenticationProvider selectedProvider = null;
        if (numSelected == 1) {
        	selectedProvider = (AuthenticationProvider)selectedProviders.get(0);
        }

        switch (curTab) {
            case DETAILS_TAB_NUM:
                boolean needsRefresh;
                if (selectedProvider == null) {
                    needsRefresh = (detailsPanel.getProvider() != null);
                } else {
                    needsRefresh = (!selectedProvider.equals(detailsPanel.getProvider()));
                }
                if (needsRefresh) {
                    detailsPanel.setProvider(selectedProvider);
                    detailsPanel.setBorder(createTabBorder(selectedProvider));
                }
                break;
            case PROPERTIES_TAB_NUM:
                if (selectedProvider == null) {
                    needsRefresh = (propertiesPanel.getProvider() != null);
                } else {
                    needsRefresh = (!selectedProvider.equals(propertiesPanel.getProvider()));
                }
                if (needsRefresh) {
                    propertiesPanel.setProvider(selectedProvider);
                    propertiesPanel.setBorder(createTabBorder(selectedProvider));
                }
                break;
        }
        forceRepaint();
    }

    private Border createTabBorder(AuthenticationProvider defn) {
        TitledBorder border;
        String title;

        if (defn == null) {
            title = ""; //$NON-NLS-1$
        } else {
            title = "Membership Domain Provider:  " + defn.toString(); //$NON-NLS-1$
        }
        border = new TitledBorder(title);
        return border;
    }

    public java.util.List /* <Action> */resume() {
        return currentActions;
    }

    public String getTitle() {
        return "Membership Domain Providers"; //$NON-NLS-1$
    }

    public ConnectionInfo getConnection() {
        return connection;
    }

    private HashMap getProviderXref() {
        if (nameProviderMap == null) {
            constructProviderXref();
        }
        return nameProviderMap;
    }

    private void constructProviderXref() {
        nameProviderMap = new HashMap();
        List providersList = null;
        try {
            providersList = new ArrayList(getAuthenticationProviderManager().getAllProviders());
        } catch (Exception e) {
            ExceptionUtility.showMessage("Failed while loading data into Membership Domain Provider Panel", e); //$NON-NLS-1$
        }

        Iterator it = providersList.iterator();
        AuthenticationProvider tempDefn = null;

        while (it.hasNext()) {
            tempDefn = (AuthenticationProvider)it.next();
            nameProviderMap.put(tempDefn.toString(), tempDefn);
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
    
    
    ///////////////////////////////////////////////////////////////////////////
    // INNER CLASSES
    ///////////////////////////////////////////////////////////////////////////

    public final class ProvidersCellRenderer
        extends DefaultTableCellRenderer {

        public Component getTableCellRendererComponent(
            JTable theTable,
            Object theValue,
            boolean theSelectedFlag,
            boolean theHasFocusFlag,
            int theRow,
            int theColumn) {

            // call super to set all background/foreground colors for isSelected, hasFocus
            Component comp = super.getTableCellRendererComponent(
                                 theTable, theValue, theSelectedFlag,
                                 theHasFocusFlag, theRow, theColumn);

            if (theHasFocusFlag) {
                ((JComponent)comp).setBorder(
                    UIDefaults.getInstance().getBorder(
                        TableWidget.FOCUS_BORDER_PROPERTY));
            } else {
                ((JComponent)comp).setBorder(
                    UIDefaults.getInstance().getBorder(
                        TableWidget.NO_FOCUS_BORDER_PROPERTY));
            }
            if (theSelectedFlag) {
                comp.setBackground(theTable.getSelectionBackground());
            } else if(!rowIsEnabled(tableModel,theRow)) {
            	comp.setBackground(Color.PINK);
            } else {
                comp.setBackground(theTable.getBackground());
            }
            return comp;
        }
    }
    
    private boolean rowIsEnabled(TableModel model, int row) {
        boolean enabled = false;
        
        AuthenticationProvider provider = getProviderForRow(row);
        
        if(provider!=null) {
        	enabled = provider.isActivated();
        }
        return enabled;
    }
    
    
}// end ProvidersMain

class ProviderPanelAction extends AbstractPanelAction {

    public static final int NEW_PROVIDER = 0;
    public static final int DELETE_PROVIDER = 1;

    private ProvidersMain panel;

    public ProviderPanelAction(int theType,
                                ProvidersMain panel) {
        super(theType);
        this.panel = panel;
    }

    public void actionImpl(ActionEvent theEvent) {
        if (type == NEW_PROVIDER) {
            panel.newPressed();
        } else if (type == DELETE_PROVIDER) {
            panel.deletePressed();
        }
    }
}// end ProviderPanelAction
