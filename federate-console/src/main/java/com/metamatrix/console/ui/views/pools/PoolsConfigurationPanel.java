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

package com.metamatrix.console.ui.views.pools;

import java.awt.*;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.util.*;

import javax.swing.*;
import javax.swing.event.ListSelectionEvent;
import javax.swing.event.ListSelectionListener;

import com.metamatrix.common.actions.ModificationActionQueue;
import com.metamatrix.common.config.api.*;
import com.metamatrix.common.log.LogManager;
import com.metamatrix.common.object.PropertiedObject;
import com.metamatrix.common.object.PropertiedObjectEditor;
import com.metamatrix.console.connections.ConnectionInfo;
import com.metamatrix.console.models.PoolManager;
import com.metamatrix.console.notification.RuntimeUpdateNotification;
import com.metamatrix.console.ui.NotifyOnExitConsole;
import com.metamatrix.console.ui.layout.BasePanel;
import com.metamatrix.console.ui.layout.WorkspacePanel;
import com.metamatrix.console.ui.util.*;
import com.metamatrix.console.ui.views.DefaultConsoleTableComparator;
import com.metamatrix.console.ui.views.properties.PropertiesMasterPanel;
import com.metamatrix.console.util.*;
import com.metamatrix.toolbox.ui.widget.*;
import com.metamatrix.toolbox.ui.widget.property.PropertiedObjectPanel;
import com.metamatrix.toolbox.ui.widget.table.EnhancedTableColumn;

public class PoolsConfigurationPanel extends BasePanel implements
		NotifyOnExitConsole, WorkspacePanel, POPWithButtonsController {
	public final static int POOL_NAME_COL_NUM = 0;
	public final static int POOL_TYPE_COL_NUM = 1;
	public final static int ACTIVE_COL_NUM = 2;
	public final static int NUM_COLUMNS = 3;
	public final static String POOL_NAME_COL_HDR = "Pool Name";
	public final static String POOL_TYPE_COL_HDR = "Pool Type";
//	public final static String ACTIVE_COL_HDR = "Active Connections";
	public final static String[] COL_HDRS = new String[] {POOL_NAME_COL_HDR,
	    	POOL_TYPE_COL_HDR};		    

	public final static int NEXT_STARTUP_TAB_INDEX = 0;
	public final static int STARTUP_TAB_INDEX = 1;

	private final static double INITIAL_SPLIT_PANE_PROPORTION = 0.5;
					
    // this tracks the current index based on the above indexes                    
	private PoolManager manager;
	private boolean canModify;
	private TableWidget table;
	private com.metamatrix.toolbox.ui.widget.table.DefaultTableModel tableModel;
	private boolean programmaticTableSelectionChange = false;
	private boolean tableHasBeenPopulated = false;
	private PoolConfigTableRowData[] poolInfo;
	private Map /*<pool name + pool type (concatenated) to PoolConfigTableRowData>*/
			poolInfoMap = new HashMap();
	private POPWithButtons[] popContainers;
	private ConfigurationID[] configIDs;
	private AbstractButton newButton;
	
	//Note: each PropertiedObjectEditor is created with the ModificationActionQueue
	//to the corresponding ConfigurationObjectEditor as an argument.  This causes
	//the actions to the PropertiedObjectEditor to be written to the 
	//ConfigurationObjectEditor's actions queue.  Then executing the transactions
	//in this queue will cause the PropertiedObjectEditor changes to be committed.
	private PropertiedObjectEditor[] editors;
	private ConfigurationObjectEditor[] configEditors;
	private ConnectionInfo connection;
	private JPanel lowerPanel;
	private JTabbedPane configTabbedPane;
	
	public PoolsConfigurationPanel(PoolManager mgr, boolean modifiable,
			ConnectionInfo conn) throws Exception {
	    super();
	    this.manager = mgr;
	    this.canModify = modifiable;
	    this.connection = conn;
	    init();
	    layoutStuff();
	    populateTable(null);
	}
	
	private void init() throws Exception {
	    Vector colHdrsVec = new Vector(COL_HDRS.length);
	    for (int i = 0; i < COL_HDRS.length; i++) {
	        colHdrsVec.add(COL_HDRS[i]);
	    }
	    tableModel = new com.metamatrix.toolbox.ui.widget.table.DefaultTableModel(
	    		colHdrsVec);
	    table = new TableWidget(tableModel, true);
        table.setEditable(false);
        table.getSelectionModel().setSelectionMode(
        		ListSelectionModel.SINGLE_SELECTION);
        table.setComparator(DefaultConsoleTableComparator.getInstance());
        table.getTableHeader().setReorderingAllowed(false);
        EnhancedTableColumn nameColumn = (EnhancedTableColumn)table.getColumn(
                POOL_NAME_COL_HDR);
        table.setColumnSortedAscending(nameColumn, false);
        table.getSelectionModel().addListSelectionListener(
        		new ListSelectionListener() {
        	public void valueChanged(ListSelectionEvent ev) {
				if (!programmaticTableSelectionChange) {
				    if (!ev.getValueIsAdjusting()) {
				        tableSelectionChanged();
				    }
				}
        	}
        });
		popContainers = new POPWithButtons[2];
		editors = new PropertiedObjectEditor[2];
		configEditors = new ConfigurationObjectEditor[2];
		configIDs = new ConfigurationID[2];
		for (int i = 0; i < 2; i++) {
		    configEditors[i] = manager.getConfigurationObjectEditor();
		    ModificationActionQueue maq = configEditors[i].getDestination();
		    editors[i] = manager.getPropertiedObjectEditorForPool(maq);
		    switch (i) {
		        case NEXT_STARTUP_TAB_INDEX:
		        	configIDs[i] = manager.getConfigurationID(
		        			PoolManager.NEXT_STARTUP_CONFIG);
		        	break;
		        case STARTUP_TAB_INDEX:
		        	configIDs[i] = manager.getConfigurationID(
							PoolManager.STARTUP_CONFIG);
					break;
		    } 
		}
		
		newButton = new ButtonWidget("New...");
		newButton.addActionListener(new ActionListener() {
		    public void actionPerformed(ActionEvent ev) {
		        newPressed();
		    }
		});
		newButton.setEnabled(canModify);
	}
	
	private void layoutStuff() {
	    GridBagLayout layout = new GridBagLayout();
	    this.setLayout(layout);
	    JScrollPane tableSP = new JScrollPane(table);
	    JPanel upperPanel = new JPanel();
	    GridBagLayout ul = new GridBagLayout();
	    upperPanel.setLayout(ul);
	    upperPanel.add(tableSP);
	    ul.setConstraints(tableSP, new GridBagConstraints(0, 0, 1, 1, 1.0, 1.0,
	    		GridBagConstraints.CENTER, GridBagConstraints.BOTH, 
	    		new Insets(2, 2, 2, 2), 0, 0));
	    upperPanel.add(newButton);
	    ul.setConstraints(newButton, new GridBagConstraints(1, 0, 1, 1, 
	    		0.0, 0.0, GridBagConstraints.CENTER, GridBagConstraints.NONE,
	    		new Insets(2, 2, 2, 2), 0, 0));
		lowerPanel = new JPanel();
		lowerPanel.setLayout(new GridLayout(1, 1));
		JPanel detailPanel = new JPanel();
		GridBagLayout dl = new GridBagLayout();
		detailPanel.setLayout(dl);
		lowerPanel.add(detailPanel);
		configTabbedPane = new JTabbedPane();
		for (int i = 0; i < 2; i++) {
		    PropertiedObjectPanel pop = new PropertiedObjectPanel(editors[i], manager.getEncryptor());
		    pop.setShowExpertProperties(true);
		    pop.setShowOptionalProperties(true);
		    pop.createComponent();
		    PropertiedObjectPanelHolder popHolder = 
		    		new PropertiedObjectPanelHolder(pop, null);
		    popContainers[i] = new POPWithButtons(popHolder, editors[i], this);
		    switch (i) {
		        case NEXT_STARTUP_TAB_INDEX:
		        	configTabbedPane.addTab("Next Startup", 
		        			PropertiesMasterPanel.NEXT_STARTUP_ICON,
		        			popContainers[i]);
		        	pop.setReadOnlyForced(!canModify);
		        	break;
		        case STARTUP_TAB_INDEX:
		        	configTabbedPane.addTab("Startup", 
		        			PropertiesMasterPanel.STARTUP_ICON, popContainers[i]);
		        	pop.setReadOnlyForced(true);
		        	break;
		    }
		}
		detailPanel.add(configTabbedPane);
		dl.setConstraints(configTabbedPane, new GridBagConstraints(0, 0, 1, 1,
				1.0, 1.0, GridBagConstraints.CENTER, GridBagConstraints.BOTH,
				new Insets(2, 2, 2, 2), 0, 0));
		
		JSplitPane splitPane = new PoolsConfigSplitPane(upperPanel, lowerPanel,
				INITIAL_SPLIT_PANE_PROPORTION);
		splitPane.setOneTouchExpandable(true);
		this.add(splitPane);
		layout.setConstraints(splitPane, new GridBagConstraints(0, 0, 1, 1,
				1.0, 1.0, GridBagConstraints.CENTER, GridBagConstraints.BOTH,
				new Insets(0, 0, 0, 0), 0, 0));
	}
	
	public String getTitle() {
	    return "Connection Pools Configuration";
	}
	
	public ConnectionInfo getConnection() {
		return connection;
	}
	
	public java.util.List resume() {
	    return new ArrayList(0);
	}
	
	private void newPressed() {
	    doAddNewPoolWizard();
	}
	
	private void doAddNewPoolWizard() {
        String[] poolTypes = null;
        try {
            poolTypes = manager.getPoolTypes();
        } catch (Exception ex) {
            String msg = "Error retrieving connection pool types";
            LogManager.logError(LogContexts.RESOURCE_POOLS, ex, msg);
            ExceptionUtility.showMessage(msg, ex);
        }
        if (poolTypes != null) {
            PoolAdder adder = new PoolAdder(manager, poolTypes, 
            		configIDs[NEXT_STARTUP_TAB_INDEX]);
            PoolNameAndType pnt = adder.go();
            if (pnt != null) {
                populateTable(pnt.getName());
            }
        }
    }
	
	public boolean doApplyChanges(PropertiedObjectPanel pop) {
	    boolean proceeding = true;
	   	int i = 0;
	   	ConfigurationObjectEditor ed = null;
        PropertiedObject po=null;
	   	while (ed == null) {
	   	    PropertiedObjectPanel curPOP = 
	   	    		popContainers[i].getPropertiedObjectPanel();
                    
	   	    if (pop == curPOP) {

               po = curPOP.getPropertiedObject();
	   	        ed = configEditors[i];
	   	    } else {
	   	        i++;
	   	    }
	   	}
	   	try {
            
   	        manager.updatePoolProperties(ed, po);
            manager.applyPropertiesToActivePool(po);
        
	   	} catch (Exception ex) {
	   	    String msg = "Error applying Connection Pool property changes";
	   	    LogManager.logError(LogContexts.RESOURCE_POOLS, ex, msg);
	   	    ExceptionUtility.showMessage(msg, ex);
	   	}
	   	return proceeding;
	}
	
	private PoolConfigTableRowData getSelectedPoolInfo() {
	    PoolConfigTableRowData item = null;
	    int viewRow = table.getSelectedRow();
	    if (viewRow >= 0) {
	        int modelRow = table.convertRowIndexToModel(viewRow);
	        String name = (String)tableModel.getValueAt(modelRow, POOL_NAME_COL_NUM);
	        String type = (String)tableModel.getValueAt(modelRow, POOL_TYPE_COL_NUM);
	        String key = name + type;
	        item = (PoolConfigTableRowData)poolInfoMap.get(key);
	    }
	    return item;
	}
	
	private void tableSelectionChanged() {
	    PoolConfigTableRowData info = getSelectedPoolInfo();
	    displayDetailForPool(info);
	}
	
	private void displayDetailForPool(PoolConfigTableRowData info) {
	    if (info == null) {
	        lowerPanel.setBorder(new TitledBorder("Properties for Connection Pool"));
	        for (int i = 0; i < popContainers.length; i++) {
	            //Switch each tabbed pane to display an empty panel
	            configTabbedPane.setComponentAt(i, new JPanel());
	        }
	    } else {
	        lowerPanel.setBorder(new TitledBorder(
	        		"Properties for Connection Pool " + info.getPoolName()));
	        try {
	            for (int i = 0; i < popContainers.length; i++) {
	            	ResourceDescriptor rd = null;
	        		switch (i) {
	        			case NEXT_STARTUP_TAB_INDEX:
	        				rd = info.getNextStartupResourceDescriptor();
	        				break;
	        			case STARTUP_TAB_INDEX:
	        				rd = info.getStartupResourceDescriptor();
	        				break;
	        		}
	            	if (rd == null) {
	            	    configTabbedPane.setComponentAt(i, new JPanel());
	            	} else {
	            		PropertiedObject po = 
	            				manager.getPropertiedObjectForResourceDescriptor(rd);
	            		popContainers[i].setPropertiedObject(po);
	            		Component curComponent = configTabbedPane.getComponentAt(i);
	            		if (curComponent != popContainers[i]) {
	                		configTabbedPane.setComponentAt(i, popContainers[i]);
	            		}
	            	}
	            }
	        } catch (Exception ex) {
	            String msg = "Error retrieving connection pool properties";
	            LogManager.logError(LogContexts.RESOURCE_POOLS, ex, msg);
	            ExceptionUtility.showMessage(msg, ex);
	        }
	   	}
	}
	
	private void populateTable(String poolToSelect) {
	    poolInfo = null;
	    boolean continuing  = true;
	    try {
			poolInfo = manager.getPoolConfigData();
			setMapFromPoolInfo();
	    } catch (Exception ex) {
	        String msg = "Error retrieving connection pool information.";
	        LogManager.logError(LogContexts.RESOURCE_POOLS, ex, msg);
	        ExceptionUtility.showMessage(msg, ex);
	        continuing = false;
	    }
	    if (continuing) {
	        tableModel.setRowCount(0);
	        for (int i = 0; i < poolInfo.length; i++) {
				Object[] rowValues = new Object[NUM_COLUMNS];
				rowValues[POOL_NAME_COL_NUM] = poolInfo[i].getPoolName();
				rowValues[POOL_TYPE_COL_NUM] = poolInfo[i].getPoolType();
//				rowValues[ACTIVE_COL_NUM] = new Boolean(poolInfo[i].isActive());
				tableModel.addRow(rowValues);
	        }
	    }
	    if (!tableHasBeenPopulated) {
	    	tableHasBeenPopulated = true;
	    	table.sizeColumnsToFitData();
	    }
		if (poolToSelect != null) {
	    	int modelRow = modelRowForPoolName(poolToSelect);
	    	if (modelRow >= 0) {
	        	int viewRow = table.convertRowIndexToView(modelRow);
	        	table.setRowSelectionInterval(viewRow, viewRow);
	    	}
	    }
	}
	
	private void setMapFromPoolInfo() {
	    poolInfoMap.clear();
	    for (int i = 0; i < poolInfo.length; i++) {
	        String key = poolInfo[i].getPoolName() + poolInfo[i].getPoolType();
	        poolInfoMap.put(key, poolInfo[i]);
	    }
	}
	
	private String getSelectedName() {
	    String name = null;
	    int selectedViewRow = table.getSelectedRow();
	    if (selectedViewRow >= 0) {
	        int modelRow = table.convertRowIndexToModel(selectedViewRow);
	        name = (String)tableModel.getValueAt(modelRow, POOL_NAME_COL_NUM);
	    }
	    return name;
	}
	
	private int modelRowForPoolName(String poolName) {
	    int matchRow = -1;
	    int numRows = table.getRowCount();
	    int i = 0;
	    while ((i < numRows) && (matchRow < 0)) {
	        String curPoolName = (String)tableModel.getValueAt(i,
	        		POOL_NAME_COL_NUM);
	        if (poolName.equals(curPoolName)) {
	            matchRow = i;
	        } else {
	            i++;
	        }
	    }
	    return matchRow;
	}
	
	public boolean havePendingChanges() {
	    boolean havePending = false;
	    int i = 0;
	    while ((!havePending) && (i < popContainers.length)) {
	        havePending = popContainers[i].havePendingChanges();
	        if (!havePending) {
	            i++;
	        }
	    }
	    return havePending;
	}
	
    public boolean finishUp() {
        boolean stayingHere = false;
        int i = 0;
        while ((i < popContainers.length) && (!stayingHere)) {
            if (popContainers[i].havePendingChanges()) {
                configTabbedPane.setSelectedIndex(i);
                String configName = "";
                switch (i) {
                    case NEXT_STARTUP_TAB_INDEX:
						configName = "Next Startup";
						break;
					case STARTUP_TAB_INDEX:
						configName = "Startup";
						break;
                }
                String msg = "Save property changes to connection pool \"" + 
        				getSelectedName() + "\" for " + configName +
        				" configuration?";
        		int response = DialogUtility.showPendingChangesDialog(msg,
        				manager.getConnection().getURL(),
        				manager.getConnection().getUser());
        		switch (response) {
            		case DialogUtility.YES:
            			PropertiedObjectPanel pop = 
            					popContainers[i].getPropertiedObjectPanel();
            			doApplyChanges(pop);
                		stayingHere = false;
                		break;
            		case DialogUtility.NO:
                		stayingHere = false;
                		break;
            		case DialogUtility.CANCEL:
                		stayingHere = true;
                		break;
        		}
            }
            i++;
        }
        return (!stayingHere);
    }
    
    public void receiveUpdateNotification(RuntimeUpdateNotification notification) {
    	//TODO
    }
}//end PoolsConfigurationPanel




class PoolsConfigSplitPane extends JSplitPane {
    double initialDividerLoc;
    boolean hasBeenPainted = false;
    
    public PoolsConfigSplitPane(Component upper, Component lower, 
    		double dividerLoc) {
        super(JSplitPane.VERTICAL_SPLIT, true, upper, lower);
        initialDividerLoc = dividerLoc;
    }
    
    public void paint(Graphics g) {
        super.paint(g);
        if (!hasBeenPainted) {
            hasBeenPainted = true;
            this.setDividerLocation(initialDividerLoc);
        }
    }
}//end PoolsConfigSplitPane   		
