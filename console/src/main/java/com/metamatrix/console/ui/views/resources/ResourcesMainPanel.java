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

package com.metamatrix.console.ui.views.resources;

import java.awt.Dimension;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.GridLayout;
import java.awt.Insets;
import java.awt.Point;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Vector;

import javax.swing.JCheckBox;
import javax.swing.JDialog;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JSplitPane;
import javax.swing.ListSelectionModel;
import javax.swing.event.ListSelectionEvent;
import javax.swing.event.ListSelectionListener;

import com.metamatrix.common.config.api.SharedResource;
import com.metamatrix.common.log.LogManager;
import com.metamatrix.common.object.PropertiedObject;
import com.metamatrix.console.connections.ConnectionInfo;
import com.metamatrix.console.models.ResourceManager;
import com.metamatrix.console.models.ResourcePropertiedObjectEditor;
import com.metamatrix.console.notification.RuntimeUpdateNotification;
import com.metamatrix.console.ui.NotifyOnExitConsole;
import com.metamatrix.console.ui.layout.BasePanel;
import com.metamatrix.console.ui.layout.ConsoleMainFrame;
import com.metamatrix.console.ui.layout.WorkspacePanel;
import com.metamatrix.console.ui.util.POPWithButtons;
import com.metamatrix.console.ui.util.POPWithButtonsController;
import com.metamatrix.console.ui.util.PropertiedObjectPanelHolder;
import com.metamatrix.console.ui.views.DefaultConsoleTableComparator;
import com.metamatrix.console.util.DialogUtility;
import com.metamatrix.console.util.ExceptionUtility;
import com.metamatrix.console.util.LogContexts;
import com.metamatrix.console.util.Refreshable;
import com.metamatrix.console.util.StaticUtilities;
import com.metamatrix.toolbox.ui.widget.ButtonWidget;
import com.metamatrix.toolbox.ui.widget.CheckBox;
import com.metamatrix.toolbox.ui.widget.LabelWidget;
import com.metamatrix.toolbox.ui.widget.Splitter;
import com.metamatrix.toolbox.ui.widget.TableWidget;
import com.metamatrix.toolbox.ui.widget.TitledBorder;
import com.metamatrix.toolbox.ui.widget.property.PropertiedObjectPanel;
import com.metamatrix.toolbox.ui.widget.table.EnhancedTableColumn;


public class ResourcesMainPanel extends BasePanel implements
		WorkspacePanel, NotifyOnExitConsole, POPWithButtonsController,
		Refreshable {
	public final static int RESOURCE_NAME_COL_NUM = 0;
	public final static int RESOURCE_TYPE_COL_NUM = 1;
	public final static int NUM_COLUMNS = 2;
	
	public final static double SPLIT_PANE_DIVIDER_LOC_FRAME_PROPORTION = 0.6;
	
	public final static String RESOURCE_NAME_COL_HDR = "Resource Name"; //$NON-NLS-1$
	public final static String RESOURCE_TYPE_COL_HDR = "Resource Type"; //$NON-NLS-1$
	
	private ConnectionInfo connection;	    
	private TableWidget table;
	private com.metamatrix.toolbox.ui.widget.table.DefaultTableModel tableModel;
	private PropertiedObjectPanel pop = null;
	private POPWithButtons popWithButtons = null;
	private ResourcePropertiedObjectEditor rpoe;
	private ResourceManager manager;
	private JPanel lowerPanel;
	private Map /*<String pool name to ResourceData>*/ resourceMap =
			new HashMap();
	private boolean canModify;
	private String currentSelectedResource = null;
	private boolean programmaticTableRowSelection = false;
	private boolean showingNeedRestartToActivateDialog = true;
	
	public ResourcesMainPanel(ResourceManager manager, boolean modifiable,
			ConnectionInfo conn) throws Exception {
	    super();
	    this.manager = manager;
	    this.canModify = modifiable;
	    this.connection = conn;
	    init();
	    populateTable(null);
	    //Select first resource in table
	    if (table.getRowCount() > 0) {
	    	table.getSelectionModel().setSelectionInterval(0, 0);
	    }
	}
	
	private void init() throws Exception {
	    String[] colHdrs = new String[NUM_COLUMNS];
	    colHdrs[RESOURCE_NAME_COL_NUM] = RESOURCE_NAME_COL_HDR;
	    colHdrs[RESOURCE_TYPE_COL_NUM] = RESOURCE_TYPE_COL_HDR;
	    Vector colHdrsVec = new Vector(Arrays.asList(colHdrs));
	    tableModel = new com.metamatrix.toolbox.ui.widget.table.DefaultTableModel(
	    		colHdrsVec);
	    table = new TableWidget(tableModel, true);
        table.getSelectionModel().setSelectionMode(
        		ListSelectionModel.SINGLE_SELECTION);
        table.setEditable(false);
        table.setComparator(DefaultConsoleTableComparator.getInstance());
        EnhancedTableColumn nameColumn = (EnhancedTableColumn)table.getColumn(
                RESOURCE_NAME_COL_HDR);
        table.setColumnSortedAscending(nameColumn, false);
        table.getSelectionModel().addListSelectionListener(new ListSelectionListener() {
            public void valueChanged(ListSelectionEvent ev) {
			    tableRowSelectionChanged(ev);
			}
		});
		JScrollPane tableJSP = new JScrollPane(table);
        
        lowerPanel = new JPanel();
        GridBagLayout ll = new GridBagLayout();
        lowerPanel.setLayout(ll);
        
        rpoe = manager.getResourcePropertiedObjectEditor();
        pop = new PropertiedObjectPanel(rpoe, manager.getEncryptor());
        pop.setReadOnlyForced(!canModify);
        pop.createComponent();
        PropertiedObjectPanelHolder popHolder = new PropertiedObjectPanelHolder(
        		pop, null);
        popWithButtons = new POPWithButtons(popHolder, rpoe, this);
        popWithButtons.setButtonsVisible(canModify);		
        lowerPanel.add(popWithButtons);
        ll.setConstraints(popWithButtons, new GridBagConstraints(0, 0, 1, 1, 
        		1.0, 1.0, GridBagConstraints.CENTER, GridBagConstraints.BOTH,
        		new Insets(5, 5, 5, 5), 0, 0));
        
        JSplitPane splitter = new Splitter(JSplitPane.VERTICAL_SPLIT, true,
        		tableJSP, lowerPanel);
        splitter.setOneTouchExpandable(true);
        int frameHeight = ConsoleMainFrame.getInstance().getSize().height;
        int dividerLoc = (int)(frameHeight *  
        		SPLIT_PANE_DIVIDER_LOC_FRAME_PROPORTION);
        splitter.setDividerLocation(dividerLoc);
        
        this.setLayout(new GridLayout(1, 1));
        this.add(splitter);
	}
   	
   	private void tableRowSelectionChanged(ListSelectionEvent ev) {
   	    if (!programmaticTableRowSelection) {
   	        if (!ev.getValueIsAdjusting()) {
   	            boolean continuing = true;
   	            if (havePendingChanges()) {
   	                continuing = finishUp();
   	            }
   	            if (continuing) {
   	            	int selectedModelRow = 
   	        				table.getSelectionModel().getLeadSelectionIndex();
   	        		if (selectedModelRow >= 0) {
   	        	    	selectedModelRow = table.convertRowIndexToModel(
   	        	    			selectedModelRow);
   	        		}
					String resourceName = resourceNameForModelRow(
							selectedModelRow);
					ResourceData rData = (ResourceData)resourceMap.get(
							resourceName);
					currentSelectedResource = resourceName;
					PropertiedObject po = rData.getPropertiedObject();
					displayDetail(currentSelectedResource, po);
				} else {
   	                //User cancelled.  Reselect previous entitlement.
   	                int modelRow = modelRowForResource(currentSelectedResource);
   	                int viewRow = table.convertRowIndexToView(modelRow);
   	                programmaticTableRowSelection = true;
   	                table.getSelectionModel().setSelectionInterval(viewRow, 
   	                		viewRow);
   	                programmaticTableRowSelection = false;
   	            }
			}
   	    }
   	}
	
	public boolean havePendingChanges() {
	    return popWithButtons.havePendingChanges();
	}
	
	public boolean finishUp() {
		boolean stayingHere = false;
        String msg = "Save changes to Resource \"" +  //$NON-NLS-1$
        		currentSelectedResource + "\"?"; //$NON-NLS-1$
        int response = DialogUtility.showPendingChangesDialog(msg,
        		manager.getConnection().getURL(),
        		manager.getConnection().getUser());
        switch (response) {
            case DialogUtility.YES:
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
        return (!stayingHere);
	}
		
	public java.util.List /*<Action>*/ resume() {
	    ArrayList actions = new ArrayList(1);
		return actions;
	}
	
	public String getTitle() {
	    return "Resources"; //$NON-NLS-1$
	}

	public ConnectionInfo getConnection() {
		return connection;
	}
		
	private void populateTable(String resourceToSelect) {
	    SharedResource[] resources = null;
	    boolean continuing = true;
	    try {
	        resources = manager.getResources();
	    } catch (Exception ex) {
	        String msg = "Error retrieving list of Resources"; //$NON-NLS-1$
	        LogManager.logError(LogContexts.RESOURCES, ex, msg);
	        ExceptionUtility.showMessage(msg, ex);
	        continuing = false;
	    }
	    if (continuing) {
	    	resourceMap.clear();
	    	programmaticTableRowSelection = true;
	    	tableModel.setRowCount(0);
	    	programmaticTableRowSelection = false;
	    	for (int i = 0; i < resources.length; i++) {
	    	    ResourceData rData = new ResourceData(resources[i], manager);
				resourceMap.put(rData.getName(), rData);
				Object[] rowData = new Object[NUM_COLUMNS];
				rowData[RESOURCE_NAME_COL_NUM] = rData.getName();
				rowData[RESOURCE_TYPE_COL_NUM] = rData.getType();
				tableModel.addRow(rowData);
	    	}
	    	if (resourceToSelect != null) {
    	        int modelRow = modelRowForResource(resourceToSelect);
    	        if (modelRow != 0) {
    	            int viewRow = table.convertRowIndexToView(modelRow);
    	            programmaticTableRowSelection = true;
    	            table.setRowSelectionInterval(viewRow, viewRow);
    	            programmaticTableRowSelection = false;
    	            currentSelectedResource = resourceToSelect;
    	        } else {
    	            currentSelectedResource = null;
    	        }
    	    } else {
    	        currentSelectedResource = null;
    	    }
	    }
	}

	private void displayDetail(String resourceName, PropertiedObject po) {
	    lowerPanel.setBorder(new TitledBorder("Properties for Resource " + //$NON-NLS-1$
	    		resourceName));
	    popWithButtons.setPropertiedObject(po);
	    //This must set the apply and reset buttons to disabled, or we have a problem
	    popWithButtons.setButtons();
	}
		
	private int modelRowForResource(String resourceName) {
	    int matchingRow = -1;
	    int i = 0;
	    int numRows = table.getRowCount();
	    while ((matchingRow < 0) && (i < numRows)) {
	        String curName = (String)tableModel.getValueAt(i,
	        		RESOURCE_NAME_COL_NUM);
	        if (resourceName.equals(curName)) {
	            matchingRow = i;
	        } else {
	            i++;
	        }
	    }
	    return matchingRow;
	}

	private String resourceNameForModelRow(int modelRow) {
		String name = null;
		if (modelRow < tableModel.getRowCount()) {
			name = (String)tableModel.getValueAt(modelRow, 
	   				RESOURCE_NAME_COL_NUM);
		}
	   	return name;
	}
		
	public boolean doApplyChanges(PropertiedObjectPanel pop) {
	    boolean proceeding = true;
	    if (showingNeedRestartToActivateDialog) {
	        proceeding = showNeedRestartDialog();
        }
        
	    if (proceeding) {
            try {
                manager.updateResourceProperties(rpoe);
                populateTable(currentSelectedResource);
            } catch (Exception ex) {
                String msg = "Error applying Resources property changes"; //$NON-NLS-1$
                LogManager.logError(LogContexts.RESOURCES, ex, msg);
                ExceptionUtility.showMessage(msg, ex);
            }
        }
	   	
	   	return proceeding;
	}
	 
	private boolean showNeedRestartDialog() {
		boolean proceeding = true;
		RestartToActivateDialog dialog = new RestartToActivateDialog();
		dialog.show();
		proceeding = dialog.wasOKPressed();
		showingNeedRestartToActivateDialog = (!dialog.doNotShowChecked());
		return proceeding;
	}
    
    public void receiveUpdateNotification(RuntimeUpdateNotification notification) {
    	//TODO
    }
				       
	public void refresh() {
	    boolean continuing = true;
	    if (havePendingChanges()) {
	        continuing = finishUp();
	    }
	    if (continuing) {
	        populateTable(currentSelectedResource);
	    }
	}
}//ResourcesMainPanel




class RestartToActivateDialog extends JDialog {
    private boolean okPressed = false;
    private JCheckBox doNotShowWarning;
    
    public RestartToActivateDialog() {
        super(ConsoleMainFrame.getInstance(), "Server Restart Needed"); //$NON-NLS-1$
        this.setModal(true);
        initialize();
    }
    
    private void initialize() {
        GridBagLayout layout = new GridBagLayout();
        this.getContentPane().setLayout(layout);
        LabelWidget noteLabel = new LabelWidget(
        		"Note-- Changes to Resource property values will only take " + //$NON-NLS-1$
        		"effect at next server restart."); //$NON-NLS-1$
        doNotShowWarning = new CheckBox(
        		"In the future, do not show this warning."); //$NON-NLS-1$
        ButtonWidget okButton = new ButtonWidget("OK"); //$NON-NLS-1$
        okButton.addActionListener(new ActionListener() {
            public void actionPerformed(ActionEvent ev) {
                okPressed();
            }
        });
        ButtonWidget cancelButton = new ButtonWidget("Cancel"); //$NON-NLS-1$
        cancelButton.addActionListener(new ActionListener() {
            public void actionPerformed(ActionEvent ev) {
                cancelPressed();
            }
        });
        JPanel buttonsPanel = new JPanel(new GridLayout(1, 2, 20, 0));
        buttonsPanel.add(okButton);
        buttonsPanel.add(cancelButton);
        this.getContentPane().add(noteLabel);
		this.getContentPane().add(doNotShowWarning);
        this.getContentPane().add(buttonsPanel);
        layout.setConstraints(noteLabel, new GridBagConstraints(0, 0, 1, 1,
        		0.0, 0.0, GridBagConstraints.WEST, GridBagConstraints.NONE,
        		new Insets(10, 10, 20, 10), 0, 0));
        layout.setConstraints(doNotShowWarning, new GridBagConstraints(0, 1, 
        		1, 1, 0.0, 0.0, GridBagConstraints.WEST, GridBagConstraints.NONE,
        		new Insets(10, 20, 0, 4), 0, 0));
        layout.setConstraints(buttonsPanel, new GridBagConstraints(0, 2, 0, 0,
        		0.0, 0.0, GridBagConstraints.CENTER, GridBagConstraints.NONE,
        		new Insets(10, 4, 4, 4), 0, 0));
        this.pack();
        Dimension size = this.getSize();
        Point loc = StaticUtilities.centerFrame(size);
        this.setLocation(loc);
    }
    
    private void okPressed() {
        okPressed = true;
      	this.dispose();
    }
    
    private void cancelPressed() {
        this.dispose();
    }
    
    public boolean wasOKPressed() {
        return okPressed;
    }
    
    public boolean doNotShowChecked() {
        return doNotShowWarning.isSelected();
    }
}//end RestartToActivateDialog
