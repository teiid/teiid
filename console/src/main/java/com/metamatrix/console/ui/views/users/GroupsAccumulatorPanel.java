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

package com.metamatrix.console.ui.views.users;

import java.awt.Component;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.GridLayout;
import java.awt.Insets;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Vector;

import javax.swing.JPanel;
import javax.swing.JPopupMenu;
import javax.swing.JScrollPane;
import javax.swing.ListSelectionModel;
import javax.swing.event.ListSelectionEvent;
import javax.swing.event.ListSelectionListener;

import com.metamatrix.console.ConsolePlugin;
import com.metamatrix.console.connections.ConnectionInfo;
import com.metamatrix.console.models.AuthenticationProviderManager;
import com.metamatrix.console.models.ModelManager;
import com.metamatrix.console.security.UserCapabilities;
import com.metamatrix.console.ui.layout.BasePanel;
import com.metamatrix.console.ui.layout.ConsoleMainFrame;
import com.metamatrix.console.ui.views.DefaultConsoleTableComparator;
import com.metamatrix.console.ui.views.authorization.ProvidersChangedEvent;
import com.metamatrix.console.ui.views.authorization.ProvidersChangedListener;
import com.metamatrix.console.util.DialogUtility;
import com.metamatrix.console.util.ExceptionUtility;
import com.metamatrix.platform.security.api.MetaMatrixPrincipalName;
import com.metamatrix.toolbox.ui.widget.ButtonWidget;
import com.metamatrix.toolbox.ui.widget.PopupMenuFactory;
import com.metamatrix.toolbox.ui.widget.TableWidget;
import com.metamatrix.toolbox.ui.widget.table.EnhancedTableColumn;



public class GroupsAccumulatorPanel extends BasePanel implements 
		PopupMenuFactory, ProvidersChangedListener {

    private final static String REMOVE_DLG_TITLE = ConsolePlugin.Util.getString(
    "GroupsAccumulatorPanel.removeDialog.title"); //$NON-NLS-1$

    private final static String REMOVE_DLG_MSG = ConsolePlugin.Util.getString(
    "GroupsAccumulatorPanel.removeDialog.msg"); //$NON-NLS-1$

	
	private JScrollPane groupsTablePane;
    private JPanel buttonsPanel;
    private ButtonWidget addButton;
    private ButtonWidget removeButton;
    private Object[] blockedItems;
    private GroupsTable groupsTable;
    private GroupsTableModel groupsTableModel;
    private GroupAccumulatorController groupController;
    private List initialGroups;
    private List currentGroups;
    private ListSelectionListener listener;

    public GroupsAccumulatorPanel(List initialGroups, GroupAccumulatorController gc) {
        super();
        this.initialGroups = initialGroups!=null ? initialGroups : new ArrayList();
        this.currentGroups = initialGroups;
        this.groupController = gc;
        init();
    }
    
    /** 
     * @see com.metamatrix.console.ui.views.authorization.ProvidersChangedListener#providersChanged(com.metamatrix.console.ui.views.authorization.ProvidersChangedEvent)
     * 
     */
    public void providersChanged(ProvidersChangedEvent event) {
        final int type = event.getType();
        if(type == ProvidersChangedEvent.DELETED || type == ProvidersChangedEvent.NEW) {
            setButtonStates();
        }
    }



    private void init( ) {
    	// Init table
    	groupsTablePane = createGroupsTablePane();
        
        addButton = new ButtonWidget("Add ...");
        addButton.addActionListener(new ActionListener() {
            public void actionPerformed(ActionEvent ev) {
                addButtonPressed();
            }
        });
        removeButton = new ButtonWidget("Remove");
        removeButton.addActionListener(new ActionListener() {
            public void actionPerformed(ActionEvent ev) {
                removeButtonPressed();
            }
        });
        layoutStuff();
        
        // Create selection listener
        listener = new ListSelectionListener() {
            public void valueChanged(ListSelectionEvent ev) {
			    tableRowSelectionChanged(ev);
			}
        };
        addSelectionListener();
        setButtonStates();
    }

    private void addSelectionListener() {
    	this.groupsTable.getSelectionModel().addListSelectionListener(this.listener);
    }
    
    private void removeSelectionListener() {
    	this.groupsTable.getSelectionModel().removeListSelectionListener(this.listener);
    }
    
    private void layoutStuff() {
    	
        GridBagLayout layout = new GridBagLayout();
        setLayout(layout);

        add(groupsTablePane);
        layout.setConstraints(groupsTablePane, new GridBagConstraints(0, 0, 1, 1,
                1.0, 1.0, GridBagConstraints.CENTER, GridBagConstraints.BOTH,
                new Insets(2, 2, 2, 2), 0, 0));
        
        buttonsPanel = new JPanel();
        add(buttonsPanel);
        buttonsPanel.setLayout(new GridLayout(1, 2, 0, 10));
        buttonsPanel.add(addButton);
        buttonsPanel.add(removeButton);
        layout.setConstraints(buttonsPanel, new GridBagConstraints(0, 1, 1, 1,
                0.0, 0.0, GridBagConstraints.CENTER, GridBagConstraints.NONE,
                new Insets(5, 3, 5, 3), 0, 0));

    }

	public Object[] getBlockedItems() {
	    return blockedItems;
	}
	
    public void itemSelectionBlocked() {
        //Nothing to do here
    }
    
    private void addButtonPressed() {
    	removeSelectionListener();
    	
    	Collection addedGroups = this.groupController.addPressed(this);
    	if(addedGroups != null) {
    		Iterator iter = addedGroups.iterator();
    		while( iter.hasNext() ) {
        		groupsTableModel.addRow((MetaMatrixPrincipalName)iter.next());
    		}
    	}
    	setButtonStates();
    	
    	addSelectionListener();
    }

    private void removeButtonPressed() {
    	removeSelectionListener();
    	
    	List removedGroups = new ArrayList();
    	int selectedRowIndex = this.groupsTable.getSelectedRow();
    	int modelRow = this.groupsTable.convertRowIndexToModel(selectedRowIndex);
    	if(modelRow >= 0) {
        	// Really remove groups from the table?
            boolean reallyRemove = 
            	DialogUtility.yesNoDialog(ConsoleMainFrame.getInstance(),REMOVE_DLG_MSG,REMOVE_DLG_TITLE);
        	if(reallyRemove) {
        		removedGroups.add(this.currentGroups.remove(modelRow));
        		boolean success = this.groupController.removeConfirmed(this,removedGroups);
        		if(success) {
        			groupsTableModel.removeRow(modelRow);
        		}
        	}
    	}
    	int nRows = this.groupsTable.getRowCount();
    	if(nRows>0) {
    		this.groupsTable.getSelectionModel().setSelectionInterval(0,0);
     	}
    	setButtonStates();
    	
    	addSelectionListener();
    }
    
    private void setButtonStates() {   	
        //Capture edit privs
        final ConnectionInfo conn = groupController.getGroupsManager().getConnection();
        final UserCapabilities cap = UserCapabilities.getInstance();
        final boolean canModify = cap.canModifySecurity(conn);
        if(!canModify) {
            this.removeButton.setEnabled(canModify);
            this.addButton.setEnabled(canModify);
            return;
        }

        boolean enableRemove = false;
    	if(groupsTableModel.getRowCount() > 0) {
        	int selectedModelRow = groupsTable.getSelectionModel().getMinSelectionIndex();
    		if(selectedModelRow >= 0) {
    			enableRemove = true;
    		}
    	}
		this.removeButton.setEnabled(enableRemove);
        
        boolean enableAdd = true;
        try {
            AuthenticationProviderManager authMgr = ModelManager.getAuthenticationProviderManager(conn);
            int providerCnt = authMgr.getAllProviders().size();
            enableAdd = providerCnt > 0;
        } catch (Exception err) {
            ExceptionUtility.showMessage("Error setting button states", err);
        }
        
        this.addButton.setEnabled(enableAdd);
    }
    
    public JPopupMenu getPopupMenu(Component context) {
        return new JPopupMenu();
    }
    
    
    
    public void repopulateTable(List groups) {
    	this.removeSelectionListener();
    	
    	this.initialGroups = groups;
    	this.currentGroups = new ArrayList(this.initialGroups);
    	groupsTableModel.init(this.initialGroups);
    	groupsTable.setEditable(false);
    	
    	this.addSelectionListener();
    }
    
   	private void tableRowSelectionChanged(ListSelectionEvent ev) {
        if (!ev.getValueIsAdjusting()) {
        	setButtonStates();
		}
   	}

    private JScrollPane createGroupsTablePane() {
        groupsTableModel = new GroupsTableModel(this.initialGroups);
        groupsTable = new GroupsTable(groupsTableModel);
        groupsTable.getSelectionModel().setSelectionMode(ListSelectionModel.SINGLE_SELECTION);
        groupsTable.setEditable(false);
        
    	JScrollPane groupsSP = new JScrollPane(groupsTable);
    	        
        return groupsSP;
    }
    
    class GroupsTable extends TableWidget {
        public GroupsTable(GroupsTableModel model) {
            super(model, true);
            init();
        }

        private void init() {
            getSelectionModel().setSelectionMode(ListSelectionModel.SINGLE_SELECTION);
            this.setEditable(false);
            this.setComparator(DefaultConsoleTableComparator.getInstance());
            this.getTableHeader().setReorderingAllowed(false);
            EnhancedTableColumn nameColumn = (EnhancedTableColumn)this.getColumn(GroupsTableModel.GROUP_NAME);
            this.setColumnSortedAscending(nameColumn, false);
        }
    }
    
    public class GroupsTableModel
    				extends com.metamatrix.toolbox.ui.widget.table.DefaultTableModel {
		public final static int  GROUP_COL_NUM = 0;
		public final static String GROUP_NAME = "Group Name";
//		public final static int GROUP_DOMAIN_COL_NUM = 1;
//		public final static String GROUP_DOMAIN = "Group Domain";
		public final static int NUM_COLUMNS = 1;
		
		public GroupsTableModel(Collection rows) {
		    //NOTE-- columns must be given in order as defined above
		    super(new Vector(Arrays.asList(new String[] {GROUP_NAME})), 0);
		    init(rows);
		}
		
		public Class getColumnClass(int col) {
		    return String.class;
		}
		
		public void init(Collection rows) {
		    //Remove rows one at a time.
		    int numRows = getRowCount();
		    for (int i = numRows - 1; i >= 0; i--) {
		        removeRow(i);
		    }
		    //Add new rows
		    Iterator iter = rows.iterator();
		    while(iter.hasNext()) {
		    	MetaMatrixPrincipalName group = (MetaMatrixPrincipalName)iter.next();
	            addRow(group);
		    }
		}
		
		public void addRow(MetaMatrixPrincipalName group) {
	    	Object[] rowData = new Object[NUM_COLUMNS];
            rowData[GROUP_COL_NUM] = group.getName();
            //rowData[GROUP_DOMAIN_COL_NUM] = group.getDomain();
            addRow(rowData);
		}

	}

}//end GroupsAccumulatorPanel
