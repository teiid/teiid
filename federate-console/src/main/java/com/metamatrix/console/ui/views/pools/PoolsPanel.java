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
import javax.swing.table.TableCellRenderer;
import javax.swing.table.TableColumn;

import com.metamatrix.common.log.LogManager;
import com.metamatrix.common.pooling.api.*;
import com.metamatrix.console.connections.ConnectionInfo;
import com.metamatrix.console.models.PoolManager;
import com.metamatrix.console.notification.RuntimeUpdateNotification;
import com.metamatrix.console.ui.NotifyOnExitConsole;
import com.metamatrix.console.ui.layout.*;
import com.metamatrix.console.ui.util.ColumnSortInfo;
import com.metamatrix.console.ui.views.DefaultConsoleTableComparator;
import com.metamatrix.console.util.*;
import com.metamatrix.platform.admin.api.runtime.ResourcePoolStats;
import com.metamatrix.toolbox.ui.widget.*;
import com.metamatrix.toolbox.ui.widget.table.DefaultTableCellRenderer;
import com.metamatrix.toolbox.ui.widget.table.EnhancedTableColumn;

public class PoolsPanel extends BasePanel implements WorkspacePanel,
		NotifyOnExitConsole, AutoRefreshable {
	public final static double SPLIT_PANE_DIVIDER_LOCATION = 0.4;
	public final static double LOWER_SPLIT_PANE_DIVIDER_LOCATION = 0.6;
	
	//Note-- these items used as array indices, so numbering must start with 0.
	public final static int NO_AGGREGATING = 0;
	public final static int AGGREGATED_BY_PROCESS = 1;
	public final static int AGGREGATED_BY_POOL = 2;
	public final static int NUM_AGGREGATION_TYPES = 3;

	private final static String POOL_HDR = "Connection Pool";
	private final static String PROCESS_HDR = "Process";
	private final static String HOST_HDR = "Host";
	private static String[] STATISTICS_SHOWN_IN_TABLE = null;
     
	//		new String[] {"Number of Resources in Pool", "Physical Resources Created"};

	private final static String PROPERTY_HDR = "Property";
	private final static String VALUE_HDR = "Value";
	public final static int PROPERTY_COL_NUM = 0;
	public final static int VALUE_COL_NUM = 1;
	private final static int NUM_DETAIL_COLS = 2;
	private final static String[] DETAIL_COL_HDRS = new String[] {
	    	PROPERTY_HDR, VALUE_HDR};
	
	private final static String CREATION_TIME_HDR = "Creation Time";
	private final static String IN_USE_BY_HDR = "In Use By";
	private final static String LAST_ACCESS_HDR = "Last Access";
	private final static int RESOURCE_TBL_POOL_COL_NUM = 0;
	private final static int RESOURCE_TBL_PROCESS_COL_NUM = 1;
	private final static int RESOURCE_TBL_HOST_COL_NUM = 2;
	private final static int CREATION_TIME_COL_NUM = 3;
	private final static int IN_USE_BY_COL_NUM = 4;
	private final static int LAST_ACCESS_COL_NUM = 5;
	private final static int NUM_RESOURCE_COLS = 6;
	private final static String[] RESOURCE_COL_HDRS = new String[] {
	    	POOL_HDR, PROCESS_HDR, HOST_HDR, CREATION_TIME_HDR, IN_USE_BY_HDR, 
	    	LAST_ACCESS_HDR};
	    				
	private final static String[] NO_AGGREGATING_NON_STATS_COL_HDRS = 
			new String[] {POOL_HDR, PROCESS_HDR, HOST_HDR};
	private final static String[] AGGREGATED_BY_PROCESS_NON_STATS_COL_HDRS =
			new String[] {PROCESS_HDR, HOST_HDR};
	private final static String[] AGGREGATED_BY_POOL_NON_STATS_COL_HDRS =
			new String[] {POOL_HDR};
	
	private PoolManager manager;
//	private boolean canModify;
	private ConnectionInfo connection;
	private AbstractButton refreshButton;
	private java.util.List /*<Action>*/ actions = new ArrayList(1);
	private int currentAggregationType;
	private JRadioButton noAggregatingJRB = new PoolsPanelRadioButton(
			"No Subtotaling");
	private JRadioButton byProcessJRB = new PoolsPanelRadioButton(
			"Subtotals by Process");
	private JRadioButton byPoolJRB = new PoolsPanelRadioButton(
			"Subtotals by Connection Pool");
			
	//Note-- each Map value in this map has a key for each non-statistic column in
	//the table for its aggregation type.  This includes Pool, Process, and Host.
	private Map /*<PoolTypeWithAggregationType to Map[] (row data)>*/ 
			valuesMap = new HashMap();
	
	private ResourcePoolStats[] currentStats;		
	private ArrayList /*<String>*/ tabHeaders = new ArrayList(10);
	private ArrayList /*<TableWidget>*/ tables = new ArrayList(10);
	private JPanel aggregationPanel;
	private JPanel allDetailsPanel;
	private JPanel lowerPanel;
	private JTabbedPane tabbedPane;
	private TableWidget detailTable;
	private com.metamatrix.toolbox.ui.widget.table.DefaultTableModel 
			detailTableModel;
	private JPanel detailPanel;
	private TableWidget resourceTable;
	private com.metamatrix.toolbox.ui.widget.table.DefaultTableModel
			resourceTableModel;
	private JPanel resourcePanel;
	private PoolsPanelSplitPane splitPane = null;
	private PoolsPanelSplitPane lowerSplitPane = null;
	private boolean initializing = false;
	private boolean tablesUpdatedOnce[];
	private AutoRefresher autoRefresher;
	private boolean programmaticTableSelection = false;
	private boolean refreshWithRawData = true;
	private Map[] /*<Integer (tab index) to ColumnSortInfo[], array indexed by
			aggregation type>*/ aggCSI;
	private Map[] /*<Integer (tab index) to ColumnWidthInfo[], array indexed by
			aggregation type>*/ colWidthsMap;
		
    private boolean refreshingTable;
    
	public PoolsPanel(PoolManager mgr, boolean canModify,
			ConnectionInfo conn) {
	    super();
	    this.manager = mgr;
//	    this.canModify = canModify;
	    this.connection = conn;
	    this.autoRefresher = new AutoRefresher(this, 15, false, connection);
	    tablesUpdatedOnce = new boolean[NUM_AGGREGATION_TYPES];
	    aggCSI = new HashMap[NUM_AGGREGATION_TYPES];
	    colWidthsMap = new HashMap[NUM_AGGREGATION_TYPES];
	    for (int i = 0; i < NUM_AGGREGATION_TYPES; i++) {
	        tablesUpdatedOnce[i] = false;
	        aggCSI[i] = new HashMap();
	        colWidthsMap[i] = new HashMap();
	    }
        STATISTICS_SHOWN_IN_TABLE = new String[2];
        STATISTICS_SHOWN_IN_TABLE[0] = ResourcePoolStatisticNames.getDisplayName(ResourcePoolStatisticNames.NUM_OF_RESOURCES_IN_POOL);
        STATISTICS_SHOWN_IN_TABLE[1] = ResourcePoolStatisticNames.getDisplayName(ResourcePoolStatisticNames.TOTAL_PHYSICAL_RESOURCES_USED);
        
	    init();
	}
	
	private void init() {
		initializing = true;
	    refreshButton = new ButtonWidget("Refresh");
	    refreshButton.addActionListener(new ActionListener() {
	    	public void actionPerformed(ActionEvent ev) {
	    		refresh();
	    	}
	    });
                		
    	ButtonGroup aggregationGroup = new ButtonGroup();
    	aggregationGroup.add(noAggregatingJRB);
    	aggregationGroup.add(byProcessJRB);
    	aggregationGroup.add(byPoolJRB);
    	ActionListener buttonListener = new ActionListener() {
    	    public void actionPerformed(ActionEvent ev) {
    	        aggregationSelectionChanged();
    	    }
    	};
	
		currentAggregationType = NO_AGGREGATING;
		noAggregatingJRB.setSelected(true);
		
    	noAggregatingJRB.addActionListener(buttonListener);
    	byProcessJRB.addActionListener(buttonListener);
    	byPoolJRB.addActionListener(buttonListener);

		Vector detailColsVec = new Vector(DETAIL_COL_HDRS.length);
		for (int i = 0; i < DETAIL_COL_HDRS.length; i++) {
		    detailColsVec.add(DETAIL_COL_HDRS[i]);
		}
		detailTableModel = 
				new com.metamatrix.toolbox.ui.widget.table.DefaultTableModel(
				detailColsVec);
		detailTable = new DetailTable(detailTableModel);
        detailTable.getSelectionModel().setSelectionMode(
        		ListSelectionModel.SINGLE_SELECTION);
        detailTable.setEditable(false);
        detailTable.setComparator(DefaultConsoleTableComparator.getInstance());
        EnhancedTableColumn propColumn = 
        		(EnhancedTableColumn)detailTable.getColumn(PROPERTY_HDR);
        detailTable.setColumnSortedAscending(propColumn, false);
        JScrollPane detailTableJSP = new JScrollPane(detailTable);
        detailPanel = new JPanel(new GridLayout(1, 1));
        detailPanel.add(detailTableJSP);
        detailPanel.setBorder(new TitledBorder("Additional Statistics"));
        
        Vector resourceColsVec = new Vector(RESOURCE_COL_HDRS.length);
        for (int i = 0; i < RESOURCE_COL_HDRS.length; i++) {
            resourceColsVec.add(RESOURCE_COL_HDRS[i]);
        }
        resourceTableModel = 
        		new com.metamatrix.toolbox.ui.widget.table.DefaultTableModel(
        		resourceColsVec);
        resourceTable = new TableWidget(resourceTableModel);
        resourceTable.getSelectionModel().setSelectionMode(
        		ListSelectionModel.SINGLE_SELECTION);
        resourceTable.setEditable(false);
        resourceTable.setComparator(DefaultConsoleTableComparator.getInstance());
        EnhancedTableColumn resourceColumn = 
        		(EnhancedTableColumn)resourceTable.getColumn(LAST_ACCESS_HDR);
        resourceTable.setColumnSortedAscending(resourceColumn, false);
        JScrollPane resourceTableJSP = new JScrollPane(resourceTable);
        resourcePanel = new JPanel(new GridLayout(1, 1));
        resourcePanel.add(resourceTableJSP);
	    resourcePanel.setBorder(new TitledBorder("Connections"));
        
		layoutStuff();
			
		refresh();
	    int tabIndex = tabbedPane.getSelectedIndex();
	    if (tabIndex >= 0) {
	        TableWidget table = (TableWidget)tables.get(tabIndex);
			if (table.getRowCount() > 0) {
				refreshWithRawData = false;
    		    table.getSelectionModel().setSelectionInterval(0, 0);
    		    refreshWithRawData = true;
			}
		}
		int numTables = tables.size();
		for (int i = 0; i < numTables; i++) {
			TableWidget table = (TableWidget)tables.get(i);
			table.sizeColumnsToFitData();
		}
		initializing = false;
	}
	
	private void layoutStuff() {
	    GridBagLayout layout = new GridBagLayout();
	    this.setLayout(layout);
	    tabbedPane = new JTabbedPane();
	    lowerPanel = new JPanel();
	    GridBagLayout ll = new GridBagLayout();
	    lowerPanel.setLayout(ll);
	    JPanel refreshButtonPanel = new JPanel();
	    lowerPanel.add(refreshButtonPanel);
	    ll.setConstraints(refreshButtonPanel, new GridBagConstraints(0, 0, 1, 1,
	    		1.0, 1.0, GridBagConstraints.CENTER, GridBagConstraints.BOTH,
	    		new Insets(0, 0, 0, 0), 0, 0));
	    GridBagLayout rl = new GridBagLayout();
	    refreshButtonPanel.setLayout(rl);
	    refreshButtonPanel.add(refreshButton);
	    rl.setConstraints(refreshButton, new GridBagConstraints(0, 0, 1, 1,
	    		0.0, 0.0, GridBagConstraints.CENTER, GridBagConstraints.NONE,
	    		new Insets(1, 1, 1, 1), 0, 0));
	    aggregationPanel = new JPanel();
	    aggregationPanel.setBorder(new TitledBorder(""));
	    lowerPanel.add(aggregationPanel);
	    ll.setConstraints(aggregationPanel, new GridBagConstraints(1, 0, 1, 1,
	    		0.0, 0.0, GridBagConstraints.CENTER, GridBagConstraints.NONE,
	    		new Insets(1, 1, 1, 1), 0, 0));
	    GridBagLayout sl = new GridBagLayout();
	    aggregationPanel.setLayout(sl);
	    aggregationPanel.add(noAggregatingJRB);
	    sl.setConstraints(noAggregatingJRB, new GridBagConstraints(0, 0, 1, 1,
	    		0.0, 0.0, GridBagConstraints.WEST, GridBagConstraints.NONE,
	    		new Insets(0, 0, 0, 0), 0, 0));
	    aggregationPanel.add(byProcessJRB);
	    sl.setConstraints(byProcessJRB, new GridBagConstraints(0, 1, 1, 1,
	    		0.0, 0.0, GridBagConstraints.WEST, GridBagConstraints.NONE,
	    		new Insets(0, 0, 0, 0), 0, 0));
	    aggregationPanel.add(byPoolJRB);
	    sl.setConstraints(byPoolJRB, new GridBagConstraints(0, 2, 1, 1,
	    		0.0, 0.0, GridBagConstraints.WEST, GridBagConstraints.NONE,
	    		new Insets(0, 0, 0, 0), 0, 0));
	    		
		lowerSplitPane = new PoolsPanelSplitPane(detailPanel, resourcePanel,
				LOWER_SPLIT_PANE_DIVIDER_LOCATION);
		allDetailsPanel = new JPanel(new GridLayout(1, 1));
		allDetailsPanel.add(lowerSplitPane);
		splitPane = new PoolsPanelSplitPane(tabbedPane, allDetailsPanel,
				SPLIT_PANE_DIVIDER_LOCATION);
						
		this.add(splitPane);
		this.add(lowerPanel);
		layout.setConstraints(splitPane, new GridBagConstraints(0, 0, 1, 1,
				1.0, 1.0, GridBagConstraints.CENTER, GridBagConstraints.BOTH,
				new Insets(0, 0, 0, 0), 0, 0));
		layout.setConstraints(lowerPanel, new GridBagConstraints(0, 1, 1, 1,
				0.0, 0.0, GridBagConstraints.CENTER, GridBagConstraints.BOTH,
				new Insets(0, 0, 0, 0), 0, 0));
	}

	private void aggregationSelectionChanged() {
	    int aggregationState = -1;
	    if (noAggregatingJRB.isSelected()) {
	        aggregationState = NO_AGGREGATING;
	    } else if (byProcessJRB.isSelected()) {
	        aggregationState = AGGREGATED_BY_PROCESS;
	    } else if (byPoolJRB.isSelected()) {
	        aggregationState = AGGREGATED_BY_POOL;
	    }
	    if ((aggregationState >= 0) && (aggregationState != 
	    		currentAggregationType)) {
	    	saveAllColumnWidths(currentAggregationType);
	    	currentAggregationType = aggregationState;
			updateTables(null, true);
	    }
	}
	
	private void updateTables(PoolProcessHost selectedPoolProcessHost,
			boolean aggregationChanged) {
	    for (int i = 0; i < tabHeaders.size(); i++) {
	        String tabHeader = (String)tabHeaders.get(i);
	        TableWidget table = (TableWidget)tables.get(i);
	        ColumnSortInfo[] csi = null;
	        if (!tablesUpdatedOnce[currentAggregationType]) {
	            switch (currentAggregationType) {
	                case NO_AGGREGATING:
	                	ColumnSortInfo poolSort = new ColumnSortInfo(POOL_HDR, 
	                			true);
	        			ColumnSortInfo processSort = new ColumnSortInfo(
	        					PROCESS_HDR, true);
	        			ColumnSortInfo hostSort = new ColumnSortInfo(HOST_HDR, 
	        					true);
	        			csi = new ColumnSortInfo[] {poolSort, processSort, 
	        			    	hostSort};
	        			break;
	        		case AGGREGATED_BY_POOL:
	        			ColumnSortInfo poolSort2 = new ColumnSortInfo(POOL_HDR,
	        					true);
	        			csi = new ColumnSortInfo[] {poolSort2};
	        			break;
	        		case AGGREGATED_BY_PROCESS:
	        			ColumnSortInfo processSort2 = new ColumnSortInfo(
	        					PROCESS_HDR, true);
	        			ColumnSortInfo hostSort2 = new ColumnSortInfo(HOST_HDR,
	        					true);
	        			csi = new ColumnSortInfo[] {processSort2, hostSort2};
	        			break;
	            }
	            aggCSI[currentAggregationType].put(new Integer(i), csi);
	      	} else if (aggregationChanged) {
	      	    csi = (ColumnSortInfo[])aggCSI[currentAggregationType].get(
	      	    		new Integer(i));
	      	} else {
	            csi = ColumnSortInfo.getTableColumnSortInfo(table);
	        }
	        updateTable(table, i, tabHeader, csi);
	    }
	    tablesUpdatedOnce[currentAggregationType] = true;
	    int tabIndex = tabbedPane.getSelectedIndex();
	    if (tabIndex >= 0) {
	        TableWidget table = (TableWidget)tables.get(tabIndex);
	        selectPoolProcessHost(table, selectedPoolProcessHost);
	    }
	    updateDetailTable(selectedPoolProcessHost);
	    updateResourcePanel(selectedPoolProcessHost);
	}
	
	private void updateTable(TableWidget table, int tableIndex, String poolType,
			ColumnSortInfo[] csi) {
		com.metamatrix.toolbox.ui.widget.table.DefaultTableModel tableModel =
	    		(com.metamatrix.toolbox.ui.widget.table.DefaultTableModel)
	    		table.getModel();
	    programmaticTableSelection = true;
	    tableModel.setRowCount(0);
	    programmaticTableSelection = false;
	    PoolTypeWithAggregationType mapKey = new PoolTypeWithAggregationType(
	    		poolType, new Integer(currentAggregationType));
		Map[] rowData = (Map[])valuesMap.get(mapKey);
		Vector columnNames = new Vector(10);
		switch (currentAggregationType) {
			case NO_AGGREGATING:
				for (int i = 0; i < NO_AGGREGATING_NON_STATS_COL_HDRS.length;
						i++) {
					columnNames.add(NO_AGGREGATING_NON_STATS_COL_HDRS[i]);
				}
				break;
			case AGGREGATED_BY_PROCESS:
				for (int i = 0; i < AGGREGATED_BY_PROCESS_NON_STATS_COL_HDRS.length;
						i++) {
					columnNames.add(AGGREGATED_BY_PROCESS_NON_STATS_COL_HDRS[i]);
				}
				break;
			case AGGREGATED_BY_POOL:
				for (int i = 0; i < AGGREGATED_BY_POOL_NON_STATS_COL_HDRS.length;
						i++) {
					columnNames.add(AGGREGATED_BY_POOL_NON_STATS_COL_HDRS[i]);
				}
				break;
		}
	    if (rowData.length > 0) {
	        //Each element of rowData will contain the same set of keys.  If 
	        //there was no statistic value for a key, the key will still be 
	        //present with a value of null.
	    	for (int i = 0; i < STATISTICS_SHOWN_IN_TABLE.length; i++) {
	    	    if (rowData[0].containsKey(STATISTICS_SHOWN_IN_TABLE[i])) {
	    	        columnNames.add(STATISTICS_SHOWN_IN_TABLE[i]);
	    	    }
	    	}
	    }
	    boolean columnsHaveChanged = haveColumnsChanged(tableModel, columnNames);
	    if (columnsHaveChanged) {
	        tableModel.setColumnIdentifiers(columnNames);
	        //Must re-call setEditable(false) after calling 
	        //setColumnIdentifiers(), as per defect 5505.
			table.setEditable(false);
	    }
	    if (rowData.length > 0) {
	        int numColumns = tableModel.getColumnCount();
	    	for (int i = 0; i < rowData.length; i++) {
	    	    Object[] values = new Object[numColumns];
	    	    for (int j = 0; j < values.length; j++) {
	    	        values[j] = null;
	    	    }
	    	    Iterator it = rowData[i].entrySet().iterator();
	    	    while (it.hasNext()) {
	    	        Map.Entry me = (Map.Entry)it.next();
	    	        String key = (String)me.getKey();
	    	        if (me.getValue() instanceof PoolStatistic) {
	    	            key = ((PoolStatistic)me.getValue()).getDisplayName();
	    	        }
	    	        int columnNum = columnNames.indexOf(key);
	    	        if (columnNum >= 0) {
	    	            if (me.getValue() instanceof PoolStatistic) {
	    	                PoolStatistic ps = (PoolStatistic)me.getValue();
	    	                values[columnNum] = ps.getValue();
	    	            } else {
	    	                values[columnNum] = me.getValue();
	    	            }
	    	        }
	    	    }
	    	    tableModel.addRow(values);
			}
	    }
	    if (columnsHaveChanged) {
	    	ColumnSortInfo.setColumnSortOrder(csi, table);
	    }
	    ColumnWidthInfo[] columnWidthInfo = retrieveColumnWidthInfo(tableIndex,
	    		currentAggregationType);
		if (columnWidthInfo != null) {
	    	sizeColumnsToWidthInfo(table, columnWidthInfo);
	    } else {
	    	table.sizeColumnsToFitData();
	    }
	}
	
	private boolean haveColumnsChanged(
			com.metamatrix.toolbox.ui.widget.table.DefaultTableModel tableModel,
			java.util.List newColumnNames) {
		boolean same = true;
		int oldNumColumns = tableModel.getColumnCount();
		int newNumColumns = newColumnNames.size();
		if (oldNumColumns != newNumColumns) {
		    same = false;
		} else {
		    int i = 0;
		    while (same && (i < oldNumColumns)) {
		        String oldColName = tableModel.getColumnName(i);
		        String newColName = (String)newColumnNames.get(i);
		        if (!oldColName.equals(newColName)) {
		            same = false;
		        } else {
		            i++;
		        }
		    }
		}
		return (!same);
	}
	
	private void getRawData() throws Exception {
	    //Since it is expected that the number of resource pools will be fairly
	    //small, we will go ahead and do all aggregating at the time of raw
	    //data retrieval.
	    currentStats = manager.getPoolStats();
	    String[] poolTypes = poolTypesInStats(currentStats);
	   	String[] sortedPoolTypes = StaticQuickSorter.quickStringSort(poolTypes);
	   	//Add a tab for each pool type for which we do not already have a tab.
	   	//This should add all tabs the first time executed.
	   	for (int i = 0; i < sortedPoolTypes.length; i++) {
	   	    int index = tabHeaders.indexOf(sortedPoolTypes[i]);
	   	    if (index < 0) {
	   	        addTabAndTable(sortedPoolTypes[i]);
	   	    }
	   	}
	   	//Replace  contents of valuesMap with values computed from new stats.
	   	valuesMap.clear();
	   	int numTabs = tabHeaders.size();
	   	for (int i = 0; i < numTabs; i++) {
	   	    String tabHeader = (String)tabHeaders.get(i);
	   	    ResourcePoolStats[] statsForType = statsForPoolType(tabHeader, 
	   	    		currentStats);
	   	    doAggregating(tabHeader, statsForType);
	   	}
	}

	private String[] poolTypesInStats(ResourcePoolStats[] stats) {
	    ArrayList /*<String>*/ typesList = new ArrayList(stats.length);
	    for (int i = 0; i < stats.length; i++) {
	        String poolType = stats[i].getPoolType();
	        int index = typesList.indexOf(poolType);
	        if (index < 0) {
	            typesList.add(poolType);
	        }
	    }
	    String[] types = new String[typesList.size()];
	    Iterator it = typesList.iterator();
	    for (int i = 0; it.hasNext(); i++) {
	        types[i] = (String)it.next();
	    }
	    return types;
	}
	
	private ResourcePoolStats[] statsForPoolType(String poolType,
			ResourcePoolStats[] allStats) {
	    ArrayList /*<ResourcePoolStats>*/ statsList = new ArrayList(
	    		allStats.length);
		for (int i = 0; i < allStats.length; i++) {
		    String curType = allStats[i].getPoolType();
		    if (poolType.equals(curType)) {
		        statsList.add(allStats[i]);
		    }
		}
		ResourcePoolStats[] stats = new ResourcePoolStats[statsList.size()];
		Iterator it = statsList.iterator();
		for (int i = 0; it.hasNext(); i++) {
		    stats[i] = (ResourcePoolStats)it.next();
		}
		return stats;
	}

	private void selectPoolProcessHost(TableWidget table,
			PoolProcessHost poolProcessHost) {
		if (poolProcessHost != null) {
    		int poolColumn = StaticTableUtilities.getColumnNumForTableColumn(
    				table, POOL_HDR);
    		int processColumn = StaticTableUtilities.getColumnNumForTableColumn(
    				table, PROCESS_HDR);
    		int hostColumn = StaticTableUtilities.getColumnNumForTableColumn(
    				table, HOST_HDR);	    
    		int rowCount = table.getRowCount();
    		int row = 0;
    		boolean matchFound = false;
    		while ((!matchFound) && (row < rowCount)) {
    		    boolean poolMatches = true;
    		    String poolName = poolProcessHost.getPool();
    		    if ((poolColumn >= 0) && (poolName != null)) {
    		        String curPoolName = (String)table.getModel().getValueAt(row,
    		        		poolColumn);
    		        poolMatches = poolName.equals(curPoolName);
    		    }
    		    if (poolMatches) {
    		        boolean processMatches = true;
    		        String processName = poolProcessHost.getProcess();
    		        if ((processColumn >= 0) && (processName != null)) {
    		            String curProcessName = 
    		            		(String)table.getModel().getValueAt(row, 
    		            		processColumn);
    		            processMatches = processName.equals(curProcessName);
    		        }
    		        if (processMatches) {
    		            boolean hostMatches = true;
    		            String hostName = poolProcessHost.getHost();
    		            if ((hostColumn >= 0) && (hostName != null)) {
    		                String curHostName = 
    		                		(String)table.getModel().getValueAt(row, 
    		                		hostColumn);
    						hostMatches = hostName.equals(curHostName);
    		            }
    		        	if (poolMatches && processMatches && hostMatches) {
    		        	    matchFound = true;
    		        	}
    		        }
    		    }
    		    if (!matchFound) {
    		        row++;
    		    }
    		}
    		if (matchFound) {
    		    int viewIndex = table.convertRowIndexToView(row);
    		    programmaticTableSelection = true;
    		    table.getSelectionModel().setSelectionInterval(viewIndex, 
    		    		viewIndex);
    		    programmaticTableSelection = false;
			}
		}
	}
	
	private void doAggregating(String poolType, ResourcePoolStats[] stats) {
	    //First do the no-aggregation properties
	    PoolTypeWithAggregationType key = new PoolTypeWithAggregationType(poolType,
	    		new Integer(NO_AGGREGATING));
	    Map[] props = new Map[stats.length];
	    for (int i = 0; i < stats.length; i++) {
	        props[i] = new HashMap();
	        for (int j = 0; j < NO_AGGREGATING_NON_STATS_COL_HDRS.length; j++) {
	            String hdr = NO_AGGREGATING_NON_STATS_COL_HDRS[j];
	            if (hdr.equals(POOL_HDR)) {
	                String poolName = stats[i].getPoolName();
	                props[i].put(POOL_HDR, poolName);
	            } else if (hdr.equals(PROCESS_HDR)) {
	                String processName = stats[i].getProcessName();
	                props[i].put(PROCESS_HDR, processName);
	            } else if (hdr.equals(HOST_HDR)) {
	                String hostName = stats[i].getHostName();
	                props[i].put(HOST_HDR, hostName);
	            }
	        }
	        Map statsMap = stats[i].getPoolStatistics();
	        Iterator it = statsMap.entrySet().iterator();
	        while (it.hasNext()) {
	            Map.Entry me = (Map.Entry)it.next();
	            PoolStatistic ps = (PoolStatistic)me.getValue();
	            String displayNamekey = ps.getDisplayName();
	            props[i].put(displayNamekey, ps);
	        }
	        //Must cover all columns to be shown in table.  So if do not have
	        //a value for a column, insert a null.
	        for (int j = 0; j < STATISTICS_SHOWN_IN_TABLE.length; j++) {
	            if (!props[i].containsKey(STATISTICS_SHOWN_IN_TABLE[j])) {
	                props[i].put(STATISTICS_SHOWN_IN_TABLE[j], null);
	            }
	        }
	    }
	    valuesMap.put(key, props);
	    
	    //Do aggregating by pool and process
	    boolean byPool = false;
//	    boolean byProcess = false;
	    for (int ii = 0; ii <= 1; ii++) {
	        if (ii == 0) {
	            byPool = true;
//	            byProcess = false;
	        } else {
	            byPool = false;
//	            byProcess = true;
	        }
			if (byPool) {
			    key = new PoolTypeWithAggregationType(poolType,
			    		new Integer(AGGREGATED_BY_POOL));
			} else {
			    key = new PoolTypeWithAggregationType(poolType,
	    				new Integer(AGGREGATED_BY_PROCESS));
			}
			Map /*<pool name to Map<stat name to aggregate value>>*/ aggMap = 
	    			new HashMap();
	    	Map /*<name-property concatenation to intermediate status object>*/
	    			intermediateMap = new HashMap();
	    	for (int i = 0; i < stats.length; i++) {
	    	    String name;
	    	    if (byPool) {
	    	        name = stats[i].getPoolName();
	    	    } else {
	    	        name = stats[i].getProcessName();
	    	    }
	        	if (!aggMap.containsKey(name)) {
	            	Map propsMap = new HashMap();
	            	if (byPool) {
	            	    propsMap.put(POOL_HDR, name);
	            	} else {
	            	    //Note-- there will be only one host for the process.  
	            	    //The same process name cannot be used on multiple hosts.  
	            	    //MetaMatrix limitation.
	            		propsMap.put(PROCESS_HDR, name);
	            		propsMap.put(HOST_HDR, stats[i].getHostName());
	            	}
	            	aggMap.put(name, propsMap);
	        	}
	        	Map propsMap = (Map)aggMap.get(name);
	        	Map statsForPoolInstance = stats[i].getPoolStatistics();
				Iterator it = statsForPoolInstance.entrySet().iterator();
	        	while (it.hasNext()) {
	        		Map.Entry me = (Map.Entry)it.next();
	            	String propsKey = (String)me.getKey();
	            	if (me.getValue() instanceof PoolStatistic) {
	            	    propsKey = ((PoolStatistic)me.getValue()).getDisplayName();
	            	}
	            	if (!propsMap.containsKey(propsKey)) {
	                	PoolStatistic ps = (PoolStatistic)me.getValue();
	                	int aggType = ps.getAggregationType();
	                	if (aggType == PoolStatistic.NO_AGGREGATE_TYPE) {
	                		propsMap.put(propsKey, 
	                				new ValueAndAggregationType(null,
	                				PoolStatistic.NO_AGGREGATE_TYPE));
	                	} else {
	                	    ValueAndAggregationType vat = 
	                	    		new ValueAndAggregationType(ps.getValue(),
	                	    		aggType);
	                    	propsMap.put(propsKey, vat);
	                    	if (aggType == PoolStatistic.AVG_AGGREGATE_TYPE) {
								String intermediateKey = name + propsKey;
								intermediateMap.put(intermediateKey,
										new CounterAndSum(1, 
										(Number)ps.getValue()));
							} else if (aggType == 
	                    			PoolStatistic.COMMON_AGGREGATE_TYPE) {
								String intermediateKey = name + propsKey;
								intermediateMap.put(intermediateKey, 
										new HashMap());
	                    	}
	                	}
	            	} else {
	                	ValueAndAggregationType va = (ValueAndAggregationType)
	                			propsMap.get(propsKey);
	                	int aggType = va.getAggregationType();
	                	Object value = va.getValue();
	                	PoolStatistic ps = (PoolStatistic)me.getValue();
	                	switch (aggType) {
	                    	case PoolStatistic.NO_AGGREGATE_TYPE:
	                    		//nothing to do for this
	                    		break;
	                    	case PoolStatistic.SUM_AGGREGATE_TYPE:
	            				Object sum = value;
	                			if (sum instanceof Double) {
	                				double curSum = ((Double)sum).doubleValue();
	                    			double increment = 
	                    					((Double)ps.getValue()).doubleValue();
	                    			double newSum = curSum + increment;
	                    			propsMap.put(propsKey, 
	                    					new ValueAndAggregationType(
	                    					new Double(newSum), aggType));
	                			} else if (sum instanceof Float) {
	                				float curSum = ((Float)sum).floatValue();
	                    			float increment = 	
	                    					((Float)ps.getValue()).floatValue();
	                    			float newSum = curSum + increment;
	                    			propsMap.put(propsKey, 
	                    					new ValueAndAggregationType(
	                    					new Float(newSum), aggType));
	                			} else if (sum instanceof Long) {
	                				long curSum = ((Long)sum).longValue();
	                    			long increment = 
	                    					((Long)ps.getValue()).longValue();
	                    			long newSum = curSum + increment;
	                    			propsMap.put(propsKey, 
	                    					new ValueAndAggregationType(
	                    					new Long(newSum), aggType));
	                			} else if (sum instanceof Integer) {
	                				int curSum = ((Integer)sum).intValue();
	                    			int increment = 
	                    					((Integer)ps.getValue()).intValue();
	                    			int newSum = curSum + increment;
	                    			propsMap.put(propsKey, 
	                    					new ValueAndAggregationType(
	                    					new Integer(newSum), aggType));
	                			}
	                			break;
	                    	case PoolStatistic.AVG_AGGREGATE_TYPE:
	            				Object avg = value;
								String intermediateKey = name + propsKey; 
	            				CounterAndSum cs =
	            						(CounterAndSum)intermediateMap.get(
	            						intermediateKey);
	                			if (avg instanceof Double) {
	                		    	cs.incrementCounter();
	                		    	double curSum = 
	                		    			((Double)cs.getSum()).doubleValue();
	                    			double increment = 
	                    					((Double)ps.getValue()).doubleValue();
	                    			double newSum = curSum + increment;
	                    			cs.setSum(new Double(newSum));
	                    			double avgVal = newSum / cs.getCounter();
	                    			propsMap.put(propsKey, 
	                    					new ValueAndAggregationType(
	                    					new Double(avgVal), aggType));
	                			} else if (avg instanceof Float) {
	                		    	cs.incrementCounter();
	                				float curSum = 
	                						((Float)cs.getSum()).floatValue();
	                    			float increment = 	
	                    					((Float)ps.getValue()).floatValue();
	                    			float newSum = curSum + increment;
	                    			cs.setSum(new Float(newSum));
	                    			float avgVal = newSum / cs.getCounter();
	                    			propsMap.put(propsKey, 
	                    					new ValueAndAggregationType(
	                    					new Float(avgVal), aggType));
	                			} else if (avg instanceof Long) {
	                		    	cs.incrementCounter();
	                				long curSum = ((Long)cs.getSum()).longValue();
	                    			long increment = 
	                    					((Long)ps.getValue()).longValue();
	                    			long newSum = curSum + increment;
	                    			cs.setSum(new Long(newSum));
	                    			long avgVal = (newSum / 
	                    					cs.getCounter());
	                    			propsMap.put(propsKey, 
	                    					new ValueAndAggregationType(
	                    					new Long(avgVal), aggType));
	                			} else if (avg instanceof Integer) {
	                		    	cs.incrementCounter();
	                				int curSum = 
	                						((Integer)cs.getSum()).intValue();
	                    			int increment = 
	                    					((Integer)ps.getValue()).intValue();
	                    			int newSum = curSum + increment;
	                    			cs.setSum(new Integer(newSum));
	                    			int avgVal = (newSum / cs.getCounter());
	                    			propsMap.put(propsKey, 
	                    					new ValueAndAggregationType(
	                    					new Integer(avgVal), aggType));
	                			}
	                			break;
	                		case PoolStatistic.MIN_AGGREGATE_TYPE:
	                			Object num = value;
	                			if (num instanceof Double) {
	                		    	double curMin = ((Double)num).doubleValue();
	                		    	double thisVal = 
	                		    			((Double)ps.getValue()).doubleValue();
	                		    	if (thisVal < curMin) {
	                		        	propsMap.put(propsKey,
	                		        			new ValueAndAggregationType( 
	                		        			new Double(thisVal), aggType));
	                		    	}
	                			} else if (num instanceof Float) {
	                		    	float curMin = ((Float)num).floatValue();
	                		    	float thisVal = 
	                		    			((Float)ps.getValue()).floatValue();
	                		    	if (thisVal < curMin) {
	                		        	propsMap.put(propsKey, 
	                		        			new ValueAndAggregationType(
	                		        			new Float(thisVal), aggType));
	                		    	}
	                			} else if (num instanceof Long) {
	                		    	long curMin = ((Long)num).longValue();
	                		    	long thisVal = 
	                		    			((Long)ps.getValue()).longValue();
	                		    	if (thisVal < curMin) {
	                		        	propsMap.put(propsKey,
	                		        			new ValueAndAggregationType(
	                		        			new Long(thisVal), aggType));
	                		    	}
	                			} else if (num instanceof Integer) {
	                		    	int curMin = ((Integer)num).intValue();
	                		    	int thisVal = 
	                		    			((Integer)ps.getValue()).intValue();
	                		    	if (thisVal < curMin) {
	                		        	propsMap.put(propsKey,
	                		        			new ValueAndAggregationType( 
	                		        			new Integer(thisVal), aggType));
	                		    	}
	                			}
	                			break;
	                		case PoolStatistic.MAX_AGGREGATE_TYPE:
	                			num = value;
	                			if (num instanceof Double) {
	                		    	double curMax = ((Double)num).doubleValue();
	                		    	double thisVal = 
	                		    			((Double)ps.getValue()).doubleValue();
	                		    	if (thisVal > curMax) {
	                		        	propsMap.put(propsKey,
	                		        			new ValueAndAggregationType(  
	                		        			new Double(thisVal), aggType));
	                		    	}
	                			} else if (num instanceof Float) {
	                		    	float curMax = ((Float)num).floatValue();
	                		    	float thisVal = 
	                		    			((Float)ps.getValue()).floatValue();
	                		    	if (thisVal > curMax) {
	                		        	propsMap.put(propsKey, 
	                		        			new ValueAndAggregationType(
	                		        			new Float(thisVal), aggType));
	                		    	}
	                			} else if (num instanceof Long) {
	                		    	long curMax = ((Long)num).longValue();
	                		    	long thisVal = 
	                		    			((Long)ps.getValue()).longValue();
	                		    	if (thisVal > curMax) {
	                		        	propsMap.put(propsKey, 
	                		        			new ValueAndAggregationType(
	                		        			new Long(thisVal), aggType));
	                		    	}
	                			} else if (num instanceof Integer) {
	                		    	int curMax = ((Integer)num).intValue();
	                		    	int thisVal = 
	                		    			((Integer)ps.getValue()).intValue();
	                		    	if (thisVal > curMax) {
	                		        	propsMap.put(propsKey, 
	                		        			new ValueAndAggregationType(
	                		        			new Integer(thisVal), aggType));
	                		    	}
	                			}
	                			break;
	                		case PoolStatistic.MOST_RESENT_AGGREGATE_TYPE:
	                			Object when = value;
	                			if (when instanceof Long) {
	                		    	long curTime = ((Long)when).longValue();
	                		    	long thisTime = 
	                		    			((Long)ps.getValue()).longValue();
	                		    	if (thisTime > curTime) {
	                		        	propsMap.put(propsKey, 
	                		        			new ValueAndAggregationType(
	                		        			new Long(thisTime), aggType));
	                		    	}
	                			} else if (when instanceof Date) {
	                		    	Date curTime = (Date)when;
	                		    	Date thisTime = (Date)ps.getValue();
	                		    	if (thisTime.after(curTime)) {
	                		        	propsMap.put(propsKey, 
	                		        			new ValueAndAggregationType(
	                		        			when, aggType));
	                		    	}
	                			}
	                			break;
	                		case PoolStatistic.LEAST_RESENT_AGGREGATE_TYPE:
	                			when = value;
	                			if (when instanceof Long) {
	                		    	long curTime = ((Long)when).longValue();
	                		    	long thisTime = 
	                		    			((Long)ps.getValue()).longValue();
	                		    	if (thisTime > curTime) {
	                		        	propsMap.put(propsKey, 
	                		        			new ValueAndAggregationType(
	                		        			new Long(thisTime), aggType));
	                		    	}
	                			} else if (when instanceof Date) {
	                		    	Date curTime = (Date)when;
	                		    	Date thisTime = (Date)ps.getValue();
	                		    	if (thisTime.before(curTime)) {
	                		        	propsMap.put(propsKey, 
	                		        			new ValueAndAggregationType(
	                		        			when, aggType));
	                		    	}
	                			}
	                			break;
	                		case PoolStatistic.COMMON_AGGREGATE_TYPE:
	                			Object thisVal = ps.getValue();
	     						intermediateKey = name + propsKey;
	     						Map occurrencesMap = (Map)intermediateMap.get(
	     								intermediateKey);
	     						if (!occurrencesMap.containsKey(thisVal)) {
	     							occurrencesMap.put(thisVal, new Integer(0));
	     						}
	     						Integer numOccurrences = 
	     								(Integer)occurrencesMap.get(thisVal);
	     						occurrencesMap.put(thisVal, new Integer(
	     								numOccurrences.intValue() + 1));
	     						int largest = 0;
	     						Object keyForLargest = null;
	     						Iterator itt = 
	     								occurrencesMap.entrySet().iterator();
	     						while (itt.hasNext()) {
	     					    	Map.Entry me3 = (Map.Entry)itt.next();
	     					    	int curVal = 
	     					    			((Integer)me3.getValue()).intValue();
	     					    	if (curVal > largest) {
	     					        	largest = curVal;
	     					        	keyForLargest = me3.getKey();
	     					    	}
	     						}
	     						propsMap.put(propsKey, 
	     								new ValueAndAggregationType(
	     								keyForLargest, aggType));
	     						break;
						}
	            	}
	        	}
	    	}
	    	//Now we will replace the value in each map entry, which currently
	    	//contains an object containing both the value and its aggregation
	    	//type, with just the object itself.
	    	Iterator iter = aggMap.entrySet().iterator();
	    	while (iter.hasNext()) {
	    	    Map.Entry me = (Map.Entry)iter.next();
	    	    Map propsMap = (Map)me.getValue();
	    	    Iterator it2 = propsMap.entrySet().iterator();
	    	    while (it2.hasNext()) {
	    	        Map.Entry me2 = (Map.Entry)it2.next();
	    	        if (me2.getValue() instanceof ValueAndAggregationType) {
	    	            propsMap.put(me2.getKey(), 
	    	            		((ValueAndAggregationType)
	    	            		me2.getValue()).getValue());
	    	        }
	    	    }
	    	}
			//Must cover all columns to be shown in table.  So if do not have
	        //a value for a column, insert a null.
	    	iter = aggMap.entrySet().iterator();
	    	while (iter.hasNext()) {
	        	Map.Entry me = (Map.Entry)iter.next();
	        	Map propsMap = (Map)me.getValue();
	        	for (int j = 0; j < STATISTICS_SHOWN_IN_TABLE.length; j++) {
	            	if (!propsMap.containsKey(STATISTICS_SHOWN_IN_TABLE[j])) {
	                	propsMap.put(STATISTICS_SHOWN_IN_TABLE[j], null);
	            	}
	        	}
	    	}
	    	Map[] propsArray = new Map[aggMap.size()];
	 		iter = aggMap.entrySet().iterator();
			for (int i = 0; iter.hasNext(); i++) {
		    	Map.Entry me = (Map.Entry)iter.next();
		    	propsArray[i] = (Map)me.getValue();
			}	 	
	 		valuesMap.put(key, propsArray);
	    }
	}
	
	private void addTabAndTable(String name) {
	 	tabHeaders.add(name);
	 	MainTable table = new MainTable(this);
        table.getSelectionModel().addListSelectionListener(
	 			new ListSelectionListener() {
	 		public void valueChanged(ListSelectionEvent ev) {
	 		    if ((!programmaticTableSelection) && 
	 		    		(!ev.getValueIsAdjusting())) {
                        poolSelectionChanged();
	 		    }
	 		}
	 	});
	 	JPanel tablePanel = new JPanel();
	 	GridBagLayout tl = new GridBagLayout();
	 	tablePanel.setLayout(tl);
	 	JScrollPane tableSP = new JScrollPane(table);
	 	tablePanel.add(tableSP);
	 	tl.setConstraints(tableSP, new GridBagConstraints(0, 0, 1, 1, 1.0, 1.0,
	 			GridBagConstraints.CENTER, GridBagConstraints.BOTH,
	 			new Insets(2, 2, 2, 2), 0, 0));
	 	tabbedPane.addTab(name, tablePanel);
	 	tables.add(table);
	}	 			

	public void tableSorted(MainTable table) {
	    ColumnSortInfo[] csi = ColumnSortInfo.getTableColumnSortInfo(table);
	    int tabIndex = tabbedPane.getSelectedIndex();
	    Integer key = new Integer(tabIndex);
		aggCSI[currentAggregationType].put(key, csi);
	}
	
	private void poolSelectionChanged() {
	    refresh();
	}
	
	private PoolProcessHost getSelectedPoolProcessHost() {
	    PoolProcessHost poolProcessHost = null;
	    int tabIndex = tabbedPane.getSelectedIndex();
	    if (tabIndex >= 0) {
	    	TableWidget table = (TableWidget)tables.get(tabIndex);
	    	int viewIndex = table.getSelectedRow();
	    	if (viewIndex >= 0) {
	    	    int modelIndex = table.convertRowIndexToModel(viewIndex);
	    	    String pool = null;
	    	    String process = null;
	    	    String host = null;
	    	    switch (currentAggregationType) {
	    	        case NO_AGGREGATING:
        	    	    int viewColumnNum = 
        	    	    		table.getColumnModel().getColumnIndex(POOL_HDR);
        	    	    int modelColumnNum = table.convertColumnIndexToModel(
        	    	    		viewColumnNum);
        	    	    pool = (String)table.getModel().getValueAt(modelIndex, 
        	    	    		modelColumnNum);
        	    	    viewColumnNum = 
        	    	    		table.getColumnModel().getColumnIndex(PROCESS_HDR);
        	    	    modelColumnNum = table.convertColumnIndexToModel(
        	    	    		viewColumnNum);
        	    	    process = (String)table.getModel().getValueAt(
        	    	    		modelIndex, modelColumnNum);
        	    	    viewColumnNum = 
        	    	    		table.getColumnModel().getColumnIndex(HOST_HDR);
        	    	    modelColumnNum = table.convertColumnIndexToModel(
        	    	    		viewColumnNum);
        	    	    host = (String)table.getModel().getValueAt(modelIndex,
        	    	    		modelColumnNum);
	    	        	break;
	    	        case AGGREGATED_BY_PROCESS:
        	    	    viewColumnNum = 
        	    	    		table.getColumnModel().getColumnIndex(PROCESS_HDR);
        	    	    modelColumnNum = table.convertColumnIndexToModel(
        	    	    		viewColumnNum);
        	    	    process = (String)table.getModel().getValueAt(
        	    	    		modelIndex, modelColumnNum);
        	    	    viewColumnNum = 
        	    	    		table.getColumnModel().getColumnIndex(HOST_HDR);
        	    	    modelColumnNum = table.convertColumnIndexToModel(
        	    	    		viewColumnNum);
        	    	    host = (String)table.getModel().getValueAt(modelIndex,
        	    	    		modelColumnNum);
	    	        	break;
	    	        case AGGREGATED_BY_POOL:
        	    	    viewColumnNum = 
        	    	    		table.getColumnModel().getColumnIndex(POOL_HDR);
        	    	    modelColumnNum = table.convertColumnIndexToModel(
        	    	    		viewColumnNum);
        	    	    pool = (String)table.getModel().getValueAt(modelIndex, 
        	    	    		modelColumnNum);
        	    	    break;
	    	    }
	    	    poolProcessHost = new PoolProcessHost(pool, process, host);
	    	}
	    }
	    return poolProcessHost;
	}
	
	private void updateDetailTable(PoolProcessHost poolProcessHost) {
		int tabIndex = tabbedPane.getSelectedIndex();
	    if ((poolProcessHost == null) || (tabIndex < 0)) {
	        detailTableModel.setRowCount(0);
	    } else {
	        String poolName = poolProcessHost.getPool();
	        String processName = poolProcessHost.getProcess();
	        String hostName = poolProcessHost.getHost();
	        String poolType = (String)tabHeaders.get(tabIndex);
	    	Integer aggregationType = new Integer(currentAggregationType);
	    	PoolTypeWithAggregationType mapKey = new PoolTypeWithAggregationType(
	    			poolType, aggregationType);
	    	Map[] rowData = (Map[])valuesMap.get(mapKey);
	    
	    	//Now need to simply iterate through the rows until find the row for
	    	//this pool, process, and host.
	    	Map detailProps = null;
	    	int i = 0;
	    	while ((detailProps == null) && (i < rowData.length)) {
	        	String curPoolName = (String)rowData[i].get(POOL_HDR);
	        	String curProcessName = (String)rowData[i].get(PROCESS_HDR);
	        	String curHostName = (String)rowData[i].get(HOST_HDR);
	        	boolean poolsMatch;
	        	if (poolName == null) {
	        	    poolsMatch = (curPoolName == null);
	        	} else {
	        	    poolsMatch = poolName.equals(curPoolName);
	        	}
	        	boolean processesMatch;
	        	if (processName == null) {
	        	    processesMatch = (curProcessName == null);
	        	} else {
	        	    processesMatch = processName.equals(curProcessName);
	        	}
	        	boolean hostsMatch;
	        	if (hostName == null) {
	        	    hostsMatch = (curHostName == null);
	        	} else {
	        	    hostsMatch = hostName.equals(curHostName);
	        	}
	        	boolean allMatch = (poolsMatch && processesMatch && hostsMatch);
	        	if (allMatch) {
	            	detailProps = rowData[i];
				} else {
	            	i++;
	        	}
	    	}
	    	if (detailProps != null) {
//	    	    TableWidget table = (TableWidget)tables.get(tabIndex);
	        	ArrayList /*<NameValuePair>*/ displayPropsList = 
	        			new ArrayList(10);
	        	Iterator it = detailProps.entrySet().iterator();
	        	while (it.hasNext()) {
					Map.Entry me = (Map.Entry)it.next();
					String propName = (String)me.getKey();
					if (me.getValue() instanceof PoolStatistic) {
					    PoolStatistic ps = (PoolStatistic)me.getValue();
					    propName = ps.getDisplayName();					    
						NameValuePair entry = new NameValuePair(propName,
								ps.getValue());
						displayPropsList.add(entry);
					} else {
					    NameValuePair entry = new NameValuePair(propName, 
					    		me.getValue());
					    displayPropsList.add(entry);
					}
				}
				detailTableModel.setRowCount(0);
				it = displayPropsList.iterator();
				for (int j = 0; it.hasNext(); j++) {
				    NameValuePair nvp = (NameValuePair)it.next();
				    //We will only add rows for properties that were not already
				    //displayed as a column in the main table.
				    String propName = nvp.getName();
				    if (!(propName.equals(POOL_HDR) || propName.equals(
				    		PROCESS_HDR) || propName.equals(HOST_HDR))) {
						int index = StaticQuickSorter.unsortedStringArrayIndex(
    				    		STATISTICS_SHOWN_IN_TABLE, propName);
    				    if (index < 0) {
    				    	Object[] rowValues = new Object[NUM_DETAIL_COLS];
    				    	rowValues[PROPERTY_COL_NUM] = propName;
    				    	rowValues[VALUE_COL_NUM] = nvp.getValue();
							detailTableModel.addRow(rowValues);
    				    }
				    }
				}
			}
	    }
	}
	
	private void updateResourcePanel(PoolProcessHost poolProcessHost) {
		resourceTableModel.setRowCount(0);
	    if (poolProcessHost != null) {
	        String hostName = poolProcessHost.getHost();
	    	String poolName = poolProcessHost.getPool();
	    	String processName = poolProcessHost.getProcess();
	    	//Iterate through currentStats and find all matches on pool, process,
	    	//and host.  Where they are null, anything matches.
	    	for (int i = 0; i < currentStats.length; i++) {
	    	    boolean allMatch = false;
	    	    boolean hostsMatch = true;
	    	    if (hostName != null) {
	    	        hostsMatch = hostName.equals(currentStats[i].getHostName());
	    	    }
	    	    if (hostsMatch) {
	    	        boolean processesMatch = true;
	    	        if (processName != null) {
	    	            processesMatch = processName.equals(
	    	            		currentStats[i].getProcessName());
	    	        }
	    	        if (processesMatch) {
	    	            boolean poolsMatch = true;
	    	            if (poolName != null) {
	    	                poolsMatch = poolName.equals(
	    	                		currentStats[i].getPoolName());
	    	            }
	    	            allMatch = poolsMatch;
	    	        }
	    	    }
	    	    if (allMatch) {
	    	        Collection /*<ResourceStatistics>*/ rsColl = 
	    	        		currentStats[i].getResourcesStatistics();
	    	        Iterator it = rsColl.iterator();
	    	        while (it.hasNext()) {
	    	            ResourceStatistics rs = (ResourceStatistics)it.next();
	    	            Object[] rowVals = new Object[NUM_RESOURCE_COLS];
    	    	        rowVals[RESOURCE_TBL_HOST_COL_NUM] = 
    	    	        		currentStats[i].getHostName();
    	    	        rowVals[RESOURCE_TBL_PROCESS_COL_NUM] =
    	    	        		currentStats[i].getProcessName();
    	    	        rowVals[RESOURCE_TBL_POOL_COL_NUM] =
    	    	        		currentStats[i].getPoolName();
    	    	        rowVals[CREATION_TIME_COL_NUM] = new Date(
    	    	        		rs.getCreationTime());
    	    	        rowVals[IN_USE_BY_COL_NUM] = rs.getUserName();
    	    	        rowVals[LAST_ACCESS_COL_NUM] = new Date(
    	    	        		rs.getLastUsed());
    	    	        resourceTableModel.addRow(rowVals);
	    	        }
	    	    }
	    	}
	   	}
	}
		 			   	
	public String getTitle() {
	    return "Connection Pools";
	}

	public ConnectionInfo getConnection() {
		return connection;
	}
		
	public java.util.List /*<Action>*/ resume() {
	    return actions;
	}
	
	public boolean havePendingChanges() {
	    return false;
	}
	
	public boolean finishUp() {
	    return true;
	}
    
    public void receiveUpdateNotification(RuntimeUpdateNotification notification) {
    	//TODO
    }
	
	public void refresh() {
        if(!refreshingTable) {
            refreshingTable = true;
    		PoolProcessHost selectedPoolProcessHost = getSelectedPoolProcessHost();
    		if (!initializing) {
    			saveAllColumnWidths(currentAggregationType);
    		}
    	    boolean continuing = true;
    	    if (refreshWithRawData) {
    		    try {
    		        getRawData();
    		    } catch (Exception ex) {
    		        String msg = "Error retrieving resource pool information";
    		        LogManager.logError(LogContexts.RESOURCE_POOLS, ex, msg);
    		        ExceptionUtility.showMessage(msg, ex);
    		        continuing = false;
    		    }
    	    }
    	    if (continuing) {
    	        updateTables(selectedPoolProcessHost, false);
    	    }
            refreshingTable = false;
        }
	}
	
	private void saveAllColumnWidths(int aggType) {
		int numTables = tables.size();
		for (int i = 0; i < numTables; i++) {
			ColumnWidthInfo[] info = getColumnWidthInfo(i);
			colWidthsMap[aggType].put(new Integer(i), info);
		}
	}
	
	private ColumnWidthInfo[] getColumnWidthInfo(int tabIndex) {
		ColumnWidthInfo[] info = null;
	    if (tabIndex >= 0) {
	        TableWidget table = (TableWidget)tables.get(tabIndex);
	        int numColumns = table.getColumnCount();
	        info = new ColumnWidthInfo[numColumns];
	        for (int i = 0; i < numColumns; i++) {
	            String colName = table.getColumnName(i);
	            TableColumn tc = table.getColumn(colName);
	            int width = tc.getWidth();
	            info[i] = new ColumnWidthInfo(colName, width);
	        }
	    }
	    return info;
	}
	
	private void sizeColumnsToWidthInfo(TableWidget table, 
			ColumnWidthInfo[] info) {
	    for (int i = 0; i < info.length; i++) {
    		TableColumn tc = table.getColumn(info[i].getName());
    	    if (tc != null) {
    	    	tc.setWidth(info[i].getWidth());
	        }
	    }
	}
	
	private ColumnWidthInfo[] retrieveColumnWidthInfo(int tabIndex, 
			int aggType) {
		Object key = new Integer(tabIndex);
		return (ColumnWidthInfo[])colWidthsMap[aggType].get(key);
	}
			
	public AutoRefresher getAutoRefresher() {
	    return autoRefresher;
	}
	
	public void setAutoRefresher(AutoRefresher refresher) {
	    autoRefresher = refresher;
	}
	
	public void setAutoRefreshEnabled(boolean flag) {
	    autoRefresher.setAutoRefreshEnabled(flag);
	}
	
    public void setRefreshRate(int iRate) {
        autoRefresher.setRefreshRate(iRate);
    }
    
//    private String valuesMapToString() {
//        String str = "valuesMap contents:";
//        Iterator it = valuesMap.entrySet().iterator();
//        while (it.hasNext()) {
//            Map.Entry me = (Map.Entry)it.next();
//            PoolTypeWithAggregationType pt = (PoolTypeWithAggregationType)me.getKey();
//            Map[] rows = (Map[])me.getValue();
//            str += '\n' + "  " + pt.toString();
//           	if (rows.length == 0) {
//           	    str += '\n' + "    " + "(no rows)";
//           	} else {
//           	    for (int i = 0; i < rows.length; i++) {
//           	        str += '\n' + "    " + "row " + i;
//           	        Iterator rowIter = rows[i].entrySet().iterator();
//           	        while (rowIter.hasNext()) {
//           	            Map.Entry rowME = (Map.Entry)rowIter.next();
//           	            Object rowKey = rowME.getKey();
//           	            Object rowValue = rowME.getValue();
//           	        	String keyStr;
//           	        	String valueStr;
//           	        	if (rowKey instanceof PoolStatistic) {
//           	            	keyStr = PoolManager.poolStatisticToString(
//           	            			(PoolStatistic)rowKey);
//           	        	} else {
//           	            	keyStr = rowKey.toString();
//           	        	}
//           	        	if (rowValue instanceof PoolStatistic) {
//           	            	valueStr = PoolManager.poolStatisticToString(
//           	            			(PoolStatistic)rowValue);
//           	        	} else {
//           	            	valueStr = rowValue.toString();
//           	        	}
//           	        	str += '\n' + "      " + "key=" + keyStr + ",  value=" + 
//           	        			valueStr;
//           	        }
//           	    }
//           	}
//        }
//        return str;
//    }
}//end PoolsPanel




class MainTable extends TableWidget {
    private PoolsPanel caller;
    
    public MainTable(PoolsPanel caller) {
        super(new com.metamatrix.toolbox.ui.widget.table.DefaultTableModel(),
	 			true);
	 	this.caller = caller;
        this.getSelectionModel().setSelectionMode(
        		ListSelectionModel.SINGLE_SELECTION);
        this.setEditable(false);
        this.setComparator(DefaultConsoleTableComparator.getInstance());
    }
    
    public void sort() {
        super.sort();
        caller.tableSorted(this);
    }
}//end MainTable




class PoolTypeWithAggregationType {
    private String poolType;
    private Integer aggregationType;
    
    public PoolTypeWithAggregationType(String poolType, Integer aggregationType) {
        super();
        this.poolType = poolType;
        this.aggregationType = aggregationType;
    }
    
    public String getPoolType() {
        return poolType;
    }
    
    public Integer getAggregationType() {
        return aggregationType;
    }
    
    public String toString() {
        String aggStr;
        switch (aggregationType.intValue()) {
            case PoolsPanel.NO_AGGREGATING:
            	aggStr = "NO_AGGREGATING";
            	break;
            case PoolsPanel.AGGREGATED_BY_PROCESS:
            	aggStr = "AGGREGATED_BY_PROCESS";
            	break;
            case PoolsPanel.AGGREGATED_BY_POOL:
            	aggStr = "AGGREGATED_BY_POOL";
            	break;
            default:
            	aggStr = "UNKNOWN";
            	break;
        }
        String str = "PoolTypeWithAggregationType: poolType=" + poolType +
        		",aggregationType=" + aggStr;
        return str;
    }
    
    public boolean equals(Object obj) {
        boolean same = false;
        if (obj == this) {
            same = true;
        } else if (obj instanceof PoolTypeWithAggregationType) {
            PoolTypeWithAggregationType pt = (PoolTypeWithAggregationType)obj;
            same = (poolType.equals(pt.getPoolType()) && 
            		aggregationType.equals(pt.getAggregationType()));
        }
        return same;
    }
    
    public int hashCode() {
        String str = poolType + aggregationType.toString();
        int code = str.hashCode();
        return code;
    }
}//end PoolTypeWithAggregationType




class NameValuePair {
    private String name;
    private Object value;
    
    public NameValuePair(String name, Object value) {
        super();
        this.name = name;
        this.value = value;
    }
    
    public String getName() {
        return name;
    }
    
    public Object getValue() {
        return value;
    }
}//end NameValuePair




class CounterAndSum {
    private int counter;
    private Number sum;
    
    public CounterAndSum(int counter, Number sum) {
        super();
        this.counter = counter;
        this.sum = sum;
    }
    
    public int getCounter() {
        return counter;
    }
    
    public void incrementCounter() {
        this.counter += 1;
    }
    
    public Number getSum() {
        return sum;
    }
    
    public void setSum(Number sum) {
        this.sum = sum;
    }
}//end CounterAndSum




class ValueAndAggregationType {
    private Object value;
    private int aggregationType;
    
    public ValueAndAggregationType(Object value, int aggType) {
        super();
        this.value = value;
        this.aggregationType = aggType;
    }
    
    public Object getValue() {
        return value;
    }
    
    public int getAggregationType() {
        return aggregationType;
    }
    
    public String toString() {
        String str = "ValueAndAggregationType: value=" + value.toString() +
        		",aggregationType=" + aggregationType;
       	return str;
    }
}//end ValueAndAggregationType




class DetailTable extends TableWidget {
    private DetailTableCellRenderer renderer;
    
    public DetailTable(com.metamatrix.toolbox.ui.widget.table.DefaultTableModel
    		tableModel) {
 		super(tableModel, true);
 		renderer = new DetailTableCellRenderer();
    }
    
    public TableCellRenderer getCellRenderer(int row, int col) {
        return renderer;
    }
}//end DetailTable




class DetailTableCellRenderer extends DefaultTableCellRenderer {
    public DetailTableCellRenderer() {
        super();
    }
    
    public Component getTableCellRendererComponent(JTable table, 
    		Object value, boolean isSelected, boolean hasFocus, int row,
    		int column) {
    	JLabel label = (JLabel)super.getTableCellRendererComponent(table, value,
    			isSelected, hasFocus, row, column);
    	if (column == PoolsPanel.VALUE_COL_NUM)  {
    	    label.setHorizontalAlignment(SwingConstants.RIGHT);
    	}
    	return label;
    }
}//end DetailTableCellRenderer




class PoolsPanelSplitPane extends JSplitPane {
	private double initialDividerProportion;
	private int paintCount = 0;
    
    public PoolsPanelSplitPane(Component top, Component bottom,
    		double initialDividerProportion) {
    	super(JSplitPane.VERTICAL_SPLIT, true, top, bottom);
    	this.initialDividerProportion = initialDividerProportion;
    	this.setOneTouchExpandable(true);
	}
    
	public void paint(Graphics g) {
 		super.paint(g);
 		if (paintCount < 2) {
  			paintCount++;
			this.setDividerLocation(initialDividerProportion);
 		}
 	}
}//end PoolsPanelSplitPane




class PoolsPanelRadioButton extends JRadioButton {
    public PoolsPanelRadioButton(String label) {
        super(label);
    }
    
    public Dimension getPreferredSize() {
        Dimension size = super.getPreferredSize();
        Dimension newSize = new Dimension(size.width, size.height - 8);
        return newSize;
    }
}//end PoolsPanelRadioButton




class PoolProcessHost {
    private String pool;
    private String process;
    private String host;
    
    public PoolProcessHost(String pool, String process, String host) {
        super();
        this.pool = pool;
        this.process = process;
        this.host = host;
    }
    
    public String getPool() {
        return pool;
    }
    
    public String getProcess() {
        return process;
    }
    
    public String getHost() {
        return host;
    }
    
    public String toString() {
        String str = "PoolProcessHost: pool=" + pool + ",process=" + process +
        		",host=" + host;
        return str;
    }
}//end PoolProcessHost




class ColumnWidthInfo {
    private String name;
    private int width;
    
    public ColumnWidthInfo(String name, int width) {
        super();
        this.name = name;
        this.width = width;
    }
    
    public String getName() {
        return name;
    }
    
    public int getWidth() {
        return width;
    }
}//end ColumnWidthInfo
