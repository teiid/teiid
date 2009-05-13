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

package com.metamatrix.console.ui.views.deploy;

import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.GridLayout;
import java.awt.Insets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Vector;

import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JSplitPane;
import javax.swing.JTabbedPane;
import javax.swing.border.Border;
import javax.swing.event.ChangeListener;
import javax.swing.event.ListSelectionEvent;
import javax.swing.event.ListSelectionListener;
import javax.swing.table.TableColumn;

import com.metamatrix.common.config.api.Configuration;
import com.metamatrix.common.config.api.ConnectorBinding;
import com.metamatrix.common.config.api.ServiceComponentDefn;
import com.metamatrix.common.log.LogManager;
import com.metamatrix.console.connections.ConnectionInfo;
import com.metamatrix.console.models.ConfigurationManager;
import com.metamatrix.console.models.ModelManager;
import com.metamatrix.console.notification.RuntimeUpdateNotification;
import com.metamatrix.console.security.UserCapabilities;
import com.metamatrix.console.ui.ViewManager;
import com.metamatrix.console.ui.layout.BasePanel;
import com.metamatrix.console.ui.layout.PanelsTreeModel;
import com.metamatrix.console.ui.layout.WorkspacePanel;
import com.metamatrix.console.ui.views.deploy.event.ConfigurationChangeEvent;
import com.metamatrix.console.ui.views.deploy.event.ConfigurationChangeListener;
import com.metamatrix.console.util.DialogUtility;
import com.metamatrix.console.util.ExceptionUtility;
import com.metamatrix.console.util.LogContexts;
import com.metamatrix.console.util.Refreshable;
import com.metamatrix.console.util.StaticUtilities;
import com.metamatrix.toolbox.ui.widget.Splitter;
import com.metamatrix.toolbox.ui.widget.TableWidget;
import com.metamatrix.toolbox.ui.widget.TitledBorder;
import com.metamatrix.toolbox.ui.widget.table.DefaultTableComparator;
import com.metamatrix.toolbox.ui.widget.table.EnhancedTableColumn;
import com.metamatrix.toolbox.ui.widget.table.EnhancedTableColumnModel;

public class ServiceDefinitionPanel extends BasePanel implements
                                                    ConfigurationChangeListener,
                                                    WorkspacePanel,
                                                    Refreshable {

    public final static int SVC_DEFN_COL_NUM = 0;
    public final static int ESSENTIAL_COL_NUM = 1;
    public final static int DESCRIPTION_COL_NUM = 2;

    public final static int PROPERTIES_TAB_NUM = 1;


    private ConnectionInfo connection;

    private TableWidget table;
    private com.metamatrix.toolbox.ui.widget.table.DefaultTableModel tableModel;
    private ServiceDefinitionPropertiesPanel propertiesPanel;
 
    /** List <ServiceComponentDefn> */
    private ServiceComponentDefn selectedSvc = null;

    private ListSelectionListener listSelectionListener;
    
    private JTabbedPane tabbedPane;
    private JSplitPane splitPane;
    private java.util.List /* <Action> */currentActions = new ArrayList();
    private HashMap nameServiceDefnMap = new HashMap();
    private boolean canModify;

    public ServiceDefinitionPanel(ConnectionInfo conn) {
        super();
        this.connection = conn;
        init();
    }


    @Override
	public void receiveUpdateNotification(RuntimeUpdateNotification notification) {
		// TODO Auto-generated method stub
		
	}

	private ConfigurationManager getConfigurationManager() {
        return ModelManager.getConfigurationManager(connection);
    }


    private void init() {

        UserCapabilities cap = null;
        try {
            cap = UserCapabilities.getInstance();
            canModify = cap.canUpdateConfiguration(connection);

            if (canModify) {
                getConfigurationManager().addConfigurationChangeListener(this);
            }
        } catch (Exception ex) {
            // Cannot occur
        }

 
        // NOTE-- columns must be in order by indices above
        tableModel = new com.metamatrix.toolbox.ui.widget.table.DefaultTableModel(new Vector(Arrays.asList(new String[] {
            "Service", "Is Essential", "Description"})), 0); //$NON-NLS-1$ //$NON-NLS-2$
        table = new TableWidget(tableModel, true);
        table.setEditable(false);
        table.sizeColumnsToFitContainer(SVC_DEFN_COL_NUM);
        table.sizeColumnsToFitContainer(ESSENTIAL_COL_NUM);
        table.sizeColumnsToFitContainer(DESCRIPTION_COL_NUM);
//        table.setSelectionMode(ListSelectionModel.MULTIPLE_INTERVAL_SELECTION);
        DefaultTableComparator dtcComparator = (DefaultTableComparator)table.getComparator();
        dtcComparator.setIgnoresCase(true);

        // set the binding name column to be sorted by default
        EnhancedTableColumnModel columnModel = (EnhancedTableColumnModel)table.getColumnModel();
        TableColumn firstColumn = columnModel.getColumn(SVC_DEFN_COL_NUM);
        columnModel.setColumnSortedAscending((EnhancedTableColumn)firstColumn, true);

        JScrollPane tableSP = new JScrollPane(table);

        GridBagLayout layout = new GridBagLayout();
        setLayout(layout);

        JPanel buttonsPanel = new JPanel();
        buttonsPanel.setLayout(new GridLayout(1, 4, 5, 5));
 
        tabbedPane = new JTabbedPane();
         propertiesPanel = new ServiceDefinitionPropertiesPanel(canModify, connection);
 
        // NOTE-- tabs MUST be inserted in order by indices above
        tabbedPane.addTab("Properties", propertiesPanel); //$NON-NLS-1$
//         tabbedPane.addChangeListener(new ChangeListener() {
//
//            public void stateChanged(ChangeEvent ev) {
//                selectedTabChanged();
//            }
//        });
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



//    public void addActionToList(String sID,
//                                Action act) {
//        currentActions.add(new MenuEntry(sID, act));
//    }

//    private void setup(AbstractButton theButton,
//                       AbstractPanelAction theAction) {
//        theAction.addComponent(theButton);
//    }

    private void populateTable() {
        tableModel.setNumRows(0);
        try {
        	nameServiceDefnMap.clear();
        	
        	Configuration config = this.getConfigurationManager().getConfig(Configuration.NEXT_STARTUP_ID);
            Collection<ServiceComponentDefn> svcs = config.getServiceComponentDefns();
            for (Iterator<ServiceComponentDefn> it = svcs.iterator(); it.hasNext();) {
                ServiceComponentDefn svc = it.next();
 
                Vector vec = new Vector(Arrays.asList(new Object[] {
                    null, null, null
                }));
                vec.setElementAt(svc.getName(), SVC_DEFN_COL_NUM);
                vec.setElementAt(svc.isEssential(), ESSENTIAL_COL_NUM);
                vec.setElementAt(svc.getDescription(), DESCRIPTION_COL_NUM);
                tableModel.addRow(vec);
                 
                nameServiceDefnMap.put(svc.getName(), svc);


            }
            
 
        } catch (Exception ex) {
            ExceptionUtility.showMessage("Retrieve service definition info", ex); //$NON-NLS-1$
            LogManager.logError(LogContexts.CONFIG, ex, "Error retrieving service definition info."); //$NON-NLS-1$
        }

       
    }

    /**
     * Set the widget to display as selected the specified list of bindings.
     * 
     * @param binding
     *            List<ConnectorBinding>
     * @since 4.2
     */
    private void forceTableSelection(ServiceComponentDefn svc) {
        table.getSelectionModel().clearSelection();

        int rowForSelected = rowForService(svc.getName());
		if (rowForSelected >= 0) {
			table.getSelectionModel().addSelectionInterval(rowForSelected,
					rowForSelected);
			table.scrollRectToVisible(table
					.getCellRect(rowForSelected, 0, true));
		}

    }

    public void postRealize() {
        splitPane.setDividerLocation(0.4);
        if (table.getRowCount() > 0) {
            table.setRowSelectionInterval(0, 0);
        }
    }




 
  
     private int rowForService(String svc) {
        int matchRow = -1;
        int numRows = table.getRowCount();
        int row = 0;
        while ((row < numRows) && (matchRow < 0)) {
            String curRowSvc = table.getValueAt(row, SVC_DEFN_COL_NUM).toString();
            if (curRowSvc.equals(svc)) {
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
    private void setSelectedService(ServiceComponentDefn svc) {
        selectedSvc = svc;
    }

    /**
     * Set the list of selected connector bindings to contain the specified bindings.
     * 
     * @param bindings
     *            List<ConnectorBinding>
     * @since 4.2
     */
//    private void setSelectedServiceDefn(List svcs) {
//        selectedSvc.clear();
//        selectedSvc.addAll(svcs);
//    }

    /**
     * Clear the list of selected connector bindings.
     * 
     * @param bindings
     *            List<ConnectorBinding>
     * @since 4.2
     */
    private void clearSelectedService() {
        selectedSvc=null;
    }

    /**
     * Get list of selected bindings.
     * 
     * @return List<ConnectorBinding>
     * @since 4.2
     */
    private ServiceComponentDefn getSelectedService() {
        int[] selectedRows = table.getSelectedRows();
        int numSelected = selectedRows.length;

        if (numSelected == 0) {
        	return null;
  //          tableSelectionChangedNoneSelected();
        } else if (numSelected == 1) {
            int convertedRow = table.convertRowIndexToModel(selectedRows[0]);
            String selectedName = (String)table.getModel().getValueAt(convertedRow, SVC_DEFN_COL_NUM);
            ServiceComponentDefn svc = (ServiceComponentDefn)getServiceDefnXref().get(selectedName);
            return svc;
           
        } 
        return null;
    }

    public void refresh() {
        // save the currently selected bindings
        ServiceComponentDefn olddefn = getSelectedService();
 
        try {
            populateTable();
            
            // get first row
            if (olddefn == null) {
	            String curRowSvc = table.getValueAt(1, SVC_DEFN_COL_NUM).toString();
	            olddefn = (ServiceComponentDefn) nameServiceDefnMap.get(curRowSvc);	           
            }
            
         } catch (Exception ex) {
            ExceptionUtility.showMessage("Failed while refreshing the Services list", ex); //$NON-NLS-1$
            LogManager.logError(LogContexts.CONFIG, ex, "Failed while refreshing the Services list"); //$NON-NLS-1$
        }

        // restore the binding and select it in the table:
        setSelectedService(olddefn);
        forceTableSelection(olddefn);
    }

    private void tableSelectionChanged() {
        boolean cancelled = false;        

        ServiceComponentDefn svc = getSelectedService();
        if (anyValueChanged() && svc != null) {
             
            String message = "Save changes to Service Definition " + svc + " ?"; //$NON-NLS-1$//$NON-NLS-2$
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
                     forceTableSelection(svc);
                    
                    //restore the listener
                    table.getSelectionModel().addListSelectionListener(listSelectionListener);

                    break;
            }
        }        
        
        
        if (! cancelled) {            
            StaticUtilities.startWait(ViewManager.getMainFrame());
    
 //           int[] selectedRows = table.getSelectedRows();
 //           int numSelected = selectedRows.length;
    
            if (svc == null) {
            	clearSelectedService();
            } else  {
            	setSelectedService(svc);
               tableSelectionChangedOneSelected(svc);
            } 
    
            forceRepaint();
            StaticUtilities.endWait(ViewManager.getMainFrame());
        }        
    }

//    private void tableSelectionChangedNoneSelected() {
//        // clear list of selected bindings
//        clearSelectedService();
//
//    }

    private void tableSelectionChangedOneSelected(ServiceComponentDefn defn) {
        Border border = createTabBorder(defn);
        int curTab = tabbedPane.getSelectedIndex();
   	
        propertiesPanel.setServiceComponentDefn(defn);
        propertiesPanel.setBorder(border);

        // set list of selected bindings
//        int convertedRow = table.convertRowIndexToModel(selectedRow);
//        String selectedName = (String)table.getModel().getValueAt(convertedRow, SVC_DEFN_COL_NUM);
//        ServiceComponentDefn svc = (ServiceComponentDefn)getServiceDefnXref().get(selectedName);
        setSelectedService(defn);


    }

    private Border createTabBorder(ServiceComponentDefn defn) {
        TitledBorder border;
        String title;

        if (defn == null) {
            title = ""; //$NON-NLS-1$
        } else {
            title = "Service:  " + defn.toString(); //$NON-NLS-1$
        }
        border = new TitledBorder(title);
        return border;
    }

 
    public java.util.List /* <Action> */resume() {
        return currentActions;
    }

    public String getTitle() {
        return PanelsTreeModel.SERVICE_DEFINTIONS; 
    }

    public ConnectionInfo getConnection() {
        return connection;
    }

    private HashMap getServiceDefnXref() {
//        if (nameServiceDefnMap == null) {
//            constructServiceDefnXref();
//        }
        return nameServiceDefnMap;
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
    
    
}// end Panel

