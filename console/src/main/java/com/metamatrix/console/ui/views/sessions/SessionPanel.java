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

package com.metamatrix.console.ui.views.sessions;


import java.awt.Component;
import java.awt.Dimension;
import java.awt.Graphics;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;
import java.awt.event.ActionEvent;
import java.awt.event.MouseAdapter;
import java.awt.event.MouseEvent;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import javax.swing.AbstractAction;
import javax.swing.Action;
import javax.swing.JOptionPane;
import javax.swing.JPopupMenu;
import javax.swing.JScrollPane;
import javax.swing.JTable;
import javax.swing.ListSelectionModel;
import javax.swing.SwingUtilities;
import javax.swing.border.EmptyBorder;
import javax.swing.event.ListSelectionEvent;
import javax.swing.event.ListSelectionListener;
import javax.swing.table.TableColumn;

import com.metamatrix.admin.api.objects.Session;
import com.metamatrix.common.log.LogManager;
import com.metamatrix.console.ConsolePlugin;
import com.metamatrix.console.connections.ConnectionInfo;
import com.metamatrix.console.models.ModelManager;
import com.metamatrix.console.models.SessionManager;
import com.metamatrix.console.notification.RuntimeUpdateNotification;
import com.metamatrix.console.security.UserCapabilities;
import com.metamatrix.console.ui.layout.BasePanel;
import com.metamatrix.console.ui.layout.ConsoleMainFrame;
import com.metamatrix.console.ui.layout.MenuEntry;
import com.metamatrix.console.ui.layout.WorkspacePanel;
import com.metamatrix.console.ui.util.AbstractPanelAction;
import com.metamatrix.console.ui.views.DefaultConsoleTableComparator;
import com.metamatrix.console.ui.views.DefaultConsoleTableSorter;
import com.metamatrix.console.ui.views.deploy.util.DeployPkgUtils;
import com.metamatrix.console.util.AutoRefreshable;
import com.metamatrix.console.util.AutoRefresher;
import com.metamatrix.console.util.DialogUtility;
import com.metamatrix.console.util.ExceptionUtility;
import com.metamatrix.console.util.ExternalException;
import com.metamatrix.console.util.LogContexts;
import com.metamatrix.console.util.StaticProperties;
import com.metamatrix.console.util.StaticUtilities;
import com.metamatrix.platform.security.api.MetaMatrixSessionID;
import com.metamatrix.toolbox.ui.widget.TableWidget;
import com.metamatrix.toolbox.ui.widget.table.EnhancedTableColumn;

/**
 *
 */
public class SessionPanel
     extends BasePanel
  implements WorkspacePanel,
             ListSelectionListener,
             AutoRefreshable {
    public static final String SESSION_PANEL_NAME = "Session Panel Name."; //$NON-NLS-1$
    public static final String SESSION_TABLE_NAME = ConsolePlugin.Util.getString("SessionPanel.SessionTable_2"); //$NON-NLS-1$
    public static final String SESSION_TABLE_HDR_NAME = "SessionTable.header"; //$NON-NLS-1$

    public static final int TERMINATE = 0;
    public static final int REFRESH = 1;
    
    private static final String TITLE = ConsolePlugin.Util.getString("SessionPanel.Sessions_30");//$NON-NLS-1$

   /**
     * Thickness of empty border around this panel
     */
    private static final Insets BORDER_THICKNESS = new Insets(5,5,5,5);

    private static final String EXPIRED_TEXT = ConsolePlugin.Util.getString("SessionPanel.Expired_9"); //$NON-NLS-1$
    private static final String ACTIVE_TEXT = ConsolePlugin.Util.getString("SessionPanel.Active_10"); //$NON-NLS-1$
    private static final String CLOSED_TEXT = ConsolePlugin.Util.getString("SessionPanel.Closed_11"); //$NON-NLS-1$
    private static final String TERMINATED_TEXT = ConsolePlugin.Util.getString("SessionPanel.Terminated_12"); //$NON-NLS-1$
    private static final String DEFAULT_TEXT = ConsolePlugin.Util.getString("SessionPanel.Unknown_14"); //$NON-NLS-1$
    
    private static final String NO_PRODUCT = ConsolePlugin.Util.getString("SessionPanel.none_18"); //$NON-NLS-1$
    
    
    private static final String CANNOTTERMHDR = ConsolePlugin.Util.getString("SessionPanel.cannotTerminateMsgHdr");  //$NON-NLS-1$
    private static final String CANNOTTERMHDR1 = ConsolePlugin.Util.getString("SessionPanel.cannotTerminate_1") ;  //$NON-NLS-1$
    private static final String CANNOTTERMHDR2 = ConsolePlugin.Util.getString("SessionPanel.cannotTerminate_2");  //$NON-NLS-1$
    

	private ConnectionInfo connection;
    private SessionTableWidget sessionTable;
    private SessionTableModel tableModel;
    //private DefaultTableModel tableModel;

    private ArrayList actions = new ArrayList();
    private AbstractAction refreshAction;
    private AbstractAction terminateAction;
    
    private MenuEntry terminateMenuAction = null;
    private MenuEntry refreshMenuAction =null;
    

    private AutoRefresher arRefresher = null;

	//the original universe of current sessions
    private List<Session> allSessions;

    private boolean programaticSelectionChange = false;

    private UserCapabilities cap;
    
	public SessionPanel(ConnectionInfo conn) {
		super();
		this.connection = conn;
	}
    
    private MenuEntry createMenuAction(int action) {
        if (action == TERMINATE && terminateAction != null ) {
            return terminateMenuAction;
        }
       
        if (action == REFRESH && refreshAction != null ) {
            return refreshMenuAction;
        }

        
        class PanelAction extends AbstractPanelAction {
            public PanelAction(int theType) {
                super(theType);
                if (theType == TERMINATE) {
                    putValue(NAME, ConsolePlugin.Util.getString("SessionPanel.Terminate_31")); //$NON-NLS-1$
                    putValue(SHORT_DESCRIPTION, ConsolePlugin.Util.getString("SessionPanel.Terminates_all_selected_sessions_32")); //$NON-NLS-1$
                }
                else if (theType == REFRESH) {
                    putValue(NAME, ConsolePlugin.Util.getString("SessionPanel.Refresh_33")); //$NON-NLS-1$
                    putValue(SHORT_DESCRIPTION, ConsolePlugin.Util.getString("SessionPanel.Query_for_current_list_of_sessions_34")); //$NON-NLS-1$
                }
            }
            protected void actionImpl(ActionEvent theEvent)
                throws ExternalException {
    
                if (type == TERMINATE) {
                    terminateSelectedSessions();
                }
                else if (type == REFRESH) {
                    refresh();
                }
            }
     
        }
        MenuEntry me = null;
        if (action == TERMINATE  ) {
            terminateAction =  new PanelAction(action);
            terminateAction.setEnabled(false);
            me = new MenuEntry(MenuEntry.ACTION_MENUITEM, terminateAction);

            
        } else if (action == REFRESH ) {
            refreshAction =  new PanelAction(action);
            refreshAction.putValue(Action.SMALL_ICON, 
                                   DeployPkgUtils.getIcon("icon.refresh")); //$NON-NLS-1$
           
            me = new MenuEntry(MenuEntry.VIEW_REFRESH_MENUITEM, refreshAction);
        }
        
        return me;

    }
    
	
    public void createComponent() throws Exception {
        initializeTable();
		JScrollPane scrollPane = new JScrollPane(sessionTable);

        // Set this JPanel's content
		GridBagLayout layout = new GridBagLayout();
		this.setLayout(layout);

        this.setBorder(new EmptyBorder(SessionPanel.BORDER_THICKNESS));

		this.add(scrollPane);
		layout.setConstraints(scrollPane, new GridBagConstraints(0, 0, 1, 1, 
				1.0, 1.0, GridBagConstraints.CENTER, GridBagConstraints.BOTH, 
				new Insets(0, 0, 0, 0), 0, 0));

//        refreshAction = new PanelAction(PanelAction.REFRESH);
//        refreshAction.putValue(Action.SMALL_ICON, 
//        		DeployPkgUtils.getIcon("icon.refresh")); //$NON-NLS-1$
//
//        actions.add(new MenuEntry(MenuEntry.VIEW_REFRESH_MENUITEM,
//                                  refreshAction));
        
        actions.add(createMenuAction(REFRESH));        

        cap = null;
        try {
            cap = UserCapabilities.getInstance();
        } catch (Exception ex) {
            //Cannot occur
        }
        if (cap.canModifySessions(connection)) {
//            terminateAction = new PanelAction(PanelAction.TERMINATE);
//            terminateAction.setEnabled(false);
//            actions.add(new MenuEntry(MenuEntry.ACTION_MENUITEM,
//                                      terminateAction));
            
            actions.add(createMenuAction(TERMINATE));
            
            sessionTable.addMouseListener(new MouseAdapter() {
                public void mouseClicked(MouseEvent ev) {
                    if (ev.isPopupTrigger() || SwingUtilities.isRightMouseButton(ev)) {
                        int row = sessionTable.rowAtPoint(ev.getPoint());
                        if (sessionTable.getSelectionModel().isSelectedIndex(row)) {
                            handlePopupTrigger(ev, sessionTable);
                        }
                    }
                }
            });
        }
        // Establish AutoRefresher
        arRefresher = new AutoRefresher(this, 15, false, connection);
        arRefresher.init();
        arRefresher.startTimer();

    }

    private void initializeTable(){
        tableModel = new SessionTableModel();
		this.sessionTable = new SessionTableWidget(tableModel);
		this.sessionTable.setName( SESSION_TABLE_NAME );
		sessionTable.getTableHeader().setName( SESSION_TABLE_HDR_NAME );
        sessionTable.setAutoResizeMode(JTable.AUTO_RESIZE_ALL_COLUMNS);
        sessionTable.getSelectionModel().addListSelectionListener(this);
        sessionTable.setEditable(false);
        sessionTable.setSortable(true);
        sessionTable.setComparator(DefaultConsoleTableComparator.getInstance());
        sessionTable.setSorter(DefaultConsoleTableSorter.getInstance());
        EnhancedTableColumn nameColumn =
                (EnhancedTableColumn)sessionTable.getColumn(SessionTableModel.LOGGED_IN_AT);
        sessionTable.setColumnSortedAscending(nameColumn, false);
        sessionTable.sizeColumnsToFitData();
		sessionTable.getSelectionModel().setSelectionMode(ListSelectionModel.MULTIPLE_INTERVAL_SELECTION);
		sessionTable.setRowSelectionAllowed(true);
		sessionTable.setColumnSelectionAllowed(false);
		sessionTable.setAllowsMultipleColumnSorting(false);
		
		SessionTableCellRenderer renderer = new SessionTableCellRenderer();
		for (int i = 0; i < SessionTableModel.COLUMN_NAMES.length; i++) {
   			TableColumn col = sessionTable.getColumnModel().getColumn(i);
   			col.setCellRenderer(renderer);
		}
    }

    public void valueChanged(ListSelectionEvent ev) {
        if (!programaticSelectionChange && !ev.getValueIsAdjusting()) {
            if (sessionTable.getSelectedRow() == -1) {
                if ((terminateAction != null) && terminateAction.isEnabled()) {
                    terminateAction.setEnabled(false);
                }
            }
            else {
                if ((terminateAction != null) &&
                    !terminateAction.isEnabled()) {
                    terminateAction.setEnabled(true);
                }
            }
        }
    }

    private void deselectSessionRow(int theRow, String msgHdr,
    		String theMessage) {
        StaticUtilities.displayModalDialogWithOK(msgHdr, theMessage,
        		JOptionPane.WARNING_MESSAGE);
        programaticSelectionChange = true;
        sessionTable.getSelectionModel()
                    .removeSelectionInterval(theRow, theRow);
        programaticSelectionChange = false;
        if ((sessionTable.getSelectedRowCount() == 0) &&
            (terminateAction != null) &&
            terminateAction.isEnabled()) {
            terminateAction.setEnabled(false);
        }
    }

    private boolean terminationAllowed(int theRow,
        	MetaMatrixSessionID[] consoleSessionIDs) {
        boolean canTerminate = true;
        int modelIndex = sessionTable.convertRowIndexToModel(theRow);
        String rowSessionID =
            (String)tableModel.getValueAt(modelIndex, SessionTableModel.SESSION_ID_COLUMN_NUM);
        boolean isAConsoleSession = false;
        int i = 0;
        while ((i < consoleSessionIDs.length) && (!isAConsoleSession)) {
        	String curConsoleSessionID = consoleSessionIDs[i].toString();
        	if (rowSessionID.equals(curConsoleSessionID)) {
//        		String msgHdr = ConsolePlugin.Util.getString(
//        				"SessionPanel.cannotTerminateMsgHdr");  //$NON-NLS-1$
//        		String msg = ConsolePlugin.Util.getString("SessionPanel.cannotTerminate_1")  //$NON-NLS-1$
//        				+ ' ' + rowSessionID + ' ' +
//        				ConsolePlugin.Util.getString("SessionPanel.cannotTerminate_2");  //$NON-NLS-1$
                
                String msg = CANNOTTERMHDR1 + ' ' + rowSessionID + ' ' + CANNOTTERMHDR2;
                
        		deselectSessionRow(theRow, CANNOTTERMHDR, msg);
        		isAConsoleSession = true;
        		canTerminate = false;
        	} else {
        		i++;
        	}
        }
        return canTerminate;
    }

	private MetaMatrixSessionID[] getConsoleSessionIDs() {
		ConnectionInfo[] consoleSessions = 
				ConsoleMainFrame.getInstance().getConnections();
		MetaMatrixSessionID[] ids = new MetaMatrixSessionID[consoleSessions.length];
		for (int i = 0; i < consoleSessions.length; i++) {
			ids[i] = consoleSessions[i].getSessionID();
		}
		return ids;
	}
	
    private void repopulateModel(){
		try{
//            java.util.List /*<EnhancedTableColumn>*/ sortingColumns =
//                    sessionTable.getSortedColumns();
            tableModel.setDataVector(getSessionData());
            
            setSortingColumns(sessionTable.getSortedColumns());
        } catch (NullPointerException e) {
            LogManager.logError(LogContexts.SESSIONS, e,
                    ConsolePlugin.Util.getString("SessionPanel.repopulateModel()_NullPointerException_15")); //$NON-NLS-1$
			ExceptionUtility.showMessage(ConsolePlugin.Util.getString("SessionPanel.Error_repopulating_Session_table_16"), e); //$NON-NLS-1$
		}
		sessionTable.sizeColumnsToFitData();

        sessionTable.getColumnModel().setColumnMargin(5);
		sessionTable.sort();
        //NOTE-- TableWidget is somehow losing setEditable(false), so resetting.
        //BWP  07/25/02
        sessionTable.setEditable(false);
        sessionTable.setVisible(true);
	}

	private Object[][] getSessionData() {
		Object[][] data = new Object[allSessions.size()][SessionTableModel.COLUMN_COUNT];
		
		// Populate the table data.
		for (int i=0; i < allSessions.size(); i++){
		    Session u = allSessions.get(i);
		
		    //match these up with column names
		    data[i][SessionTableModel.SESSION_ID_COLUMN_NUM] = u.getSessionID().toString();
		    data[i][SessionTableModel.USER_NAME_COLUMN_NUM] = u.getUserName();
		    data[i][SessionTableModel.APPLICATION_COLUMN_NUM] = u.getApplicationName();
		    data[i][SessionTableModel.LOGGED_IN_COLUMN_NUM] = u.getCreatedDate();
		    data[i][SessionTableModel.LAST_PING_TIME] = u.getLastPingTime();
		    String vdbName = u.getVDBName();
		    data[i][SessionTableModel.VDB_NAME_COLUMN_NUM] = (vdbName!=null?vdbName:"");//$NON-NLS-1$
		    String vdbVersStr = u.getVDBVersion();
		    if (vdbVersStr != null) {
		        vdbVersStr = vdbVersStr.trim();
 		    } else {
               vdbVersStr = "";//$NON-NLS-1$
            }
		    
		    data[i][SessionTableModel.VDB_VERSION_COLUMN_NUM] = vdbVersStr;
		    String sessionState = ACTIVE_TEXT;  
		    data[i][SessionTableModel.STATE_COLUMN_NUM] = sessionState;
		    data[i][SessionTableModel.PRODUCT_COLUMN_NUM]= u.getProductName();
		
		}
		return data;
	}

    private void setSortingColumns(java.util.List /*<EnhancedTableColumn>*/ columns) {
        
        Iterator it = columns.iterator();
        for (int i = 0; it.hasNext(); i++) {
            EnhancedTableColumn col = (EnhancedTableColumn)it.next();

            boolean ascending = col.isSortedAscending();
            boolean clearPrevious = (i == 0);

            if (ascending) {
                sessionTable.setColumnSortedAscending(col,
                        (!clearPrevious));
            } else {
                sessionTable.setColumnSortedDescending(col,
                        (!clearPrevious));
            }        
        }
    
    }
        
    //returns list of SessionInfoIndexPair objects with model indeces of selected rows and the corresponding sessinfos
    public java.util.List /*SessionInfoIndexPair*/ getCurrentSelections(){
        ArrayList result = new ArrayList();
        ListSelectionModel lsm = sessionTable.getSelectionModel();
        for (int i = lsm.getMinSelectionIndex(); i <= lsm.getMaxSelectionIndex(); i++){
            if (lsm.isSelectedIndex(i)){
                int modelIndex = sessionTable.convertRowIndexToModel(i);
                Session sessInfo = allSessions.get(modelIndex);
                result.add(new SessionInfoIndexPair(sessInfo,modelIndex));
            }
        }

        return result;
    }

    private void handlePopupTrigger(MouseEvent ev, Component comp){
        JPopupMenu popupMenu = new JPopupMenu();
        if (!terminateAction.isEnabled()) {
            terminateAction.setEnabled(true);
        }
        popupMenu.add(terminateAction);
        popupMenu.show(comp, ev.getX() + 10, ev.getY());
    }

    //BUSINESS METHODS

    public void terminateSelectedSessions(){
        TableWidget table = sessionTable;
        java.util.List selections = getCurrentSelections();
        
        int numSelections = selections.size();
        int numDeletableSessions = 0;
        MetaMatrixSessionID[] consoleSessionIDs = getConsoleSessionIDs();
        int[] selectedRows = table.getSelectedRows();
        for (int i=0; i<selectedRows.length; i++) {
        	boolean canTerminateSession = terminationAllowed(selectedRows[i], 
        			consoleSessionIDs);
        	if (canTerminateSession) {
        		numDeletableSessions += 1;
        	}
        }
        if (numDeletableSessions == 0) {
            return;
        }
        boolean confirmed;
        if (numSelections > 1) {
            confirmed = DialogUtility.yesNoDialog(null,
                    ConsolePlugin.Util.getString("SessionPanel.Terminate_the__19") + numDeletableSessions + ConsolePlugin.Util.getString("SessionPanel._selected_sessions__20"), //$NON-NLS-1$ //$NON-NLS-2$
                    ConsolePlugin.Util.getString("SessionPanel.Confirm_Termination_21")); //$NON-NLS-1$
        } else {
            int tableRow = sessionTable.getSelectedRow();
            int modelRow = sessionTable.convertRowIndexToModel(tableRow);
            Session sessToKill = allSessions.get(modelRow);
            String session = sessToKill.toString(); //$NON-NLS-1$
            confirmed = DialogUtility.yesNoDialog(null,
                    ConsolePlugin.Util.getString("SessionPanel.Terminate_Session__23") + session + "?", //$NON-NLS-1$ //$NON-NLS-2$
                    ConsolePlugin.Util.getString("SessionPanel.Confirm_Termination_25")); //$NON-NLS-1$
        }

        if (confirmed) {
            Object[] pairArray = getCurrentSelections().toArray();
            Arrays.sort(pairArray);
            ArrayList orderedSelections = new ArrayList(Arrays.asList(pairArray));

            Iterator it = orderedSelections.iterator();
            try {
                while (it.hasNext()) {
                    SessionInfoIndexPair pair = (SessionInfoIndexPair)it.next();
                    Session sessInfo = pair.sessInfo;
                    int modelIndex =pair.modelIndex;
                    String id = sessInfo.getSessionID();
                    if (!id.equals(connection.getSessionID().toString())) {
                        getSessionManager().terminateSession(id);

                        //Sanity check here- our index and sessinfo should still mesh
                        //once we terminate it, we should yank it from the model and display
                        //This only works because our model indexes are in descending order.  if they
                        //were in some random order, this would puke
                        if( allSessions.get(modelIndex) == sessInfo){
                            //if sessInfo and allSessions[modelIndex] refer to the same object, that's good
                            allSessions.remove(modelIndex);
                            tableModel.removeRow(modelIndex);

                        }else{
                            throw new RuntimeException(ConsolePlugin.Util.getString("SessionPanel.SessionTab_sessinfo_and_index_aren__t_paired_up_correctly._26")); //$NON-NLS-1$
                        }
                    }
                }
                sessionTable.getSelectionModel().clearSelection();

            } catch (Exception e) {
                ExceptionUtility.showMessage(ConsolePlugin.Util.getString("SessionPanel.Error_terminating_session_27"), e); //$NON-NLS-1$
            }
        }
    }
    
	public void refresh() {
		refreshTable() ;
	}
	
    /**
     * Tells manager to refresh
     */
	public void refreshTable() {
		boolean continuing = true;
		Collection<Session>	sessions = null;
		try {
			sessions = getSessionManager().getSessions(true);
		} catch (Exception ex) {
			LogManager.logError(LogContexts.SESSIONS, ex, ConsolePlugin.Util.getString("SessionPanel.Error_retrieving_sessions_information_28")); //$NON-NLS-1$
			ExceptionUtility.showMessage(ConsolePlugin.Util.getString("SessionPanel.Error_retrieving_session_information_29"), //$NON-NLS-1$
			ex);
			continuing = false;
		}
		if (continuing) {
			try {
				StaticUtilities.startWait(this);
                allSessions = new ArrayList(sessions);
                repopulateModel();
			} finally {

				StaticUtilities.endWait(this);
			}
		}
	}

    //GETTERS-SETTERS


    private SessionManager getSessionManager(){
        return ModelManager.getSessionManager(connection);
    }

    public java.util.List resume() {
        Iterator iter = getCurrentSelections().iterator();
        refresh();
        if (!iter.hasNext()){
            if (terminateAction != null) {
                terminateAction.setEnabled(false);
            }
        }
        while (iter.hasNext()) {
            SessionInfoIndexPair pair = (SessionInfoIndexPair)iter.next();
            Session sessInfo = pair.sessInfo;
            String id = sessInfo.getSessionID();
            int rowCount = sessionTable.getRowCount();
            for (int i = 0; i < rowCount ; i++){
                String rowId = (String)sessionTable.getValueAt(i,SessionTableModel.SESSION_ID_COLUMN_NUM);
                if (id.equals(rowId)){
                    sessionTable.addRowSelectionInterval(i,i);

                }
            }
        }
        return actions;
    }

    public String getTitle() {
        return TITLE; //$NON-NLS-1$
    }

	public ConnectionInfo getConnection() {
		return connection;
	}
	
    /**
     * Name must uniquely identify this Refreshable object.  Useful to support
     * applying mods to the rate and enabled state by outside agencies.
     */
    public String  getName() {
        return StaticProperties.DATA_SESSION;
    }

    /**
     * Turns the refresh feature on or off.
     *
     */
    public void setAutoRefreshEnabled( boolean b ) {
        arRefresher.setAutoRefreshEnabled( b );
    }

    /**
     * Sets the refresh rate.
     *
     */
    public void setRefreshRate( int iRate ) {
        arRefresher.setRefreshRate( iRate );
    }

    /**
     * Set the 'AutoRefresh' agent
     *
     */
    public void setAutoRefresher( AutoRefresher ar ) {
        arRefresher = ar;
    }

    /**
     * Get the 'AutoRefresh' agent
     *
     */
    public AutoRefresher getAutoRefresher() {
        return arRefresher;
    }
    
    public void receiveUpdateNotification(RuntimeUpdateNotification notification) {
    	//TODO
    }


    ///////////////////////////////////////////////////////////////////////////
    // INNER CLASSES
    ///////////////////////////////////////////////////////////////////////////

//    private class PanelAction extends AbstractPanelAction {
//        public static final int TERMINATE = 0;
//        public static final int REFRESH = 1;
//
//        public PanelAction(int theType) {
//            super(theType);
//            if (theType == TERMINATE) {
//                putValue(NAME, ConsolePlugin.Util.getString("SessionPanel.Terminate_31")); //$NON-NLS-1$
//                putValue(SHORT_DESCRIPTION, ConsolePlugin.Util.getString("SessionPanel.Terminates_all_selected_sessions_32")); //$NON-NLS-1$
//            }
//            else if (theType == REFRESH) {
//                putValue(NAME, ConsolePlugin.Util.getString("SessionPanel.Refresh_33")); //$NON-NLS-1$
//                putValue(SHORT_DESCRIPTION, ConsolePlugin.Util.getString("SessionPanel.Query_for_current_list_of_sessions_34")); //$NON-NLS-1$
//            }
//        }
//        protected void actionImpl(ActionEvent theEvent)
//            throws ExternalException {
//
//            if (type == TERMINATE) {
//                terminateSelectedSessions();
//            }
//            else if (type == REFRESH) {
//                refresh();
//            }
//        }
//    }
}//end SessionPanel




class SessionTableWidget extends TableWidget {
//	private SessionTableModel model;
	public SessionTableWidget(SessionTableModel model) {
		super(model, true);
//		this.model = model;
	}
	
	//Method inserted for debugging use
	public void paint(Graphics g) {
		super.paint(g);
	}

	//Method inserted for debugging use
	public Dimension getPreferredSize() {
		Dimension prefSize = super.getPreferredSize();
		return prefSize;
	}
}//end SessionTableWidget


	
		
class SessionInfoIndexPair implements Comparable{
    public Session sessInfo;
    public int modelIndex;

    public SessionInfoIndexPair(Session sessInfo, int modelIndex){
        this.sessInfo=sessInfo;
        this.modelIndex=modelIndex;
    }

    public boolean equals(Object o){
        if(this==o){
            return true;
        }

        //else we're equal if it's another one of these, and
        //1.) modelIndex is ==
        //2.) sessInfo ref is ==
        if(this.getClass().isInstance(o)){
            SessionInfoIndexPair other = (SessionInfoIndexPair)o;
            return (other.modelIndex == modelIndex && other.sessInfo == sessInfo);
        }
        return false;
    }

    //use modelIndex for comparison
    //NOTICE that this sorts backwards.  That way, the ascending-order sort we use (care of Arrays.sort())
    //puts the highest modelIndex first.  This is useful for the deletion algorithm we use.
    public int compareTo(Object o){
        if(this.getClass().isInstance(o)){
            SessionInfoIndexPair other = (SessionInfoIndexPair)o;
            if(other.modelIndex == modelIndex){
                return 0;
            }else if(other.modelIndex > modelIndex){
                return 1;
            }else{
                return -1;
            }

        }
        return -1;
    }
}//end SessionInfoIndexPair
