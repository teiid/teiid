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

package com.metamatrix.console.ui.views.transactions;

import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;
import java.awt.event.ActionEvent;
import java.awt.event.MouseAdapter;
import java.awt.event.MouseEvent;
import java.util.ArrayList;

import javax.swing.AbstractAction;
import javax.swing.Action;
import javax.swing.JPopupMenu;
import javax.swing.JScrollPane;
import javax.swing.SwingUtilities;
import javax.transaction.xa.Xid;

import com.metamatrix.console.connections.ConnectionInfo;
import com.metamatrix.console.models.ManagerListener;
import com.metamatrix.console.models.ModelChangedEvent;
import com.metamatrix.console.models.TransactionManager;
import com.metamatrix.console.notification.RuntimeUpdateNotification;
import com.metamatrix.console.ui.layout.BasePanel;
import com.metamatrix.console.ui.layout.ConsoleMenuBar;
import com.metamatrix.console.ui.layout.MenuEntry;
import com.metamatrix.console.ui.layout.WorkspacePanel;
import com.metamatrix.console.ui.util.AbstractPanelAction;
import com.metamatrix.console.util.AutoRefreshable;
import com.metamatrix.console.util.AutoRefresher;
import com.metamatrix.console.util.DialogUtility;
import com.metamatrix.console.util.ExceptionUtility;
import com.metamatrix.console.util.StaticProperties;
import com.metamatrix.console.util.StaticUtilities;
import com.metamatrix.toolbox.ui.widget.TableWidget;
import com.metamatrix.toolbox.ui.widget.table.DefaultTableColumnModel;

public class TransactionsPanel extends BasePanel
                            implements ManagerListener,
                                       WorkspacePanel,
                                       AutoRefreshable {
    private TransactionManager manager;
    private TableWidget table;
    private ConnectionInfo connection;
    private boolean canTerminateTransactions;
    private boolean isStopped = true;
    private AutoRefresher arRefresher = null;

    public TransactionsPanel(TransactionManager mgr, boolean canTerminate,
    		ConnectionInfo conn) {
        super();
        manager = mgr;
        manager.addManagerListener(this);
        canTerminateTransactions = canTerminate;
        this.connection = conn;
        init();
    }

    private void init() {
        table = new TableWidget(manager.getTableModel(), true);
        table.setRowSelectionAllowed(true);
        table.setEditable(false);
        table.setColumnSelectionAllowed(false);
        GridBagLayout layout = new GridBagLayout();
        setLayout(layout);
        JScrollPane jsp = new JScrollPane(table);
        add(jsp);
        layout.setConstraints(jsp, new GridBagConstraints(0, 0, 1, 1, 1.0, 1.0,
                GridBagConstraints.CENTER, GridBagConstraints.BOTH,
                new Insets(5, 5, 5, 5), 0, 0));
        if (canTerminateTransactions) {
            table.addMouseListener(new MouseAdapter() {
                public void mousePressed(MouseEvent ev) {
                    checkMouseClick(ev);
                }
            });
        }

        // Establish AutoRefresher
        arRefresher = new AutoRefresher(this, 15, false, connection);
        arRefresher.init();
        arRefresher.startTimer();

    }

    private void checkMouseClick(MouseEvent ev) {
        if (!SwingUtilities.isRightMouseButton(ev)) {
        	return;
        }
        int[] selectedRows = table.getSelectedRows();
        if (selectedRows.length != 1) {
        	return;
        }
        int row = selectedRows[0];
        //Was the mouse click over the selected row?
        int clickRow = table.rowAtPoint(ev.getPoint());
        if (row != clickRow) {
        	return;
        }
        //Yes.  Put up popup menu with single action of terminate transaction
        final String transactionNum = (String)table.getModel().getValueAt(row, TransactionTableModel.TXN_ID_INDEX);
        final String sessionId = (String)table.getModel().getValueAt(row, TransactionTableModel.SESSION_ID_INDEX);
        final Xid xid = (Xid)table.getModel().getValueAt(row, TransactionTableModel.XID_INDEX);
        
        JPopupMenu popupMenu = new JPopupMenu();
        Action terminateAction = new AbstractAction("Terminate transaction") {

	        public void actionPerformed(ActionEvent ev) {
	            boolean confirmed = DialogUtility.yesNoDialog(null,
	                    "Terminate transaction " + transactionNum.toString() + "?",
	                    "Confirm Termination");
	            if (confirmed) {
	                doTermination(transactionNum, sessionId, xid);
	            }
	        }
        };

        popupMenu.add(terminateAction);
        popupMenu.show(table, ev.getX() + 10, ev.getY());
    }

    public void start() {
        isStopped = false;
        //super.start();
        manager.refresh();
    }

    public void stop() {
        isStopped = true;
    }

    public java.util.List /*<Action>*/ resume() {
    	refresh();
        AbstractPanelAction refreshAction = new AbstractPanelAction(0){
            public void actionImpl(final ActionEvent e){
                TransactionsPanel.this.refresh();
            }
        };

        refreshAction.putValue(AbstractPanelAction.NAME,"Refresh");

        MenuEntry menuEntry=new MenuEntry(MenuEntry.VIEW_REFRESH_MENUITEM, refreshAction);
        ArrayList menuList=new ArrayList(1);
        menuList.add(menuEntry);
        ConsoleMenuBar.getInstance().addActionsFromMenuEntryObjects(menuList);

        return null;
    }

    public String getTitle() {
        return "Transactions";
    }

	public ConnectionInfo getConnection() {
		return connection;
	}
	
    public void refresh(){
        //super.refresh();
        manager.refresh();

        //this puts everything in default order, unsorted, nothing moved around or hidden, etc
        //so that when we refresh, we see everything, and we go back to what we'd originally seen
        DefaultTableColumnModel columnModel = (DefaultTableColumnModel)table.getColumnModel();
        columnModel.setColumnsNotSorted();
        for(int i=0; i<table.getColumnCount(); ++i){
            columnModel.moveColumn(i,i);
            columnModel.setColumnHidden(columnModel.getColumn(i),false);
        }

    }
    
    public void receiveUpdateNotification(RuntimeUpdateNotification notification) {
    	//TODO
    }

    public boolean isRefreshable() {
        return true;
    }

    public void modelChanged(ModelChangedEvent e) {
        if (!isStopped) {
            try {
                StaticUtilities.startWait(this);
                manager.refresh();
            } catch (Exception ex) {
                ExceptionUtility.showMessage("Update Transactions Table", ex);
            } finally {
                StaticUtilities.endWait(this);
            }
        }
    }

    public void doTermination(String transactionNum, String sessionId, Xid xid) {
        try {
        	if (xid != null) {
        		getConnection().getServerAdmin().terminateTransaction(xid);
        	} else {
        		getConnection().getServerAdmin().terminateTransaction(transactionNum, sessionId);
        	}
        } catch (Exception e) {
            ExceptionUtility.showMessage("Terminate Transaction", e);
        }
    }

    /**
     * Name must uniquely identify this Refreshable object.  Useful to support
     * applying mods to the rate and enabled state by outside agencies.
     */
    public String  getName()
    {
        return StaticProperties.DATA_TRANSACTION;
    }

    /**
     * Turns the refresh feature on or off.
     *
     */
    public void setAutoRefreshEnabled( boolean b )
    {
        arRefresher.setAutoRefreshEnabled( b );
    }

    /**
     * Sets the refresh rate.
     *
     */
    public void setRefreshRate( int iRate )
    {
        arRefresher.setRefreshRate( iRate );
    }

    /**
     * Set the 'AutoRefresh' agent
     *
     */
    public void setAutoRefresher( AutoRefresher ar )
    {
        arRefresher = ar;
    }

    /**
     * Get the 'AutoRefresh' agent
     *
     */
    public AutoRefresher getAutoRefresher()
    {
        return arRefresher;
    }

}//end TransactionsPanel

