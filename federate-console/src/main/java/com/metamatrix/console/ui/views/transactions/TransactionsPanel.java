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

package com.metamatrix.console.ui.views.transactions;

import java.awt.*;
import java.awt.event.*;
import java.util.ArrayList;

import javax.swing.*;

import com.metamatrix.console.connections.ConnectionInfo;
import com.metamatrix.console.models.*;
import com.metamatrix.console.notification.RuntimeUpdateNotification;
import com.metamatrix.console.ui.layout.*;
import com.metamatrix.console.ui.util.AbstractPanelAction;
import com.metamatrix.console.util.*;
import com.metamatrix.server.admin.api.TransactionAdminAPI;
import com.metamatrix.toolbox.ui.widget.TableWidget;
import com.metamatrix.toolbox.ui.widget.table.DefaultTableColumnModel;
import com.metamatrix.common.xa.XATransactionException;

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
                //public void mousePressed(MouseEvent ev) {
                //    checkMouseClick(ev);
                //}
                public void mouseReleased(MouseEvent ev) {
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
        if (ev.isPopupTrigger()) {
            int[] selectedRows = table.getSelectedRows();
            if (selectedRows.length == 1) {
                int row = selectedRows[0];
                //Was the mouse click over the selected row?
                int clickRow = table.rowAtPoint(ev.getPoint());
                if (row == clickRow) {
                    //Yes.  Put up popup menu with single action of terminate transaction
                    Long transactionNum = (Long)table.getModel().getValueAt(row,
                            TransactionTableModel.TRANSACTION_ID_COL);
                    JPopupMenu popupMenu = new JPopupMenu();
                    Action terminateAction = new TerminateAction(transactionNum, this);
                    popupMenu.add(terminateAction);
                    popupMenu.show(table, ev.getX() + 10, ev.getY());
                }
            }
        }
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

    public void doTermination(Long transactionNum) {
        try {
            TransactionAdminAPI api = ModelManager.getTransactionAPI(connection);
            api.terminateTransaction(manager.transactionIDForTransactionNum(transactionNum));
        } catch (XATransactionException ex) {
            StaticUtilities.displayModalDialogWithOK("Not Terminated",
                    "Transaction " + transactionNum +
                    " was not terminated.  The transaction may have already " +
                    "completed.");
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
        //System.out.println( "TransactionsPanel.getName..TOP" );
        return StaticProperties.DATA_TRANSACTION;
    }

    /**
     * Turns the refresh feature on or off.
     *
     */
    public void setAutoRefreshEnabled( boolean b )
    {
        //System.out.println( "TransactionsPanel.setAutoRefreshEnabled..TOP to: " + b );
        arRefresher.setAutoRefreshEnabled( b );
    }

    /**
     * Sets the refresh rate.
     *
     */
    public void setRefreshRate( int iRate )
    {
        //System.out.println( "TransactionsPanel.setRefreshRate..TOP to: " + iRate );
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

class TerminateAction extends AbstractAction {
    private Long transactionNum;
    private TransactionsPanel panel;

    public TerminateAction(Long trans, TransactionsPanel pnl) {
        super("Terminate transaction");
        transactionNum = trans;
        panel = pnl;
    }

    public void actionPerformed(ActionEvent ev) {
        boolean confirmed = DialogUtility.yesNoDialog(null,
                "Terminate transaction " + transactionNum.toString() + "?",
                "Confirm Termination");
        if (confirmed) {
            panel.doTermination(transactionNum);
        }
    }
}//end TerminateAction
