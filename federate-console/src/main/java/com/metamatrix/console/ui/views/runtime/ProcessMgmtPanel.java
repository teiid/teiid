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

//#############################################################################
package com.metamatrix.console.ui.views.runtime;

import java.awt.Dimension;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;
import java.util.List;
import java.util.Vector;

import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JTable;
import javax.swing.ListSelectionModel;
import javax.swing.event.ListSelectionEvent;
import javax.swing.event.ListSelectionListener;

import com.metamatrix.console.models.RuntimeMgmtManager;
import com.metamatrix.console.ui.views.runtime.util.RuntimeMgmtUtils;
import com.metamatrix.console.ui.views.runtime.util.ServiceStateConstants;
import com.metamatrix.console.util.ExternalException;
import com.metamatrix.platform.admin.api.runtime.ProcessData;
import com.metamatrix.platform.admin.api.runtime.ServiceData;
import com.metamatrix.toolbox.ui.widget.ButtonWidget;
import com.metamatrix.toolbox.ui.widget.DialogPanel;
import com.metamatrix.toolbox.ui.widget.DialogWindow;
import com.metamatrix.toolbox.ui.widget.TableWidget;
import com.metamatrix.toolbox.ui.widget.table.DefaultTableModel;

public final class ProcessMgmtPanel
    extends DialogPanel
    implements ListSelectionListener,
               OperationsDelegate,
               ServiceStateConstants {

    ///////////////////////////////////////////////////////////////////////////
    // CONSTANTS
    ///////////////////////////////////////////////////////////////////////////

    private static /*final*/ String[] HDRS;
    private static final int PROC_COL = 0;
    private static final int ID_COL = 1;
    private static final int REGISTERED_COL = 2;
    private static final int NUM_REG_PSC_COL = 3;
    private static final int NUM_NOT_REG_PSC_COL = 4;

    ///////////////////////////////////////////////////////////////////////////
    // INITIALIZER
    ///////////////////////////////////////////////////////////////////////////

    static {
        HDRS = new String[5];
        HDRS[PROC_COL] = RuntimeMgmtUtils.getString("pm.proc.hdr"); //$NON-NLS-1$
        HDRS[ID_COL] = RuntimeMgmtUtils.getString("pm.id.hdr"); //$NON-NLS-1$
        HDRS[REGISTERED_COL] = RuntimeMgmtUtils.getString("pm.registered.hdr"); //$NON-NLS-1$
        HDRS[NUM_REG_PSC_COL] = RuntimeMgmtUtils.getString("pm.numregisteredpscs.hdr"); //$NON-NLS-1$
        HDRS[NUM_NOT_REG_PSC_COL] = RuntimeMgmtUtils.getString("pm.numnotregisteredpscs.hdr"); //$NON-NLS-1$
    }

    ///////////////////////////////////////////////////////////////////////////
    // CONTROLS
    ///////////////////////////////////////////////////////////////////////////

    private TableWidget tbl;
    private OperationsPanel pnlOps;

    ///////////////////////////////////////////////////////////////////////////
    // FIELDS
    ///////////////////////////////////////////////////////////////////////////

    private DefaultTableModel tblModel;
    private RuntimeMgmtManager manager;

    ///////////////////////////////////////////////////////////////////////////
    // CONSTRUCTORS
    ///////////////////////////////////////////////////////////////////////////

    public ProcessMgmtPanel(RuntimeMgmtManager mgr)
        throws ExternalException {
		super();
		this.manager = mgr;
        JPanel pnl = new JPanel(new GridBagLayout());
        pnl.setBorder(RuntimeMgmtUtils.EMPTY_BORDER);

        tbl = new TableWidget();
        tblModel = (DefaultTableModel)tbl.getModel();
        tblModel.setColumnIdentifiers(HDRS);
        tbl.setEditable(false);
        tbl.setPreferredScrollableViewportSize(
            new Dimension(
                tbl.getPreferredSize().width,
                RuntimeMgmtUtils.getInt("pm.rows", 10) * tbl.getRowHeight())); //$NON-NLS-1$
        tbl.setAutoResizeMode(JTable.AUTO_RESIZE_ALL_COLUMNS);
        tbl.setSortable(true);
        tbl.getSelectionModel().setSelectionMode(
            ListSelectionModel.SINGLE_SELECTION);
        tbl.getSelectionModel().addListSelectionListener(this);

        JScrollPane spn = new JScrollPane(tbl);
        GridBagConstraints gbc = new GridBagConstraints();
        gbc.insets = new Insets(3, 3, 3, 3);
        gbc.gridx = 0;
        gbc.gridy = 0;
        gbc.fill = GridBagConstraints.BOTH;
        gbc.weightx = 1.0;
        gbc.weighty = 1.0;
        pnl.add(spn, gbc);

        pnlOps = new OperationsPanel(this);
        gbc.gridx = 1;
        gbc.gridy = 0;
        gbc.fill = GridBagConstraints.NONE;
        gbc.weightx = 0.0;
        gbc.weighty = 0.0;
        pnl.add(pnlOps, gbc);

        setContent(pnl);

        ButtonWidget btnOk = getAcceptButton();
        btnOk.setText(RuntimeMgmtUtils.getString("sm.btnOk")); //$NON-NLS-1$
        removeNavigationButton(getCancelButton());

    }

    ///////////////////////////////////////////////////////////////////////////
    // METHODS
    ///////////////////////////////////////////////////////////////////////////

    private ProcessData getSelectedProcess() {
        int row = tbl.getSelectedRow();
        return (ProcessData)tblModel.getValueAt(row, PROC_COL);
    }

    public void load(List theProcesses)
        throws ExternalException {

        tblModel.setNumRows(0);
        if ((theProcesses != null) && !theProcesses.isEmpty()) {
            for (int size=theProcesses.size(), i=0; i<size; i++) {
                ProcessData process = (ProcessData)theProcesses.get(i);
                Vector row = new Vector(HDRS.length);
                row.setSize(HDRS.length);
                row.setElementAt(process, PROC_COL);
                row.setElementAt(process.getProcessID(), ID_COL);
                row.setElementAt(
                    new Boolean(process.isRegistered()), REGISTERED_COL);
                Integer[] counts = manager.getPscCounts(process);
                row.setElementAt(counts[0], NUM_REG_PSC_COL);
                row.setElementAt(counts[1], NUM_NOT_REG_PSC_COL);
                tblModel.addRow(row);
 
            }
            tbl.setRowSelectionInterval(0, 0);
        }
        tbl.sizeColumnsToFitData();               
    }

    public void setActionsDisabled() {
        pnlOps.setActionsDisabled();
    }

    public void startOperation()
        throws ExternalException {

        ProcessData process = getSelectedProcess();
        manager.startProcess(process);
    }

    public void stopOperation()
        throws ExternalException {

        ProcessData process = getSelectedProcess();
        Object[] processArg = new Object[] {process};
        
        String msg = "dlg.stopprocess.msg"; //$NON-NLS-1$
        if (tblModel.getRowCount() == 1) {
            msg = "dlg.stoponlyprocess.msg"; //$NON-NLS-1$
        } 

        ConfirmationPanel pnlConfirm =
            new ConfirmationPanel(msg, processArg);
        DialogWindow.show(this,
                          RuntimeMgmtUtils.getString("dlg.stopprocess.title", //$NON-NLS-1$
                                                     processArg),
                          pnlConfirm);
        if (pnlConfirm.isConfirmed()) {
            manager.stopProcess(process);
        }
    }

    public void stopNowOperation()
        throws ExternalException {

        ProcessData process = getSelectedProcess();
        Object[] processArg = new Object[] {process};
        
        String msg = "dlg.stopnowprocess.msg"; //$NON-NLS-1$
        if (tblModel.getRowCount() == 1) {
            msg = "dlg.stopnowonlyprocess.msg"; //$NON-NLS-1$
        } 
        
        ConfirmationPanel pnlConfirm =
            new ConfirmationPanel(msg, processArg);
        DialogWindow.show(this,
                          RuntimeMgmtUtils.getString("dlg.stopnowprocess.title", //$NON-NLS-1$
                                                     processArg),
                          pnlConfirm);
        if (pnlConfirm.isConfirmed()) {
            manager.stopProcessNow(process);
        }
    }

    /**
     *  
     * @see com.metamatrix.console.ui.views.runtime.OperationsDelegate#showServcieError()
     * @since 4.4
     */
    public void showServcieError() throws ExternalException {
        // Nothing to do here - this is not a service
    }
    
    public QueueStatisticsFrame startShowQueue(ServiceData sd) {
        return null;
    }

    public boolean isServiceDisplayed (ServiceData sd){
        return false;
    }
    public void refreshService(ServiceData sd){
    }

    public VMStatisticsFrame startShowProcess(ProcessData pd){
        return null;
    }

    public boolean isProcessDisplayed (ProcessData pd){
        return false;
    }
    public void refreshProcess(ProcessData pd){
    }
    public void valueChanged(ListSelectionEvent theEvent) {
        if (!theEvent.getValueIsAdjusting()) {
            if (tbl.getSelectedRowCount() > 0) {
                ProcessData process = getSelectedProcess();
                pnlOps.setEnabled(
                    RuntimeMgmtUtils.getOperationsEnablements(process));
            }
            else {
                pnlOps.setEnabled(false);
            }
        }

    }

}
