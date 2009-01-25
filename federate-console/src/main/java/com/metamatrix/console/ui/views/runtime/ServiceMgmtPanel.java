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

import java.awt.Component;
import java.awt.Dimension;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;
import java.util.Date;
import java.util.List;
import java.util.Vector;

import javax.swing.JComponent;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JTable;
import javax.swing.ListSelectionModel;
import javax.swing.event.ListSelectionEvent;
import javax.swing.event.ListSelectionListener;
import javax.swing.table.TableColumnModel;

import com.metamatrix.console.models.RuntimeMgmtManager;
import com.metamatrix.console.ui.views.runtime.util.RuntimeMgmtUtils;
import com.metamatrix.console.ui.views.runtime.util.ServiceStateConstants;
import com.metamatrix.console.util.ExceptionUtility;
import com.metamatrix.console.util.ExternalException;
import com.metamatrix.platform.admin.api.runtime.ProcessData;
import com.metamatrix.platform.admin.api.runtime.ServiceData;
import com.metamatrix.platform.service.api.ServiceState;
import com.metamatrix.toolbox.ui.UIDefaults;
import com.metamatrix.toolbox.ui.widget.ButtonWidget;
import com.metamatrix.toolbox.ui.widget.DialogPanel;
import com.metamatrix.toolbox.ui.widget.DialogWindow;
import com.metamatrix.toolbox.ui.widget.TableWidget;
import com.metamatrix.toolbox.ui.widget.table.DefaultTableCellRenderer;
import com.metamatrix.toolbox.ui.widget.table.DefaultTableModel;

public final class ServiceMgmtPanel
    extends DialogPanel
    implements ListSelectionListener,
               OperationsDelegate,
               ServiceStateConstants {

    ///////////////////////////////////////////////////////////////////////////
    // CONSTANTS
    ///////////////////////////////////////////////////////////////////////////

    private static /*final*/ String[] HDRS;
    private static final int SERV_COL = 0;
    private static final int ID_COL = 1;
    private static final int STATE_COL = 2;
    private static final int DEPLOYED_COL = 3;
    private static final int REGISTERED_COL = 4;
    private static final int ESSENTIAL_COL = 5;
    private static final int TIME_COL = 6;

    ///////////////////////////////////////////////////////////////////////////
    // INITIALIZER
    ///////////////////////////////////////////////////////////////////////////

    static {
        HDRS = new String[7];
        HDRS[SERV_COL] = RuntimeMgmtUtils.getString("sm.service.hdr");
        HDRS[ID_COL] = RuntimeMgmtUtils.getString("sm.id.hdr");
        HDRS[STATE_COL] = RuntimeMgmtUtils.getString("sm.state.hdr");
        HDRS[DEPLOYED_COL] = RuntimeMgmtUtils.getString("sm.deployed.hdr");
        HDRS[REGISTERED_COL] = RuntimeMgmtUtils.getString("sm.registered.hdr");
        HDRS[ESSENTIAL_COL] = RuntimeMgmtUtils.getString("sm.essential.hdr");
        HDRS[TIME_COL] = RuntimeMgmtUtils.getString("sm.time.hdr");
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

    public ServiceMgmtPanel(RuntimeMgmtManager mgr) {
    	super();
    	this.manager = mgr;
        JPanel pnl = new JPanel(new GridBagLayout());
        pnl.setBorder(RuntimeMgmtUtils.EMPTY_BORDER);

        tbl = new TableWidget();
        tblModel = (DefaultTableModel)tbl.getModel();
        tblModel.setColumnIdentifiers(HDRS);
        tbl.setEditable(false);
        tbl.setPreferredScrollableViewportSize(
            new Dimension(tbl.getPreferredSize().width,
                          RuntimeMgmtUtils.getInt("servicerows", 12) *
                              tbl.getRowHeight()));
        tbl.setAutoResizeMode(JTable.AUTO_RESIZE_ALL_COLUMNS);
        tbl.setSortable(true);
        tbl.getSelectionModel().setSelectionMode(
            ListSelectionModel.SINGLE_SELECTION);
        tbl.getSelectionModel().addListSelectionListener(this);
        ServiceMgmtCellRenderer renderer = new ServiceMgmtCellRenderer();
        TableColumnModel columnModel = tbl.getColumnModel();
        columnModel.getColumn(STATE_COL).setCellRenderer(renderer);
        columnModel.getColumn(TIME_COL).setCellRenderer(renderer);

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
        btnOk.setText(RuntimeMgmtUtils.getString("sm.btnOk"));
        removeNavigationButton(getCancelButton());

    }

    ///////////////////////////////////////////////////////////////////////////
    // METHODS
    ///////////////////////////////////////////////////////////////////////////

    private ServiceData getSelectedService() {
        int row = tbl.getSelectedRow();
        return (ServiceData)tblModel.getValueAt(row, SERV_COL);
    }

    public void load(List theServices) {
        tblModel.setNumRows(0);
        if ((theServices != null) && !theServices.isEmpty()) {
            for (int size=theServices.size(), i=0; i<size; i++) {
                ServiceData service = (ServiceData)theServices.get(i);
                Vector row = new Vector(HDRS.length);
                row.setSize(HDRS.length);
                row.setElementAt(service, SERV_COL);
                row.setElementAt(service.getServiceID(), ID_COL);
                
                int state = service.getCurrentState();
                
                // VAH
                // hack for odbc service to display a more appropriate
                // text when the actual MMODBC service is not running
                if (service.getName().equalsIgnoreCase("ODBCService") &&
                    state == ServiceState.STATE_DATA_SOURCE_UNAVAILABLE) {       
                    state =  RuntimeMgmtUtils.ODBC_UNAVAILABLE_SERVICE_STATE;
                }
                row.setElementAt(new Integer(state), STATE_COL);

                row.setElementAt(new Boolean(service.isDeployed()), DEPLOYED_COL);
                row.setElementAt(new Boolean(service.isRegistered()), REGISTERED_COL);
                row.setElementAt(new Boolean(service.isEssential()), ESSENTIAL_COL);
                row.setElementAt(service.getStateChangeTime(), TIME_COL);
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

        ServiceData service = getSelectedService();
        manager.startService(service);
        tblModel.setValueAt(new Integer(OPEN), tbl.getSelectedRow(), STATE_COL);
        pnlOps.setEnabled(
            RuntimeMgmtUtils.getServiceOperationsEnablements(OPEN));
    }

    public void stopOperation()
        throws ExternalException {

        ServiceData service = getSelectedService();
        ConfirmationPanel pnlConfirm =
            new ConfirmationPanel("dlg.stopservice.msg", new Object[] {service});
        DialogWindow.show(this,
                          RuntimeMgmtUtils.getString("dlg.stopservice.title"),
                          pnlConfirm);
        if (pnlConfirm.isConfirmed()) {
            manager.stopService(service);
            tblModel.setValueAt(
                new Integer(CLOSED), tbl.getSelectedRow(), STATE_COL);
            pnlOps.setEnabled(
                RuntimeMgmtUtils.getServiceOperationsEnablements(CLOSED));
        }
    }

    public void stopNowOperation()
        throws ExternalException {

        ServiceData service = getSelectedService();
        ConfirmationPanel pnlConfirm =
            new ConfirmationPanel("dlg.stopnowservice.msg",
                                  new Object[] {service});
        DialogWindow.show(this,
                          RuntimeMgmtUtils.getString("dlg.stopnowservice.title"),
                          pnlConfirm);
        if (pnlConfirm.isConfirmed()) {
            manager.stopServiceNow(service);
            tblModel.setValueAt(
                new Integer(CLOSED), tbl.getSelectedRow(), STATE_COL);
            pnlOps.setEnabled(
                RuntimeMgmtUtils.getServiceOperationsEnablements(CLOSED));
        }
    }

    /**
     *  
     * @see com.metamatrix.console.ui.views.runtime.OperationsDelegate#showServcieError()
     * @since 4.4
     */
    public void showServcieError() throws ExternalException {
        Throwable theError = null;
        String titleId = null;
        ServiceData service = getSelectedService();

        if (service != null) {
            theError = service.getInitError();
            if (theError != null) {
                titleId = "dlg.showserviceError.title"; //$NON-NLS-1$
                String errorMsg = theError.getMessage();
                if (errorMsg == null || errorMsg.length() == 0) {
                    errorMsg = "Error message was null."; //$NON-NLS-1$
                }

                ExceptionUtility.showMessage(RuntimeMgmtUtils.getString(titleId, new Object[] {service}), errorMsg, theError);
            }
        }
    }
    

    public QueueStatisticsFrame startShowQueue(ServiceData sd){
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
                ServiceData service = getSelectedService();
                pnlOps.setEnabled(
                    RuntimeMgmtUtils.getOperationsEnablements(service));
            }
            else {
                pnlOps.setEnabled(false);
            }
        }

    }

    ///////////////////////////////////////////////////////////////////////////
    // INNER CLASSES
    ///////////////////////////////////////////////////////////////////////////

    public final class ServiceMgmtCellRenderer
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

            if (theColumn == STATE_COL) {
                int state = ((Integer)theValue).intValue();
                ((JLabel)comp).setText(RuntimeMgmtUtils.getServiceStateText(state));
            }
            else if (theColumn == TIME_COL) {
                ((JLabel)comp).setText(RuntimeMgmtUtils.DATE_FORMATTER.format((Date)theValue));
            }
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
            }
            else {
                comp.setBackground(theTable.getBackground());
            }
            return comp;
        }
    }

}
