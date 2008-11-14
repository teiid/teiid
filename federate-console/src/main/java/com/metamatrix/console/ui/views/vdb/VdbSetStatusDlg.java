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

package com.metamatrix.console.ui.views.vdb;

import java.awt.Dimension;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;

import javax.swing.JDialog;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JTextField;

import com.metamatrix.console.connections.ConnectionInfo;
import com.metamatrix.console.models.ModelManager;
import com.metamatrix.console.models.VdbManager;
import com.metamatrix.console.ui.ViewManager;
import com.metamatrix.console.util.ExceptionUtility;
import com.metamatrix.core.vdb.VDBStatus;
import com.metamatrix.metadata.runtime.api.VirtualDatabase;
import com.metamatrix.metadata.runtime.api.VirtualDatabaseID;
import com.metamatrix.toolbox.ui.widget.DialogPanel;

public class VdbSetStatusDlg extends JDialog {

    private VirtualDatabase theVdb = null;
    private ConnectionInfo connection = null;
    // private JFrame frParent;
    private DialogPanel dlgpnlDialogContainer = new DialogPanel();
    private JPanel pnlID = new JPanel();
    private JPanel pnlOuter = new JPanel();
    private short siNewStatus = 0;
    private short siCurrStatus = 0;
    private static final String SET_STATUS_TEXT = "Click OK to Change VDB Status to "; //$NON-NLS-1$

    public VdbSetStatusDlg(VirtualDatabase theVdb, ConnectionInfo connection, boolean changeDefaultStatus) {
        super(ViewManager.getMainFrame());

        // this.frParent =
        ViewManager.getMainFrame();

        this.theVdb = theVdb;
        this.connection = connection;
        siCurrStatus = theVdb.getStatus();
        siNewStatus = determinePossibleNewStatus(siCurrStatus, changeDefaultStatus);

        try {
            init();
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }
    
    private VdbManager getVdbManager() {
        return ModelManager.getVdbManager(connection);
    }

    private short determinePossibleNewStatus(short siCurrStatus, boolean changingDefaultStatus) {
    	// Not a change of default
    	if(!changingDefaultStatus) {
	        if (siCurrStatus == VDBStatus.ACTIVE || siCurrStatus == VDBStatus.ACTIVE_DEFAULT)
	            return VDBStatus.INACTIVE;
	        else if (siCurrStatus == VDBStatus.INACTIVE)
	            return VDBStatus.ACTIVE;
	    // Changing Default Status
    	} else {
    		if (siCurrStatus == VDBStatus.ACTIVE || siCurrStatus == VDBStatus.INACTIVE) {
    			return VDBStatus.ACTIVE_DEFAULT;
    		} else if (siCurrStatus == VDBStatus.ACTIVE_DEFAULT) {
    			return VDBStatus.ACTIVE;
    		}
    	}
    	return 0;
    }

    private void init() throws Exception {

        dlgpnlDialogContainer.setContent(getSetStatusPanel());
        addListeners();
        //checkConnectorBinding();
        getContentPane().add(dlgpnlDialogContainer);
        setTitle("Change Status for " + getVdb().getName()); //$NON-NLS-1$
    }
    

    private VirtualDatabase getVdb() {
        return theVdb;
    }

    private VirtualDatabaseID getVdbId() {
        VirtualDatabaseID vdbID = (VirtualDatabaseID)getVdb().getID();
        return vdbID;
    }

    private void addListeners() {
        dlgpnlDialogContainer.getAcceptButton().addActionListener(new ActionListener() {

            public void actionPerformed(ActionEvent event) {
                processAcceptButton();
            }
        });

        dlgpnlDialogContainer.getCancelButton().addActionListener(new ActionListener() {

            public void actionPerformed(ActionEvent event) {
                processCancelButton();
            }
        });

    }

    private void processAcceptButton() {
        updateStatus();
        dispose();
    }

    private void processCancelButton() {
        dispose();
    }

    private void updateStatus() {
        // getModelsToConnBindsMap()
        // verify map

        try {
            getVdbManager().setVDBState(getVdbId(), siNewStatus);
        } catch (Exception e) {
            ExceptionUtility.showMessage("Failed to update Connector Binding names ", e); //$NON-NLS-1$
        }

    }

    private JPanel getSetStatusPanel() {

        JPanel pnlBlather = new JPanel();

        JLabel lblBlather = new JLabel();
        JLabel lblNewStatus = new JLabel();

        lblBlather.setText(SET_STATUS_TEXT);

        lblNewStatus.setText(getVdbManager().getVdbStatusAsString(siNewStatus));
        lblNewStatus.setFont(new java.awt.Font("Dialog", 1, 14)); //$NON-NLS-1$

        pnlBlather.add(lblBlather);
        pnlBlather.add(lblNewStatus);

        pnlOuter.setLayout(new GridBagLayout());
        pnlID.setLayout(new GridBagLayout());

        JLabel lblVdbName = new javax.swing.JLabel("VDB Name: "); //$NON-NLS-1$
        JTextField txfVdbName = new javax.swing.JTextField();
        txfVdbName.setText(getVdb().getName());

        JLabel lblVersion = new javax.swing.JLabel("Version: "); //$NON-NLS-1$
        JTextField txfVersion = new javax.swing.JTextField();
        VirtualDatabaseID vdbID = (VirtualDatabaseID)getVdb().getID();
        txfVersion.setText(vdbID.getVersion());

        JLabel lblStatus = new javax.swing.JLabel("Current Status: "); //$NON-NLS-1$

        String sCurrStatus = getVdbManager().getVdbStatusAsString(siCurrStatus);
        JLabel lblStatusPhrase = new javax.swing.JLabel(sCurrStatus);
        lblStatusPhrase.setFont(new java.awt.Font("Dialog", 1, 14)); //$NON-NLS-1$

        // JTextField txfStatus = new javax.swing.JTextField();

        txfVdbName.setPreferredSize(new Dimension(263, 21));
        txfVdbName.setMinimumSize(new Dimension(263, 21));
        txfVdbName.setEditable(false);

        txfVersion.setPreferredSize(new Dimension(50, 21));
        txfVersion.setMinimumSize(new Dimension(50, 21));
        txfVersion.setEditable(false);

        pnlID.add(lblVdbName, new GridBagConstraints(0, 0, 1, 1, 0.0, 0.0, GridBagConstraints.EAST, GridBagConstraints.NONE,
                                                     new Insets(5, 5, 5, 5), 0, 0));

        pnlID.add(txfVdbName, new GridBagConstraints(1, 0, 1, 1, 0.0, 0.0, GridBagConstraints.WEST, GridBagConstraints.NONE,
                                                     new Insets(5, 5, 5, 5), 0, 0));

        pnlID.add(lblVersion, new GridBagConstraints(2, 0, 1, 1, 0.0, 0.0, GridBagConstraints.EAST, GridBagConstraints.NONE,
                                                     new Insets(5, 5, 5, 5), 0, 0));

        pnlID.add(txfVersion, new GridBagConstraints(3, 0, 1, 1, 0.0, 0.0, GridBagConstraints.WEST, GridBagConstraints.NONE,
                                                     new Insets(5, 5, 5, 5), 0, 0));

        pnlID.add(lblStatus, new GridBagConstraints(0, 1, 1, 1, 0.0, 0.0, GridBagConstraints.EAST, GridBagConstraints.NONE,
                                                    new Insets(5, 5, 5, 5), 0, 0));

        pnlID.add(lblStatusPhrase, new GridBagConstraints(1, 1, 1, 1, 0.0, 0.0, GridBagConstraints.WEST, GridBagConstraints.NONE,
                                                          new Insets(5, 5, 5, 5), 0, 0));

        // now construct the final panel:
        pnlOuter.add(pnlID, new GridBagConstraints(0, 0, 1, 1, 0.0, 0.0, GridBagConstraints.CENTER, GridBagConstraints.NONE,
                                                   new Insets(15, 15, 15, 15), 0, 0));

        pnlOuter.add(pnlBlather, new GridBagConstraints(0, 1, 1, 1, 0.0, 0.0, GridBagConstraints.CENTER, GridBagConstraints.NONE,
                                                        new Insets(15, 15, 15, 15), 0, 0));

        return pnlOuter;
    }

}
